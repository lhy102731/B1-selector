"""Generic Brick daily-factor Signal Quality NAV validation.

Research-only runner for APPROVED AG2-KBase handoffs that propose factor names
not covered by a dedicated Phase 6 runner. It keeps Brick production code
untouched, reads the frozen rebuilt Brick candidate parquet, enriches rows from
raw parquet or research-only parquet cache with signal-day-only daily-bar
features, and compares V2 baseline versus V2 plus the handoff's generated factors under
strict rolling forward Signal Quality NAV.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.preprocessing import RobustScaler

ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(RESEARCH_DIR))

from research_automation.discovery_execution_bridge import (  # noqa: E402
    extract_factor_batch,
    load_handoff_document,
)
from research_automation.gpu_acceleration import (  # noqa: E402
    build_compute_acceleration_plan,
    detect_nvidia_gpu,
)

from brick_erd_phase6 import (  # noqa: E402
    V2_FEATURES,
    _code_str,
    _feature_matrix,
    _probe_lightgbm_gpu,
    build_lgb_params,
    label_from_train_bins,
    trade_metrics,
)
from brick_v2_rebuilt_dual_metrics import (  # noqa: E402
    DEFAULT_END,
    DEFAULT_START,
    ROLLING_WINDOWS,
    filter_entry_window,
    filter_train_strict,
    select_top_n,
    signal_quality_nav_metrics,
    _nav_metrics_from_returns,
)


DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / "generated_daily_factor_sqnav_phase6"
DEFAULT_CANDIDATE_PATH = (
    ROOT
    / "research_state"
    / "brick"
    / "v2_rebuilt_dual_metrics_20260709_parquet_notiming_top3"
    / "rebuilt_candidates_from_daily.parquet"
)
DEFAULT_CACHE_DIR = ROOT / "data" / "raw_parquet"
DEFAULT_PANEL_CACHE_PATH = (
    ROOT
    / "research_state"
    / "brick"
    / "generated_daily_factor_cache"
    / "brick_generated_daily_factor_panel.parquet"
)

DAILY_FEATURES = [
    "signal_day_shadow_symmetry",
    "signal_day_close_range_position",
    "shadow_symmetry_x_entry_gap_interaction",
    "signal_day_volume_to_20d_median_ratio",
    "signal_day_close_to_support_distance_score",
    "position_conditional_volume_shrinkage_weight",
    "pullback_depth_percentile",
    "volume_equilibrium_20d",
    "W_bottom_absorption_score",
    "w_bottom_trough_volume_ratio",
    "w_bottom_trough_depth_ratio",
    "path_efficiency",
    "path_consistency",
    "downside_vol_skew_5d",
    "downside_vol_skew_10d",
    "downside_vol_skew_20d",
    "downside_vol_skew_40d",
    "downside_vol_skew_residualized_20d",
    "vol_authenticity_path_smoothness_10d",
    "vol_authenticity_path_smoothness_20d",
    "vol_authenticity_path_ac1_10d",
    "streak_exhaustion_high_20d",
    "streak_exhaustion_low_20d",
    "streak_exhaustion_max_20d",
    "streak_exhaustion_high_20d_peer_rank",
    "streak_exhaustion_low_20d_peer_rank",
    "streak_exhaustion_max_20d_peer_rank",
    "pool_quality_range_width_20d",
    "pool_quality_range_width_10d",
    "pool_quality_range_width_40d",
    "pool_quality_range_width_20d_peer_rank",
    "pool_quality_range_dynamic_narrowing_20d_60d",
    "pool_quality_range_width_20d_dir_flip",
    "close_position_in_range",
    "free_float_adjusted_turnover",
    "free_float_ratio",
    "free_float_adjusted_turnover_ratio",
    "adjusted_turnover_amplitude_interaction",
]

POOL_FEATURES = [
    "market_sentiment_median",
    "sentiment_x_pullback_interaction",
    "peer_signal_count",
    "signal_day_regime_label",
    "close_position_gap_interaction",
]

SUPPORTED_FEATURES = sorted(set(DAILY_FEATURES + POOL_FEATURES))
SPARSE_FEATURE_MIN_NON_NULL_PCT = 50.0
PEER_RANK_BASE = {
    "streak_exhaustion_high_20d_peer_rank": "streak_exhaustion_high_20d",
    "streak_exhaustion_low_20d_peer_rank": "streak_exhaustion_low_20d",
    "streak_exhaustion_max_20d_peer_rank": "streak_exhaustion_max_20d",
    "pool_quality_range_width_20d_peer_rank": "pool_quality_range_width_20d",
}


def _numeric_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.astype(float)
    return pd.to_numeric(
        series.replace({True: 1, False: 0, "True": 1, "False": 0, "true": 1, "false": 0}),
        errors="coerce",
    )


def _safe_div(numerator: Any, denominator: Any) -> pd.Series:
    num = pd.Series(numerator, copy=False).astype(float)
    den = pd.Series(denominator, copy=False).astype(float)
    return num / den.where(den.abs() > 1e-12)


def _rolling_percent_rank(series: pd.Series, window: int) -> pd.Series:
    try:
        return series.rolling(window, min_periods=max(5, min(window, 20))).rank(pct=True)
    except Exception:
        out = np.full(len(series), np.nan, dtype=float)
        values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
        for i, value in enumerate(values):
            start = max(0, i - window + 1)
            sample = values[start:i + 1]
            sample = sample[np.isfinite(sample)]
            if len(sample) and np.isfinite(value):
                out[i] = float((sample <= value).mean())
        return pd.Series(out, index=series.index)


def _rolling_streak(values: np.ndarray, window: int, direction: str) -> np.ndarray:
    out = np.zeros(len(values), dtype=float)
    current = 0
    history: list[int] = []
    for i in range(len(values)):
        if i == 0 or not np.isfinite(values[i]) or not np.isfinite(values[i - 1]):
            current = 0
        elif direction == "up" and values[i] > values[i - 1]:
            current += 1
        elif direction == "down" and values[i] < values[i - 1]:
            current += 1
        else:
            current = 0
        history.append(current)
        start = max(0, len(history) - window)
        out[i] = float(max(history[start:])) if history[start:] else 0.0
    return out


def _factor_names_from_handoff(path: str | Path) -> list[str]:
    if not path:
        raise ValueError("--handoff-path is required for the generic daily-factor runner")
    document = load_handoff_document(path)
    factors = extract_factor_batch(document)
    names = [str(factor.get("name") or "").strip() for factor in factors if factor.get("name")]
    if not names:
        raise ValueError("handoff factor_batch has no factor names")
    return names


def load_candidates(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"candidate parquet not found: {path}")
    df = pd.read_parquet(path).copy()
    df["code"] = df["code"].map(_code_str)
    for col in ["signal_date", "entry_date", "exit_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()
    numeric_cols = [
        "entry_price",
        "exit_price",
        "return_pct",
        "hold_days",
        *V2_FEATURES,
        "turnover_ratio_20",
        "turnover_to_60d_max",
        "volume_to_60d_max",
        "turnover_extreme_score",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = _numeric_series(df[col])
    df = df.dropna(subset=["code", "signal_date", "entry_date", "exit_date", "return_pct"])
    df = df.sort_values(["entry_date", "code"]).reset_index(drop=True)
    stats_block = {
        "source": "existing_rebuilt_candidate_parquet",
        "candidate_path": str(path.resolve()),
        "rows": int(len(df)),
        "signal_days": int(df["signal_date"].nunique()),
        "entry_days": int(df["entry_date"].nunique()),
        "signal_start": df["signal_date"].min().strftime("%Y-%m-%d"),
        "signal_end": df["signal_date"].max().strftime("%Y-%m-%d"),
        "entry_start": df["entry_date"].min().strftime("%Y-%m-%d"),
        "entry_end": df["entry_date"].max().strftime("%Y-%m-%d"),
        "use_market_timing": False,
    }
    return df, stats_block


def _stock_feature_worker(args: tuple[str, list[str], str]) -> list[dict[str, Any]]:
    code, wanted_dates, cache_dir_text = args
    cache_dir = Path(cache_dir_text)
    cache_file = cache_dir / f"{code}.parquet"
    if not cache_file.exists():
        cache_file = cache_dir / code[:2] / f"{code}.parquet"
    if not cache_file.exists() or not wanted_dates:
        return []
    wanted = set(wanted_dates)
    try:
        df = pd.read_parquet(cache_file)
    except Exception:
        return []
    if df.empty:
        return []
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df["signal_date"] = df["date"].dt.strftime("%Y-%m-%d")

    def optional_numeric(name: str) -> pd.Series:
        if name in df.columns:
            return pd.to_numeric(df[name], errors="coerce")
        return pd.Series(np.nan, index=df.index, dtype=float)

    open_ = pd.to_numeric(df["open"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce")
    change_pct = pd.to_numeric(df["change_pct"], errors="coerce") / 100.0
    turnover = pd.to_numeric(df["turnover"], errors="coerce")
    amount = optional_numeric("amount")
    market_cap = optional_numeric("market_cap")
    provider_amplitude = optional_numeric("amplitude")

    bar_range = (high - low).replace(0, np.nan)
    upper_shadow = high - pd.concat([open_, close], axis=1).max(axis=1)
    lower_shadow = pd.concat([open_, close], axis=1).min(axis=1) - low
    shadow_symmetry = (1.0 - (upper_shadow - lower_shadow).abs() / bar_range).clip(0.0, 1.0)
    close_range_position = ((close - low) / bar_range).clip(0.0, 1.0)
    amplitude_pct = (provider_amplitude / 100.0).where(
        provider_amplitude.notna(),
        _safe_div(high - low, close.shift(1)).abs(),
    )

    vol_median20 = volume.rolling(20, min_periods=5).median()
    vol_mean20 = volume.rolling(20, min_periods=5).mean()
    vol_std20 = volume.rolling(20, min_periods=5).std(ddof=0)
    volume_to_median20 = _safe_div(volume, vol_median20)
    volume_equilibrium = (1.0 / (1.0 + _safe_div(vol_std20, vol_mean20))).clip(0.0, 1.0)

    turnover_decimal = (turnover / 100.0).replace([np.inf, -np.inf], np.nan)
    free_float_shares = _safe_div(volume, turnover_decimal).replace([np.inf, -np.inf], np.nan)
    raw_price = _safe_div(amount, volume).replace([np.inf, -np.inf], np.nan)
    total_shares = _safe_div(market_cap, raw_price).replace([np.inf, -np.inf], np.nan)
    free_float_ratio = _safe_div(free_float_shares, total_shares).clip(0.0, 2.0)
    free_float_adjusted_turnover = turnover_decimal
    ff_turnover_mean20 = free_float_adjusted_turnover.rolling(20, min_periods=5).mean()
    free_float_adjusted_turnover_ratio = _safe_div(
        free_float_adjusted_turnover,
        ff_turnover_mean20,
    ).replace([np.inf, -np.inf], np.nan)
    adjusted_turnover_amplitude_interaction = (
        free_float_adjusted_turnover_ratio * amplitude_pct
    ).replace([np.inf, -np.inf], np.nan)

    high20 = high.rolling(20, min_periods=5).max()
    low20 = low.rolling(20, min_periods=5).min()
    support_score = (1.0 - _safe_div(close - low20, high20 - low20)).clip(0.0, 1.0)
    drawdown20 = (1.0 - _safe_div(close, high20)).clip(lower=0.0)
    pullback_depth_percentile = _rolling_percent_rank(drawdown20, 120).clip(0.0, 1.0)

    shrink_score = (1.0 / (1.0 + volume_to_median20)).clip(0.0, 1.0)
    conditional_volume = (shrink_score * support_score).clip(0.0, 1.0)

    ret = close.pct_change()
    ret10 = close.pct_change(10).abs()
    abs_sum10 = ret.abs().rolling(10, min_periods=5).sum()
    path_efficiency = _safe_div(ret10, abs_sum10).clip(0.0, 1.0)
    sign_sum10 = np.sign(ret).rolling(10, min_periods=5).sum().abs()
    sign_count10 = ret.rolling(10, min_periods=5).count()
    path_consistency = _safe_div(sign_sum10, sign_count10).clip(0.0, 1.0)

    width10 = _safe_div(high.rolling(10, min_periods=5).max() - low.rolling(10, min_periods=5).min(), close)
    width20 = _safe_div(high20 - low20, close)
    width40 = _safe_div(high.rolling(40, min_periods=10).max() - low.rolling(40, min_periods=10).min(), close)
    width60 = _safe_div(high.rolling(60, min_periods=15).max() - low.rolling(60, min_periods=15).min(), close)
    range_score10 = (-width10).replace([np.inf, -np.inf], np.nan)
    range_score20 = (-width20).replace([np.inf, -np.inf], np.nan)
    range_score40 = (-width40).replace([np.inf, -np.inf], np.nan)
    range_narrowing = _safe_div(width60 - width20, width60).replace([np.inf, -np.inf], np.nan)
    dir_flip = (np.sign(close.diff(3)) != np.sign(close.diff(10))).astype(float)

    w_trough_depth = _safe_div(close - low20, close).clip(lower=0.0)
    w_trough_volume_ratio = _safe_div(volume.rolling(20, min_periods=5).min(), vol_median20)
    w_absorption = (
        (1.0 / (1.0 + w_trough_depth * 10.0))
        * (1.0 / (1.0 + w_trough_volume_ratio))
    ).clip(0.0, 1.0)

    down_skew: dict[int, pd.Series] = {}
    for window in (5, 10, 20, 40):
        neg_std = ret.clip(upper=0.0).rolling(window, min_periods=max(3, window // 2)).std(ddof=0)
        pos_std = ret.clip(lower=0.0).rolling(window, min_periods=max(3, window // 2)).std(ddof=0)
        down_skew[window] = _safe_div(neg_std, pos_std + 1e-9).replace([np.inf, -np.inf], np.nan)

    vol_change = volume.pct_change().replace([np.inf, -np.inf], np.nan)
    vol_smooth10 = (1.0 / (1.0 + vol_change.abs().rolling(10, min_periods=5).mean())).clip(0.0, 1.0)
    vol_smooth20 = (1.0 / (1.0 + vol_change.abs().rolling(20, min_periods=8).mean())).clip(0.0, 1.0)
    vol_ac1_10 = vol_change.rolling(10, min_periods=6).corr(vol_change.shift(1))

    high_streak20 = _rolling_streak(high.to_numpy(dtype=float), 20, "up")
    low_streak20 = _rolling_streak(low.to_numpy(dtype=float), 20, "down")
    close_up_streak = _rolling_streak(close.to_numpy(dtype=float), 20, "up")
    close_down_streak = _rolling_streak(close.to_numpy(dtype=float), 20, "down")
    max_streak20 = np.maximum(close_up_streak, close_down_streak)

    rows: list[dict[str, Any]] = []
    mask = df["signal_date"].isin(wanted).to_numpy()
    for i in np.flatnonzero(mask):
        rows.append({
            "code": code,
            "signal_date": df.at[i, "signal_date"],
            "signal_day_shadow_symmetry": shadow_symmetry.iat[i],
            "signal_day_close_range_position": close_range_position.iat[i],
            "signal_day_volume_to_20d_median_ratio": volume_to_median20.iat[i],
            "signal_day_close_to_support_distance_score": support_score.iat[i],
            "position_conditional_volume_shrinkage_weight": conditional_volume.iat[i],
            "pullback_depth_percentile": pullback_depth_percentile.iat[i],
            "volume_equilibrium_20d": volume_equilibrium.iat[i],
            "W_bottom_absorption_score": w_absorption.iat[i],
            "w_bottom_trough_volume_ratio": w_trough_volume_ratio.iat[i],
            "w_bottom_trough_depth_ratio": w_trough_depth.iat[i],
            "path_efficiency": path_efficiency.iat[i],
            "path_consistency": path_consistency.iat[i],
            "downside_vol_skew_5d": down_skew[5].iat[i],
            "downside_vol_skew_10d": down_skew[10].iat[i],
            "downside_vol_skew_20d": down_skew[20].iat[i],
            "downside_vol_skew_40d": down_skew[40].iat[i],
            "vol_authenticity_path_smoothness_10d": vol_smooth10.iat[i],
            "vol_authenticity_path_smoothness_20d": vol_smooth20.iat[i],
            "vol_authenticity_path_ac1_10d": vol_ac1_10.iat[i],
            "streak_exhaustion_high_20d": high_streak20[i],
            "streak_exhaustion_low_20d": low_streak20[i],
            "streak_exhaustion_max_20d": max_streak20[i],
            "pool_quality_range_width_20d": range_score20.iat[i],
            "pool_quality_range_width_10d": range_score10.iat[i],
            "pool_quality_range_width_40d": range_score40.iat[i],
            "pool_quality_range_dynamic_narrowing_20d_60d": range_narrowing.iat[i],
            "pool_quality_range_width_20d_dir_flip": dir_flip.iat[i],
            "close_position_in_range": close_range_position.iat[i],
            "free_float_adjusted_turnover": free_float_adjusted_turnover.iat[i],
            "free_float_ratio": free_float_ratio.iat[i],
            "free_float_adjusted_turnover_ratio": free_float_adjusted_turnover_ratio.iat[i],
            "adjusted_turnover_amplitude_interaction": adjusted_turnover_amplitude_interaction.iat[i],
        })
    return rows


def enrich_with_daily_features(
    candidates: pd.DataFrame,
    cache_dir: Path,
    workers: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    date_map = (
        candidates.assign(signal_date_text=candidates["signal_date"].dt.strftime("%Y-%m-%d"))
        .groupby("code")["signal_date_text"]
        .apply(lambda s: sorted(set(s)))
        .to_dict()
    )
    tasks = [(code, dates, str(cache_dir)) for code, dates in date_map.items()]
    rows: list[dict[str, Any]] = []
    with mp.Pool(max(1, workers)) as pool:
        for batch in pool.imap_unordered(_stock_feature_worker, tasks, chunksize=20):
            rows.extend(batch)
    daily = pd.DataFrame(rows)
    if daily.empty:
        raise ValueError(f"no daily feature rows produced from {cache_dir}")
    daily["signal_date"] = pd.to_datetime(daily["signal_date"], errors="coerce").dt.normalize()
    merged = candidates.merge(daily, on=["code", "signal_date"], how="left", validate="many_to_one")
    before = len(merged)
    for feature in DAILY_FEATURES:
        if feature in merged.columns:
            merged[feature] = _numeric_series(merged[feature]).replace([np.inf, -np.inf], np.nan)
    stats_block = {
        "cache_dir": str(cache_dir.resolve()),
        "candidate_rows_before_daily_join": int(before),
        "daily_feature_rows": int(len(daily)),
        "daily_join_missing_rows": int(merged["signal_day_shadow_symmetry"].isna().sum()),
        "feature_date_boundary": "signal_date close only",
        "free_float_proxy_note": (
            "free_float_shares is inferred from raw_parquet volume / turnover_pct; "
            "free_float_ratio uses raw_price=amount/volume and market_cap. "
            "No shareholder filing lockup table is used."
        ),
    }
    return merged, stats_block


def add_pool_features(panel: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = panel.copy()
    out["peer_signal_count"] = out.groupby("signal_date")["code"].transform("count").astype(float)
    out["market_sentiment_median"] = out.groupby("signal_date")["ret_5d"].transform("median")
    out["signal_day_regime_label"] = np.select(
        [
            out["market_sentiment_median"] > 2.0,
            out["market_sentiment_median"] < -2.0,
        ],
        [1.0, -1.0],
        default=0.0,
    )
    if "pullback_depth_percentile" in out.columns:
        out["sentiment_x_pullback_interaction"] = (
            out["market_sentiment_median"].fillna(0.0)
            * out["pullback_depth_percentile"].fillna(0.5)
        )
    else:
        out["sentiment_x_pullback_interaction"] = 0.0
    if "signal_day_shadow_symmetry" in out.columns:
        out["shadow_symmetry_x_entry_gap_interaction"] = (
            out["signal_day_shadow_symmetry"].fillna(0.5)
            * (-out["overnight_gap_pct"].abs()).fillna(0.0)
        )
    else:
        out["shadow_symmetry_x_entry_gap_interaction"] = 0.0
    if "close_position_in_range" in out.columns:
        out["close_position_gap_interaction"] = (
            out["close_position_in_range"].fillna(0.5)
            * out["overnight_gap_pct"].fillna(0.0)
        )
    else:
        out["close_position_gap_interaction"] = 0.0
    for feature, base in PEER_RANK_BASE.items():
        if base in out.columns:
            out[feature] = out.groupby("signal_date")[base].rank(
                method="average",
                pct=True,
                ascending=True,
            ).fillna(0.5)
    if "downside_vol_skew_20d" in out.columns:
        out["downside_vol_skew_residualized_20d"] = (
            out["downside_vol_skew_20d"]
            - out.groupby("signal_date")["downside_vol_skew_20d"].transform("median")
        )
    stats_block = {
        "pool_date_column": "signal_date",
        "market_sentiment_source": "same signal_date candidate median ret_5d, known before next-open selection",
        "peer_rank_features": sorted(PEER_RANK_BASE),
    }
    return out, stats_block


def _normalise_panel_types(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    out["code"] = out["code"].map(_code_str)
    for col in ["signal_date", "entry_date", "exit_date"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.normalize()
    for col in [*V2_FEATURES, *SUPPORTED_FEATURES, "return_pct", "hold_days"]:
        if col in out.columns:
            out[col] = _numeric_series(out[col]).replace([np.inf, -np.inf], np.nan)
    return out


def load_or_build_factor_panel(
    candidates: pd.DataFrame,
    cache_dir: Path,
    workers: int,
    panel_cache_path: Path,
    force_rebuild: bool,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    required_columns = {"code", "signal_date", "entry_date", "exit_date", "return_pct", *SUPPORTED_FEATURES}
    if panel_cache_path.exists() and not force_rebuild:
        panel = _normalise_panel_types(pd.read_parquet(panel_cache_path))
        missing = sorted(required_columns - set(panel.columns))
        if not missing:
            stats = {
                "source": "shared_generated_daily_factor_panel_cache",
                "panel_cache_path": str(panel_cache_path.resolve()),
                "rows": int(len(panel)),
                "reused": True,
            }
            return panel, stats, {"reused_from_panel_cache": True}, {"reused_from_panel_cache": True}

    panel, daily_stats = enrich_with_daily_features(candidates, cache_dir, workers)
    panel, pool_stats = add_pool_features(panel)
    panel = _normalise_panel_types(panel)
    panel_cache_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(panel_cache_path, index=False)
    stats = {
        "source": "rebuilt_generated_daily_factor_panel",
        "panel_cache_path": str(panel_cache_path.resolve()),
        "rows": int(len(panel)),
        "reused": False,
    }
    return panel, stats, daily_stats, pool_stats


def validate_requested_features(panel: pd.DataFrame, requested: list[str]) -> dict[str, Any]:
    unsupported = [name for name in requested if name not in SUPPORTED_FEATURES]
    missing_columns = [name for name in requested if name not in panel.columns]
    if unsupported or missing_columns:
        raise ValueError(
            "unsupported generated daily factor names: "
            f"unsupported={unsupported}, missing_columns={missing_columns}"
        )
    availability = {}
    for name in requested:
        values = _numeric_series(panel[name]).replace([np.inf, -np.inf], np.nan)
        availability[name] = {
            "non_null_rows": int(values.notna().sum()),
            "null_rows": int(values.isna().sum()),
            "non_null_pct": round(float(values.notna().mean() * 100.0), 6),
            "mean": round(float(values.mean()), 6) if values.notna().any() else 0.0,
            "std": round(float(values.std(ddof=0)), 6) if values.notna().any() else 0.0,
        }
    too_sparse = [
        {
            "feature": name,
            "min_required_non_null_pct": SPARSE_FEATURE_MIN_NON_NULL_PCT,
            **block,
        }
        for name, block in availability.items()
        if block["non_null_pct"] < SPARSE_FEATURE_MIN_NON_NULL_PCT
    ]
    return {
        "availability": availability,
        "too_sparse": too_sparse,
        "min_required_non_null_pct": SPARSE_FEATURE_MIN_NON_NULL_PCT,
    }


def group_sizes_by(frame: pd.DataFrame, column: str) -> np.ndarray:
    return frame.groupby(column, sort=False).size().to_numpy(dtype=int)


def train_ranker(
    train: pd.DataFrame,
    features: list[str],
    params: dict[str, Any],
    num_boost_round: int,
) -> tuple[Any, RobustScaler, dict[str, Any]]:
    train = train.sort_values(["entry_date", "code"]).reset_index(drop=True)
    x_train = _feature_matrix(train, features)
    scaler = RobustScaler()
    x_train_s = scaler.fit_transform(x_train)
    y_train_raw = train["return_pct"].to_numpy(dtype=float)
    y_train = label_from_train_bins(y_train_raw, y_train_raw)
    train_set = lgb.Dataset(
        x_train_s,
        label=y_train,
        group=group_sizes_by(train, "entry_date"),
    )
    model = lgb.train(params, train_set, num_boost_round=num_boost_round)
    return model, scaler, {
        "num_boost_round": int(num_boost_round),
        "validation_used": False,
        "group_column": "entry_date",
    }


def score_frame(model: Any, scaler: RobustScaler, frame: pd.DataFrame, features: list[str]) -> np.ndarray:
    return model.predict(scaler.transform(_feature_matrix(frame, features)))


def _dcg(labels: np.ndarray, k: int) -> float:
    rel = labels[:k].astype(float)
    if len(rel) == 0:
        return 0.0
    discounts = np.log2(np.arange(2, len(rel) + 2, dtype=float))
    return float(np.sum((np.power(2.0, rel) - 1.0) / discounts))


def ndcg_by_entry_day(
    frame: pd.DataFrame,
    scores: np.ndarray,
    train_returns: np.ndarray,
    ks: tuple[int, ...] = (3, 5, 10),
) -> dict[str, float]:
    tmp = frame[["entry_date", "return_pct"]].copy()
    tmp["score"] = scores
    tmp["label"] = label_from_train_bins(train_returns, tmp["return_pct"].to_numpy(dtype=float))
    values: dict[int, list[float]] = {k: [] for k in ks}
    for _, grp in tmp.groupby("entry_date", sort=False):
        if len(grp) < 2:
            continue
        by_score = grp.sort_values("score", ascending=False)["label"].to_numpy(dtype=float)
        ideal = grp.sort_values("label", ascending=False)["label"].to_numpy(dtype=float)
        for k in ks:
            idcg = _dcg(ideal, k)
            if idcg > 0:
                values[k].append(_dcg(by_score, k) / idcg)
    return {
        f"ndcg_at_{k}": round(float(np.mean(vals)), 6) if vals else 0.0
        for k, vals in values.items()
    }


def evaluate_model_topn(
    *,
    test: pd.DataFrame,
    scores: np.ndarray,
    top_n: int,
    output_dir: Path,
    name: str,
    model_name: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> tuple[dict[str, Any], pd.DataFrame]:
    selected = select_top_n(test, scores, top_n)
    prefix = output_dir / f"{name}_{model_name}_top{top_n}"
    selected.to_csv(prefix.with_suffix(".trades.csv"), index=False, encoding="gbk")
    signal_quality, nav_df = signal_quality_nav_metrics(
        selected,
        start_ts,
        end_ts,
        top_n,
        commission_bp,
        stamp_pct,
        slippage_pct,
    )
    nav_path = prefix.with_suffix(".signal_quality.nav.csv")
    nav_df.to_csv(nav_path, index=False, encoding="gbk")
    return {
        "trade": trade_metrics(selected),
        "signal_quality": signal_quality,
        "nav_path": str(nav_path.resolve()),
        "trades_path": str(prefix.with_suffix(".trades.csv").resolve()),
    }, nav_df


def feature_importance_block(
    model: Any,
    features: list[str],
    generated_features: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    importance = pd.DataFrame({
        "feature": features,
        "importance_gain": model.feature_importance(importance_type="gain"),
        "importance_split": model.feature_importance(importance_type="split"),
    }).sort_values("importance_gain", ascending=False)
    total_gain = float(importance["importance_gain"].sum())
    generated_gain = float(
        importance.loc[importance["feature"].isin(generated_features), "importance_gain"].sum()
    )
    share = generated_gain / total_gain if total_gain > 0 else 0.0
    return importance.to_dict(orient="records"), {
        "total_gain": round(total_gain, 6),
        "generated_factor_gain": round(generated_gain, 6),
        "generated_factor_gain_share_pct": round(share * 100.0, 6),
        "low_importance_failure_threshold_pct": 5.0,
        "low_importance_failure": bool(share < 0.05),
    }


def run_window(
    df: pd.DataFrame,
    window: dict[str, Any],
    *,
    generated_features: list[str],
    output_dir: Path,
    params: dict[str, Any],
    num_boost_round: int,
    top_ns: list[int],
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> dict[str, Any]:
    name = str(window["name"])
    test_start, requested_test_end = window["test"]
    train, purge_stats = filter_train_strict(df, tuple(window["train"]), test_start)
    test = filter_entry_window(df, tuple(window["test"]))
    if train.empty or test.empty:
        raise ValueError(f"{name}: train/test window produced empty data")

    start_ts = pd.Timestamp(test_start)
    requested_end_ts = pd.Timestamp(requested_test_end)
    effective_end_ts = min(requested_end_ts, test["entry_date"].max())
    model_specs = {
        "v2_baseline": V2_FEATURES,
        "v2_plus_generated_daily": [*V2_FEATURES, *generated_features],
    }
    block: dict[str, Any] = {
        "name": name,
        "train_window": list(window["train"]),
        "test_window": list(window["test"]),
        "effective_test_window": [
            start_ts.strftime("%Y-%m-%d"),
            effective_end_ts.strftime("%Y-%m-%d"),
        ],
        "n_train": int(len(train)),
        "n_test": int(len(test)),
        "train_entry_days": int(train["entry_date"].nunique()),
        "test_entry_days": int(test["entry_date"].nunique()),
        "train_info": purge_stats,
        "models": {},
    }
    train_returns = train["return_pct"].to_numpy(dtype=float)
    for model_name, features in model_specs.items():
        model, scaler, train_info = train_ranker(train, features, params, num_boost_round)
        scores = score_frame(model, scaler, test, features)
        importance, importance_summary = feature_importance_block(model, features, generated_features)
        pd.DataFrame(importance).to_csv(
            output_dir / f"{name}_{model_name}_feature_importance.csv",
            index=False,
        )
        model_block: dict[str, Any] = {
            "features": features,
            "n_features": int(len(features)),
            "train_info": train_info,
            "rank_metrics": ndcg_by_entry_day(test, scores, train_returns),
            "feature_importance_summary": importance_summary,
            "top_importance": importance[:15],
            "topn": {},
        }
        for top_n in top_ns:
            metrics, _ = evaluate_model_topn(
                test=test,
                scores=scores,
                top_n=top_n,
                output_dir=output_dir,
                name=name,
                model_name=model_name,
                start_ts=start_ts,
                end_ts=effective_end_ts,
                commission_bp=commission_bp,
                stamp_pct=stamp_pct,
                slippage_pct=slippage_pct,
            )
            model_block["topn"][f"top{top_n}"] = metrics
        block["models"][model_name] = model_block

    block["deltas_v2_plus_generated_daily_minus_baseline"] = {}
    for top_n in top_ns:
        key = f"top{top_n}"
        base = block["models"]["v2_baseline"]["topn"][key]["signal_quality"]
        generated = block["models"]["v2_plus_generated_daily"]["topn"][key]["signal_quality"]
        block["deltas_v2_plus_generated_daily_minus_baseline"][key] = {
            metric: round(float(generated.get(metric, 0.0) - base.get(metric, 0.0)), 6)
            for metric in ["cum_return_pct", "cagr_pct", "max_dd_pct", "sharpe", "calmar", "daily_avg_ret_pct"]
        }
    base_rank = block["models"]["v2_baseline"]["rank_metrics"]
    generated_rank = block["models"]["v2_plus_generated_daily"]["rank_metrics"]
    block["rank_metric_deltas"] = {
        key: round(float(generated_rank.get(key, 0.0) - base_rank.get(key, 0.0)), 6)
        for key in ["ndcg_at_3", "ndcg_at_5", "ndcg_at_10"]
    }
    return block


def summarize_model_topn(windows: list[dict[str, Any]], model_name: str, top_n: int) -> dict[str, Any]:
    rows = []
    for item in windows:
        top = item.get("models", {}).get(model_name, {}).get("topn", {}).get(f"top{top_n}", {})
        signal_quality = top.get("signal_quality", {})
        trade = top.get("trade", {})
        rows.append({
            "name": item["name"],
            "cagr_pct": signal_quality.get("cagr_pct", 0.0),
            "cum_return_pct": signal_quality.get("cum_return_pct", 0.0),
            "max_dd_pct": signal_quality.get("max_dd_pct", 0.0),
            "sharpe": signal_quality.get("sharpe", 0.0),
            "calmar": signal_quality.get("calmar", 0.0),
            "trades": trade.get("trades", 0),
            "win_rate_pct": trade.get("win_rate_pct", 0.0),
            "avg_return_pct": trade.get("avg_return_pct", 0.0),
        })
    frame = pd.DataFrame(rows)
    if frame.empty:
        return {}
    return {
        "windows": int(len(frame)),
        "avg_cagr_pct": round(float(frame["cagr_pct"].mean()), 6),
        "worst_cagr_pct": round(float(frame["cagr_pct"].min()), 6),
        "avg_cum_return_pct": round(float(frame["cum_return_pct"].mean()), 6),
        "worst_cum_return_pct": round(float(frame["cum_return_pct"].min()), 6),
        "avg_sharpe": round(float(frame["sharpe"].mean()), 6),
        "worst_sharpe": round(float(frame["sharpe"].min()), 6),
        "avg_max_dd_pct": round(float(frame["max_dd_pct"].mean()), 6),
        "worst_max_dd_pct": round(float(frame["max_dd_pct"].min()), 6),
        "positive_cagr_pass_rate": round(float((frame["cagr_pct"] > 0).mean()), 6),
        "avg_trade_win_rate_pct": round(float(frame["win_rate_pct"].mean()), 6),
        "avg_trade_return_pct": round(float(frame["avg_return_pct"].mean()), 6),
        "rows": rows,
    }


def summarize_deltas(windows: list[dict[str, Any]], top_n: int) -> dict[str, Any]:
    rows = []
    for item in windows:
        delta = item.get("deltas_v2_plus_generated_daily_minus_baseline", {}).get(f"top{top_n}", {})
        rows.append({
            "name": item["name"],
            "cagr_delta": delta.get("cagr_pct", 0.0),
            "cum_return_delta": delta.get("cum_return_pct", 0.0),
            "max_dd_delta": delta.get("max_dd_pct", 0.0),
            "sharpe_delta": delta.get("sharpe", 0.0),
        })
    frame = pd.DataFrame(rows)
    if frame.empty:
        return {}
    return {
        "windows": int(len(frame)),
        "avg_cagr_delta": round(float(frame["cagr_delta"].mean()), 6),
        "avg_cum_return_delta": round(float(frame["cum_return_delta"].mean()), 6),
        "avg_sharpe_delta": round(float(frame["sharpe_delta"].mean()), 6),
        "positive_cagr_delta_pass_rate": round(float((frame["cagr_delta"] > 0).mean()), 6),
        "positive_cum_return_delta_pass_rate": round(float((frame["cum_return_delta"] > 0).mean()), 6),
        "worst_cum_return_delta": round(float(frame["cum_return_delta"].min()), 6),
        "best_cum_return_delta": round(float(frame["cum_return_delta"].max()), 6),
    }


def stitch_rolling_nav(
    windows: list[dict[str, Any]],
    model_name: str,
    top_n: int,
    output_dir: Path,
) -> dict[str, Any]:
    returns: list[float] = []
    dates: list[pd.Timestamp] = []
    source_windows: list[str] = []
    for item in windows:
        top = item["models"][model_name]["topn"][f"top{top_n}"]
        path = Path(top["nav_path"])
        nav = pd.read_csv(path)
        if nav.empty:
            continue
        nav["date"] = pd.to_datetime(nav["date"], errors="coerce").dt.normalize()
        nav = nav.dropna(subset=["date"]).sort_values("date")
        returns.extend(pd.to_numeric(nav["ret"], errors="coerce").fillna(0.0).to_numpy(dtype=float).tolist())
        dates.extend(nav["date"].tolist())
        source_windows.extend([item["name"]] * len(nav))
    metrics, stitched = _nav_metrics_from_returns(
        np.asarray(returns, dtype=float),
        dates,
        extra={
            "metric_surface": "signal_quality_rolling_oos_stitched",
            "model": model_name,
            "top_n": int(top_n),
            "source_windows": int(len(windows)),
        },
    )
    if not stitched.empty:
        stitched["window"] = source_windows[:len(stitched)]
    out_path = output_dir / f"rolling_oos_{model_name}_top{top_n}_signal_quality_nav.csv"
    stitched.to_csv(out_path, index=False, encoding="gbk")
    metrics["nav_path"] = str(out_path.resolve())
    return metrics


def build_feature_diagnostics(df: pd.DataFrame, generated_features: list[str]) -> dict[str, Any]:
    probe = df[
        (df["entry_date"] >= pd.Timestamp("2020-01-01"))
        & (df["entry_date"] <= pd.Timestamp("2022-12-31"))
    ].copy()
    diagnostics: dict[str, Any] = {}
    for feature in generated_features:
        valid = probe[[feature, "return_pct"]].replace([np.inf, -np.inf], np.nan).dropna()
        if len(valid) < 100 or valid[feature].std() <= 1e-12 or valid["return_pct"].std() <= 1e-12:
            corr = 0.0
        else:
            value = stats.spearmanr(valid[feature], valid["return_pct"]).correlation
            corr = float(value) if np.isfinite(value) else 0.0
        diagnostics[feature] = {
            "train_2020_2022_spearman_to_return": round(corr, 6),
            "n": int(len(valid)),
        }
    return diagnostics


def write_report(result: dict[str, Any], path: Path) -> None:
    lines = [
        "# Brick Generated Daily Factor Signal Quality NAV Phase 6",
        "",
        f"Created: {result['created_at']}",
        "",
        "## Boundary",
        "",
        f"- Handoff: `{result['data_boundary']['handoff_path']}`",
        f"- Candidate parquet: `{result['data_boundary']['candidate_path']}`",
        f"- Indicator cache: `{result['data_boundary']['indicator_cache']}`",
        "- Market timing: disabled.",
        "- Split column: `entry_date`.",
        "- Train purge: train `exit_date` must be before test start.",
        "- Generated factors use signal-day daily OHLCV only; no entry-day high/low/close or intraday fields.",
        "- Signal Quality NAV is an active selected-signal index, not a cash account.",
        "",
        "## Factors",
        "",
        "```json",
        json.dumps(result["features"]["generated_daily"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## Compute",
        "",
        f"- Backend: `{result['compute_acceleration']['selected_backend']}`",
        f"- GPU available: `{result['compute_acceleration']['gpu_available']}`",
        f"- LightGBM GPU probe: `{result['compute_acceleration']['lightgbm_gpu_probe']}`",
        "",
    ]
    if result.get("status") == "DATA_COVERAGE_STOP":
        lines.extend([
            "## Data Coverage Stop",
            "",
            f"- Stop reason: {result['stop_reason']}",
            f"- Minimum required non-null coverage: {result['feature_validation']['min_required_non_null_pct']:.2f}%.",
            "- No ranker was trained and no rolling test was run because the approved factor batch did not pass the data boundary.",
            "",
            "| Feature | Non-null Rows | Non-null % | Null Rows |",
            "| --- | ---: | ---: | ---: |",
        ])
        for item in result["too_sparse_features"]:
            lines.append(
                "| {feature} | {rows} | {pct:.6f}% | {nulls} |".format(
                    feature=item["feature"],
                    rows=item["non_null_rows"],
                    pct=item["non_null_pct"],
                    nulls=item["null_rows"],
                )
            )
        lines.extend([
            "",
            "## Feature Availability",
            "",
            "```json",
            json.dumps(result["feature_availability"], ensure_ascii=False, indent=2),
            "```",
            "",
            "## Promotion Note",
            "",
            "This factor batch is not promoted. It is preserved as research evidence; future attempts need a higher-coverage free-float/share-cap data source or a different usage mode.",
            "",
        ])
        path.write_text("\n".join(lines), encoding="utf-8")
        return
    lines.extend([
        "## Rolling OOS Stitched",
        "",
        "| Model | TopN | Final NAV | CAGR | MaxDD | Sharpe | Daily AvgRet |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for metrics in result["rolling_oos_stitched"].values():
        lines.append(
            "| {model} | {topn} | {nav:.4f} | {cagr:.2f}% | {mdd:.2f}% | {sharpe:.3f} | {avg:.4f}% |".format(
                model=metrics["model"],
                topn=metrics["top_n"],
                nav=1.0 + metrics.get("cum_return_pct", 0.0) / 100.0,
                cagr=metrics.get("cagr_pct", 0.0),
                mdd=metrics.get("max_dd_pct", 0.0),
                sharpe=metrics.get("sharpe", 0.0),
                avg=metrics.get("daily_avg_ret_pct", 0.0),
            )
        )
    lines.extend([
        "",
        "## Rolling Deltas",
        "",
        "| Window | TopN | CAGR Delta | CumRet Delta | Sharpe Delta | NDCG@5 Delta |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ])
    for item in result["rolling_windows"]:
        for key, delta in sorted(item["deltas_v2_plus_generated_daily_minus_baseline"].items()):
            lines.append(
                "| {name} | {topn} | {cagr:.2f}% | {cum:.2f}% | {sharpe:.4f} | {ndcg:.4f} |".format(
                    name=item["name"],
                    topn=key.replace("top", ""),
                    cagr=delta.get("cagr_pct", 0.0),
                    cum=delta.get("cum_return_pct", 0.0),
                    sharpe=delta.get("sharpe", 0.0),
                    ndcg=item["rank_metric_deltas"].get("ndcg_at_5", 0.0),
                )
            )
    lines.extend([
        "",
        "## Summary",
        "",
        "```json",
        json.dumps(result["summary"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## Promotion Note",
        "",
        "This report is validation evidence only. Promotion requires positive average rolling test delta, acceptable worst-fold behavior, and no material Top3 degradation.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    requested = _factor_names_from_handoff(args.handoff_path)
    candidate_path = Path(args.candidate_path)
    cache_dir = Path(args.indicator_cache)
    safe_read_sources = {"raw_parquet", "research_indicators_cache"}
    if cache_dir.name not in safe_read_sources and not args.allow_production_cache:
        raise ValueError(
            "research run must read from data/raw_parquet or data/research_indicators_cache "
            "unless explicitly allowed"
        )
    if not cache_dir.exists():
        raise FileNotFoundError(f"indicator cache not found: {cache_dir}")

    candidates, candidate_stats = load_candidates(candidate_path)
    panel, panel_cache_stats, daily_stats, pool_stats = load_or_build_factor_panel(
        candidates,
        cache_dir,
        args.workers,
        Path(args.panel_cache_path),
        args.rebuild_panel_cache,
    )
    feature_validation = validate_requested_features(panel, requested)
    feature_availability = feature_validation["availability"]
    panel_path = out_dir / "brick_generated_daily_factor_panel.parquet"
    audit_cols = [
        "code",
        "signal_date",
        "entry_date",
        "exit_date",
        "return_pct",
        *V2_FEATURES,
        *requested,
    ]
    panel[[col for col in audit_cols if col in panel.columns]].to_parquet(panel_path, index=False)

    too_sparse = feature_validation["too_sparse"]
    if too_sparse:
        acceleration = build_compute_acceleration_plan("ranker_training", detect_nvidia_gpu())
        acceleration["lightgbm_gpu_probe"] = {
            "usable": False,
            "error": "not run: data coverage stop before model training",
        }
        acceleration["selected_backend"] = "not_run_data_coverage_stop"
        result = {
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "status": "DATA_COVERAGE_STOP",
            "stop_reason": "generated features too sparse for validation",
            "too_sparse_features": too_sparse,
            "strict_forward_validation": {
                "rolling_windows": ROLLING_WINDOWS,
                "split_column": "entry_date",
                "purge_rule": "train exit_date must be before each test_start",
                "validation_used": False,
                "test_years_unseen_by_each_window_model": True,
                "rolling_test_run": False,
                "rolling_test_skip_reason": "data coverage stop before ranker training",
            },
            "data_boundary": {
                "handoff_path": str(Path(args.handoff_path).resolve()),
                "candidate_path": str(candidate_path.resolve()),
                "indicator_cache": str(cache_dir.resolve()),
                "factor_panel": str(panel_path.resolve()),
                "shared_panel_cache": str(Path(args.panel_cache_path).resolve()),
                "daily_only": True,
                "use_market_timing": False,
                "entry_open_feature_formula": (
                    "overnight_gap_pct uses entry_date open versus signal_date close; "
                    "entry_open_to_yellow_pct and entry_open_to_ma5_pct use entry_date open versus signal-day yellow/MA5."
                ),
                "forbidden_model_inputs": [
                    "return_pct",
                    "exit_date",
                    "exit_price",
                    "hold_days",
                    "entry_date high/low/close",
                    "post-09:25 intraday data",
                ],
            },
            "candidate_source": candidate_stats,
            "panel_cache": panel_cache_stats,
            "daily_feature_construction": daily_stats,
            "pool_feature_construction": pool_stats,
            "compute_acceleration": acceleration,
            "features": {
                "v2_baseline": V2_FEATURES,
                "generated_daily": requested,
                "v2_plus_generated_daily": [*V2_FEATURES, *requested],
                "supported_generated_features": SUPPORTED_FEATURES,
            },
            "feature_validation": feature_validation,
            "feature_availability": feature_availability,
            "top_ns": sorted(set(args.top_n)),
            "rolling_windows": [],
            "rolling_oos_stitched": {},
            "summary": {
                "status": "DATA_COVERAGE_STOP",
                "not_promoted": True,
                "reason": "one or more generated features failed minimum non-null coverage",
            },
        }
        results_path = out_dir / "brick_generated_daily_factor_sqnav_phase6_results.json"
        results_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        write_report(result, out_dir / "brick_generated_daily_factor_sqnav_phase6_report.md")
        return result

    gpu_capability = detect_nvidia_gpu()
    acceleration = build_compute_acceleration_plan("ranker_training", gpu_capability)
    use_gpu = False
    gpu_probe_error = None
    if args.prefer_gpu and gpu_capability.available:
        use_gpu, gpu_probe_error = _probe_lightgbm_gpu()
    acceleration["lightgbm_gpu_probe"] = {"usable": bool(use_gpu), "error": gpu_probe_error}
    acceleration["selected_backend"] = "lightgbm_gpu" if use_gpu else "cpu"
    params = build_lgb_params(use_gpu=use_gpu, num_threads=args.threads)

    top_ns = sorted(set(args.top_n))
    common = {
        "generated_features": requested,
        "output_dir": out_dir,
        "params": params,
        "num_boost_round": args.num_boost_round,
        "top_ns": top_ns,
        "commission_bp": args.commission,
        "stamp_pct": args.stamp,
        "slippage_pct": args.slippage,
    }
    rolling = [run_window(panel, window, **common) for window in ROLLING_WINDOWS]
    rolling_oos: dict[str, Any] = {}
    for model_name in ["v2_baseline", "v2_plus_generated_daily"]:
        for top_n in top_ns:
            rolling_oos[f"{model_name}_top{top_n}"] = stitch_rolling_nav(rolling, model_name, top_n, out_dir)

    result = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "VALIDATION_COMPLETED",
        "strict_forward_validation": {
            "rolling_windows": ROLLING_WINDOWS,
            "split_column": "entry_date",
            "purge_rule": "train exit_date must be before each test_start",
            "validation_used": False,
            "test_years_unseen_by_each_window_model": True,
        },
        "data_boundary": {
            "handoff_path": str(Path(args.handoff_path).resolve()),
            "candidate_path": str(candidate_path.resolve()),
            "indicator_cache": str(cache_dir.resolve()),
            "factor_panel": str(panel_path.resolve()),
            "shared_panel_cache": str(Path(args.panel_cache_path).resolve()),
            "daily_only": True,
            "use_market_timing": False,
            "entry_open_feature_formula": (
                "overnight_gap_pct uses entry_date open versus signal_date close; "
                "entry_open_to_yellow_pct and entry_open_to_ma5_pct use entry_date open versus signal-day yellow/MA5."
            ),
            "forbidden_model_inputs": [
                "return_pct",
                "exit_date",
                "exit_price",
                "hold_days",
                "entry_date high/low/close",
                "post-09:25 intraday data",
            ],
        },
        "candidate_source": candidate_stats,
        "panel_cache": panel_cache_stats,
        "daily_feature_construction": daily_stats,
        "pool_feature_construction": pool_stats,
        "compute_acceleration": acceleration,
        "features": {
            "v2_baseline": V2_FEATURES,
            "generated_daily": requested,
            "v2_plus_generated_daily": [*V2_FEATURES, *requested],
            "supported_generated_features": SUPPORTED_FEATURES,
        },
        "feature_availability": feature_availability,
        "feature_validation": feature_validation,
        "feature_diagnostics_train_2020_2022": build_feature_diagnostics(panel, requested),
        "top_ns": top_ns,
        "rolling_windows": rolling,
        "rolling_oos_stitched": rolling_oos,
        "summary": {
            "rolling": {
                model: {
                    f"top{top_n}": summarize_model_topn(rolling, model, top_n)
                    for top_n in top_ns
                }
                for model in ["v2_baseline", "v2_plus_generated_daily"]
            },
            "delta_v2_plus_generated_daily_minus_baseline": {
                f"top{top_n}": summarize_deltas(rolling, top_n)
                for top_n in top_ns
            },
        },
    }
    results_path = out_dir / "brick_generated_daily_factor_sqnav_phase6_results.json"
    results_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(result, out_dir / "brick_generated_daily_factor_sqnav_phase6_report.md")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick generated daily-factor SQ NAV strict validation")
    parser.add_argument("--handoff-path", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--candidate-path", default=str(DEFAULT_CANDIDATE_PATH))
    parser.add_argument("--indicator-cache", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--panel-cache-path", default=str(DEFAULT_PANEL_CACHE_PATH))
    parser.add_argument("--rebuild-panel-cache", action="store_true")
    parser.add_argument("--allow-production-cache", action="store_true")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--top-n", type=int, nargs="+", default=[3, 5])
    parser.add_argument("--num-boost-round", type=int, default=300)
    parser.add_argument("--commission", type=float, default=3.0)
    parser.add_argument("--stamp", type=float, default=0.05)
    parser.add_argument("--slippage", type=float, default=0.1)
    parser.add_argument("--workers", type=int, default=max(1, min(mp.cpu_count() - 1, 12)))
    parser.add_argument("--threads", type=int, default=max(1, min(mp.cpu_count() - 1, 8)))
    parser.add_argument("--prefer-gpu", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run(args)
    summary = result["summary"]
    if result.get("status") != "DATA_COVERAGE_STOP":
        summary = result["summary"]["delta_v2_plus_generated_daily_minus_baseline"]
    payload = {
        "output_dir": str(Path(args.output_dir).resolve()),
        "status": result.get("status", "UNKNOWN"),
        "generated_features": result["features"]["generated_daily"],
        "rolling_oos_stitched": result["rolling_oos_stitched"],
        "summary": summary,
        "compute_acceleration": result["compute_acceleration"],
    }
    if result.get("status") == "DATA_COVERAGE_STOP":
        payload["too_sparse_features"] = result["too_sparse_features"]
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    mp.freeze_support()
    main()
