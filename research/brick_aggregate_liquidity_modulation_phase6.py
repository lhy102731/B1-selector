"""Strict Phase 6 validation for aggregate-liquidity feature modulation.

This research-only runner consumes one approved AG2-KBase handoff. It builds
signal-day whole-market state from raw parquet, runs the locked falsification
diagnostic on train/validation data, and opens each unseen test fold only after
its validation gate passes. Brick production code and production caches are not
modified.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.preprocessing import RobustScaler


ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(RESEARCH_DIR))

from ag2_research.discovery_handoff import extract_discovery_transcript, extract_stage_outputs  # noqa: E402
from brick_erd_phase6 import (  # noqa: E402
    V2_FEATURES,
    _feature_matrix,
    _probe_lightgbm_gpu,
    build_lgb_params,
    label_from_train_bins,
)
from brick_label_reconstruction_phase6 import evaluate, metric_delta  # noqa: E402
from brick_v2_rebuilt_dual_metrics import (  # noqa: E402
    filter_entry_window,
    load_rebuilt_candidates,
)
from research_automation.discovery_execution_bridge import load_handoff_document  # noqa: E402
from research_automation.gpu_acceleration import (  # noqa: E402
    build_compute_acceleration_plan,
    detect_nvidia_gpu,
)


MECHANISM_NAME = "aggregate_liquidity_state_volume_feature_modulation"
RUNNER_ID = "brick_aggregate_liquidity_modulation_phase6"
DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / RUNNER_ID
DEFAULT_CANDIDATE_PATH = (
    ROOT
    / "research_state"
    / "brick"
    / "v2_rebuilt_dual_metrics_20260709_parquet_notiming_top3"
    / "rebuilt_candidates_from_daily.parquet"
)
DEFAULT_RAW_PARQUET_ROOT = ROOT / "data" / "raw_parquet"
DEFAULT_V5_REGIME_PATH = ROOT / "data" / "v5" / "regime_state.parquet"
VOLUME_FEATURES = ["vol_ratio_5", "vol_ratio_20", "turnover_ratio_5"]
STATE_COMPONENTS = [
    "market_amount_pctile_60d",
    "advance_decline_pctile_60d",
    "participation_breadth_pctile_60d",
]
FOLDS = [
    {
        "fold": "F1",
        "train": ("2020-01-01", "2022-12-31"),
        "validation": ("2023-01-01", "2023-12-31"),
        "test": ("2024-01-01", "2024-12-31"),
    },
    {
        "fold": "F2",
        "train": ("2021-01-01", "2023-12-31"),
        "validation": ("2024-01-01", "2024-12-31"),
        "test": ("2025-01-01", "2025-12-31"),
    },
    {
        "fold": "F3",
        "train": ("2022-01-01", "2024-12-31"),
        "validation": ("2025-01-01", "2025-12-31"),
        "test": ("2026-01-01", "2026-12-31"),
    },
]
EMBARGO_TRADING_DAYS = 20
STATE_LOOKBACK_DAYS = 60
STATE_MIN_OBSERVATIONS = 40
PARTICIPATION_LOOKBACK_DAYS = 20
PARTICIPATION_MIN_OBSERVATIONS = 15
PREFLIGHT_DELTA_MIN = 0.01
PREFLIGHT_BONFERRONI_ALPHA = 0.05
STATE_COVERAGE_MIN = 0.95
VALIDATION_SHARPE_DELTA_MIN = 0.05
MAX_DRAWDOWN_WORSENING_PP = 2.0
MAX_WITHIN_DAY_RANK_CORRELATION = 0.999
MIN_TEST_EFFECT_RETENTION = 0.20


def _json_default(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    temporary.replace(path)


def update_status(output_dir: Path, stage: str, **details: Any) -> None:
    write_json(
        output_dir / "status.json",
        {
            "status": "running",
            "stage": stage,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            **details,
        },
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_handoff(path: Path) -> dict[str, Any]:
    document = load_handoff_document(path)
    if str(document.get("status") or "").upper() != "APPROVED":
        raise ValueError("aggregate-liquidity runner requires an APPROVED handoff")
    if str(document.get("strategy_id") or "").lower() != "brick":
        raise ValueError("aggregate-liquidity runner is registered only for strategy=brick")
    transcript = extract_discovery_transcript(document) or []
    factor_output = extract_stage_outputs(transcript).get("factor_engineer") or {}
    mechanism = factor_output.get("research_mechanism")
    if not isinstance(mechanism, dict) or mechanism.get("name") != MECHANISM_NAME:
        raise ValueError(f"handoff does not contain research_mechanism={MECHANISM_NAME}")
    return mechanism


def preregistration(handoff_path: Path, mechanism: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "runner_id": RUNNER_ID,
        "handoff_path": str(handoff_path.resolve()),
        "handoff_sha256": sha256_file(handoff_path),
        "mechanism_name": mechanism.get("name"),
        "mechanism_family": mechanism.get("family"),
        "production_code_changes_allowed": False,
        "market_timing_enabled": False,
        "candidate_source": str(DEFAULT_CANDIDATE_PATH.resolve()),
        "aggregate_state": {
            "universe": "all available 00/30/60/68 raw parquet securities",
            "market_amount": "sum(amount) by signal day",
            "advance_decline": "advancers / (advancers + decliners) by signal day",
            "participation_breadth": (
                "fraction of securities with signal-day volume above their trailing "
                "20-observation median; minimum 15 observations"
            ),
            "component_transform": "rolling 60-observation percentile ending on signal day",
            "component_min_observations": STATE_MIN_OBSERVATIONS,
            "combination": "equal-weight arithmetic mean of the three percentile components",
        },
        "modulation": {
            "features": VOLUME_FEATURES,
            "formula": "feature_value * aggregate_liquidity_state_score before RobustScaler",
            "new_candidate_level_features": [],
        },
        "preflight": {
            "selection_boundary": "train outcomes orient feature signs; validation outcomes decide gate",
            "test_outcomes_opened": False,
            "daily_rankic_label": "existing Brick return_pct outcome; never a model input",
            "state_bucket_cutpoints": "train-only 1/3 and 2/3 quantiles",
            "delta_min": PREFLIGHT_DELTA_MIN,
            "bonferroni_alpha": PREFLIGHT_BONFERRONI_ALPHA,
            "comparisons": len(FOLDS) * len(VOLUME_FEATURES),
            "v5_regime_placebo": "validation-only regime RankIC spread must be smaller than aggregate-state spread",
        },
        "strict_forward_validation": {
            "folds": FOLDS,
            "embargo_trading_days": EMBARGO_TRADING_DAYS,
            "train_validation_use": "model fitting and fixed gate only",
            "test_use": "single unseen evaluation after the validation gate passes",
        },
        "model": {
            "features": V2_FEATURES,
            "objective": "LightGBM LambdaRank",
            "num_boost_round": 300,
            "random_state": 42,
            "top_n": 5,
        },
        "validation_gate": {
            "max_mean_within_day_rank_correlation": MAX_WITHIN_DAY_RANK_CORRELATION,
            "minimum_executable_sharpe_delta": VALIDATION_SHARPE_DELTA_MIN,
            "maximum_drawdown_worsening_percentage_points": MAX_DRAWDOWN_WORSENING_PP,
        },
        "test_gate": {
            "sharpe_delta_must_be_positive": True,
            "minimum_validation_effect_retention": MIN_TEST_EFFECT_RETENTION,
            "maximum_drawdown_worsening_percentage_points": MAX_DRAWDOWN_WORSENING_PP,
        },
    }


def rolling_percent_rank(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.rolling(window, min_periods=min_periods).rank(pct=True)


def add_state_percentiles(daily: pd.DataFrame) -> pd.DataFrame:
    out = daily.sort_values("date").reset_index(drop=True).copy()
    out["advance_decline_ratio"] = out["advancers"] / (
        out["advancers"] + out["decliners"]
    ).replace(0, np.nan)
    out["participation_breadth"] = out["participants"] / out[
        "participation_eligible"
    ].replace(0, np.nan)
    out["market_amount_pctile_60d"] = rolling_percent_rank(
        out["total_amount"], STATE_LOOKBACK_DAYS, STATE_MIN_OBSERVATIONS
    )
    out["advance_decline_pctile_60d"] = rolling_percent_rank(
        out["advance_decline_ratio"], STATE_LOOKBACK_DAYS, STATE_MIN_OBSERVATIONS
    )
    out["participation_breadth_pctile_60d"] = rolling_percent_rank(
        out["participation_breadth"], STATE_LOOKBACK_DAYS, STATE_MIN_OBSERVATIONS
    )
    out["aggregate_liquidity_state_score"] = out[STATE_COMPONENTS].mean(axis=1)
    out["amount_stock_coverage"] = out["amount_rows"] / out["universe_rows"].replace(0, np.nan)
    out["participation_stock_coverage"] = out["participation_eligible"] / out[
        "universe_rows"
    ].replace(0, np.nan)
    return out


def build_market_state(
    raw_root: Path,
    *,
    start_date: str,
    end_date: str,
    threads: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    files = sorted(raw_root.glob("*/*.parquet"))
    if not files:
        raise FileNotFoundError(f"no raw parquet files under {raw_root}")
    parquet_glob = str((raw_root / "*" / "*.parquet").resolve()).replace("\\", "/")
    connection = duckdb.connect()
    connection.execute(f"PRAGMA threads={max(1, int(threads))}")
    query = f"""
        WITH source AS (
            SELECT
                filename,
                CAST(date AS DATE) AS date,
                TRY_CAST(close AS DOUBLE) AS close,
                TRY_CAST(volume AS DOUBLE) AS volume,
                TRY_CAST(amount AS DOUBLE) AS amount
            FROM read_parquet(?, filename=true, union_by_name=true)
            WHERE CAST(date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        ), bars AS (
            SELECT
                *,
                LAG(close) OVER (PARTITION BY filename ORDER BY date) AS prev_close,
                MEDIAN(volume) OVER (
                    PARTITION BY filename ORDER BY date
                    ROWS BETWEEN {PARTICIPATION_LOOKBACK_DAYS - 1} PRECEDING AND CURRENT ROW
                ) AS volume_median20,
                COUNT(volume) OVER (
                    PARTITION BY filename ORDER BY date
                    ROWS BETWEEN {PARTICIPATION_LOOKBACK_DAYS - 1} PRECEDING AND CURRENT ROW
                ) AS volume_obs20
            FROM source
        )
        SELECT
            date,
            SUM(CASE WHEN amount >= 0 THEN amount ELSE NULL END) AS total_amount,
            SUM(CASE WHEN close > prev_close THEN 1 ELSE 0 END) AS advancers,
            SUM(CASE WHEN close < prev_close THEN 1 ELSE 0 END) AS decliners,
            SUM(CASE WHEN volume_obs20 >= {PARTICIPATION_MIN_OBSERVATIONS}
                      AND volume > volume_median20 THEN 1 ELSE 0 END) AS participants,
            SUM(CASE WHEN volume_obs20 >= {PARTICIPATION_MIN_OBSERVATIONS}
                      THEN 1 ELSE 0 END) AS participation_eligible,
            SUM(CASE WHEN amount >= 0 THEN 1 ELSE 0 END) AS amount_rows,
            COUNT(*) AS universe_rows
        FROM bars
        GROUP BY date
        ORDER BY date
    """
    started = time.time()
    try:
        daily = connection.execute(
            query,
            [parquet_glob, start_date, end_date],
        ).df()
    finally:
        connection.close()
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.normalize()
    state = add_state_percentiles(daily)
    return state, {
        "backend": "duckdb_cpu",
        "gpu_applicable": False,
        "gpu_fallback_reason": "DuckDB parquet window aggregation has no established GPU path in this workspace",
        "duckdb_version": duckdb.__version__,
        "raw_parquet_root": str(raw_root.resolve()),
        "raw_files": len(files),
        "query_start": start_date,
        "query_end": end_date,
        "daily_rows": int(len(state)),
        "elapsed_seconds": round(time.time() - started, 3),
    }


def load_v5_regime(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["signal_date", "v5_regime_label"])
    frame = pd.read_parquet(path)
    frame["signal_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    label = "regime_label" if "regime_label" in frame.columns else "regime"
    return frame[["signal_date", label]].rename(columns={label: "v5_regime_label"})


def merge_state(
    candidates: pd.DataFrame,
    market_state: pd.DataFrame,
    v5_regime: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    state = market_state.rename(columns={"date": "signal_date"})
    keep = [
        "signal_date",
        "total_amount",
        "advance_decline_ratio",
        "participation_breadth",
        "amount_stock_coverage",
        "participation_stock_coverage",
        *STATE_COMPONENTS,
        "aggregate_liquidity_state_score",
    ]
    panel = candidates.merge(state[keep], on="signal_date", how="left", validate="many_to_one")
    panel = panel.merge(v5_regime, on="signal_date", how="left", validate="many_to_one")
    before = len(panel)
    required = ["aggregate_liquidity_state_score", "return_pct", *V2_FEATURES]
    complete = panel.dropna(subset=required).copy()
    return complete, {
        "rows_before_state_preflight": int(before),
        "rows_after_state_and_feature_preflight": int(len(complete)),
        "row_coverage_pct": round(float(len(complete) / before * 100.0), 6) if before else 0.0,
        "state_missing_rows": int(panel["aggregate_liquidity_state_score"].isna().sum()),
        "v5_regime_row_coverage_pct": round(
            float(panel["v5_regime_label"].notna().mean() * 100.0), 6
        ),
    }


def split_with_embargo(
    panel: pd.DataFrame,
    fold: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    def purge(frame: pd.DataFrame, next_start: str) -> tuple[pd.DataFrame, dict[str, Any]]:
        before = len(frame)
        next_ts = pd.Timestamp(next_start)
        dates = sorted(pd.to_datetime(frame["entry_date"].dropna().unique()))
        embargo_dates = set(dates[-EMBARGO_TRADING_DAYS:])
        out = frame[
            (frame["exit_date"] < next_ts)
            & (~frame["entry_date"].isin(embargo_dates))
        ].copy()
        return out, {
            "rows_before": int(before),
            "rows_after": int(len(out)),
            "exit_overlap_purged": int((frame["exit_date"] >= next_ts).sum()),
            "embargo_trading_dates": [pd.Timestamp(x).strftime("%Y-%m-%d") for x in sorted(embargo_dates)],
            "rule": f"exit_date < {next_start}; exclude final {EMBARGO_TRADING_DAYS} observed entry dates",
        }

    train_raw = filter_entry_window(panel, tuple(fold["train"]))
    validation_raw = filter_entry_window(panel, tuple(fold["validation"]))
    test = filter_entry_window(panel, tuple(fold["test"]))
    train, train_audit = purge(train_raw, fold["validation"][0])
    validation, validation_audit = purge(validation_raw, fold["test"][0])
    if train.empty or validation.empty or test.empty:
        raise ValueError(f"{fold['fold']}: empty train/validation/test after purge and embargo")
    return train, validation, test, {
        "train": train_audit,
        "validation": validation_audit,
        "test_rows": int(len(test)),
        "test_outcomes_unseen_before_gate": True,
    }


def daily_rankic(panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for entry_date, group in panel.groupby("entry_date", sort=False):
        label = pd.to_numeric(group["return_pct"], errors="coerce")
        for feature in VOLUME_FEATURES:
            values = pd.to_numeric(group[feature], errors="coerce")
            valid = values.notna() & label.notna()
            if valid.sum() < 8 or values[valid].nunique() < 2 or label[valid].nunique() < 2:
                continue
            coefficient = stats.spearmanr(values[valid], label[valid]).statistic
            if not np.isfinite(coefficient):
                continue
            regimes = group.loc[valid, "v5_regime_label"].dropna().astype(str)
            rows.append({
                "entry_date": pd.Timestamp(entry_date).normalize(),
                "feature": feature,
                "rankic": float(coefficient),
                "candidates": int(valid.sum()),
                "state_score": float(group.loc[valid, "aggregate_liquidity_state_score"].median()),
                "v5_regime_label": regimes.mode().iat[0] if not regimes.empty else None,
            })
    return pd.DataFrame(rows)


def bootstrap_delta(
    high: np.ndarray,
    low: np.ndarray,
    *,
    seed: int,
    draws: int = 2000,
) -> dict[str, float]:
    high = np.asarray(high, dtype=float)
    low = np.asarray(low, dtype=float)
    high = high[np.isfinite(high)]
    low = low[np.isfinite(low)]
    if len(high) < 10 or len(low) < 10:
        return {"delta": float("nan"), "ci_low": float("nan"), "ci_high": float("nan"), "p_value": 1.0}
    rng = np.random.default_rng(seed)
    high_draws = rng.choice(high, size=(draws, len(high)), replace=True).mean(axis=1)
    low_draws = rng.choice(low, size=(draws, len(low)), replace=True).mean(axis=1)
    deltas = high_draws - low_draws
    observed = float(high.mean() - low.mean())
    p_value = float(2.0 * min(np.mean(deltas <= 0.0), np.mean(deltas >= 0.0)))
    return {
        "delta": observed,
        "ci_low": float(np.quantile(deltas, 0.025)),
        "ci_high": float(np.quantile(deltas, 0.975)),
        "p_value": min(1.0, p_value),
    }


def run_preflight(
    panel: pd.DataFrame,
    rankic: pd.DataFrame,
) -> dict[str, Any]:
    comparisons = len(FOLDS) * len(VOLUME_FEATURES)
    fold_results: list[dict[str, Any]] = []
    for fold_index, fold in enumerate(FOLDS):
        train, validation, _, split_audit = split_with_embargo(panel, fold)
        train_dates = set(train["entry_date"].unique())
        validation_dates = set(validation["entry_date"].unique())
        train_rankic = rankic[rankic["entry_date"].isin(train_dates)].copy()
        validation_rankic = rankic[rankic["entry_date"].isin(validation_dates)].copy()
        train_state = train.drop_duplicates("entry_date")["aggregate_liquidity_state_score"].dropna()
        cut_low, cut_high = np.quantile(train_state, [1.0 / 3.0, 2.0 / 3.0])
        validation_rankic["state_bucket"] = pd.cut(
            validation_rankic["state_score"],
            bins=[-np.inf, cut_low, cut_high, np.inf],
            labels=["low", "mid", "high"],
        )
        cells: list[dict[str, Any]] = []
        for feature_index, feature in enumerate(VOLUME_FEATURES):
            train_feature = train_rankic[train_rankic["feature"] == feature]
            validation_feature = validation_rankic[validation_rankic["feature"] == feature].copy()
            orientation_mean = float(train_feature["rankic"].mean())
            orientation = 1.0 if not np.isfinite(orientation_mean) or orientation_mean >= 0 else -1.0
            validation_feature["oriented_rankic"] = validation_feature["rankic"] * orientation
            high = validation_feature.loc[
                validation_feature["state_bucket"] == "high", "oriented_rankic"
            ].to_numpy(dtype=float)
            low = validation_feature.loc[
                validation_feature["state_bucket"] == "low", "oriented_rankic"
            ].to_numpy(dtype=float)
            bootstrap = bootstrap_delta(
                high,
                low,
                seed=42 + fold_index * 10 + feature_index,
            )
            bonferroni_p = min(1.0, bootstrap["p_value"] * comparisons)
            regime_means = (
                validation_feature.dropna(subset=["v5_regime_label"])
                .groupby("v5_regime_label")["oriented_rankic"]
                .mean()
            )
            regime_spread = (
                float(regime_means.max() - regime_means.min()) if len(regime_means) >= 2 else float("nan")
            )
            placebo_pass = bool(
                np.isfinite(regime_spread)
                and abs(regime_spread) < abs(bootstrap["delta"])
            )
            passed = bool(
                bootstrap["delta"] >= PREFLIGHT_DELTA_MIN
                and bootstrap["ci_low"] > 0.0
                and bonferroni_p < PREFLIGHT_BONFERRONI_ALPHA
                and placebo_pass
            )
            cells.append({
                "feature": feature,
                "orientation_selected_on_train": "positive" if orientation > 0 else "negative",
                "train_mean_rankic": round(orientation_mean, 8),
                "validation_high_days": int(len(high)),
                "validation_low_days": int(len(low)),
                "validation_high_mean_oriented_rankic": round(float(np.mean(high)), 8) if len(high) else None,
                "validation_low_mean_oriented_rankic": round(float(np.mean(low)), 8) if len(low) else None,
                "delta": round(float(bootstrap["delta"]), 8) if np.isfinite(bootstrap["delta"]) else None,
                "bootstrap_ci_95": [
                    round(float(bootstrap["ci_low"]), 8) if np.isfinite(bootstrap["ci_low"]) else None,
                    round(float(bootstrap["ci_high"]), 8) if np.isfinite(bootstrap["ci_high"]) else None,
                ],
                "raw_p_value": round(float(bootstrap["p_value"]), 8),
                "bonferroni_p_value": round(float(bonferroni_p), 8),
                "v5_placebo_regime_spread": round(regime_spread, 8) if np.isfinite(regime_spread) else None,
                "v5_placebo_passed": placebo_pass,
                "passed": passed,
            })
        validation_days = validation.drop_duplicates("entry_date")
        state_coverage = float(validation_days["aggregate_liquidity_state_score"].notna().mean())
        fold_passed = bool(state_coverage >= STATE_COVERAGE_MIN and all(cell["passed"] for cell in cells))
        fold_results.append({
            "fold": fold["fold"],
            "train_window": list(fold["train"]),
            "validation_window": list(fold["validation"]),
            "test_window_reserved_unseen": list(fold["test"]),
            "train_state_tertile_cutpoints": [round(float(cut_low), 8), round(float(cut_high), 8)],
            "validation_state_day_coverage_pct": round(state_coverage * 100.0, 6),
            "validation_v5_regime_day_coverage_pct": round(
                float(validation_days["v5_regime_label"].notna().mean() * 100.0), 6
            ),
            "split_audit": split_audit,
            "features": cells,
            "passed": fold_passed,
        })
    passed = bool(all(item["passed"] for item in fold_results))
    return {
        "status": "PASS" if passed else "PREFLIGHT_STOP",
        "passed": passed,
        "test_outcomes_opened": False,
        "hard_rule": (
            f"all {len(FOLDS)} folds x {len(VOLUME_FEATURES)} features require delta >= "
            f"{PREFLIGHT_DELTA_MIN}, Bonferroni p < {PREFLIGHT_BONFERRONI_ALPHA}, "
            "positive CI lower bound, and smaller V5 placebo spread"
        ),
        "folds": fold_results,
    }


def model_matrix(frame: pd.DataFrame, *, modulated: bool) -> pd.DataFrame:
    matrix = _feature_matrix(frame, V2_FEATURES)
    if modulated:
        state = pd.to_numeric(
            frame["aggregate_liquidity_state_score"], errors="coerce"
        ).fillna(0.0)
        for feature in VOLUME_FEATURES:
            matrix[feature] = matrix[feature] * state.to_numpy(dtype=float)
    return matrix


def group_sizes(frame: pd.DataFrame) -> np.ndarray:
    return frame.groupby("entry_date", sort=False).size().to_numpy(dtype=int)


def train_ranker(
    frame: pd.DataFrame,
    *,
    params: dict[str, Any],
    num_boost_round: int,
    modulated: bool,
) -> tuple[Any, RobustScaler, dict[str, Any]]:
    train = frame.sort_values(["entry_date", "code"]).reset_index(drop=True)
    matrix = model_matrix(train, modulated=modulated)
    scaler = RobustScaler()
    transformed = scaler.fit_transform(matrix)
    raw_label = train["return_pct"].to_numpy(dtype=float)
    thresholds = np.percentile(raw_label, [20, 40, 60, 80])
    labels = label_from_train_bins(raw_label, raw_label)
    dataset = lgb.Dataset(transformed, label=labels, group=group_sizes(train))
    model = lgb.train(params, dataset, num_boost_round=num_boost_round)
    return model, scaler, {
        "rows": int(len(train)),
        "entry_days": int(train["entry_date"].nunique()),
        "features": V2_FEATURES,
        "modulated_features": VOLUME_FEATURES if modulated else [],
        "label_thresholds_selected_on_train": [round(float(x), 8) for x in thresholds],
        "num_boost_round": int(num_boost_round),
        "test_rows_seen": 0,
    }


def score_ranker(model: Any, scaler: RobustScaler, frame: pd.DataFrame, *, modulated: bool) -> np.ndarray:
    matrix = model_matrix(frame, modulated=modulated)
    return model.predict(scaler.transform(matrix))


def mean_within_day_rank_correlation(
    frame: pd.DataFrame,
    baseline_scores: np.ndarray,
    modulated_scores: np.ndarray,
) -> dict[str, Any]:
    scored = frame[["entry_date"]].copy()
    scored["baseline"] = baseline_scores
    scored["modulated"] = modulated_scores
    values: list[float] = []
    for _, group in scored.groupby("entry_date", sort=False):
        if len(group) < 3:
            continue
        coefficient = stats.spearmanr(group["baseline"], group["modulated"]).statistic
        if np.isfinite(coefficient):
            values.append(float(coefficient))
    return {
        "days": int(len(values)),
        "mean": round(float(np.mean(values)), 8) if values else None,
        "median": round(float(np.median(values)), 8) if values else None,
        "min": round(float(np.min(values)), 8) if values else None,
        "max": round(float(np.max(values)), 8) if values else None,
    }


def save_model_artifacts(
    output_dir: Path,
    fold_name: str,
    label: str,
    model: Any,
    scaler: RobustScaler,
) -> dict[str, str]:
    model_path = output_dir / f"{fold_name}.{label}.model.txt"
    scaler_path = output_dir / f"{fold_name}.{label}.scaler.joblib"
    model.save_model(str(model_path))
    joblib.dump(scaler, scaler_path)
    return {
        "model": str(model_path.resolve()),
        "model_sha256": sha256_file(model_path),
        "scaler": str(scaler_path.resolve()),
        "scaler_sha256": sha256_file(scaler_path),
    }


def run_model_fold(
    panel: pd.DataFrame,
    fold: dict[str, Any],
    *,
    output_dir: Path,
    params: dict[str, Any],
    num_boost_round: int,
    top_n: int,
    target_position_pct: float,
    max_positions: int,
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> dict[str, Any]:
    name = str(fold["fold"])
    train, validation, test, split_audit = split_with_embargo(panel, fold)
    baseline_model, baseline_scaler, baseline_info = train_ranker(
        train, params=params, num_boost_round=num_boost_round, modulated=False
    )
    modulated_model, modulated_scaler, modulated_info = train_ranker(
        train, params=params, num_boost_round=num_boost_round, modulated=True
    )
    artifacts = {
        "baseline": save_model_artifacts(output_dir, name, "baseline", baseline_model, baseline_scaler),
        "modulated": save_model_artifacts(output_dir, name, "modulated", modulated_model, modulated_scaler),
    }
    val_start = pd.Timestamp(fold["validation"][0])
    val_end = min(pd.Timestamp(fold["validation"][1]), validation["entry_date"].max())
    baseline_val_scores = score_ranker(baseline_model, baseline_scaler, validation, modulated=False)
    modulated_val_scores = score_ranker(modulated_model, modulated_scaler, validation, modulated=True)
    correlation = mean_within_day_rank_correlation(validation, baseline_val_scores, modulated_val_scores)
    baseline_validation = evaluate(
        validation,
        baseline_val_scores,
        output_dir=output_dir,
        prefix=f"{name}.validation.baseline",
        top_n=top_n,
        start_ts=val_start,
        end_ts=val_end,
        target_position_pct=target_position_pct,
        max_positions=max_positions,
        commission_bp=commission_bp,
        stamp_pct=stamp_pct,
        slippage_pct=slippage_pct,
    )
    modulated_validation = evaluate(
        validation,
        modulated_val_scores,
        output_dir=output_dir,
        prefix=f"{name}.validation.modulated",
        top_n=top_n,
        start_ts=val_start,
        end_ts=val_end,
        target_position_pct=target_position_pct,
        max_positions=max_positions,
        commission_bp=commission_bp,
        stamp_pct=stamp_pct,
        slippage_pct=slippage_pct,
    )
    validation_sharpe_delta = metric_delta(
        modulated_validation, baseline_validation, "executable_portfolio", "sharpe"
    )
    validation_mdd_delta = metric_delta(
        modulated_validation, baseline_validation, "executable_portfolio", "max_dd_pct"
    )
    correlation_mean = correlation.get("mean")
    validation_gate = {
        "rank_order_changed": bool(
            correlation_mean is not None and correlation_mean <= MAX_WITHIN_DAY_RANK_CORRELATION
        ),
        "executable_sharpe_delta_passed": bool(
            validation_sharpe_delta >= VALIDATION_SHARPE_DELTA_MIN
        ),
        "max_drawdown_delta_passed": bool(
            validation_mdd_delta >= -MAX_DRAWDOWN_WORSENING_PP
        ),
    }
    block: dict[str, Any] = {
        "fold": name,
        "train_window": list(fold["train"]),
        "validation_window": list(fold["validation"]),
        "test_window": list(fold["test"]),
        "test_was_unseen_by_models": True,
        "split_audit": split_audit,
        "train_info": {"baseline": baseline_info, "modulated": modulated_info},
        "artifacts": artifacts,
        "validation": {"baseline": baseline_validation, "modulated": modulated_validation},
        "validation_deltas": {
            "executable_sharpe": validation_sharpe_delta,
            "executable_max_dd_pct": validation_mdd_delta,
            "executable_cagr_pct": metric_delta(
                modulated_validation, baseline_validation, "executable_portfolio", "cagr_pct"
            ),
        },
        "within_day_score_rank_correlation": correlation,
        "validation_gate": validation_gate,
        "validation_gate_passed": bool(all(validation_gate.values())),
    }
    if not block["validation_gate_passed"]:
        block.update({
            "status": "VALIDATION_STOP",
            "test_outcomes_opened": False,
            "test": {},
            "test_gate": {},
            "test_gate_passed": False,
        })
        return block

    test_start = pd.Timestamp(fold["test"][0])
    test_end = min(pd.Timestamp(fold["test"][1]), test["entry_date"].max())
    baseline_test_scores = score_ranker(baseline_model, baseline_scaler, test, modulated=False)
    modulated_test_scores = score_ranker(modulated_model, modulated_scaler, test, modulated=True)
    baseline_test = evaluate(
        test,
        baseline_test_scores,
        output_dir=output_dir,
        prefix=f"{name}.test.baseline",
        top_n=top_n,
        start_ts=test_start,
        end_ts=test_end,
        target_position_pct=target_position_pct,
        max_positions=max_positions,
        commission_bp=commission_bp,
        stamp_pct=stamp_pct,
        slippage_pct=slippage_pct,
    )
    modulated_test = evaluate(
        test,
        modulated_test_scores,
        output_dir=output_dir,
        prefix=f"{name}.test.modulated",
        top_n=top_n,
        start_ts=test_start,
        end_ts=test_end,
        target_position_pct=target_position_pct,
        max_positions=max_positions,
        commission_bp=commission_bp,
        stamp_pct=stamp_pct,
        slippage_pct=slippage_pct,
    )
    test_sharpe_delta = metric_delta(
        modulated_test, baseline_test, "executable_portfolio", "sharpe"
    )
    test_mdd_delta = metric_delta(
        modulated_test, baseline_test, "executable_portfolio", "max_dd_pct"
    )
    retention = (
        test_sharpe_delta / validation_sharpe_delta if validation_sharpe_delta > 0 else float("-inf")
    )
    test_gate = {
        "sharpe_direction_positive": bool(test_sharpe_delta > 0.0),
        "validation_effect_retained_20pct": bool(retention >= MIN_TEST_EFFECT_RETENTION),
        "max_drawdown_delta_passed": bool(test_mdd_delta >= -MAX_DRAWDOWN_WORSENING_PP),
    }
    block.update({
        "status": "TEST_COMPLETED",
        "test_outcomes_opened": True,
        "test": {"baseline": baseline_test, "modulated": modulated_test},
        "test_deltas": {
            "executable_sharpe": test_sharpe_delta,
            "executable_max_dd_pct": test_mdd_delta,
            "executable_cagr_pct": metric_delta(
                modulated_test, baseline_test, "executable_portfolio", "cagr_pct"
            ),
            "signal_quality_sharpe": metric_delta(
                modulated_test, baseline_test, "signal_quality", "sharpe"
            ),
            "signal_quality_cagr_pct": metric_delta(
                modulated_test, baseline_test, "signal_quality", "cagr_pct"
            ),
            "validation_sharpe_effect_retention": round(float(retention), 8),
        },
        "test_gate": test_gate,
        "test_gate_passed": bool(all(test_gate.values())),
    })
    return block


def summarize_model_folds(folds: list[dict[str, Any]]) -> dict[str, Any]:
    completed = [item for item in folds if item.get("status") == "TEST_COMPLETED"]
    passed = [item for item in completed if item.get("test_gate_passed")]
    metrics = [item["test_deltas"]["executable_sharpe"] for item in completed]
    cagr = [item["test_deltas"]["executable_cagr_pct"] for item in completed]
    return {
        "folds_total": len(folds),
        "test_folds_opened": len(completed),
        "test_folds_unopened_after_validation_stop": len(folds) - len(completed),
        "passed_test_folds": len(passed),
        "pass_rate": round(len(passed) / len(folds), 6) if folds else 0.0,
        "average_test_executable_sharpe_delta": round(float(np.mean(metrics)), 8) if metrics else None,
        "worst_test_executable_sharpe_delta": round(float(np.min(metrics)), 8) if metrics else None,
        "test_executable_sharpe_delta_dispersion": round(float(np.std(metrics)), 8) if metrics else None,
        "average_test_executable_cagr_delta_pct": round(float(np.mean(cagr)), 8) if cagr else None,
        "worst_test_executable_cagr_delta_pct": round(float(np.min(cagr)), 8) if cagr else None,
        "promotion_gate_passed": bool(len(folds) == len(completed) == len(passed)),
    }


def write_report(result: dict[str, Any], path: Path) -> None:
    lines = [
        "# Brick Aggregate Liquidity Modulation Phase 6",
        "",
        f"Created: {result['created_at']}",
        f"Status: {result['status']}",
        "",
        "## Boundary",
        "",
        f"- Handoff: `{result['data_boundary']['handoff_path']}`",
        f"- Candidate panel: `{result['data_boundary']['candidate_path']}`",
        "- Production code untouched; market timing disabled.",
        "- Aggregate state uses signal-day settled data only.",
        "- Test outcomes remain unopened unless the corresponding validation gate passes.",
        "",
        "## Preflight",
        "",
        "```json",
        json.dumps(result["preflight"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## Model Summary",
        "",
        "```json",
        json.dumps(result.get("summary", {}), ensure_ascii=False, indent=2),
        "```",
        "",
        "## Promotion",
        "",
        "This automated research output cannot modify production. Promotion requires the fixed gates to pass and explicit human confirmation.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    handoff_path = Path(args.handoff_path).resolve()
    mechanism = validate_handoff(handoff_path)
    prereg = preregistration(handoff_path, mechanism)
    prereg_path = output_dir / "preregistration.json"
    write_json(prereg_path, prereg)
    write_json(
        output_dir / "preregistration.lock.json",
        {"preregistration_sha256": sha256_file(prereg_path), "locked_at": datetime.now().isoformat(timespec="seconds")},
    )
    update_status(output_dir, "load_candidates")

    candidate_path = Path(args.candidate_path).resolve()
    candidates, candidate_stats = load_rebuilt_candidates(candidate_path)
    query_end = candidates["signal_date"].max().strftime("%Y-%m-%d")
    update_status(output_dir, "build_market_state", raw_files="pending")
    market_state, market_build = build_market_state(
        Path(args.raw_parquet_root).resolve(),
        start_date="2018-01-01",
        end_date=query_end,
        threads=args.threads,
    )
    market_state_path = output_dir / "market_aggregate_state.parquet"
    market_state.to_parquet(market_state_path, index=False)
    v5_regime = load_v5_regime(Path(args.v5_regime_path).resolve())
    panel, merge_stats = merge_state(candidates, market_state, v5_regime)
    panel_audit_path = output_dir / "candidate_state_audit.parquet"
    audit_columns = [
        "code", "signal_date", "entry_date", "exit_date", "return_pct",
        *VOLUME_FEATURES, *STATE_COMPONENTS, "aggregate_liquidity_state_score",
        "v5_regime_label",
    ]
    panel[audit_columns].to_parquet(panel_audit_path, index=False)

    update_status(output_dir, "falsification_preflight", panel_rows=len(panel))
    rankic = daily_rankic(panel)
    rankic_path = output_dir / "daily_volume_feature_rankic.parquet"
    rankic.to_parquet(rankic_path, index=False)
    preflight = run_preflight(panel, rankic)
    preflight_path = output_dir / "preflight_results.json"
    write_json(preflight_path, preflight)

    gpu_capability = detect_nvidia_gpu()
    acceleration = build_compute_acceleration_plan("ranker_training", gpu_capability)
    use_gpu = False
    gpu_probe_error = None
    if args.prefer_gpu and gpu_capability.available:
        use_gpu, gpu_probe_error = _probe_lightgbm_gpu()
    acceleration["lightgbm_gpu_probe"] = {"usable": bool(use_gpu), "error": gpu_probe_error}
    acceleration["selected_backend"] = "lightgbm_gpu" if use_gpu else "cpu"
    acceleration["market_aggregate_backend"] = market_build

    result: dict[str, Any] = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "runner_id": RUNNER_ID,
        "status": "PREFLIGHT_STOP" if not preflight["passed"] else "MODEL_VALIDATION_RUNNING",
        "data_boundary": {
            "handoff_path": str(handoff_path),
            "handoff_sha256": sha256_file(handoff_path),
            "candidate_path": str(candidate_path),
            "candidate_state_audit": str(panel_audit_path),
            "market_state_path": str(market_state_path),
            "daily_rankic_path": str(rankic_path),
            "preregistration_path": str(prereg_path),
            "production_script_touched": False,
            "production_cache_touched": False,
            "kbase_write_performed": False,
            "market_timing_enabled": False,
            "model_inputs_exclude": ["return_pct", "exit_date", "exit_price", "hold_days"],
        },
        "candidate_panel": {**candidate_stats, **merge_stats},
        "market_state": market_build,
        "compute_acceleration": acceleration,
        "features": {
            "baseline": V2_FEATURES,
            "modulated": VOLUME_FEATURES,
            "state_components": STATE_COMPONENTS,
            "state_combination": "equal_weight_mean",
        },
        "strict_forward_validation": {
            "folds": FOLDS,
            "embargo_trading_days": EMBARGO_TRADING_DAYS,
            "test_years_unseen_before_each_fold_gate": True,
        },
        "preflight": preflight,
        "folds": [],
        "summary": {
            "promotion_gate_passed": False,
            "reason": "preflight falsification stop" if not preflight["passed"] else "model validation pending",
        },
    }
    if preflight["passed"]:
        params = build_lgb_params(use_gpu=use_gpu, num_threads=args.threads)
        folds: list[dict[str, Any]] = []
        for fold in FOLDS:
            update_status(output_dir, "model_fold", fold=fold["fold"])
            fold_result = run_model_fold(
                panel,
                fold,
                output_dir=output_dir,
                params=params,
                num_boost_round=args.num_boost_round,
                top_n=args.top_n,
                target_position_pct=args.target_position_pct,
                max_positions=args.max_positions,
                commission_bp=args.commission,
                stamp_pct=args.stamp,
                slippage_pct=args.slippage,
            )
            folds.append(fold_result)
            write_json(output_dir / f"{fold['fold']}.result.json", fold_result)
        summary = summarize_model_folds(folds)
        result["folds"] = folds
        result["summary"] = summary
        result["status"] = "VALIDATION_COMPLETED"

    result_path = output_dir / "brick_aggregate_liquidity_modulation_results.json"
    write_json(result_path, result)
    report_path = output_dir / "brick_aggregate_liquidity_modulation_report.md"
    write_report(result, report_path)
    write_json(
        output_dir / "status.json",
        {
            "status": "complete",
            "research_status": result["status"],
            "result_path": str(result_path),
            "report_path": str(report_path),
            "promotion_gate_passed": bool(result.get("summary", {}).get("promotion_gate_passed")),
            "completed_at": datetime.now().isoformat(timespec="seconds"),
        },
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick aggregate-liquidity modulation Phase 6")
    parser.add_argument("--handoff-path", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--candidate-path", default=str(DEFAULT_CANDIDATE_PATH))
    parser.add_argument("--raw-parquet-root", default=str(DEFAULT_RAW_PARQUET_ROOT))
    parser.add_argument("--v5-regime-path", default=str(DEFAULT_V5_REGIME_PATH))
    parser.add_argument("--num-boost-round", type=int, default=300)
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--target-position-pct", type=float, default=0.10)
    parser.add_argument("--max-positions", type=int, default=10)
    parser.add_argument("--commission", type=float, default=3.0)
    parser.add_argument("--stamp", type=float, default=0.05)
    parser.add_argument("--slippage", type=float, default=0.1)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--prefer-gpu", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run(args)
    print(json.dumps({
        "output_dir": str(Path(args.output_dir).resolve()),
        "status": result["status"],
        "preflight_passed": result["preflight"]["passed"],
        "summary": result["summary"],
        "compute_acceleration": result["compute_acceleration"],
    }, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    main()
