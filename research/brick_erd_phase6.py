"""Brick ERD factor strict rolling-forward validation.

This is a research-only runner. It keeps the current Brick V2 signal boundary
fixed, enriches signal rows from data/research_indicators_cache, and evaluates
the AG2-KBase ERD factor batch with strict train -> validation -> unseen test
folds.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT))

try:
    import lightgbm as lgb
    from scipy import stats
    from sklearn.preprocessing import RobustScaler
except ImportError as exc:  # pragma: no cover - exercised by runtime smoke
    raise SystemExit(f"Missing required package: {exc}") from exc

from research_automation.gpu_acceleration import (
    build_compute_acceleration_plan,
    detect_nvidia_gpu,
)


DEFAULT_SIGNAL_PATH = ROOT / "research_state" / "brick" / "brick_v2_raw_2020_2026_erd_base.csv"
DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / "erd_phase6"
DEFAULT_CACHE_NAME = "research_indicators_cache"
DATA_DIR = ROOT / "data"
PREFIXES = ("00", "30", "60", "68")

V2_FEATURES = [
    "red_height", "brick_slope_3d", "brick_slope_5d", "brick_value", "red_green_ratio",
    "rsi_6", "rsi_14", "bb_pct_b", "wr_14", "close_to_yellow_pct",
    "close_to_ma5_pct", "close_to_ma10_pct", "close_to_ma20_pct", "close_to_ma60_pct",
    "close_to_white_pct", "ret_5d", "ret_10d", "bullish_ratio_5d", "bullish_ratio_10d",
    "new_high_20d", "obv_trend_up", "vol_ratio_5", "macd_hist_rising",
    "turnover_ratio_5", "vol_ratio_20",
    "overnight_gap_pct", "entry_open_to_yellow_pct", "entry_open_to_ma5_pct",
]

ERD_FEATURES = [
    "erd_cross_sectional_absorption",
    "erd_rolling_absorption",
    "price_range_efficiency_score",
    "high_volume_absorption_interaction",
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


@dataclass
class FoldFrame:
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame
    purged_train: int
    purged_validation: int


def _code_str(value: object) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6)


def _date_str(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def _numeric_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.astype(float)
    return pd.to_numeric(
        series.replace({True: 1, False: 0, "True": 1, "False": 0, "true": 1, "false": 0}),
        errors="coerce",
    )


def _feature_matrix(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for feature in features:
        if feature in df.columns:
            out[feature] = _numeric_series(df[feature]).replace([np.inf, -np.inf], np.nan)
        else:
            out[feature] = 0.0
    return out.fillna(0.0)


def _rolling_percent_rank(values: np.ndarray, window: int = 20) -> np.ndarray:
    series = pd.Series(values)
    try:
        return series.rolling(window, min_periods=1).rank(pct=True).to_numpy(dtype=float)
    except Exception:
        ranks = np.full(len(values), np.nan, dtype=float)
        for i, value in enumerate(values):
            start = max(0, i - window + 1)
            sample = values[start:i + 1]
            sample = sample[np.isfinite(sample)]
            if len(sample) == 0 or not np.isfinite(value):
                continue
            ranks[i] = float(np.sum(sample <= value) / len(sample))
        return ranks


def _rolling_timeseries_residual(
    y: np.ndarray,
    x1: np.ndarray,
    x2: np.ndarray,
    signal_mask: np.ndarray,
    lookback: int = 60,
    min_obs: int = 20,
) -> np.ndarray:
    residual = np.full(len(y), np.nan, dtype=float)
    for i in np.flatnonzero(signal_mask):
        if not (np.isfinite(y[i]) and np.isfinite(x1[i]) and np.isfinite(x2[i])):
            continue
        start = max(0, i - lookback)
        past = np.arange(start, i)
        if len(past) < min_obs:
            continue
        valid = (
            np.isfinite(y[past])
            & np.isfinite(x1[past])
            & np.isfinite(x2[past])
        )
        if valid.sum() < min_obs:
            continue
        idx = past[valid]
        x = np.column_stack([np.ones(len(idx)), x1[idx], x2[idx]])
        try:
            beta = np.linalg.lstsq(x, y[idx], rcond=None)[0]
        except np.linalg.LinAlgError:
            continue
        pred = float(np.dot(np.array([1.0, x1[i], x2[i]], dtype=float), beta))
        residual[i] = y[i] - pred
    return residual


def _stock_feature_worker(args: tuple[str, list[str], str]) -> list[dict]:
    code, wanted_dates, cache_dir_text = args
    cache_file = Path(cache_dir_text) / f"{code}.parquet"
    if not cache_file.exists():
        return []
    wanted = set(wanted_dates)
    if not wanted:
        return []
    try:
        df = pd.read_parquet(
            cache_file,
            columns=["date", "open", "high", "low", "close", "volume"],
        )
    except Exception:
        return []
    if df.empty:
        return []
    df = df.sort_values("date").reset_index(drop=True)
    df["signal_date"] = _date_str(df["date"])

    open_ = pd.to_numeric(df["open"], errors="coerce").to_numpy(dtype=float)
    high = pd.to_numeric(df["high"], errors="coerce").to_numpy(dtype=float)
    low = pd.to_numeric(df["low"], errors="coerce").to_numpy(dtype=float)
    close = pd.to_numeric(df["close"], errors="coerce").to_numpy(dtype=float)
    volume = pd.to_numeric(df["volume"], errors="coerce").to_numpy(dtype=float)
    prev_close = pd.Series(close).shift(1).to_numpy(dtype=float)

    tr = np.nanmax(
        np.vstack([
            high - low,
            np.abs(high - prev_close),
            np.abs(low - prev_close),
        ]),
        axis=0,
    )
    tr[~np.isfinite(tr)] = high[~np.isfinite(tr)] - low[~np.isfinite(tr)]
    atr14 = pd.Series(tr).rolling(14, min_periods=1).mean().to_numpy(dtype=float)
    atr14_pct = atr14 / np.where(close != 0, close, np.nan)
    volume_rank = _rolling_percent_rank(volume, 20)
    body_abs = np.abs(close - open_) / np.where(open_ != 0, open_, np.nan)
    price_range_eff = ((high - low) / np.where(prev_close != 0, prev_close, np.nan)) / (
        volume_rank + 1e-6
    )

    signal_mask = df["signal_date"].isin(wanted).to_numpy()
    ts_resid = _rolling_timeseries_residual(body_abs, volume_rank, atr14_pct, signal_mask)
    rows = []
    for i in np.flatnonzero(signal_mask):
        rows.append({
            "code": code,
            "signal_date": df.at[i, "signal_date"],
            "signal_open": open_[i],
            "signal_high": high[i],
            "signal_low": low[i],
            "signal_close": close[i],
            "signal_volume": volume[i],
            "body_abs_pct": body_abs[i],
            "atr14_pct": atr14_pct[i],
            "volume_rank_20d": volume_rank[i],
            "erd_rolling_timeseries_residual": ts_resid[i],
            "price_range_efficiency_ratio": price_range_eff[i],
        })
    return rows


def load_signals(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="gbk", converters={"code": _code_str})
    required = {"code", "signal_date", "entry_date", "exit_date", "return_pct"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"signal file missing columns: {sorted(missing)}")
    df["code"] = df["code"].map(_code_str)
    for col in ["signal_date", "entry_date", "exit_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()
    df = df.dropna(subset=["signal_date", "entry_date", "exit_date"])
    df["return_pct"] = pd.to_numeric(df["return_pct"], errors="coerce")
    df = df.dropna(subset=["return_pct"])
    return df.reset_index(drop=True)


def enrich_with_research_cache(
    signals: pd.DataFrame,
    cache_dir: Path,
    workers: int,
) -> tuple[pd.DataFrame, dict]:
    date_map = (
        signals.assign(signal_date_text=signals["signal_date"].dt.strftime("%Y-%m-%d"))
        .groupby("code")["signal_date_text"]
        .apply(lambda s: sorted(set(s)))
        .to_dict()
    )
    tasks = [(code, dates, str(cache_dir)) for code, dates in date_map.items()]
    rows: list[dict] = []
    with mp.Pool(max(1, workers)) as pool:
        for batch in pool.imap_unordered(_stock_feature_worker, tasks, chunksize=20):
            rows.extend(batch)

    feat = pd.DataFrame(rows)
    if feat.empty:
        raise ValueError(f"no feature rows produced from {cache_dir}")
    feat["signal_date"] = pd.to_datetime(feat["signal_date"]).dt.normalize()
    merged = signals.merge(feat, on=["code", "signal_date"], how="left", validate="many_to_one")
    before = len(merged)
    merged = merged.dropna(subset=["body_abs_pct", "atr14_pct", "volume_rank_20d"]).copy()
    stats_block = {
        "signal_rows_before_cache_join": int(before),
        "signal_rows_after_cache_join": int(len(merged)),
        "cache_join_drop_rows": int(before - len(merged)),
        "cache_dir": str(cache_dir),
    }
    return merged.reset_index(drop=True), stats_block


def add_erd_features(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df = df.copy()
    df["erd_cross_sectional_residual"] = np.nan
    r2_values = []
    n_dates = 0
    for _, grp in df.groupby("signal_date", sort=False):
        valid_cols = ["body_abs_pct", "volume_rank_20d", "atr14_pct"]
        valid = grp[valid_cols].replace([np.inf, -np.inf], np.nan).dropna()
        if len(valid) < 20:
            df.loc[grp.index, "erd_cross_sectional_residual"] = 0.0
            continue
        idx = valid.index
        y = valid["body_abs_pct"].to_numpy(dtype=float)
        x = np.column_stack([
            np.ones(len(valid)),
            valid["volume_rank_20d"].to_numpy(dtype=float),
            valid["atr14_pct"].to_numpy(dtype=float),
        ])
        try:
            beta = np.linalg.lstsq(x, y, rcond=None)[0]
        except np.linalg.LinAlgError:
            df.loc[grp.index, "erd_cross_sectional_residual"] = 0.0
            continue
        pred = x @ beta
        resid = y - pred
        df.loc[idx, "erd_cross_sectional_residual"] = resid
        ss_res = float(np.sum((y - pred) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        if ss_tot > 1e-12:
            r2_values.append(max(0.0, 1.0 - ss_res / ss_tot))
            n_dates += 1

    df["erd_cross_sectional_residual"] = df["erd_cross_sectional_residual"].fillna(0.0)
    df["erd_rolling_timeseries_residual"] = df["erd_rolling_timeseries_residual"].fillna(0.0)
    df["price_range_efficiency_ratio"] = df["price_range_efficiency_ratio"].replace(
        [np.inf, -np.inf], np.nan
    ).fillna(0.0)

    df["erd_cross_sectional_absorption"] = -df["erd_cross_sectional_residual"]
    df["erd_rolling_absorption"] = -df["erd_rolling_timeseries_residual"]
    df["price_range_efficiency_score"] = -df["price_range_efficiency_ratio"]
    df["high_volume_absorption_interaction"] = (
        df["erd_cross_sectional_absorption"]
        * (df["volume_rank_20d"] > 0.80).astype(float)
    )
    stats_block = {
        "cross_sectional_regression_dates": int(n_dates),
        "cross_sectional_regression_mean_r2": float(np.mean(r2_values)) if r2_values else 0.0,
        "cross_sectional_regression_median_r2": float(np.median(r2_values)) if r2_values else 0.0,
        "cross_sectional_regression_failure_r2_below_5pct": bool(
            (float(np.mean(r2_values)) if r2_values else 0.0) < 0.05
        ),
    }
    return df, stats_block


def compute_rank_ic(df: pd.DataFrame, factor: str, label_col: str = "return_pct") -> dict:
    values = []
    for _, grp in df.groupby("signal_date"):
        valid = grp[[factor, label_col]].replace([np.inf, -np.inf], np.nan).dropna()
        if len(valid) < 5:
            continue
        if valid[factor].std() <= 1e-12 or valid[label_col].std() <= 1e-12:
            continue
        ic = stats.spearmanr(valid[factor], valid[label_col]).correlation
        if np.isfinite(ic):
            values.append(float(ic))
    if not values:
        return {"mean": 0.0, "std": 0.0, "n_dates": 0}
    arr = np.array(values, dtype=float)
    return {"mean": float(arr.mean()), "std": float(arr.std()), "n_dates": int(len(arr))}


def compute_residual_rank_ic(
    df: pd.DataFrame,
    factor: str,
    controls: list[str],
    label_col: str = "return_pct",
) -> dict:
    ic_values = []
    r2_values = []
    controls = [c for c in controls if c in df.columns]
    for _, grp in df.groupby("signal_date"):
        cols = [factor, label_col, *controls]
        valid = grp[cols].replace([np.inf, -np.inf], np.nan).dropna()
        if len(valid) < max(35, len(controls) + 5):
            continue
        y = _numeric_series(valid[factor]).to_numpy(dtype=float)
        labels = _numeric_series(valid[label_col]).to_numpy(dtype=float)
        x = _feature_matrix(valid, controls).to_numpy(dtype=float)
        x = np.column_stack([np.ones(len(valid)), x])
        if np.std(y) <= 1e-12 or np.std(labels) <= 1e-12:
            continue
        try:
            beta = np.linalg.lstsq(x, y, rcond=None)[0]
        except np.linalg.LinAlgError:
            continue
        pred = x @ beta
        resid = y - pred
        ss_res = float(np.sum((y - pred) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        if ss_tot > 1e-12:
            r2_values.append(max(0.0, 1.0 - ss_res / ss_tot))
        if np.std(resid) <= 1e-12:
            continue
        ic = stats.spearmanr(resid, labels).correlation
        if np.isfinite(ic):
            ic_values.append(float(ic))
    return {
        "residual_rank_ic_mean": float(np.mean(ic_values)) if ic_values else 0.0,
        "residual_rank_ic_std": float(np.std(ic_values)) if ic_values else 0.0,
        "residual_rank_ic_dates": int(len(ic_values)),
        "v2_explained_r2_mean": float(np.mean(r2_values)) if r2_values else 0.0,
        "v2_explained_r2_median": float(np.median(r2_values)) if r2_values else 0.0,
    }


def split_fold(df: pd.DataFrame, fold: dict, embargo_days: int) -> FoldFrame:
    train_start, train_end = [pd.Timestamp(x) for x in fold["train"]]
    val_start, val_end = [pd.Timestamp(x) for x in fold["validation"]]
    test_start, test_end = [pd.Timestamp(x) for x in fold["test"]]
    train_signal_cutoff = val_start - pd.Timedelta(days=embargo_days)
    val_signal_cutoff = test_start - pd.Timedelta(days=embargo_days)

    train_raw = (
        (df["signal_date"] >= train_start)
        & (df["signal_date"] <= train_end)
    )
    val_raw = (
        (df["signal_date"] >= val_start)
        & (df["signal_date"] <= val_end)
    )
    test_mask = (
        (df["signal_date"] >= test_start)
        & (df["signal_date"] <= test_end)
    )
    train_mask = train_raw & (df["signal_date"] < train_signal_cutoff) & (df["exit_date"] < val_start)
    val_mask = val_raw & (df["signal_date"] < val_signal_cutoff) & (df["exit_date"] < test_start)

    return FoldFrame(
        train=df[train_mask].copy(),
        validation=df[val_mask].copy(),
        test=df[test_mask].copy(),
        purged_train=int(train_raw.sum() - train_mask.sum()),
        purged_validation=int(val_raw.sum() - val_mask.sum()),
    )


def label_from_train_bins(train_y: np.ndarray, values: np.ndarray) -> np.ndarray:
    bins = np.percentile(train_y, [20, 40, 60, 80])
    labels = np.zeros(len(values), dtype=int)
    for threshold in bins:
        labels += values > threshold
    return labels


def group_sizes(frame: pd.DataFrame) -> np.ndarray:
    date_text = frame["signal_date"].dt.strftime("%Y-%m-%d")
    return date_text.groupby(date_text).size().to_numpy(dtype=int)


def _probe_lightgbm_gpu() -> tuple[bool, str | None]:
    try:
        x = np.random.default_rng(42).normal(size=(80, 4))
        y = np.tile(np.arange(4), 20)
        group = np.array([4] * 20)
        ds = lgb.Dataset(x, label=y, group=group)
        lgb.train(
            {
                "objective": "lambdarank",
                "metric": "ndcg",
                "verbosity": -1,
                "device_type": "gpu",
                "num_leaves": 7,
                "min_data_in_leaf": 2,
                "label_gain": [0, 1, 2, 3, 4],
            },
            ds,
            num_boost_round=2,
        )
        return True, None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def build_lgb_params(use_gpu: bool, num_threads: int) -> dict:
    params = {
        "objective": "lambdarank",
        "metric": "ndcg",
        "ndcg_eval_at": [3, 5, 10],
        "boosting_type": "gbdt",
        "num_leaves": 31,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "min_data_in_leaf": 50,
        "lambda_l1": 0.1,
        "lambda_l2": 0.1,
        "verbose": -1,
        "random_state": 42,
        "num_threads": max(1, num_threads),
        "label_gain": [0, 1, 2, 3, 4],
    }
    if use_gpu:
        params["device_type"] = "gpu"
    return params


def train_ranker(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    features: list[str],
    params: dict,
    num_boost_round: int,
) -> tuple[object, RobustScaler, dict]:
    train = train.sort_values(["signal_date", "code"]).reset_index(drop=True)
    validation = validation.sort_values(["signal_date", "code"]).reset_index(drop=True)
    x_train = _feature_matrix(train, features)
    x_val = _feature_matrix(validation, features)
    scaler = RobustScaler()
    x_train_s = scaler.fit_transform(x_train)
    x_val_s = scaler.transform(x_val)
    y_train_raw = train["return_pct"].to_numpy(dtype=float)
    y_val_raw = validation["return_pct"].to_numpy(dtype=float)
    y_train = label_from_train_bins(y_train_raw, y_train_raw)
    y_val = label_from_train_bins(y_train_raw, y_val_raw)
    train_set = lgb.Dataset(x_train_s, label=y_train, group=group_sizes(train))
    val_set = lgb.Dataset(x_val_s, label=y_val, group=group_sizes(validation), reference=train_set)
    model = lgb.train(
        params,
        train_set,
        valid_sets=[train_set, val_set],
        valid_names=["train", "validation"],
        num_boost_round=num_boost_round,
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )
    best_scores = {
        "best_iteration": int(model.best_iteration or num_boost_round),
        "validation_ndcg_at_3": float(model.best_score.get("validation", {}).get("ndcg@3", 0.0)),
        "validation_ndcg_at_5": float(model.best_score.get("validation", {}).get("ndcg@5", 0.0)),
        "validation_ndcg_at_10": float(model.best_score.get("validation", {}).get("ndcg@10", 0.0)),
    }
    return model, scaler, best_scores


def score_frame(model: object, scaler: RobustScaler, frame: pd.DataFrame, features: list[str]) -> np.ndarray:
    x = _feature_matrix(frame, features)
    return model.predict(scaler.transform(x), num_iteration=getattr(model, "best_iteration", None))


def select_top_n(frame: pd.DataFrame, scores: np.ndarray, top_n: int) -> pd.DataFrame:
    out = frame.copy()
    out["score"] = scores
    out["_rank"] = out.groupby("entry_date")["score"].rank(ascending=False, method="first")
    return out[out["_rank"] <= top_n].copy()


def _load_price_cache(codes: Iterable[str], cache_dir: Path) -> tuple[dict[str, dict[pd.Timestamp, float]], dict[str, np.ndarray]]:
    price_cache: dict[str, dict[pd.Timestamp, float]] = {}
    price_dates: dict[str, np.ndarray] = {}
    for code in sorted(set(map(_code_str, codes))):
        path = cache_dir / f"{code}.parquet"
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path, columns=["date", "close"])
        except Exception:
            continue
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        df = df.dropna(subset=["date"]).sort_values("date")
        price_cache[code] = dict(zip(df["date"], pd.to_numeric(df["close"], errors="coerce")))
        price_dates[code] = df["date"].to_numpy(dtype="datetime64[ns]")
    return price_cache, price_dates


def account_nav_metrics(
    trades: pd.DataFrame,
    cache_dir: Path,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    top_n: int,
    commission: float = 3.0,
    stamp: float = 0.05,
    slippage: float = 0.1,
) -> tuple[dict, pd.DataFrame]:
    if trades.empty:
        return {}, pd.DataFrame()
    trades = trades.copy()
    trades["code"] = trades["code"].map(_code_str)
    trades["entry_date"] = pd.to_datetime(trades["entry_date"]).dt.normalize()
    trades["exit_date"] = pd.to_datetime(trades["exit_date"]).dt.normalize()
    price_cache, price_dates = _load_price_cache(trades["code"].unique(), cache_dir)
    all_dates = [pd.Timestamp(d).normalize() for d in pd.date_range(start_date, end_date, freq="B")]
    positions = []
    daily_returns = []
    for today in all_dates:
        for _, trade in trades[trades["entry_date"] == today].iterrows():
            positions.append({
                "code": _code_str(trade["code"]),
                "entry": today,
                "exit": trade["exit_date"],
                "entry_price": float(trade["entry_price"]),
                "exit_price": float(trade["exit_price"]),
            })
        positions = [pos for pos in positions if pos["exit"] >= today]
        if not positions:
            daily_returns.append(0.0)
            continue
        pos_rets = []
        for pos in positions:
            cache = price_cache.get(pos["code"], {})
            dates = price_dates.get(pos["code"])
            cur_date = today.normalize()
            prev = None
            if dates is not None and len(dates) > 0:
                today64 = np.datetime64(cur_date.to_datetime64())
                prev_idx = np.searchsorted(dates, today64, side="left") - 1
                if prev_idx >= 0:
                    prev = cache.get(pd.Timestamp(dates[prev_idx]).normalize())
            cur = cache.get(cur_date)
            if pos["entry"] == today and pos["exit"] == today:
                if pos["entry_price"] > 0:
                    pos_rets.append((pos["exit_price"] - pos["entry_price"]) / pos["entry_price"])
            elif pos["entry"] == today:
                if cur and pos["entry_price"] > 0:
                    pos_rets.append((cur - pos["entry_price"]) / pos["entry_price"])
            elif pos["exit"] == today:
                if prev and prev > 0:
                    pos_rets.append((pos["exit_price"] - prev) / prev)
            elif prev and cur and prev > 0:
                pos_rets.append((cur - prev) / prev)
        daily_returns.append(float(np.mean(pos_rets)) if pos_rets else 0.0)

    cost_entry = commission / 10000.0 + slippage / 100.0
    cost_exit = commission / 10000.0 + stamp / 100.0 + slippage / 100.0
    date_to_idx = {d: i for i, d in enumerate(all_dates)}
    daily_cost = np.zeros(len(daily_returns))
    for _, trade in trades.iterrows():
        ed = pd.Timestamp(trade["entry_date"]).normalize()
        xd = pd.Timestamp(trade["exit_date"]).normalize()
        if ed in date_to_idx:
            daily_cost[date_to_idx[ed]] -= cost_entry / top_n
        if xd in date_to_idx:
            daily_cost[date_to_idx[xd]] -= cost_exit / top_n
    r = np.array(daily_returns, dtype=float) + daily_cost
    nav = (1.0 + r).cumprod()
    if len(nav) == 0:
        return {}, pd.DataFrame()
    cum_ret = (nav[-1] - 1.0) * 100.0
    peak = np.maximum.accumulate(nav)
    dd = (nav - peak) / peak
    mdd = float(dd.min() * 100.0)
    ann_ret = (1.0 + cum_ret / 100.0) ** (252.0 / len(r)) - 1.0 if cum_ret > -100 else -1.0
    ann_vol = float(np.std(r, ddof=1) * np.sqrt(252.0)) if len(r) > 1 else 0.0
    sharpe = float((ann_ret - 0.02) / ann_vol) if ann_vol > 0 else 0.0
    calmar = float(ann_ret / abs(mdd / 100.0)) if mdd < 0 else 0.0
    nav_df = pd.DataFrame({"date": all_dates[:len(r)], "nav": nav, "ret": r})
    metrics = {
        "trades": int(len(trades)),
        "days": int(len(r)),
        "cum_return_pct": round(float(cum_ret), 4),
        "cagr_pct": round(float(ann_ret * 100.0), 4),
        "max_dd_pct": round(mdd, 4),
        "sharpe": round(sharpe, 6),
        "calmar": round(calmar, 6),
        "daily_wr_pct": round(float((r > 0).mean() * 100.0), 4),
        "daily_avg_ret_pct": round(float(np.mean(r) * 100.0), 6),
    }
    return metrics, nav_df


def trade_metrics(trades: pd.DataFrame) -> dict:
    if trades.empty:
        return {}
    ret = trades["return_pct"].to_numpy(dtype=float)
    return {
        "trades": int(len(trades)),
        "entry_days": int(trades["entry_date"].nunique()),
        "win_rate_pct": round(float((ret > 0).mean() * 100.0), 4),
        "avg_return_pct": round(float(np.mean(ret)), 6),
        "median_return_pct": round(float(np.median(ret)), 6),
        "std_return_pct": round(float(np.std(ret, ddof=1)), 6) if len(ret) > 1 else 0.0,
        "avg_hold_days": round(float(pd.to_numeric(trades["hold_days"], errors="coerce").mean()), 4),
    }


def evaluate_model(
    frame: pd.DataFrame,
    scores: np.ndarray,
    top_n: int,
    cache_dir: Path,
    test_start: pd.Timestamp,
    test_end: pd.Timestamp,
    output_path: Path,
) -> dict:
    selected = select_top_n(frame, scores, top_n)
    selected.to_csv(output_path.with_suffix(".trades.csv"), index=False, encoding="gbk")
    account, nav_df = account_nav_metrics(selected, cache_dir, test_start, test_end, top_n)
    nav_df.to_csv(output_path.with_suffix(".nav.csv"), index=False, encoding="gbk")
    return {"trade": trade_metrics(selected), "account": account}


def run_phase6(args: argparse.Namespace) -> dict:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = DATA_DIR / args.cache_name
    if args.cache_name != DEFAULT_CACHE_NAME and not args.allow_production_cache:
        raise ValueError("research run must use research_indicators_cache unless explicitly allowed")
    if not cache_dir.exists():
        raise FileNotFoundError(f"cache not found: {cache_dir}")

    gpu_capability = detect_nvidia_gpu()
    acceleration = build_compute_acceleration_plan("ranker_training", gpu_capability)
    use_gpu = False
    gpu_probe_error = None
    if args.prefer_gpu and gpu_capability.available:
        use_gpu, gpu_probe_error = _probe_lightgbm_gpu()
    acceleration["lightgbm_gpu_probe"] = {"usable": bool(use_gpu), "error": gpu_probe_error}
    acceleration["selected_backend"] = "lightgbm_gpu" if use_gpu else "cpu"

    signals = load_signals(Path(args.signal_path))
    enriched, join_stats = enrich_with_research_cache(signals, cache_dir, args.workers)
    panel, erd_stats = add_erd_features(enriched)
    panel_path = out_dir / "brick_erd_factor_panel.parquet"
    panel.to_parquet(panel_path, index=False)

    train_probe = panel[
        (panel["signal_date"] >= pd.Timestamp("2020-01-01"))
        & (panel["signal_date"] <= pd.Timestamp("2022-12-31"))
    ].copy()
    orthogonality = {}
    for factor in ERD_FEATURES:
        orthogonality[factor] = {
            "rank_ic": compute_rank_ic(train_probe, factor),
            **compute_residual_rank_ic(train_probe, factor, V2_FEATURES),
        }

    params = build_lgb_params(use_gpu=use_gpu, num_threads=args.threads)
    model_specs = {
        "v2_baseline": V2_FEATURES,
        "v2_plus_erd": [*V2_FEATURES, *ERD_FEATURES],
    }
    fold_results = []
    for fold in FOLDS:
        split = split_fold(panel, fold, args.embargo_days)
        if min(len(split.train), len(split.validation), len(split.test)) < 100:
            fold_results.append({
                "fold": fold["fold"],
                "error": "too_few_samples",
                "n_train": len(split.train),
                "n_validation": len(split.validation),
                "n_test": len(split.test),
            })
            continue
        fold_block = {
            "fold": fold["fold"],
            "windows": fold,
            "embargo_days": args.embargo_days,
            "n_train": int(len(split.train)),
            "n_validation": int(len(split.validation)),
            "n_test": int(len(split.test)),
            "purged_train": split.purged_train,
            "purged_validation": split.purged_validation,
            "models": {},
        }
        test_start, test_end = [pd.Timestamp(x) for x in fold["test"]]
        effective_test_end = min(test_end, panel["signal_date"].max())
        fold_block["effective_test_window"] = [
            test_start.strftime("%Y-%m-%d"),
            effective_test_end.strftime("%Y-%m-%d"),
        ]
        val_start, val_end = [pd.Timestamp(x) for x in fold["validation"]]
        for name, features in model_specs.items():
            model, scaler, best = train_ranker(
                split.train,
                split.validation,
                features,
                params,
                num_boost_round=args.num_boost_round,
            )
            val_scores = score_frame(model, scaler, split.validation, features)
            test_scores = score_frame(model, scaler, split.test, features)
            prefix = out_dir / f"{fold['fold']}_{name}_validation"
            validation_metrics = evaluate_model(
                split.validation,
                val_scores,
                args.top_n,
                cache_dir,
                val_start,
                val_end,
                prefix,
            )
            prefix = out_dir / f"{fold['fold']}_{name}_test"
            test_metrics = evaluate_model(
                split.test,
                test_scores,
                args.top_n,
                cache_dir,
                test_start,
                effective_test_end,
                prefix,
            )
            importance = pd.DataFrame({
                "feature": features,
                "importance_gain": model.feature_importance(importance_type="gain"),
                "importance_split": model.feature_importance(importance_type="split"),
            }).sort_values("importance_gain", ascending=False)
            importance.to_csv(out_dir / f"{fold['fold']}_{name}_feature_importance.csv", index=False)
            fold_block["models"][name] = {
                "features": features,
                "n_features": len(features),
                "best_scores": best,
                "validation_metrics": validation_metrics,
                "test_metrics": test_metrics,
                "top_importance": importance.head(15).to_dict(orient="records"),
            }
        base = fold_block["models"]["v2_baseline"]["test_metrics"]["account"]
        erd = fold_block["models"]["v2_plus_erd"]["test_metrics"]["account"]
        fold_block["test_delta_v2_plus_erd_minus_baseline"] = {
            key: round(float(erd.get(key, 0.0) - base.get(key, 0.0)), 6)
            for key in ["cagr_pct", "cum_return_pct", "max_dd_pct", "sharpe", "calmar"]
        }
        fold_results.append(fold_block)

    summary = summarize_results(fold_results)
    result = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "strict_forward_validation": {
            "folds": FOLDS,
            "embargo_days": args.embargo_days,
            "test_years_unseen_by_fold_model": True,
            "purge_rule": "train exit_date < validation_start; validation exit_date < test_start; signal embargo before boundaries",
        },
        "data_boundary": {
            "signal_path": str(Path(args.signal_path).resolve()),
            "indicator_cache": str(cache_dir.resolve()),
            "factor_panel": str(panel_path.resolve()),
            "signal_data_start": panel["signal_date"].min().strftime("%Y-%m-%d"),
            "signal_data_end": panel["signal_date"].max().strftime("%Y-%m-%d"),
            "daily_only": True,
            "uses_l2_tick_orderbook_minute_auction": False,
        },
        "compute_acceleration": acceleration,
        "cache_join": join_stats,
        "erd_construction": erd_stats,
        "orthogonality_train_2020_2022": orthogonality,
        "fold_results": fold_results,
        "summary": summary,
    }
    metrics_path = out_dir / "brick_erd_phase6_results.json"
    metrics_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown_report(result, out_dir / "brick_erd_phase6_report.md")
    return result


def summarize_results(fold_results: list[dict]) -> dict:
    rows = []
    for fold in fold_results:
        if "models" not in fold:
            continue
        for model_name, block in fold["models"].items():
            account = block["test_metrics"]["account"]
            trade = block["test_metrics"]["trade"]
            rows.append({
                "fold": fold["fold"],
                "model": model_name,
                "cagr_pct": account.get("cagr_pct", 0.0),
                "cum_return_pct": account.get("cum_return_pct", 0.0),
                "max_dd_pct": account.get("max_dd_pct", 0.0),
                "sharpe": account.get("sharpe", 0.0),
                "calmar": account.get("calmar", 0.0),
                "trades": trade.get("trades", 0),
                "avg_return_pct": trade.get("avg_return_pct", 0.0),
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return {}
    summary = {}
    for model_name, grp in df.groupby("model"):
        summary[model_name] = {
            "folds": int(len(grp)),
            "avg_test_cagr_pct": round(float(grp["cagr_pct"].mean()), 6),
            "worst_test_cagr_pct": round(float(grp["cagr_pct"].min()), 6),
            "avg_test_sharpe": round(float(grp["sharpe"].mean()), 6),
            "worst_test_sharpe": round(float(grp["sharpe"].min()), 6),
            "avg_test_max_dd_pct": round(float(grp["max_dd_pct"].mean()), 6),
            "worst_test_max_dd_pct": round(float(grp["max_dd_pct"].min()), 6),
            "positive_cagr_pass_rate": round(float((grp["cagr_pct"] > 0).mean()), 6),
            "sharpe_positive_pass_rate": round(float((grp["sharpe"] > 0).mean()), 6),
            "cagr_std": round(float(grp["cagr_pct"].std(ddof=0)), 6),
            "sharpe_std": round(float(grp["sharpe"].std(ddof=0)), 6),
        }
    if {"v2_baseline", "v2_plus_erd"}.issubset(summary):
        pivot = df.pivot(index="fold", columns="model", values=["cagr_pct", "sharpe", "max_dd_pct"])
        deltas = {}
        for metric in ["cagr_pct", "sharpe", "max_dd_pct"]:
            diff = pivot[(metric, "v2_plus_erd")] - pivot[(metric, "v2_baseline")]
            deltas[metric] = {
                "avg_delta": round(float(diff.mean()), 6),
                "worst_delta": round(float(diff.min()), 6),
                "positive_delta_pass_rate": round(float((diff > 0).mean()), 6),
            }
        summary["v2_plus_erd_minus_baseline"] = deltas
    return summary


def write_markdown_report(result: dict, path: Path) -> None:
    lines = [
        "# Brick ERD Phase 6 Strict Forward Validation",
        "",
        f"Created: {result['created_at']}",
        "",
        "## Boundary",
        "",
        f"- Signal file: `{result['data_boundary']['signal_path']}`",
        f"- Indicator cache: `{result['data_boundary']['indicator_cache']}`",
        f"- Signal data: {result['data_boundary']['signal_data_start']} to {result['data_boundary']['signal_data_end']}",
        "- Data modality: daily OHLCV only; no L2/tick/orderbook/minute/auction data.",
        f"- Embargo days: {result['strict_forward_validation']['embargo_days']}",
        "",
        "## Compute",
        "",
        f"- Backend: `{result['compute_acceleration']['selected_backend']}`",
        f"- GPU available: `{result['compute_acceleration']['gpu_available']}`",
        f"- LightGBM GPU probe: `{result['compute_acceleration']['lightgbm_gpu_probe']}`",
        "",
        "## ERD Construction",
        "",
        f"- Cache joined rows: {result['cache_join']['signal_rows_after_cache_join']} / {result['cache_join']['signal_rows_before_cache_join']}",
        f"- Cross-sectional ERD mean R2: {result['erd_construction']['cross_sectional_regression_mean_r2']:.4f}",
        f"- R2 below 5 pct failure flag: {result['erd_construction']['cross_sectional_regression_failure_r2_below_5pct']}",
        "",
        "## Orthogonality 2020-2022",
        "",
        "| Factor | RankIC | Residual RankIC vs V2 | V2 explained R2 | Dates |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for factor, block in result["orthogonality_train_2020_2022"].items():
        lines.append(
            "| {factor} | {rank_ic:.4f} | {res_ic:.4f} | {r2:.4f} | {dates} |".format(
                factor=factor,
                rank_ic=block["rank_ic"]["mean"],
                res_ic=block["residual_rank_ic_mean"],
                r2=block["v2_explained_r2_mean"],
                dates=block["residual_rank_ic_dates"],
            )
        )
    lines.extend([
        "",
        "## Test Results",
        "",
        "| Fold | Model | Test window | Trades | CAGR | CumRet | MaxDD | Sharpe | Calmar |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for fold in result["fold_results"]:
        if "models" not in fold:
            continue
        planned_window = " to ".join(fold["windows"]["test"])
        effective_window = " to ".join(fold.get("effective_test_window") or fold["windows"]["test"])
        test_window = effective_window if effective_window == planned_window else f"{effective_window} (planned {planned_window})"
        for model_name, block in fold["models"].items():
            account = block["test_metrics"]["account"]
            trade = block["test_metrics"]["trade"]
            lines.append(
                "| {fold} | {model} | {window} | {trades} | {cagr:.2f}% | {cum:.2f}% | {mdd:.2f}% | {sharpe:.3f} | {calmar:.3f} |".format(
                    fold=fold["fold"],
                    model=model_name,
                    window=test_window,
                    trades=trade.get("trades", 0),
                    cagr=account.get("cagr_pct", 0.0),
                    cum=account.get("cum_return_pct", 0.0),
                    mdd=account.get("max_dd_pct", 0.0),
                    sharpe=account.get("sharpe", 0.0),
                    calmar=account.get("calmar", 0.0),
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
        "A factor is not promotion-valid unless V2+ERD improves average test performance, has acceptable worst-fold behavior, and does not rely on validation or test leakage.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick ERD strict Phase 6 validation")
    parser.add_argument("--signal-path", default=str(DEFAULT_SIGNAL_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--cache-name", default=DEFAULT_CACHE_NAME)
    parser.add_argument("--allow-production-cache", action="store_true")
    parser.add_argument("--workers", type=int, default=max(1, min(mp.cpu_count() - 1, 8)))
    parser.add_argument("--threads", type=int, default=max(1, min(mp.cpu_count() - 1, 8)))
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--embargo-days", type=int, default=20)
    parser.add_argument("--num-boost-round", type=int, default=500)
    parser.add_argument("--prefer-gpu", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_phase6(args)
    print(json.dumps({
        "output_dir": str(Path(args.output_dir).resolve()),
        "summary": result.get("summary", {}),
        "compute_acceleration": result.get("compute_acceleration", {}),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    mp.freeze_support()
    main()
