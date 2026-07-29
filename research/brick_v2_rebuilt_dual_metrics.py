"""Brick V2 rebuilt-candidate window baseline with dual metric surfaces.

This research runner does not read legacy signals_raw_*.csv files. It rebuilds
Brick V2 candidates from raw parquet daily bars, trains a fresh LightGBM ranker
per forward window, and reports two different result surfaces:

1. signal_quality: active-signal average return index, useful for signal/factor
   research. This preserves the historical "average active positions" idea but
   avoids calling it an account.
2. executable_portfolio: cash-constrained portfolio simulation with fixed
   single-stock allocation and max-position limits, useful for live feasibility.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.preprocessing import RobustScaler

ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(RESEARCH_DIR))

from backtest_brick_v2_research import (  # noqa: E402
    ACTIVE_CAP_PATH,
    DATA_DIR,
    PREFIXES,
    _compute_extra,
    _extract_features,
    _one_word_limit_down,
    _open_limit_up,
)
from research_automation.gpu_acceleration import (  # noqa: E402
    build_compute_acceleration_plan,
    detect_nvidia_gpu,
)
from utils.market_timing import MarketTiming  # noqa: E402
from strategy.brick_chart_strategy import BrickChartStrategy  # noqa: E402

from brick_erd_phase6 import (  # noqa: E402
    V2_FEATURES,
    _code_str,
    _feature_matrix,
    _probe_lightgbm_gpu,
    build_lgb_params,
    label_from_train_bins,
    trade_metrics,
)


DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / "v2_rebuilt_dual_metrics"
DEFAULT_START = "2018-01-01"
DEFAULT_END = "2026-12-31"
DEFAULT_INDUSTRY_MAP_PATH = ROOT / "data" / "block" / "ths_industry_map.json"

FIXED_WINDOWS = [
    {
        "name": "fixed_2018_2020_train_2021_2023_test",
        "train": ("2018-01-01", "2020-12-31"),
        "test": ("2021-01-01", "2023-12-31"),
    },
    {
        "name": "fixed_2021_2023_train_2024_2026_test",
        "train": ("2021-01-01", "2023-12-31"),
        "test": ("2024-01-01", "2026-12-31"),
    },
    {
        "name": "fixed_2020_2022_train_2024_2026_test",
        "train": ("2020-01-01", "2022-12-31"),
        "test": ("2024-01-01", "2026-12-31"),
    },
]

ROLLING_WINDOWS = [
    {
        "name": f"rolling_{start}_{start + 2}_train_{start + 3}_test",
        "train": (f"{start}-01-01", f"{start + 2}-12-31"),
        "test": (f"{start + 3}-01-01", f"{start + 3}-12-31"),
    }
    for start in range(2018, 2024)
]


def discover_stock_files() -> list[Path]:
    files: list[Path] = []
    for prefix in PREFIXES:
        folder = DATA_DIR / prefix
        if folder.exists():
            files.extend(sorted(folder.glob("*.csv")))
    return files


def discover_raw_parquet_files() -> list[Path]:
    files: list[Path] = []
    root = DATA_DIR / "raw_parquet"
    for prefix in PREFIXES:
        folder = root / prefix
        if folder.exists():
            files.extend(sorted(folder.glob("*.parquet")))
    return files


def load_bullish_dates(path: Path, enabled: bool) -> set[str] | None:
    if not enabled:
        return None
    if not path.exists():
        return None
    mt = MarketTiming(str(path))
    return {str(d)[:10] for d, state in mt.states.items() if state == "bullish"}


def collect_signals_from_raw_parquet(args_tuple: tuple[str, set[str] | None, str, str, str, str]) -> list[dict[str, Any]]:
    parquet_path, bullish_dates, pre_start, start_date, end_date, entry_ma_source = args_tuple
    path = Path(parquet_path)
    code = path.stem
    try:
        df = pd.read_parquet(path)
    except Exception:
        return []

    if len(df) < 120 or "date" not in df.columns:
        return []

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    df = df[df["date"] >= pre_start]
    if len(df) < 60:
        return []

    strategy = BrickChartStrategy()
    try:
        result = strategy.calculate_indicators(df)
    except Exception:
        return []
    if result is None or result.empty:
        return []

    if "turnover" in df.columns and len(df) >= len(result):
        result["turnover"] = df["turnover"].values[:len(result)]
    else:
        result["turnover"] = 0
    result["turnover_ma5"] = result["turnover"].rolling(5, min_periods=1).mean()
    result["turnover_ma20"] = result["turnover"].rolling(20, min_periods=1).mean()
    result["turnover_max60"] = result["turnover"].rolling(60, min_periods=1).max()
    result = _compute_extra(result)

    brick = result["brick"].values
    close = result["close"].values
    open_ = result["open"].values
    dates = result["date"].values
    signals: list[dict[str, Any]] = []

    i = 2
    n = len(result)
    while i < n:
        if not result["brick_signal"].iloc[i]:
            i += 1
            continue

        sig_date = str(dates[i])[:10]
        if sig_date < start_date:
            i += 1
            continue
        if end_date and sig_date > end_date:
            i += 1
            continue
        if bullish_dates is not None and sig_date not in bullish_dates:
            i += 1
            continue

        entry_i = i + 1
        entry_ok = entry_i < n and not _open_limit_up(code, open_[entry_i], close[entry_i - 1])
        if entry_ok:
            entry_price = open_[entry_i]
            entry_date = str(dates[entry_i])[:10]
        else:
            entry_price = close[i]
            entry_date = str(dates[i])[:10]

        exit_i = None
        exit_price = close[i]
        for j in range(max(entry_i, i + 1), n):
            if brick[j] < brick[j - 1]:
                exit_i = j
                while exit_i < n and _one_word_limit_down(code, open_[exit_i], close[exit_i], close[exit_i - 1]):
                    exit_i += 1
                if exit_i >= n:
                    exit_i = None
                break

        ret_pct = 0.0
        if exit_i is not None and entry_ok:
            exit_price = close[exit_i]
            ret_pct = (exit_price - entry_price) / entry_price * 100

        feats = _extract_features(result, i)
        sig_close = float(result.iloc[i]["close"])
        feat_idx = entry_i if entry_ok else i
        erow = result.iloc[feat_idx]
        e_open = float(erow["open"])

        if entry_ma_source == "t0":
            # Match daily_select.py: entry_date open versus signal-day
            # yellow/MA5, which are known before the 09:25 selection.
            e_yellow = float(result.iloc[i]["yellow_line"])
            e_ma5 = float(result.iloc[i]["ma5"])
        else:
            delta = e_open - float(erow["close"])
            yf = (1 / 14 + 1 / 28 + 1 / 57 + 1 / 114) / 4
            e_yellow = float(erow["yellow_line"]) + delta * yf
            e_ma5 = float(erow["ma5"]) + delta / 5

        feats["overnight_gap_pct"] = (e_open - sig_close) / sig_close * 100 if sig_close > 0 else 0
        feats["entry_open_to_yellow_pct"] = (e_open - e_yellow) / e_yellow * 100 if e_yellow > 0 else 0
        feats["entry_open_to_ma5_pct"] = (e_open - e_ma5) / e_ma5 * 100 if e_ma5 > 0 else 0

        signals.append({
            "code": code,
            "signal_date": sig_date,
            "entry_date": entry_date,
            "entry_price": round(float(entry_price), 2),
            "exit_date": str(dates[exit_i])[:10] if exit_i else str(dates[i])[:10],
            "exit_price": round(float(exit_price), 2),
            "return_pct": round(float(ret_pct), 2),
            "hold_days": (exit_i - entry_i) if exit_i else 0,
            **feats,
        })
        i += 1

    return signals


def rebuild_candidates(
    *,
    start_date: str,
    end_date: str,
    entry_ma_source: str,
    use_market_timing: bool,
    workers: int,
    output_dir: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    stock_files = discover_stock_files()
    parquet_files = discover_raw_parquet_files()
    bullish_dates = load_bullish_dates(Path(ACTIVE_CAP_PATH), use_market_timing)
    pre_start = (pd.Timestamp(start_date) - pd.DateOffset(months=7)).strftime("%Y-%m-%d")
    tasks = [
        (str(path), bullish_dates, pre_start, start_date, end_date, entry_ma_source)
        for path in parquet_files
    ]
    if not tasks:
        raise FileNotFoundError(f"no raw parquet files found under {DATA_DIR / 'raw_parquet'}")

    rows: list[dict[str, Any]] = []
    t0 = datetime.now()
    with mp.Pool(max(1, workers)) as pool:
        for batch in pool.imap_unordered(collect_signals_from_raw_parquet, tasks, chunksize=20):
            if batch:
                rows.extend(batch)

    elapsed = (datetime.now() - t0).total_seconds()
    if not rows:
        raise ValueError("rebuilt candidate scan produced no signals")

    df = pd.DataFrame(rows)
    df["code"] = df["code"].map(_code_str)
    for col in ["signal_date", "entry_date", "exit_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()
    for col in ["entry_price", "exit_price", "return_pct", "hold_days", *V2_FEATURES]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].replace({True: 1, False: 0, "True": 1, "False": 0}),
                errors="coerce",
            )
    df = df.dropna(subset=["code", "signal_date", "entry_date", "exit_date", "return_pct"])
    df = df.sort_values(["entry_date", "code"]).reset_index(drop=True)

    candidate_path = output_dir / "rebuilt_candidates_from_daily.parquet"
    df.to_parquet(candidate_path, index=False)
    audit_csv = output_dir / "rebuilt_candidates_sample.csv"
    df.head(5000).to_csv(audit_csv, index=False, encoding="gbk")

    stats = {
        "source": "raw_parquet_rebuild",
        "csv_stock_files_seen": int(len(stock_files)),
        "raw_parquet_files": int(len(parquet_files)),
        "missing_raw_parquet_vs_csv": int(max(0, len(stock_files) - len(parquet_files))),
        "rows": int(len(df)),
        "entry_days": int(df["entry_date"].nunique()),
        "signal_days": int(df["signal_date"].nunique()),
        "entry_start": df["entry_date"].min().strftime("%Y-%m-%d"),
        "entry_end": df["entry_date"].max().strftime("%Y-%m-%d"),
        "pre_start": pre_start,
        "requested_start": start_date,
        "requested_end": end_date,
        "entry_ma_source": entry_ma_source,
        "use_market_timing": bool(use_market_timing and bullish_dates is not None),
        "bullish_days": int(len(bullish_dates or [])),
        "elapsed_seconds": round(float(elapsed), 3),
        "candidate_path": str(candidate_path.resolve()),
        "audit_sample_csv": str(audit_csv.resolve()),
    }
    return df, stats


def load_rebuilt_candidates(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"candidate parquet not found: {path}")
    df = pd.read_parquet(path)
    df["code"] = df["code"].map(_code_str)
    for col in ["signal_date", "entry_date", "exit_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()
    for col in ["entry_price", "exit_price", "return_pct", "hold_days", *V2_FEATURES]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].replace({True: 1, False: 0, "True": 1, "False": 0}),
                errors="coerce",
            )
    df = df.dropna(subset=["code", "signal_date", "entry_date", "exit_date", "return_pct"])
    df = df.sort_values(["entry_date", "code"]).reset_index(drop=True)
    return df, {
        "source": "provided_rebuilt_candidate_parquet",
        "candidate_path": str(path.resolve()),
        "rows": int(len(df)),
        "entry_days": int(df["entry_date"].nunique()),
        "signal_days": int(df["signal_date"].nunique()),
        "entry_start": df["entry_date"].min().strftime("%Y-%m-%d"),
        "entry_end": df["entry_date"].max().strftime("%Y-%m-%d"),
        "entry_ma_source": "unknown_from_candidate_cache",
        "use_market_timing": False,
    }


def load_industry_map(path: Path, *, max_per_ind: int) -> dict[str, str]:
    if max_per_ind >= 999:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    stocks = payload.get("stocks", payload)
    mapping: dict[str, str] = {}
    for code, blocks in stocks.items():
        code_key = str(code).zfill(6)
        if isinstance(blocks, list) and blocks:
            mapping[code_key] = str(blocks[0])
        elif isinstance(blocks, str):
            mapping[code_key] = blocks
        else:
            mapping[code_key] = "UNKNOWN"
    return mapping


def _industry_for_code(code: Any, industry_map: dict[str, str]) -> str:
    if not industry_map:
        return "UNCONSTRAINED"
    return industry_map.get(_code_str(code), "UNKNOWN")


def group_sizes_by(frame: pd.DataFrame, column: str) -> np.ndarray:
    return frame.groupby(column, sort=False).size().to_numpy(dtype=int)


def train_ranker_no_validation(
    train: pd.DataFrame,
    params: dict[str, Any],
    num_boost_round: int,
) -> tuple[Any, RobustScaler, dict[str, Any]]:
    train = train.sort_values(["entry_date", "code"]).reset_index(drop=True)
    x_train = _feature_matrix(train, V2_FEATURES)
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


def filter_entry_window(df: pd.DataFrame, window: tuple[str, str]) -> pd.DataFrame:
    start, end = [pd.Timestamp(x) for x in window]
    mask = (df["entry_date"] >= start) & (df["entry_date"] <= end)
    return df[mask].copy()


def filter_train_strict(
    df: pd.DataFrame,
    train_window: tuple[str, str],
    test_start: str,
) -> tuple[pd.DataFrame, dict[str, int]]:
    raw = filter_entry_window(df, train_window)
    before = len(raw)
    purged = raw[raw["exit_date"] < pd.Timestamp(test_start)].copy()
    return purged, {
        "train_rows_before_exit_purge": int(before),
        "train_rows_after_exit_purge": int(len(purged)),
        "purged_rows": int(before - len(purged)),
        "purge_rule": f"train exit_date < {test_start}",
    }


def select_top_n(
    frame: pd.DataFrame,
    scores: np.ndarray,
    top_n: int,
    *,
    max_per_ind: int = 999,
    industry_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    out = frame.copy()
    out["score"] = scores
    industry_map = industry_map or {}
    out["_industry"] = out["code"].map(lambda code: _industry_for_code(code, industry_map))
    if max_per_ind >= 999:
        out["_rank"] = out.groupby("entry_date")["score"].rank(ascending=False, method="first")
        return out[out["_rank"] <= top_n].sort_values(["entry_date", "_rank"]).copy()

    kept: list[int] = []
    ranks = pd.Series(np.nan, index=out.index, dtype=float)
    for _, grp in out.groupby("entry_date", sort=False):
        ordered = grp.sort_values(["score", "code"], ascending=[False, True], na_position="last")
        industry_counts: dict[str, int] = {}
        rank = 0
        for idx, row in ordered.iterrows():
            score = row["score"]
            if not np.isfinite(score):
                continue
            industry = str(row["_industry"])
            if industry_counts.get(industry, 0) >= max_per_ind:
                continue
            rank += 1
            ranks.loc[idx] = rank
            kept.append(idx)
            industry_counts[industry] = industry_counts.get(industry, 0) + 1
            if rank >= top_n:
                break
    selected = out.loc[kept].copy()
    selected["_rank"] = ranks.loc[kept]
    return selected.sort_values(["entry_date", "_rank"]).copy()


def _load_price_cache(codes: Iterable[str]) -> tuple[dict[str, dict[pd.Timestamp, float]], dict[str, np.ndarray]]:
    price_cache: dict[str, dict[pd.Timestamp, float]] = {}
    price_dates: dict[str, np.ndarray] = {}
    for code in sorted(set(map(_code_str, codes))):
        path = DATA_DIR / "raw_parquet" / code[:2] / f"{code}.parquet"
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path, columns=["date", "close"])
        except Exception:
            continue
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["date", "close"]).sort_values("date")
        price_cache[code] = dict(zip(df["date"], df["close"]))
        price_dates[code] = df["date"].to_numpy(dtype="datetime64[ns]")
    return price_cache, price_dates


def _close_on_or_before(
    code: str,
    date: pd.Timestamp,
    price_cache: dict[str, dict[pd.Timestamp, float]],
    price_dates: dict[str, np.ndarray],
) -> float | None:
    code = _code_str(code)
    dates = price_dates.get(code)
    if dates is None or len(dates) == 0:
        return None
    date64 = np.datetime64(pd.Timestamp(date).normalize().to_datetime64())
    idx = np.searchsorted(dates, date64, side="right") - 1
    if idx < 0:
        return None
    return price_cache.get(code, {}).get(pd.Timestamp(dates[idx]).normalize())


def signal_quality_nav_metrics(
    trades: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    top_n: int,
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> tuple[dict[str, Any], pd.DataFrame]:
    if trades.empty:
        return {}, pd.DataFrame()
    trades = trades.copy()
    trades["code"] = trades["code"].map(_code_str)
    trades["entry_date"] = pd.to_datetime(trades["entry_date"]).dt.normalize()
    trades["exit_date"] = pd.to_datetime(trades["exit_date"]).dt.normalize()
    price_cache, price_dates = _load_price_cache(trades["code"].unique())
    all_dates = [pd.Timestamp(d).normalize() for d in pd.date_range(start_date, end_date, freq="B")]
    positions: list[dict[str, Any]] = []
    daily_returns: list[float] = []
    daily_active_counts: list[int] = []

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
        daily_active_counts.append(len(positions))
        if not positions:
            daily_returns.append(0.0)
            continue
        pos_rets: list[float] = []
        for pos in positions:
            cur = _close_on_or_before(pos["code"], today, price_cache, price_dates)
            prev = _close_on_or_before(pos["code"], today - pd.Timedelta(days=1), price_cache, price_dates)
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

    cost_entry = commission_bp / 10000.0 + slippage_pct / 100.0
    cost_exit = commission_bp / 10000.0 + stamp_pct / 100.0 + slippage_pct / 100.0
    date_to_idx = {d: i for i, d in enumerate(all_dates)}
    daily_cost = np.zeros(len(daily_returns))
    for _, trade in trades.iterrows():
        ed = pd.Timestamp(trade["entry_date"]).normalize()
        xd = pd.Timestamp(trade["exit_date"]).normalize()
        if ed in date_to_idx:
            active_count = max(1, daily_active_counts[date_to_idx[ed]])
            daily_cost[date_to_idx[ed]] -= cost_entry / active_count
        if xd in date_to_idx:
            active_count = max(1, daily_active_counts[date_to_idx[xd]])
            daily_cost[date_to_idx[xd]] -= cost_exit / active_count

    r = np.array(daily_returns, dtype=float) + daily_cost
    return _nav_metrics_from_returns(r, all_dates, extra={
        "metric_surface": "signal_quality",
        "interpretation": "active selected signal average return index; costs allocated by active positions; not a cash account",
        "trades": int(len(trades)),
        "avg_active_positions": round(float(np.mean(daily_active_counts)), 6) if daily_active_counts else 0.0,
    })


def executable_portfolio_metrics(
    trades: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    *,
    target_position_pct: float,
    max_positions: int,
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> tuple[dict[str, Any], pd.DataFrame]:
    if trades.empty:
        return {}, pd.DataFrame()
    trades = trades.copy()
    trades["code"] = trades["code"].map(_code_str)
    trades["entry_date"] = pd.to_datetime(trades["entry_date"]).dt.normalize()
    trades["exit_date"] = pd.to_datetime(trades["exit_date"]).dt.normalize()
    trades = trades.sort_values(["entry_date", "score"], ascending=[True, False])

    price_cache, price_dates = _load_price_cache(trades["code"].unique())
    all_dates = [pd.Timestamp(d).normalize() for d in pd.date_range(start_date, end_date, freq="B")]
    cost_entry = commission_bp / 10000.0 + slippage_pct / 100.0
    cost_exit = commission_bp / 10000.0 + stamp_pct / 100.0 + slippage_pct / 100.0
    cash = 1.0
    positions: list[dict[str, Any]] = []
    nav_rows: list[dict[str, Any]] = []
    entered = 0
    skipped_cash = 0
    skipped_max_positions = 0

    entries = {d: g.copy() for d, g in trades.groupby("entry_date")}

    def mark_equity(today: pd.Timestamp) -> tuple[float, float]:
        pos_value = 0.0
        for pos in positions:
            close = _close_on_or_before(pos["code"], today, price_cache, price_dates)
            if close is None:
                close = pos["entry_price"]
            pos_value += float(pos["shares"]) * float(close)
        return cash + pos_value, pos_value

    for today in all_dates:
        equity_before_entries, _ = mark_equity(today - pd.Timedelta(days=1))
        todays_entries = entries.get(today)
        if todays_entries is not None:
            for _, trade in todays_entries.iterrows():
                if len(positions) >= max_positions:
                    skipped_max_positions += 1
                    continue
                target_cash = equity_before_entries * target_position_pct
                if cash + 1e-12 < target_cash or target_cash <= 0:
                    skipped_cash += 1
                    continue
                entry_price = float(trade["entry_price"])
                if entry_price <= 0:
                    skipped_cash += 1
                    continue
                shares = target_cash * (1.0 - cost_entry) / entry_price
                cash -= target_cash
                positions.append({
                    "code": _code_str(trade["code"]),
                    "entry": today,
                    "exit": trade["exit_date"],
                    "entry_price": entry_price,
                    "exit_price": float(trade["exit_price"]),
                    "shares": shares,
                })
                entered += 1

        remaining: list[dict[str, Any]] = []
        for pos in positions:
            if pd.Timestamp(pos["exit"]).normalize() <= today:
                cash += float(pos["shares"]) * float(pos["exit_price"]) * (1.0 - cost_exit)
            else:
                remaining.append(pos)
        positions = remaining

        equity, pos_value = mark_equity(today)
        nav_rows.append({
            "date": today,
            "nav": equity,
            "cash": cash,
            "positions": len(positions),
            "exposure_pct": (pos_value / equity * 100.0) if equity > 0 else 0.0,
        })

    nav_df = pd.DataFrame(nav_rows)
    r = nav_df["nav"].pct_change().fillna(nav_df["nav"] - 1.0).to_numpy(dtype=float)
    metrics, nav_df = _nav_metrics_from_returns(r, all_dates, extra={
        "metric_surface": "executable_portfolio",
        "interpretation": "cash-constrained portfolio NAV",
        "selected_trades": int(len(trades)),
        "entered_trades": int(entered),
        "skipped_cash": int(skipped_cash),
        "skipped_max_positions": int(skipped_max_positions),
        "target_position_pct": round(float(target_position_pct * 100.0), 6),
        "max_positions": int(max_positions),
        "avg_positions": round(float(nav_df["positions"].mean()), 6) if not nav_df.empty else 0.0,
        "avg_exposure_pct": round(float(nav_df["exposure_pct"].mean()), 6) if not nav_df.empty else 0.0,
        "end_open_positions": int(nav_df["positions"].iloc[-1]) if not nav_df.empty else 0,
    })
    nav_df["cash"] = [row["cash"] for row in nav_rows]
    nav_df["positions"] = [row["positions"] for row in nav_rows]
    nav_df["exposure_pct"] = [row["exposure_pct"] for row in nav_rows]
    return metrics, nav_df


def _nav_metrics_from_returns(
    returns: np.ndarray,
    dates: list[pd.Timestamp],
    extra: dict[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame]:
    r = np.asarray(returns, dtype=float)
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
    nav_df = pd.DataFrame({
        "date": dates[:len(r)],
        "nav": nav,
        "ret": r,
    })
    metrics = {
        **extra,
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


def run_window(
    df: pd.DataFrame,
    window: dict[str, Any],
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
    max_per_ind: int,
    industry_map: dict[str, str],
) -> dict[str, Any]:
    name = str(window["name"])
    test_start, requested_test_end = window["test"]
    train, purge_stats = filter_train_strict(df, tuple(window["train"]), test_start)
    test = filter_entry_window(df, tuple(window["test"]))
    if train.empty or test.empty:
        raise ValueError(f"{name}: train/test window produced empty data")
    model, scaler, train_info = train_ranker_no_validation(train, params, num_boost_round)
    scores = model.predict(scaler.transform(_feature_matrix(test, V2_FEATURES)))
    selected = select_top_n(
        test,
        scores,
        top_n,
        max_per_ind=max_per_ind,
        industry_map=industry_map,
    )
    selected_days = selected.groupby("entry_date").size() if not selected.empty else pd.Series(dtype=int)
    candidate_days = int(test["entry_date"].nunique())
    below_top_n_days = (
        int((selected_days < top_n).sum())
        + int(candidate_days - selected_days.index.nunique())
    )

    start_ts = pd.Timestamp(test_start)
    requested_end_ts = pd.Timestamp(requested_test_end)
    effective_end_ts = min(requested_end_ts, test["entry_date"].max())
    prefix = output_dir / name
    selected.to_csv(prefix.with_suffix(".trades.csv"), index=False, encoding="gbk")

    signal_quality, sq_nav = signal_quality_nav_metrics(
        selected,
        start_ts,
        effective_end_ts,
        top_n,
        commission_bp,
        stamp_pct,
        slippage_pct,
    )
    sq_nav.to_csv(output_dir / f"{name}.signal_quality.nav.csv", index=False, encoding="gbk")

    executable, ex_nav = executable_portfolio_metrics(
        selected,
        start_ts,
        effective_end_ts,
        target_position_pct=target_position_pct,
        max_positions=max_positions,
        commission_bp=commission_bp,
        stamp_pct=stamp_pct,
        slippage_pct=slippage_pct,
    )
    ex_nav.to_csv(output_dir / f"{name}.executable_portfolio.nav.csv", index=False, encoding="gbk")

    importance = pd.DataFrame({
        "feature": V2_FEATURES,
        "importance_gain": model.feature_importance(importance_type="gain"),
        "importance_split": model.feature_importance(importance_type="split"),
    }).sort_values("importance_gain", ascending=False)
    importance.to_csv(output_dir / f"{name}_feature_importance.csv", index=False)

    return {
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
        "train_info": {**train_info, **purge_stats},
        "trade": trade_metrics(selected),
        "signal_quality": signal_quality,
        "executable_portfolio": executable,
        "selection_constraint": {
            "top_n": int(top_n),
            "max_per_ind": int(max_per_ind),
            "industry_constraint_applied": bool(max_per_ind < 999),
            "entry_days_below_top_n": int(below_top_n_days),
            "selected_rows": int(len(selected)),
            "candidate_rows": int(len(test)),
        },
        "top_importance": importance.head(15).to_dict(orient="records"),
    }


def summarize_windows(windows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    rows = []
    for item in windows:
        metrics = item.get(key, {})
        trade = item.get("trade", {})
        rows.append({
            "name": item["name"],
            "cagr_pct": metrics.get("cagr_pct", 0.0),
            "cum_return_pct": metrics.get("cum_return_pct", 0.0),
            "max_dd_pct": metrics.get("max_dd_pct", 0.0),
            "sharpe": metrics.get("sharpe", 0.0),
            "calmar": metrics.get("calmar", 0.0),
            "trades": trade.get("trades", 0),
            "win_rate_pct": trade.get("win_rate_pct", 0.0),
            "avg_return_pct": trade.get("avg_return_pct", 0.0),
            "entered_trades": metrics.get("entered_trades", metrics.get("trades", 0)),
        })
    frame = pd.DataFrame(rows)
    if frame.empty:
        return {}
    return {
        "windows": int(len(frame)),
        "avg_cagr_pct": round(float(frame["cagr_pct"].mean()), 6),
        "worst_cagr_pct": round(float(frame["cagr_pct"].min()), 6),
        "avg_sharpe": round(float(frame["sharpe"].mean()), 6),
        "worst_sharpe": round(float(frame["sharpe"].min()), 6),
        "avg_max_dd_pct": round(float(frame["max_dd_pct"].mean()), 6),
        "worst_max_dd_pct": round(float(frame["max_dd_pct"].min()), 6),
        "positive_cagr_pass_rate": round(float((frame["cagr_pct"] > 0).mean()), 6),
        "positive_sharpe_pass_rate": round(float((frame["sharpe"] > 0).mean()), 6),
        "avg_trade_win_rate_pct": round(float(frame["win_rate_pct"].mean()), 6),
        "avg_trade_return_pct": round(float(frame["avg_return_pct"].mean()), 6),
        "avg_entered_trades": round(float(frame["entered_trades"].mean()), 6),
    }


def write_report(result: dict[str, Any], path: Path) -> None:
    lines = [
        "# Brick V2 Rebuilt Dual Metrics",
        "",
        f"Created: {result['created_at']}",
        "",
        "## Boundary",
        "",
        f"- Candidate source: `{result['candidate_rebuild']['source']}`; legacy signals_raw files are not read.",
        f"- Date split column: `{result['date_split_column']}`",
        f"- Train label purge: `{result['train_label_purge']}`",
        f"- TopN: `{result['top_n']}`",
        f"- Max per industry: `{result['selection_constraint']['max_per_ind']}`",
        f"- Entry MA source: `{result['candidate_rebuild']['entry_ma_source']}`",
        f"- Market timing enabled: `{result['candidate_rebuild']['use_market_timing']}`",
        "- Signal quality is an active-signal index, not a cash account.",
        "- Executable portfolio uses fixed cash allocation, max positions, and cash constraints.",
        "",
        "## Fixed Windows",
        "",
        "| Window | Trades | WR | AvgRet | SQ CAGR | SQ MaxDD | EX CAGR | EX MaxDD | EX Entered |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in result["fixed_windows"]:
        trade = item["trade"]
        sq = item["signal_quality"]
        ex = item["executable_portfolio"]
        lines.append(
            "| {name} | {trades} | {wr:.2f}% | {avg:.3f}% | {sq_cagr:.2f}% | {sq_mdd:.2f}% | {ex_cagr:.2f}% | {ex_mdd:.2f}% | {entered} |".format(
                name=item["name"],
                trades=trade.get("trades", 0),
                wr=trade.get("win_rate_pct", 0.0),
                avg=trade.get("avg_return_pct", 0.0),
                sq_cagr=sq.get("cagr_pct", 0.0),
                sq_mdd=sq.get("max_dd_pct", 0.0),
                ex_cagr=ex.get("cagr_pct", 0.0),
                ex_mdd=ex.get("max_dd_pct", 0.0),
                entered=ex.get("entered_trades", 0),
            )
        )
    lines.extend([
        "",
        "## Rolling Windows",
        "",
        "| Window | Trades | WR | AvgRet | SQ CAGR | SQ MaxDD | EX CAGR | EX MaxDD | EX Entered |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for item in result["rolling_windows"]:
        trade = item["trade"]
        sq = item["signal_quality"]
        ex = item["executable_portfolio"]
        lines.append(
            "| {name} | {trades} | {wr:.2f}% | {avg:.3f}% | {sq_cagr:.2f}% | {sq_mdd:.2f}% | {ex_cagr:.2f}% | {ex_mdd:.2f}% | {entered} |".format(
                name=item["name"],
                trades=trade.get("trades", 0),
                wr=trade.get("win_rate_pct", 0.0),
                avg=trade.get("avg_return_pct", 0.0),
                sq_cagr=sq.get("cagr_pct", 0.0),
                sq_mdd=sq.get("max_dd_pct", 0.0),
                ex_cagr=ex.get("cagr_pct", 0.0),
                ex_mdd=ex.get("max_dd_pct", 0.0),
                entered=ex.get("entered_trades", 0),
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
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    status_path = out_dir / "status.json"

    if args.candidate_path:
        candidates, candidate_stats = load_rebuilt_candidates(Path(args.candidate_path))
        candidate_stats.update({
            "requested_start": args.start,
            "requested_end": args.end,
            "entry_ma_source": args.entry_ma_source,
            "use_market_timing": False,
            "use_market_timing_argument_ignored_for_provided_candidates": bool(args.use_market_timing),
        })
    else:
        candidates, candidate_stats = rebuild_candidates(
            start_date=args.start,
            end_date=args.end,
            entry_ma_source=args.entry_ma_source,
            use_market_timing=args.use_market_timing,
            workers=args.workers,
            output_dir=out_dir,
        )

    gpu_capability = detect_nvidia_gpu()
    acceleration = build_compute_acceleration_plan("ranker_training", gpu_capability)
    use_gpu = False
    gpu_probe_error = None
    if args.prefer_gpu and gpu_capability.available:
        use_gpu, gpu_probe_error = _probe_lightgbm_gpu()
    acceleration["lightgbm_gpu_probe"] = {"usable": bool(use_gpu), "error": gpu_probe_error}
    acceleration["selected_backend"] = "lightgbm_gpu" if use_gpu else "cpu"
    params = build_lgb_params(use_gpu=use_gpu, num_threads=args.threads)
    industry_map = load_industry_map(Path(args.industry_map_path), max_per_ind=args.max_per_ind)

    fixed_source = FIXED_WINDOWS if args.window_scope in {"all", "fixed"} else []
    rolling_source = ROLLING_WINDOWS if args.window_scope in {"all", "rolling"} else []
    if args.max_windows > 0:
        fixed_source = fixed_source[: args.max_windows]
        rolling_source = rolling_source[: args.max_windows]

    fixed = []
    total_windows = len(fixed_source) + len(rolling_source)
    completed_windows = 0
    for window in fixed_source:
        status_path.write_text(json.dumps({
            "status": "running",
            "phase": "fixed",
            "completed_windows": completed_windows,
            "total_windows": total_windows,
            "current_window": window["name"],
            "top_n": int(args.top_n),
            "max_per_ind": int(args.max_per_ind),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        fixed.append(run_window(
            candidates,
            window,
            output_dir=out_dir,
            params=params,
            num_boost_round=args.num_boost_round,
            top_n=args.top_n,
            target_position_pct=args.target_position_pct,
            max_positions=args.max_positions,
            commission_bp=args.commission,
            stamp_pct=args.stamp,
            slippage_pct=args.slippage,
            max_per_ind=args.max_per_ind,
            industry_map=industry_map,
        ))
        completed_windows += 1

    rolling = []
    for window in rolling_source:
        status_path.write_text(json.dumps({
            "status": "running",
            "phase": "rolling",
            "completed_windows": completed_windows,
            "total_windows": total_windows,
            "current_window": window["name"],
            "top_n": int(args.top_n),
            "max_per_ind": int(args.max_per_ind),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        rolling.append(run_window(
            candidates,
            window,
            output_dir=out_dir,
            params=params,
            num_boost_round=args.num_boost_round,
            top_n=args.top_n,
            target_position_pct=args.target_position_pct,
            max_positions=args.max_positions,
            commission_bp=args.commission,
            stamp_pct=args.stamp,
            slippage_pct=args.slippage,
            max_per_ind=args.max_per_ind,
            industry_map=industry_map,
        ))
        completed_windows += 1

    result = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "date_split_column": "entry_date",
        "train_label_purge": "train exit_date must be before each test_start",
        "top_n": int(args.top_n),
        "num_boost_round": int(args.num_boost_round),
        "candidate_rebuild": candidate_stats,
        "feature_availability_boundary": {
            "split_date": "entry_date",
            "reason": "V2 uses next-open features available at 09:25 before selection.",
            "model_input_columns": V2_FEATURES,
            "entry_open_feature_formula": (
                "overnight_gap_pct uses entry_date open versus signal_date close; "
                "entry_open_to_yellow_pct and entry_open_to_ma5_pct use entry_date open "
                "versus signal-day yellow/MA5, matching daily_select.py."
            ),
            "entry_day_allowed_features": [
                "overnight_gap_pct",
                "entry_open_to_yellow_pct",
                "entry_open_to_ma5_pct",
            ],
            "label_or_evaluation_only_columns": [
                "return_pct",
                "exit_date",
                "exit_price",
                "hold_days",
            ],
            "forbidden_entry_day_information": "No entry-day high/low/close/intraday future fields are passed to the model.",
        },
        "executable_portfolio_config": {
            "target_position_pct": float(args.target_position_pct),
            "max_positions": int(args.max_positions),
            "commission_bp": float(args.commission),
            "stamp_pct": float(args.stamp),
            "slippage_pct": float(args.slippage),
        },
        "selection_constraint": {
            "top_n": int(args.top_n),
            "max_per_ind": int(args.max_per_ind),
            "industry_constraint_applied": bool(args.max_per_ind < 999),
            "industry_map_path": str(Path(args.industry_map_path).resolve()) if args.max_per_ind < 999 else None,
        },
        "compute_acceleration": acceleration,
        "features": V2_FEATURES,
        "fixed_windows": fixed,
        "rolling_windows": rolling,
        "summary": {
            "fixed_signal_quality": summarize_windows(fixed, "signal_quality"),
            "fixed_executable_portfolio": summarize_windows(fixed, "executable_portfolio"),
            "rolling_signal_quality": summarize_windows(rolling, "signal_quality"),
            "rolling_executable_portfolio": summarize_windows(rolling, "executable_portfolio"),
        },
    }
    results_path = out_dir / "brick_v2_rebuilt_dual_metrics_results.json"
    results_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(result, out_dir / "brick_v2_rebuilt_dual_metrics_report.md")
    status_path.write_text(json.dumps({
        "status": "complete",
        "completed_windows": completed_windows,
        "total_windows": total_windows,
        "top_n": int(args.top_n),
        "max_per_ind": int(args.max_per_ind),
        "result_path": str(results_path.resolve()),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick V2 rebuilt-candidate dual metrics")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--candidate-path", default="")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--entry-ma-source", default="t0", choices=["t0", "t1_open"])
    parser.add_argument("--use-market-timing", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--max-per-ind", type=int, default=999)
    parser.add_argument("--industry-map-path", default=str(DEFAULT_INDUSTRY_MAP_PATH))
    parser.add_argument("--window-scope", default="all", choices=["all", "fixed", "rolling"])
    parser.add_argument("--max-windows", type=int, default=0)
    parser.add_argument("--num-boost-round", type=int, default=300)
    parser.add_argument("--target-position-pct", type=float, default=0.10)
    parser.add_argument("--max-positions", type=int, default=10)
    parser.add_argument("--commission", type=float, default=3.0)
    parser.add_argument("--stamp", type=float, default=0.05)
    parser.add_argument("--slippage", type=float, default=0.1)
    parser.add_argument("--workers", type=int, default=max(1, mp.cpu_count() - 1))
    parser.add_argument("--threads", type=int, default=max(1, min(mp.cpu_count() - 1, 8)))
    parser.add_argument("--prefer-gpu", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run(args)
    print(json.dumps({
        "output_dir": str(Path(args.output_dir).resolve()),
        "candidate_rows": result["candidate_rebuild"]["rows"],
        "summary": result["summary"],
        "compute_acceleration": result["compute_acceleration"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
