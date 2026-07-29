"""Brick sector-relative factor strict rolling-forward validation.

Research-only runner for the AG2-KBase sector-relative factor batch. The
features use daily OHLCV windows ending at the signal day and static THS block
membership as a research proxy for sector classification. Labels and realized
trade outcomes are used only for training and evaluation.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(RESEARCH_DIR))

try:
    from scipy import stats
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(f"Missing required package: {exc}") from exc

from research_automation.gpu_acceleration import (
    build_compute_acceleration_plan,
    detect_nvidia_gpu,
)

from brick_erd_phase6 import (
    DATA_DIR,
    DEFAULT_CACHE_NAME,
    DEFAULT_SIGNAL_PATH,
    FOLDS,
    V2_FEATURES,
    _code_str,
    _probe_lightgbm_gpu,
    build_lgb_params,
    compute_rank_ic,
    compute_residual_rank_ic,
    evaluate_model,
    load_signals,
    score_frame,
    split_fold,
    train_ranker,
)


DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / "sector_relative_phase6"
DEFAULT_SECTOR_MAP_PATH = DATA_DIR / "block" / "ths_industry_map.json"

RAW_METRICS = [
    "raw_volume_trend_5_20",
    "raw_path_return_10d",
    "raw_rsi_14d",
    "raw_distance_from_20d_high",
    "raw_turnover_trend_10_30",
]

SECTOR_RELATIVE_FEATURES = [
    "sector_relative_volume_contraction_5d",
    "sector_relative_path_steepness_10d",
    "sector_relative_rsi_14d",
    "sector_relative_distance_from_20d_high",
    "sector_relative_turnover_trend_10d",
    "sector_relative_composite_score",
]

FEATURE_RANK_SPECS = {
    "sector_relative_volume_contraction_5d": ("raw_volume_trend_5_20", True),
    "sector_relative_path_steepness_10d": ("raw_path_return_10d", False),
    "sector_relative_rsi_14d": ("raw_rsi_14d", True),
    "sector_relative_distance_from_20d_high": ("raw_distance_from_20d_high", False),
    "sector_relative_turnover_trend_10d": ("raw_turnover_trend_10_30", True),
}

V2_VOLUME_TURNOVER_FEATURES = [
    "obv_trend_up",
    "vol_ratio_5",
    "turnover_ratio_5",
    "vol_ratio_20",
]

V2_RETURN_PATH_FEATURES = [
    "ret_5d",
    "ret_10d",
    "bullish_ratio_5d",
    "bullish_ratio_10d",
    "new_high_20d",
]

_WANTED_DATES: set[str] = set()


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    den = denominator.replace(0, np.nan)
    return (numerator / den).replace([np.inf, -np.inf], np.nan)


def _calc_rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0).rolling(window, min_periods=window).mean()
    loss = (-delta.clip(upper=0.0)).rolling(window, min_periods=window).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100.0 - 100.0 / (1.0 + rs)
    rsi = rsi.mask((loss == 0) & (gain > 0), 100.0)
    rsi = rsi.mask((loss == 0) & (gain == 0), 50.0)
    return rsi


def _init_metric_worker(wanted_dates: list[str]) -> None:
    global _WANTED_DATES
    _WANTED_DATES = set(wanted_dates)


def _all_stock_metric_worker(args: tuple[str, str]) -> pd.DataFrame:
    code, cache_dir_text = args
    path = Path(cache_dir_text) / f"{code}.parquet"
    if not path.exists() or not _WANTED_DATES:
        return pd.DataFrame()
    try:
        df = pd.read_parquet(
            path,
            columns=["date", "close", "high", "volume", "turnover"],
        )
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    df = df.sort_values("date").reset_index(drop=True)
    df["signal_date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce")
    turnover = pd.to_numeric(df["turnover"], errors="coerce")

    df["raw_volume_trend_5_20"] = _safe_ratio(
        volume.rolling(5, min_periods=5).mean(),
        volume.rolling(20, min_periods=20).mean(),
    )
    df["raw_path_return_10d"] = _safe_ratio(close, close.shift(9)) - 1.0
    df["raw_rsi_14d"] = _calc_rsi(close, 14)
    df["raw_distance_from_20d_high"] = _safe_ratio(
        close,
        high.rolling(20, min_periods=20).max(),
    ) - 1.0
    df["raw_turnover_trend_10_30"] = _safe_ratio(
        turnover.rolling(10, min_periods=10).mean(),
        turnover.rolling(30, min_periods=30).mean(),
    )

    mask = df["signal_date"].isin(_WANTED_DATES)
    cols = ["signal_date", *RAW_METRICS]
    out = df.loc[mask, cols].copy()
    if out.empty:
        return pd.DataFrame()
    out.insert(0, "code", code)
    return out


def load_sector_assignments(
    path: Path,
    min_block_size: int,
    max_block_size: int,
    max_blocks_per_stock: int,
) -> tuple[pd.DataFrame, dict]:
    if not path.exists():
        raise FileNotFoundError(f"sector map not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    stocks = payload.get("stocks", {})
    block_names = payload.get("block_names", {})
    sizes: Counter[str] = Counter()
    for blocks in stocks.values():
        sizes.update(map(str, blocks))

    rows = []
    selected_blocks: Counter[str] = Counter()
    eligible_stock_count = 0
    for raw_code, blocks in stocks.items():
        code = _code_str(raw_code)
        eligible = [
            str(block)
            for block in blocks
            if min_block_size <= sizes[str(block)] <= max_block_size
        ]
        if eligible:
            eligible_stock_count += 1
        for order, block in enumerate(eligible[:max(1, max_blocks_per_stock)], start=1):
            rows.append({
                "code": code,
                "sector_proxy": block,
                "sector_order": order,
                "sector_proxy_size": int(sizes[block]),
            })
            selected_blocks[block] += 1

    assignments = pd.DataFrame(rows)
    if assignments.empty:
        raise ValueError("no THS block assignments survived the block-size filter")
    stats_block = {
        "sector_map_path": str(path.resolve()),
        "sector_proxy": "ths_block_multilabel_proxy",
        "source_block_count": int(len(block_names)),
        "source_stock_count": int(len(stocks)),
        "eligible_stock_count": int(eligible_stock_count),
        "assignment_rows": int(len(assignments)),
        "unique_selected_blocks": int(assignments["sector_proxy"].nunique()),
        "min_block_size": int(min_block_size),
        "max_block_size": int(max_block_size),
        "max_blocks_per_stock": int(max_blocks_per_stock),
        "top_selected_blocks": [
            {
                "block_id": block,
                "block_name": block_names.get(block, ""),
                "assignment_count": int(count),
                "source_member_count": int(sizes[block]),
            }
            for block, count in selected_blocks.most_common(20)
        ],
    }
    return assignments, stats_block


def build_all_stock_metric_panel(
    cache_dir: Path,
    wanted_dates: list[str],
    workers: int,
) -> tuple[pd.DataFrame, dict]:
    files = sorted(cache_dir.glob("*.parquet"))
    tasks = [(_code_str(path.stem), str(cache_dir)) for path in files]
    frames: list[pd.DataFrame] = []
    with mp.Pool(
        max(1, workers),
        initializer=_init_metric_worker,
        initargs=(wanted_dates,),
    ) as pool:
        for frame in pool.imap_unordered(_all_stock_metric_worker, tasks, chunksize=20):
            if not frame.empty:
                frames.append(frame)
    if not frames:
        raise ValueError(f"no all-stock metric rows produced from {cache_dir}")
    panel = pd.concat(frames, ignore_index=True)
    panel["signal_date"] = pd.to_datetime(panel["signal_date"], errors="coerce").dt.normalize()
    panel = panel.dropna(subset=["signal_date"]).reset_index(drop=True)
    stats_block = {
        "cache_files_scanned": int(len(files)),
        "metric_rows": int(len(panel)),
        "unique_metric_codes": int(panel["code"].nunique()),
        "unique_metric_dates": int(panel["signal_date"].nunique()),
        "metric_date_start": panel["signal_date"].min().strftime("%Y-%m-%d"),
        "metric_date_end": panel["signal_date"].max().strftime("%Y-%m-%d"),
        "rank_universe": "all cache stocks on Brick signal dates",
    }
    return panel, stats_block


def compute_sector_relative_panel(
    metrics: pd.DataFrame,
    assignments: pd.DataFrame,
    min_group_size: int,
) -> tuple[pd.DataFrame, dict]:
    exploded = metrics.merge(assignments, on="code", how="left")
    missing = exploded["sector_proxy"].isna()
    missing_rows = int(missing.sum())
    exploded.loc[missing, "sector_proxy"] = "UNKNOWN"
    exploded.loc[missing, "sector_order"] = 0
    exploded.loc[missing, "sector_proxy_size"] = 0

    group_cols = ["signal_date", "sector_proxy"]
    exploded["sector_group_size"] = exploded.groupby(group_cols)["code"].transform("nunique")
    for feature, (raw, inverse) in FEATURE_RANK_SPECS.items():
        count = exploded.groupby(group_cols)[raw].transform("count")
        rank = exploded.groupby(group_cols)[raw].rank(method="average", pct=True)
        value = 1.0 - rank if inverse else rank
        value = value.where((count >= min_group_size) & np.isfinite(value), 0.5)
        exploded[feature] = value.fillna(0.5)

    z_cols = []
    for feature in FEATURE_RANK_SPECS:
        mean = exploded.groupby(group_cols)[feature].transform("mean")
        std = exploded.groupby(group_cols)[feature].transform("std")
        z_name = f"_z_{feature}"
        exploded[z_name] = ((exploded[feature] - mean) / std).where(std > 1e-12, 0.0)
        exploded[z_name] = exploded[z_name].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        z_cols.append(z_name)
    exploded["sector_relative_composite_score"] = exploded[z_cols].mean(axis=1)

    aggregations = {feature: "mean" for feature in SECTOR_RELATIVE_FEATURES}
    aggregations.update({
        "sector_proxy": "nunique",
        "sector_group_size": ["min", "median", "max"],
    })
    panel = exploded.groupby(["code", "signal_date"], as_index=False).agg(aggregations)
    panel.columns = [
        "_".join(col).strip("_") if isinstance(col, tuple) else col
        for col in panel.columns.to_flat_index()
    ]
    rename_map = {f"{feature}_mean": feature for feature in SECTOR_RELATIVE_FEATURES}
    rename_map.update({
        "sector_proxy_nunique": "sector_context_count",
        "sector_group_size_min": "sector_group_size_min",
        "sector_group_size_median": "sector_group_size_median",
        "sector_group_size_max": "sector_group_size_max",
    })
    panel = panel.rename(columns=rename_map)

    group_sizes = exploded[["signal_date", "sector_proxy", "sector_group_size"]].drop_duplicates()
    stats_block = {
        "exploded_sector_rows": int(len(exploded)),
        "missing_sector_assignment_rows": int(missing_rows),
        "feature_rows": int(len(panel)),
        "min_group_size": int(min_group_size),
        "sector_group_size_mean": round(float(group_sizes["sector_group_size"].mean()), 6),
        "sector_group_size_median": round(float(group_sizes["sector_group_size"].median()), 6),
        "sector_group_size_p10": round(float(group_sizes["sector_group_size"].quantile(0.10)), 6),
        "sector_group_size_p90": round(float(group_sizes["sector_group_size"].quantile(0.90)), 6),
    }
    return panel, stats_block


def enrich_with_sector_relative_features(
    signals: pd.DataFrame,
    cache_dir: Path,
    sector_map_path: Path,
    workers: int,
    min_block_size: int,
    max_block_size: int,
    max_blocks_per_stock: int,
    min_group_size: int,
) -> tuple[pd.DataFrame, dict]:
    wanted_dates = sorted(set(signals["signal_date"].dt.strftime("%Y-%m-%d")))
    assignments, assignment_stats = load_sector_assignments(
        sector_map_path,
        min_block_size,
        max_block_size,
        max_blocks_per_stock,
    )
    metrics, metric_stats = build_all_stock_metric_panel(cache_dir, wanted_dates, workers)
    relative, rank_stats = compute_sector_relative_panel(metrics, assignments, min_group_size)

    merged = signals.merge(relative, on=["code", "signal_date"], how="left", validate="many_to_one")
    before = len(merged)
    merged = merged.dropna(subset=SECTOR_RELATIVE_FEATURES).copy()
    join_stats = {
        "signal_rows_before_cache_join": int(before),
        "signal_rows_after_cache_join": int(len(merged)),
        "cache_join_drop_rows": int(before - len(merged)),
        "cache_dir": str(cache_dir.resolve()),
        "sector_assignments": assignment_stats,
        "all_stock_metrics": metric_stats,
        "sector_rank": rank_stats,
    }
    return merged.reset_index(drop=True), join_stats


def _max_abs_spearman(df: pd.DataFrame, factor: str, controls: list[str]) -> dict:
    rows = []
    for control in controls:
        if control not in df.columns:
            continue
        valid = df[[factor, control]].replace([np.inf, -np.inf], np.nan).dropna()
        if len(valid) < 100:
            continue
        if valid[factor].std() <= 1e-12 or valid[control].std() <= 1e-12:
            continue
        corr = stats.spearmanr(valid[factor], valid[control]).correlation
        if np.isfinite(corr):
            rows.append((control, float(corr)))
    if not rows:
        return {"feature": None, "spearman": 0.0, "abs_spearman": 0.0}
    control, corr = max(rows, key=lambda item: abs(item[1]))
    return {
        "feature": control,
        "spearman": round(float(corr), 6),
        "abs_spearman": round(float(abs(corr)), 6),
    }


def build_falsification_checks(train_probe: pd.DataFrame) -> dict:
    checks = {}
    for factor in SECTOR_RELATIVE_FEATURES:
        checks[factor] = {
            "max_corr_vs_v2_volume_turnover": _max_abs_spearman(
                train_probe,
                factor,
                V2_VOLUME_TURNOVER_FEATURES,
            ),
            "max_corr_vs_v2_return_path": _max_abs_spearman(
                train_probe,
                factor,
                V2_RETURN_PATH_FEATURES,
            ),
            "failure_threshold_abs_spearman": 0.75,
        }
    return checks


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
    if {"v2_baseline", "v2_plus_sector_relative"}.issubset(summary):
        pivot = df.pivot(index="fold", columns="model", values=["cagr_pct", "sharpe", "max_dd_pct"])
        deltas = {}
        for metric in ["cagr_pct", "sharpe", "max_dd_pct"]:
            diff = pivot[(metric, "v2_plus_sector_relative")] - pivot[(metric, "v2_baseline")]
            deltas[metric] = {
                "avg_delta": round(float(diff.mean()), 6),
                "worst_delta": round(float(diff.min()), 6),
                "positive_delta_pass_rate": round(float((diff > 0).mean()), 6),
            }
        summary["v2_plus_sector_relative_minus_baseline"] = deltas
    return summary


def write_markdown_report(result: dict, path: Path) -> None:
    lines = [
        "# Brick Sector-Relative Phase 6 Strict Forward Validation",
        "",
        f"Created: {result['created_at']}",
        "",
        "## Boundary",
        "",
        f"- Handoff: `{result['data_boundary']['handoff_path']}`",
        f"- Signal file: `{result['data_boundary']['signal_path']}`",
        f"- Indicator cache: `{result['data_boundary']['indicator_cache']}`",
        f"- Signal data: {result['data_boundary']['signal_data_start']} to {result['data_boundary']['signal_data_end']}",
        "- Factor window: daily OHLCV lookbacks ending at signal day only.",
        "- Sector field: THS static multi-label block proxy, not a clean Shenwan primary industry field.",
        f"- Rank universe: {result['cache_join']['all_stock_metrics']['rank_universe']}",
        f"- Embargo days: {result['strict_forward_validation']['embargo_days']}",
        "",
        "## Compute",
        "",
        f"- Backend: `{result['compute_acceleration']['selected_backend']}`",
        f"- GPU available: `{result['compute_acceleration']['gpu_available']}`",
        f"- LightGBM GPU probe: `{result['compute_acceleration']['lightgbm_gpu_probe']}`",
        "",
        "## Sector-Relative Construction",
        "",
        f"- Cache joined rows: {result['cache_join']['signal_rows_after_cache_join']} / {result['cache_join']['signal_rows_before_cache_join']}",
        f"- Metric rows: {result['cache_join']['all_stock_metrics']['metric_rows']}",
        f"- Exploded sector rows: {result['cache_join']['sector_rank']['exploded_sector_rows']}",
        f"- Unique selected THS blocks: {result['cache_join']['sector_assignments']['unique_selected_blocks']}",
        f"- Min sector group size: {result['cache_join']['sector_rank']['min_group_size']}",
        "- Path and 20d-high polarity follow the approved handoff expression exactly.",
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
        "## Falsification Checks",
        "",
        "| Factor | Max abs corr vs V2 volume/turnover | Max abs corr vs V2 return path |",
        "| --- | ---: | ---: |",
    ])
    for factor, block in result["falsification_checks_train_2020_2022"].items():
        volume = block["max_corr_vs_v2_volume_turnover"]
        path_block = block["max_corr_vs_v2_return_path"]
        lines.append(
            "| {factor} | {v:.4f} ({vf}) | {p:.4f} ({pf}) |".format(
                factor=factor,
                v=volume["abs_spearman"],
                vf=volume["feature"],
                p=path_block["abs_spearman"],
                pf=path_block["feature"],
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
        "A factor is not promotion-valid unless V2+SectorRelative improves average test performance, preserves acceptable worst-fold behavior, and remains orthogonal enough to the V2 feature family.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


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
    panel, join_stats = enrich_with_sector_relative_features(
        signals=signals,
        cache_dir=cache_dir,
        sector_map_path=Path(args.sector_map_path),
        workers=args.workers,
        min_block_size=args.min_block_size,
        max_block_size=args.max_block_size,
        max_blocks_per_stock=args.max_blocks_per_stock,
        min_group_size=args.min_group_size,
    )
    panel_path = out_dir / "brick_sector_relative_factor_panel.parquet"
    panel.to_parquet(panel_path, index=False)

    train_probe = panel[
        (panel["signal_date"] >= pd.Timestamp("2020-01-01"))
        & (panel["signal_date"] <= pd.Timestamp("2022-12-31"))
    ].copy()
    orthogonality = {}
    for factor in SECTOR_RELATIVE_FEATURES:
        orthogonality[factor] = {
            "rank_ic": compute_rank_ic(train_probe, factor),
            **compute_residual_rank_ic(train_probe, factor, V2_FEATURES),
        }
    falsification_checks = build_falsification_checks(train_probe)

    params = build_lgb_params(use_gpu=use_gpu, num_threads=args.threads)
    model_specs = {
        "v2_baseline": V2_FEATURES,
        "v2_plus_sector_relative": [*V2_FEATURES, *SECTOR_RELATIVE_FEATURES],
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
        sr = fold_block["models"]["v2_plus_sector_relative"]["test_metrics"]["account"]
        fold_block["test_delta_v2_plus_sector_relative_minus_baseline"] = {
            key: round(float(sr.get(key, 0.0) - base.get(key, 0.0)), 6)
            for key in ["cagr_pct", "cum_return_pct", "max_dd_pct", "sharpe", "calmar"]
        }
        fold_results.append(fold_block)

    result = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "strict_forward_validation": {
            "folds": FOLDS,
            "embargo_days": args.embargo_days,
            "test_years_unseen_by_fold_model": True,
            "purge_rule": "train exit_date < validation_start; validation exit_date < test_start; signal embargo before boundaries",
        },
        "data_boundary": {
            "handoff_path": str(Path(args.handoff_path).resolve()) if args.handoff_path else "",
            "signal_path": str(Path(args.signal_path).resolve()),
            "indicator_cache": str(cache_dir.resolve()),
            "factor_panel": str(panel_path.resolve()),
            "signal_data_start": panel["signal_date"].min().strftime("%Y-%m-%d"),
            "signal_data_end": panel["signal_date"].max().strftime("%Y-%m-%d"),
            "factor_window_rule": "all OHLCV windows end at signal_date; no post-signal bars",
            "sector_classification_proxy": "static THS multi-label block map",
            "daily_only": True,
            "uses_l2_tick_orderbook_minute_auction": False,
        },
        "compute_acceleration": acceleration,
        "cache_join": join_stats,
        "factor_features": SECTOR_RELATIVE_FEATURES,
        "factor_polarity_source": "approved AG2 handoff expressions",
        "orthogonality_train_2020_2022": orthogonality,
        "falsification_checks_train_2020_2022": falsification_checks,
        "fold_results": fold_results,
        "summary": summarize_results(fold_results),
    }
    metrics_path = out_dir / "brick_sector_relative_phase6_results.json"
    metrics_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown_report(result, out_dir / "brick_sector_relative_phase6_report.md")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick sector-relative strict Phase 6 validation")
    parser.add_argument("--signal-path", default=str(DEFAULT_SIGNAL_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--cache-name", default=DEFAULT_CACHE_NAME)
    parser.add_argument("--sector-map-path", default=str(DEFAULT_SECTOR_MAP_PATH))
    parser.add_argument("--handoff-path", default="")
    parser.add_argument("--allow-production-cache", action="store_true")
    parser.add_argument("--workers", type=int, default=max(1, min(mp.cpu_count() - 1, 8)))
    parser.add_argument("--threads", type=int, default=max(1, min(mp.cpu_count() - 1, 8)))
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--embargo-days", type=int, default=20)
    parser.add_argument("--num-boost-round", type=int, default=500)
    parser.add_argument("--min-block-size", type=int, default=20)
    parser.add_argument("--max-block-size", type=int, default=600)
    parser.add_argument("--max-blocks-per-stock", type=int, default=3)
    parser.add_argument("--min-group-size", type=int, default=5)
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
