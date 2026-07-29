"""Brick Path-Volume Interaction strict rolling-forward validation.

Research-only runner for the AG2-KBase Path-Volume Interaction candidate.
The factor uses daily OHLCV windows ending at the Brick signal day only.
It never uses post-signal bars, realized trade outcomes, exit dates, or labels
as features. Labels remain available only for training/evaluation.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT))

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
    _date_str,
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


DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / "path_volume_phase6"

PATH_VOLUME_FEATURES = [
    "pv_wbottom_presence_5d",
    "pv_wbottom_volume_contraction_5d",
    "pv_wbottom_time_contraction_5d",
    "pv_wbottom_retest_quality_5d",
    "pv_wbottom_score_5d",
    "pv_wbottom_presence_10d",
    "pv_wbottom_volume_contraction_10d",
    "pv_wbottom_time_contraction_10d",
    "pv_wbottom_retest_quality_10d",
    "pv_wbottom_score_10d",
    "pv_path_volume_exhaustion_score",
]


def _safe_log_ratio(numerator: float, denominator: float) -> float:
    if not np.isfinite(numerator) or not np.isfinite(denominator):
        return 0.0
    return float(np.log((max(numerator, 0.0) + 1.0) / (max(denominator, 0.0) + 1.0)))


def _down_runs(close: np.ndarray, min_down_pct: float = 0.001) -> list[tuple[int, int]]:
    """Return contiguous down runs in local close array.

    A run is expressed in return-index coordinates: run (s, e) uses returns
    close[s+1] / close[s] - 1 through close[e+1] / close[e] - 1.
    """
    if len(close) < 3:
        return []
    ret = np.diff(close) / np.where(close[:-1] != 0, close[:-1], np.nan)
    down = np.isfinite(ret) & (ret < -abs(min_down_pct))
    runs: list[tuple[int, int]] = []
    i = 0
    while i < len(down):
        if not down[i]:
            i += 1
            continue
        start = i
        while i + 1 < len(down) and down[i + 1]:
            i += 1
        runs.append((start, i))
        i += 1
    return runs


def _leg_stats(
    close: np.ndarray,
    low: np.ndarray,
    volume: np.ndarray,
    leg: tuple[int, int],
) -> dict:
    start, end = leg
    end_bar = min(end + 1, len(close) - 1)
    vol_slice = volume[start + 1:end_bar + 1]
    length = max(1, end - start + 1)
    price_drop = float(close[start] - close[end_bar]) if np.isfinite(close[start]) else 0.0
    low_at_end = float(low[end_bar]) if np.isfinite(low[end_bar]) else np.nan
    return {
        "length": float(length),
        "volume_sum": float(np.nansum(vol_slice)),
        "volume_avg": float(np.nanmean(vol_slice)) if len(vol_slice) else 0.0,
        "price_drop": max(0.0, price_drop),
        "low_at_end": low_at_end,
    }


def _path_volume_window_features(
    close: np.ndarray,
    low: np.ndarray,
    volume: np.ndarray,
    lookback: int,
) -> dict:
    out = {
        f"pv_wbottom_presence_{lookback}d": 0.0,
        f"pv_wbottom_volume_contraction_{lookback}d": 0.0,
        f"pv_wbottom_time_contraction_{lookback}d": 0.0,
        f"pv_wbottom_retest_quality_{lookback}d": 0.0,
        f"pv_wbottom_score_{lookback}d": 0.0,
    }
    if len(close) < max(4, min(lookback, 4)):
        return out
    local_close = close[-lookback:]
    local_low = low[-lookback:]
    local_volume = volume[-lookback:]
    if len(local_close) < 4:
        return out
    runs = _down_runs(local_close)
    if len(runs) < 2:
        return out

    first = _leg_stats(local_close, local_low, local_volume, runs[0])
    second = _leg_stats(local_close, local_low, local_volume, runs[-1])
    first_end_bar = min(runs[0][1] + 1, len(local_close) - 1)
    second_start_bar = runs[-1][0]
    if second_start_bar <= first_end_bar:
        return out
    middle = local_close[first_end_bar:second_start_bar + 1]
    if len(middle) < 2:
        return out
    middle_ret = np.diff(middle) / np.where(middle[:-1] != 0, middle[:-1], np.nan)
    rebound_return = float(np.nanmax(middle) / middle[0] - 1.0) if middle[0] else 0.0
    has_middle_rebound = bool(
        np.isfinite(rebound_return)
        and rebound_return > 0.002
        and np.nansum(middle_ret > 0.0) >= 1
    )
    if not has_middle_rebound:
        return out
    volume_contraction = _safe_log_ratio(first["volume_avg"], second["volume_avg"])
    time_contraction = _safe_log_ratio(first["length"], second["length"])
    close_last = float(local_close[-1]) if np.isfinite(local_close[-1]) else np.nan
    if close_last and np.isfinite(first["low_at_end"]) and np.isfinite(second["low_at_end"]):
        retest_quality = 1.0 - abs(second["low_at_end"] - first["low_at_end"]) / max(abs(close_last), 1e-6)
    else:
        retest_quality = 0.0
    retest_quality = float(np.clip(retest_quality, -1.0, 1.0))

    second_not_heavier = 1.0 if second["volume_avg"] <= first["volume_avg"] else 0.0
    second_not_longer = 1.0 if second["length"] <= first["length"] else 0.0
    two_leg_score = max(0.0, volume_contraction) * (0.5 + 0.25 * second_not_heavier + 0.25 * second_not_longer)
    score = two_leg_score * max(0.0, retest_quality)

    out[f"pv_wbottom_presence_{lookback}d"] = 1.0
    out[f"pv_wbottom_volume_contraction_{lookback}d"] = volume_contraction
    out[f"pv_wbottom_time_contraction_{lookback}d"] = time_contraction
    out[f"pv_wbottom_retest_quality_{lookback}d"] = retest_quality
    out[f"pv_wbottom_score_{lookback}d"] = score
    return out


def _signal_path_volume_features(df: pd.DataFrame, signal_index: int) -> dict:
    # All slices end at signal_index. No future bars are touched.
    start = max(0, signal_index - 20 + 1)
    close = pd.to_numeric(df.loc[start:signal_index, "close"], errors="coerce").to_numpy(dtype=float)
    low = pd.to_numeric(df.loc[start:signal_index, "low"], errors="coerce").to_numpy(dtype=float)
    volume = pd.to_numeric(df.loc[start:signal_index, "volume"], errors="coerce").to_numpy(dtype=float)
    features = {}
    for lookback in (5, 10):
        features.update(_path_volume_window_features(close, low, volume, lookback))
    features["pv_path_volume_exhaustion_score"] = (
        0.6 * features["pv_wbottom_score_5d"]
        + 0.4 * features["pv_wbottom_score_10d"]
    )
    return features


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
    rows = []
    for i in df.index[df["signal_date"].isin(wanted)].to_list():
        row = {
            "code": code,
            "signal_date": df.at[i, "signal_date"],
        }
        row.update(_signal_path_volume_features(df, int(i)))
        rows.append(row)
    return rows


def enrich_with_path_volume_cache(
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
        raise ValueError(f"no path-volume feature rows produced from {cache_dir}")
    feat["signal_date"] = pd.to_datetime(feat["signal_date"]).dt.normalize()
    merged = signals.merge(feat, on=["code", "signal_date"], how="left", validate="many_to_one")
    before = len(merged)
    merged = merged.dropna(subset=PATH_VOLUME_FEATURES).copy()
    return merged.reset_index(drop=True), {
        "signal_rows_before_cache_join": int(before),
        "signal_rows_after_cache_join": int(len(merged)),
        "cache_join_drop_rows": int(before - len(merged)),
        "cache_dir": str(cache_dir),
    }


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
    if {"v2_baseline", "v2_plus_path_volume"}.issubset(summary):
        pivot = df.pivot(index="fold", columns="model", values=["cagr_pct", "sharpe", "max_dd_pct"])
        deltas = {}
        for metric in ["cagr_pct", "sharpe", "max_dd_pct"]:
            diff = pivot[(metric, "v2_plus_path_volume")] - pivot[(metric, "v2_baseline")]
            deltas[metric] = {
                "avg_delta": round(float(diff.mean()), 6),
                "worst_delta": round(float(diff.min()), 6),
                "positive_delta_pass_rate": round(float((diff > 0).mean()), 6),
            }
        summary["v2_plus_path_volume_minus_baseline"] = deltas
    return summary


def write_markdown_report(result: dict, path: Path) -> None:
    lines = [
        "# Brick Path-Volume Phase 6 Strict Forward Validation",
        "",
        f"Created: {result['created_at']}",
        "",
        "## Boundary",
        "",
        f"- Signal file: `{result['data_boundary']['signal_path']}`",
        f"- Indicator cache: `{result['data_boundary']['indicator_cache']}`",
        f"- Signal data: {result['data_boundary']['signal_data_start']} to {result['data_boundary']['signal_data_end']}",
        "- Factor window: daily OHLCV lookbacks ending at signal day only.",
        "- Data modality: daily OHLCV only; no L2/tick/orderbook/minute/auction data.",
        f"- Embargo days: {result['strict_forward_validation']['embargo_days']}",
        "",
        "## Compute",
        "",
        f"- Backend: `{result['compute_acceleration']['selected_backend']}`",
        f"- GPU available: `{result['compute_acceleration']['gpu_available']}`",
        f"- LightGBM GPU probe: `{result['compute_acceleration']['lightgbm_gpu_probe']}`",
        "",
        "## Path-Volume Construction",
        "",
        f"- Cache joined rows: {result['cache_join']['signal_rows_after_cache_join']} / {result['cache_join']['signal_rows_before_cache_join']}",
        "- Features: local W-bottom / two-down-leg volume contraction over 5d and 10d windows ending at signal day.",
        "- W-bottom gate: requires a middle rebound between the first and second down legs.",
        f"- 5d W-bottom coverage: {result['factor_diagnostics']['wbottom_presence_rate_5d']:.2%} ({result['factor_diagnostics']['wbottom_presence_count_5d']} rows)",
        f"- 10d W-bottom coverage: {result['factor_diagnostics']['wbottom_presence_rate_10d']:.2%} ({result['factor_diagnostics']['wbottom_presence_count_10d']} rows)",
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
        "A factor is not promotion-valid unless V2+Path-Volume improves average test performance, preserves acceptable worst-fold behavior, and uses only pre-trade features.",
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
    panel, join_stats = enrich_with_path_volume_cache(signals, cache_dir, args.workers)
    panel_path = out_dir / "brick_path_volume_factor_panel.parquet"
    panel.to_parquet(panel_path, index=False)
    factor_diagnostics = {
        "wbottom_presence_rate_5d": round(float(panel["pv_wbottom_presence_5d"].mean()), 6),
        "wbottom_presence_rate_10d": round(float(panel["pv_wbottom_presence_10d"].mean()), 6),
        "wbottom_presence_count_5d": int(panel["pv_wbottom_presence_5d"].sum()),
        "wbottom_presence_count_10d": int(panel["pv_wbottom_presence_10d"].sum()),
        "panel_rows": int(len(panel)),
        "middle_rebound_required": True,
    }

    train_probe = panel[
        (panel["signal_date"] >= pd.Timestamp("2020-01-01"))
        & (panel["signal_date"] <= pd.Timestamp("2022-12-31"))
    ].copy()
    orthogonality = {}
    for factor in PATH_VOLUME_FEATURES:
        orthogonality[factor] = {
            "rank_ic": compute_rank_ic(train_probe, factor),
            **compute_residual_rank_ic(train_probe, factor, V2_FEATURES),
        }

    params = build_lgb_params(use_gpu=use_gpu, num_threads=args.threads)
    model_specs = {
        "v2_baseline": V2_FEATURES,
        "v2_plus_path_volume": [*V2_FEATURES, *PATH_VOLUME_FEATURES],
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
        pv = fold_block["models"]["v2_plus_path_volume"]["test_metrics"]["account"]
        fold_block["test_delta_v2_plus_path_volume_minus_baseline"] = {
            key: round(float(pv.get(key, 0.0) - base.get(key, 0.0)), 6)
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
            "signal_path": str(Path(args.signal_path).resolve()),
            "indicator_cache": str(cache_dir.resolve()),
            "factor_panel": str(panel_path.resolve()),
            "signal_data_start": panel["signal_date"].min().strftime("%Y-%m-%d"),
            "signal_data_end": panel["signal_date"].max().strftime("%Y-%m-%d"),
            "factor_window_rule": "all path-volume windows end at signal_date; no post-signal bars",
            "daily_only": True,
            "uses_l2_tick_orderbook_minute_auction": False,
        },
        "compute_acceleration": acceleration,
        "cache_join": join_stats,
        "factor_diagnostics": factor_diagnostics,
        "factor_features": PATH_VOLUME_FEATURES,
        "orthogonality_train_2020_2022": orthogonality,
        "fold_results": fold_results,
        "summary": summarize_results(fold_results),
    }
    metrics_path = out_dir / "brick_path_volume_phase6_results.json"
    metrics_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown_report(result, out_dir / "brick_path_volume_phase6_report.md")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick Path-Volume strict Phase 6 validation")
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
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
