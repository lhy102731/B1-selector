"""Brick regime-conditioned abstention strict forward validation.

Research-only runner for AG2-KBase handoffs that propose no new alpha factor,
but instead abstain from emitting Top3 on days where the trained V2 ranker looks
unstable inside a signal-day regime. Production Brick code is not touched.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(RESEARCH_DIR))

from brick_erd_phase6 import (  # noqa: E402
    V2_FEATURES,
    _feature_matrix,
    _probe_lightgbm_gpu,
    build_lgb_params,
    trade_metrics,
)
from brick_generated_daily_factor_sqnav_phase6 import (  # noqa: E402
    DEFAULT_CANDIDATE_PATH,
    add_pool_features,
    load_candidates,
)
from brick_v2_rebuilt_dual_metrics import (  # noqa: E402
    executable_portfolio_metrics,
    filter_entry_window,
    filter_train_strict,
    signal_quality_nav_metrics,
    train_ranker_no_validation,
)
from research_automation.gpu_acceleration import (  # noqa: E402
    build_compute_acceleration_plan,
    detect_nvidia_gpu,
)


DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / "regime_abstention_phase6"
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


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)


def load_panel(candidate_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    candidates, stats_block = load_candidates(candidate_path)
    panel, pool_stats = add_pool_features(candidates)
    required = {"peer_signal_count", "signal_day_regime_label", *V2_FEATURES}
    missing = sorted(required - set(panel.columns))
    if missing:
        raise ValueError(f"abstention schema preflight failed, missing columns: {missing}")
    for col in ["peer_signal_count", "signal_day_regime_label", *V2_FEATURES]:
        panel[col] = _numeric(panel[col])
    coverage = {
        col: round(float(panel[col].notna().mean() * 100.0), 6)
        for col in ["peer_signal_count", "signal_day_regime_label"]
    }
    sparse = {col: pct for col, pct in coverage.items() if pct < 80.0}
    if sparse:
        raise ValueError(f"abstention schema preflight coverage failed: {sparse}")
    return panel, {
        "candidate_source": stats_block,
        "pool_feature_construction": pool_stats,
        "schema_preflight": {
            "required_columns": sorted(required),
            "coverage_pct": coverage,
            "minimum_required_pct": 80.0,
            "passed": True,
        },
    }


def score_frame(model: Any, scaler: Any, frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["entry_date", "code"]).copy()
    out["score"] = model.predict(scaler.transform(_feature_matrix(out, V2_FEATURES)))
    out["_rank"] = out.groupby("entry_date")["score"].rank(ascending=False, method="first")
    return out


def select_ranked(scored: pd.DataFrame, top_n: int, keep_dates: set[pd.Timestamp] | None = None) -> pd.DataFrame:
    out = scored
    if keep_dates is not None:
        out = out[out["entry_date"].isin(keep_dates)]
    selected = out[out["_rank"] <= top_n].copy()
    return selected.sort_values(["entry_date", "_rank"]).copy()


def _mode_or_median(series: pd.Series) -> float:
    valid = _numeric(series).dropna()
    if valid.empty:
        return 0.0
    mode = valid.mode()
    return float(mode.iloc[0] if not mode.empty else valid.median())


def build_day_stats(scored: pd.DataFrame, top_pool_n: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for entry_date, grp in scored.groupby("entry_date", sort=False):
        ordered = grp.sort_values(["score", "code"], ascending=[False, True])
        top_pool = ordered.head(top_pool_n)
        top3 = ordered.head(3)
        rank4_10 = ordered.iloc[3:10]
        pool_scores = _numeric(top_pool["score"]).dropna()
        rows.append({
            "entry_date": pd.Timestamp(entry_date).normalize(),
            "n_candidates": int(len(ordered)),
            "top_pool_n": int(min(top_pool_n, len(ordered))),
            "score_dispersion_top100": float(pool_scores.std(ddof=0)) if len(pool_scores) else 0.0,
            "score_gap_rank3_rank10": float(
                ordered["score"].iloc[2] - ordered["score"].iloc[9]
            ) if len(ordered) >= 10 else 0.0,
            "top3_return": float(_numeric(top3["return_pct"]).mean()) if not top3.empty else 0.0,
            "rank4_10_return": float(_numeric(rank4_10["return_pct"]).mean()) if not rank4_10.empty else 0.0,
            "peer_signal_count": float(_numeric(ordered["peer_signal_count"]).median()),
            "signal_day_regime_label": _mode_or_median(ordered["signal_day_regime_label"]),
        })
    return pd.DataFrame(rows).sort_values("entry_date").reset_index(drop=True)


def _threshold_candidates(values: pd.Series) -> list[float | None]:
    clean = _numeric(values).dropna()
    if clean.empty:
        return [None]
    quantiles = np.linspace(0.0, 0.8, 9)
    thresholds = sorted({round(float(clean.quantile(q)), 12) for q in quantiles})
    return [None, *thresholds]


def _kept_by_threshold(day_stats: pd.DataFrame, thresholds: dict[str, float | None]) -> pd.Series:
    keep = []
    for _, row in day_stats.iterrows():
        regime = str(int(row["signal_day_regime_label"]))
        threshold = thresholds.get(regime)
        if threshold is None:
            keep.append(True)
        else:
            keep.append(float(row["score_dispersion_top100"]) >= float(threshold))
    return pd.Series(keep, index=day_stats.index, dtype=bool)


def choose_regime_thresholds(validation_stats: pd.DataFrame, min_pass_rate: float) -> dict[str, Any]:
    thresholds: dict[str, float | None] = {}
    details: list[dict[str, Any]] = []
    for regime_value, grp in validation_stats.groupby("signal_day_regime_label", sort=True):
        regime = str(int(regime_value))
        best_key = None
        best_detail: dict[str, Any] | None = None
        for threshold in _threshold_candidates(grp["score_dispersion_top100"]):
            if threshold is None:
                kept = grp
            else:
                kept = grp[grp["score_dispersion_top100"] >= threshold]
            pass_rate = float(len(kept) / len(grp)) if len(grp) else 0.0
            top3_mean = float(kept["top3_return"].mean()) if not kept.empty else -999.0
            rank4_10_mean = float(kept["rank4_10_return"].mean()) if not kept.empty else 999.0
            mimo_margin = top3_mean - rank4_10_mean
            gate_pass = pass_rate >= min_pass_rate and mimo_margin >= 0.0
            key = (1 if gate_pass else 0, round(mimo_margin, 8), round(top3_mean, 8), round(pass_rate, 8))
            detail = {
                "regime": regime,
                "threshold": threshold,
                "days": int(len(grp)),
                "kept_days": int(len(kept)),
                "pass_rate": round(pass_rate, 6),
                "top3_return_mean": round(top3_mean, 6),
                "rank4_10_return_mean": round(rank4_10_mean, 6),
                "mimo_margin": round(mimo_margin, 6),
                "gate_pass": bool(gate_pass),
            }
            if best_key is None or key > best_key:
                best_key = key
                best_detail = detail
        if best_detail is None:
            best_detail = {
                "regime": regime,
                "threshold": None,
                "days": int(len(grp)),
                "kept_days": int(len(grp)),
                "pass_rate": 1.0,
                "top3_return_mean": 0.0,
                "rank4_10_return_mean": 0.0,
                "mimo_margin": 0.0,
                "gate_pass": False,
            }
        thresholds[regime] = best_detail["threshold"]
        details.append(best_detail)
    return {"thresholds": thresholds, "details": details}


def _ks_pvalue(left: pd.Series, right: pd.Series) -> float:
    a = _numeric(left).dropna()
    b = _numeric(right).dropna()
    if len(a) < 2 or len(b) < 2:
        return 0.0
    value = stats.ks_2samp(a.to_numpy(dtype=float), b.to_numpy(dtype=float)).pvalue
    return round(float(value) if np.isfinite(value) else 0.0, 6)


def evaluate_selection(
    *,
    selected: pd.DataFrame,
    output_dir: Path,
    prefix: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    top_n: int,
    target_position_pct: float,
    max_positions: int,
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> dict[str, Any]:
    selected.to_csv(output_dir / f"{prefix}.trades.csv", index=False, encoding="gbk")
    signal_quality, sq_nav = signal_quality_nav_metrics(
        selected,
        start_ts,
        end_ts,
        top_n,
        commission_bp,
        stamp_pct,
        slippage_pct,
    )
    sq_nav.to_csv(output_dir / f"{prefix}.signal_quality.nav.csv", index=False, encoding="gbk")
    executable, ex_nav = executable_portfolio_metrics(
        selected,
        start_ts,
        end_ts,
        target_position_pct=target_position_pct,
        max_positions=max_positions,
        commission_bp=commission_bp,
        stamp_pct=stamp_pct,
        slippage_pct=slippage_pct,
    )
    ex_nav.to_csv(output_dir / f"{prefix}.executable_portfolio.nav.csv", index=False, encoding="gbk")
    return {
        "trade": trade_metrics(selected),
        "signal_quality": signal_quality,
        "executable_portfolio": executable,
    }


def run_fold(
    panel: pd.DataFrame,
    fold: dict[str, Any],
    *,
    output_dir: Path,
    params: dict[str, Any],
    num_boost_round: int,
    top_n: int,
    top_pool_n: int,
    min_pass_rate: float,
    target_position_pct: float,
    max_positions: int,
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> dict[str, Any]:
    name = str(fold["fold"])
    train, train_purge = filter_train_strict(panel, tuple(fold["train"]), fold["validation"][0])
    validation_raw = filter_entry_window(panel, tuple(fold["validation"]))
    validation = validation_raw[validation_raw["exit_date"] < pd.Timestamp(fold["test"][0])].copy()
    test = filter_entry_window(panel, tuple(fold["test"]))
    if train.empty or validation.empty or test.empty:
        raise ValueError(f"{name}: train/validation/test window produced empty data")

    model, scaler, train_info = train_ranker_no_validation(train, params, num_boost_round)
    train_scored = score_frame(model, scaler, train)
    validation_scored = score_frame(model, scaler, validation)
    test_scored = score_frame(model, scaler, test)

    train_stats = build_day_stats(train_scored, top_pool_n)
    validation_stats = build_day_stats(validation_scored, top_pool_n)
    test_stats = build_day_stats(test_scored, top_pool_n)
    threshold_plan = choose_regime_thresholds(validation_stats, min_pass_rate)
    keep_mask = _kept_by_threshold(test_stats, threshold_plan["thresholds"])
    keep_dates = set(test_stats.loc[keep_mask, "entry_date"])

    test_start = pd.Timestamp(fold["test"][0])
    requested_end = pd.Timestamp(fold["test"][1])
    effective_end = min(requested_end, test["entry_date"].max())
    baseline_selected = select_ranked(test_scored, top_n)
    abstained_selected = select_ranked(test_scored, top_n, keep_dates)
    validation_keep_mask = _kept_by_threshold(validation_stats, threshold_plan["thresholds"])

    baseline = evaluate_selection(
        selected=baseline_selected,
        output_dir=output_dir,
        prefix=f"{name}.baseline_top{top_n}",
        start_ts=test_start,
        end_ts=effective_end,
        top_n=top_n,
        target_position_pct=target_position_pct,
        max_positions=max_positions,
        commission_bp=commission_bp,
        stamp_pct=stamp_pct,
        slippage_pct=slippage_pct,
    )
    abstention = evaluate_selection(
        selected=abstained_selected,
        output_dir=output_dir,
        prefix=f"{name}.abstention_top{top_n}",
        start_ts=test_start,
        end_ts=effective_end,
        top_n=top_n,
        target_position_pct=target_position_pct,
        max_positions=max_positions,
        commission_bp=commission_bp,
        stamp_pct=stamp_pct,
        slippage_pct=slippage_pct,
    )

    kept_test_stats = test_stats.loc[keep_mask].copy()
    pass_rate = float(keep_mask.mean()) if len(keep_mask) else 0.0
    val_pass_rate = float(validation_keep_mask.mean()) if len(validation_keep_mask) else 0.0
    mimo_margin = (
        float(kept_test_stats["top3_return"].mean() - kept_test_stats["rank4_10_return"].mean())
        if not kept_test_stats.empty else -999.0
    )
    baseline_ex = baseline.get("executable_portfolio", {})
    abstention_ex = abstention.get("executable_portfolio", {})
    falsification = {
        "mimo_top3_raw_ge_rank4_10": bool(mimo_margin >= 0.0),
        "pass_rate_ge_min": bool(pass_rate >= min_pass_rate),
        "ex_sharpe_gt_baseline": bool(
            abstention_ex.get("sharpe", -999.0) > baseline_ex.get("sharpe", 999.0)
        ),
        "ex_cagr_ge_2pct": bool(abstention_ex.get("cagr_pct", -999.0) >= 2.0),
        "dispersion_ks_p_ge_0_05": bool(
            _ks_pvalue(validation_stats["score_dispersion_top100"], test_stats["score_dispersion_top100"]) >= 0.05
        ),
        "regime_ks_p_ge_0_05": bool(
            _ks_pvalue(validation_stats["signal_day_regime_label"], test_stats["signal_day_regime_label"]) >= 0.05
        ),
        "validation_pass_rate_ge_50pct": bool(val_pass_rate >= 0.50),
        "schema_preflight": True,
    }
    return {
        "fold": name,
        "train_window": list(fold["train"]),
        "validation_window": list(fold["validation"]),
        "test_window": list(fold["test"]),
        "effective_test_window": [test_start.strftime("%Y-%m-%d"), effective_end.strftime("%Y-%m-%d")],
        "rows": {
            "train": int(len(train)),
            "validation_before_exit_purge": int(len(validation_raw)),
            "validation_after_exit_purge": int(len(validation)),
            "test": int(len(test)),
        },
        "train_info": {**train_info, **train_purge},
        "threshold_selection": threshold_plan,
        "abstention_stats": {
            "test_days": int(len(test_stats)),
            "kept_days": int(keep_mask.sum()),
            "abstained_days": int((~keep_mask).sum()),
            "pass_rate": round(pass_rate, 6),
            "validation_pass_rate": round(val_pass_rate, 6),
            "mimo_top3_return_mean": round(
                float(kept_test_stats["top3_return"].mean()) if not kept_test_stats.empty else 0.0,
                6,
            ),
            "mimo_rank4_10_return_mean": round(
                float(kept_test_stats["rank4_10_return"].mean()) if not kept_test_stats.empty else 0.0,
                6,
            ),
            "mimo_margin": round(mimo_margin, 6),
            "dispersion_ks_p": _ks_pvalue(
                validation_stats["score_dispersion_top100"],
                test_stats["score_dispersion_top100"],
            ),
            "regime_ks_p": _ks_pvalue(
                validation_stats["signal_day_regime_label"],
                test_stats["signal_day_regime_label"],
            ),
        },
        "baseline_top3": baseline,
        "regime_abstention_top3": abstention,
        "deltas_abstention_minus_baseline": {
            "signal_quality_cagr_pct": round(
                abstention.get("signal_quality", {}).get("cagr_pct", 0.0)
                - baseline.get("signal_quality", {}).get("cagr_pct", 0.0),
                6,
            ),
            "signal_quality_cum_return_pct": round(
                abstention.get("signal_quality", {}).get("cum_return_pct", 0.0)
                - baseline.get("signal_quality", {}).get("cum_return_pct", 0.0),
                6,
            ),
            "executable_cagr_pct": round(
                abstention_ex.get("cagr_pct", 0.0) - baseline_ex.get("cagr_pct", 0.0),
                6,
            ),
            "executable_sharpe": round(
                abstention_ex.get("sharpe", 0.0) - baseline_ex.get("sharpe", 0.0),
                6,
            ),
        },
        "falsification": falsification,
        "falsification_passed": bool(all(falsification.values())),
    }


def summarize_folds(folds: list[dict[str, Any]]) -> dict[str, Any]:
    if not folds:
        return {}
    rows = []
    for item in folds:
        base_ex = item["baseline_top3"].get("executable_portfolio", {})
        abst_ex = item["regime_abstention_top3"].get("executable_portfolio", {})
        rows.append({
            "fold": item["fold"],
            "baseline_ex_cagr": base_ex.get("cagr_pct", 0.0),
            "abstention_ex_cagr": abst_ex.get("cagr_pct", 0.0),
            "baseline_ex_sharpe": base_ex.get("sharpe", 0.0),
            "abstention_ex_sharpe": abst_ex.get("sharpe", 0.0),
            "pass_rate": item["abstention_stats"]["pass_rate"],
            "mimo_margin": item["abstention_stats"]["mimo_margin"],
            "passed": item["falsification_passed"],
        })
    frame = pd.DataFrame(rows)
    return {
        "folds": int(len(frame)),
        "passed_folds": int(frame["passed"].sum()),
        "passed_fold_rate": round(float(frame["passed"].mean()), 6),
        "avg_baseline_ex_cagr": round(float(frame["baseline_ex_cagr"].mean()), 6),
        "avg_abstention_ex_cagr": round(float(frame["abstention_ex_cagr"].mean()), 6),
        "avg_ex_cagr_delta": round(float((frame["abstention_ex_cagr"] - frame["baseline_ex_cagr"]).mean()), 6),
        "avg_ex_sharpe_delta": round(float((frame["abstention_ex_sharpe"] - frame["baseline_ex_sharpe"]).mean()), 6),
        "min_pass_rate": round(float(frame["pass_rate"].min()), 6),
        "worst_mimo_margin": round(float(frame["mimo_margin"].min()), 6),
        "rows": rows,
    }


def write_report(result: dict[str, Any], path: Path) -> None:
    lines = [
        "# Brick Regime-Conditioned Abstention Phase 6",
        "",
        f"Created: {result['created_at']}",
        "",
        "## Boundary",
        "",
        f"- Handoff: `{result['data_boundary']['handoff_path']}`",
        f"- Candidate parquet: `{result['data_boundary']['candidate_path']}`",
        "- Production script untouched.",
        "- Market timing disabled.",
        "- Split column: `entry_date`; train and validation labels are purged before the next window.",
        "- No new factor names; uses pool-level signal-day fields and V2 ranker scores.",
        "",
        "## Summary",
        "",
        "```json",
        json.dumps(result["summary"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## Fold Results",
        "",
        "| Fold | Pass | Keep Rate | Mimo Margin | Base EX CAGR | Abstain EX CAGR | EX Sharpe Delta |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in result["folds"]:
        base_ex = item["baseline_top3"].get("executable_portfolio", {})
        abst_ex = item["regime_abstention_top3"].get("executable_portfolio", {})
        delta = item["deltas_abstention_minus_baseline"]
        lines.append(
            "| {fold} | {passed} | {keep:.2f}% | {margin:.4f} | {base:.2f}% | {abst:.2f}% | {sharpe:.4f} |".format(
                fold=item["fold"],
                passed="yes" if item["falsification_passed"] else "no",
                keep=item["abstention_stats"]["pass_rate"] * 100.0,
                margin=item["abstention_stats"]["mimo_margin"],
                base=base_ex.get("cagr_pct", 0.0),
                abst=abst_ex.get("cagr_pct", 0.0),
                sharpe=delta.get("executable_sharpe", 0.0),
            )
        )
    lines.extend([
        "",
        "## Falsification",
        "",
        "All eight roundtable conditions must pass in at least 2/3 folds before this mechanism can be considered for further promotion.",
        "",
        "```json",
        json.dumps(
            {item["fold"]: item["falsification"] for item in result["folds"]},
            ensure_ascii=False,
            indent=2,
        ),
        "```",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = Path(args.candidate_path)
    panel, panel_stats = load_panel(candidate_path)
    panel_path = out_dir / "regime_abstention_panel.parquet"
    audit_cols = [
        "code",
        "signal_date",
        "entry_date",
        "exit_date",
        "return_pct",
        "entry_price",
        "exit_price",
        "hold_days",
        "peer_signal_count",
        "signal_day_regime_label",
        *V2_FEATURES,
    ]
    panel[[col for col in audit_cols if col in panel.columns]].to_parquet(panel_path, index=False)

    gpu_capability = detect_nvidia_gpu()
    acceleration = build_compute_acceleration_plan("ranker_training", gpu_capability)
    use_gpu = False
    gpu_probe_error = None
    if args.prefer_gpu and gpu_capability.available:
        use_gpu, gpu_probe_error = _probe_lightgbm_gpu()
    acceleration["lightgbm_gpu_probe"] = {"usable": bool(use_gpu), "error": gpu_probe_error}
    acceleration["selected_backend"] = "lightgbm_gpu" if use_gpu else "cpu"
    params = build_lgb_params(use_gpu=use_gpu, num_threads=args.threads)

    fold_specs = FOLDS[:args.max_folds] if args.max_folds else FOLDS
    folds = [
        run_fold(
            panel,
            fold,
            output_dir=out_dir,
            params=params,
            num_boost_round=args.num_boost_round,
            top_n=args.top_n,
            top_pool_n=args.top_pool_n,
            min_pass_rate=args.min_pass_rate,
            target_position_pct=args.target_position_pct,
            max_positions=args.max_positions,
            commission_bp=args.commission,
            stamp_pct=args.stamp,
            slippage_pct=args.slippage,
        )
        for fold in fold_specs
    ]
    result = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "VALIDATION_COMPLETED",
        "data_boundary": {
            "handoff_path": str(Path(args.handoff_path).resolve()) if args.handoff_path else "",
            "candidate_path": str(candidate_path.resolve()),
            "factor_panel": str(panel_path.resolve()),
            "use_market_timing": False,
            "production_script_touched": False,
            "split_column": "entry_date",
            "forbidden_model_inputs": [
                "return_pct",
                "exit_date",
                "exit_price",
                "hold_days",
                "entry_date high/low/close",
                "post-09:25 intraday data",
            ],
        },
        "strict_forward_validation": {
            "folds": fold_specs,
            "train_role": "train V2 ranker only",
            "validation_role": "choose regime-conditioned abstention thresholds only",
            "test_role": "single unseen evaluation",
            "purge_rule": "train exit_date < validation_start; validation exit_date < test_start",
        },
        "panel": panel_stats,
        "compute_acceleration": acceleration,
        "features": {
            "ranker_features": V2_FEATURES,
            "abstention_features": [
                "score_dispersion_top100",
                "score_gap_rank3_rank10",
                "peer_signal_count",
                "signal_day_regime_label",
            ],
            "top_pool_n": int(args.top_pool_n),
            "top_n": int(args.top_n),
        },
        "folds": folds,
        "summary": summarize_folds(folds),
    }
    results_path = out_dir / "brick_regime_abstention_phase6_results.json"
    results_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(result, out_dir / "brick_regime_abstention_phase6_report.md")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick regime-conditioned abstention validation")
    parser.add_argument("--handoff-path", default="")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--candidate-path", default=str(DEFAULT_CANDIDATE_PATH))
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--top-pool-n", type=int, default=100)
    parser.add_argument("--min-pass-rate", type=float, default=0.40)
    parser.add_argument("--num-boost-round", type=int, default=300)
    parser.add_argument("--max-folds", type=int, default=0)
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
        "summary": result["summary"],
        "compute_acceleration": result["compute_acceleration"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
