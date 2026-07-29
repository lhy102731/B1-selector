"""Brick label reconstruction strict forward validation.

Research-only runner for AG2-KBase roundtable handoffs that do not add new
inference features. It changes only the training target and sample weights:
piecewise hold-days residualized returns plus continuous hold-days decay. The
labels are used only inside train folds, never as model inputs.
"""
from __future__ import annotations

import argparse
import json
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

from brick_erd_phase6 import (  # noqa: E402
    V2_FEATURES,
    _feature_matrix,
    _probe_lightgbm_gpu,
    build_lgb_params,
    label_from_train_bins,
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


DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / "label_reconstruction_phase6"
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


def load_panel(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    panel, stats_block = load_candidates(path)
    panel, pool_stats = add_pool_features(panel)
    for col in ["return_pct", "hold_days", "signal_day_regime_label", *V2_FEATURES]:
        panel[col] = _numeric(panel[col])
    panel = panel.dropna(subset=["return_pct", "hold_days", *V2_FEATURES]).copy()
    panel = panel[panel["hold_days"] > 0].copy()
    if panel.empty:
        raise ValueError("label reconstruction panel is empty after hold_days preflight")
    return panel, {
        "candidate_source": stats_block,
        "pool_feature_construction": pool_stats,
        "rows_after_hold_days_preflight": int(len(panel)),
        "hold_days_min": round(float(panel["hold_days"].min()), 6),
        "hold_days_max": round(float(panel["hold_days"].max()), 6),
        "hold_days_mean": round(float(panel["hold_days"].mean()), 6),
    }


def group_sizes_by(frame: pd.DataFrame) -> np.ndarray:
    return frame.groupby("entry_date", sort=False).size().to_numpy(dtype=int)


class HoldDaysResidualizer:
    def __init__(self, edges: list[float], models: list[dict[str, float]], global_mean: float) -> None:
        self.edges = edges
        self.models = models
        self.global_mean = global_mean

    @staticmethod
    def fit(frame: pd.DataFrame, bins: int) -> "HoldDaysResidualizer":
        hold = _numeric(frame["hold_days"]).to_numpy(dtype=float)
        ret = _numeric(frame["return_pct"]).to_numpy(dtype=float)
        valid = np.isfinite(hold) & np.isfinite(ret)
        hold = hold[valid]
        ret = ret[valid]
        if len(hold) < 100:
            raise ValueError("not enough rows to fit hold-days residualizer")
        quantiles = np.linspace(0.0, 1.0, bins + 1)
        edges = np.unique(np.quantile(hold, quantiles)).tolist()
        if len(edges) < 2:
            edges = [float(np.nanmin(hold)), float(np.nanmax(hold) + 1.0)]
        models: list[dict[str, float]] = []
        for left, right in zip(edges[:-1], edges[1:]):
            if right == edges[-1]:
                mask = (hold >= left) & (hold <= right)
            else:
                mask = (hold >= left) & (hold < right)
            x = hold[mask]
            y = ret[mask]
            if len(x) >= 20 and np.nanstd(x) > 1e-9:
                slope, intercept = np.polyfit(x, y, deg=1)
                models.append({"left": float(left), "right": float(right), "slope": float(slope), "intercept": float(intercept)})
            else:
                models.append({"left": float(left), "right": float(right), "slope": 0.0, "intercept": float(np.nanmean(y) if len(y) else np.nanmean(ret))})
        return HoldDaysResidualizer(edges=[float(x) for x in edges], models=models, global_mean=float(np.nanmean(ret)))

    def predict(self, hold_days: pd.Series | np.ndarray) -> np.ndarray:
        hold = np.asarray(hold_days, dtype=float)
        out = np.full(len(hold), self.global_mean, dtype=float)
        for i, model in enumerate(self.models):
            left = model["left"]
            right = model["right"]
            if i == len(self.models) - 1:
                mask = (hold >= left) & (hold <= right)
            else:
                mask = (hold >= left) & (hold < right)
            out[mask] = model["slope"] * hold[mask] + model["intercept"]
        return out

    def residuals(self, frame: pd.DataFrame) -> np.ndarray:
        ret = _numeric(frame["return_pct"]).to_numpy(dtype=float)
        pred = self.predict(_numeric(frame["hold_days"]).to_numpy(dtype=float))
        return ret - pred


def residualizer_holdout_check(train: pd.DataFrame, bins: int) -> tuple[bool, dict[str, Any], HoldDaysResidualizer | None]:
    ordered = train.sort_values("entry_date").copy()
    split = int(len(ordered) * 0.70)
    fit_frame = ordered.iloc[:split].copy()
    holdout = ordered.iloc[split:].copy()
    if len(fit_frame) < 100 or len(holdout) < 50:
        return False, {"passed": False, "reason": "insufficient internal holdout rows"}, None
    try:
        model = HoldDaysResidualizer.fit(fit_frame, bins)
    except Exception as error:
        return False, {"passed": False, "reason": f"{type(error).__name__}: {error}"}, None
    residuals = model.residuals(holdout)
    residuals = residuals[np.isfinite(residuals)]
    if len(residuals) < 50:
        return False, {"passed": False, "reason": "insufficient finite residuals"}, None
    t_stat, t_p = stats.ttest_1samp(residuals, 0.0, nan_policy="omit")
    groups = []
    holdout = holdout.copy()
    holdout["_residual"] = model.residuals(holdout)
    try:
        holdout["_bucket"] = pd.qcut(holdout["hold_days"], q=min(4, holdout["hold_days"].nunique()), duplicates="drop")
        groups = [
            _numeric(group["_residual"]).dropna().to_numpy(dtype=float)
            for _, group in holdout.groupby("_bucket", observed=False)
            if len(group) >= 10
        ]
    except Exception:
        groups = []
    levene_p = 1.0
    if len(groups) >= 2:
        _, levene_p = stats.levene(*groups, center="median")
    segment_tests: list[dict[str, Any]] = []
    segment_failure = False
    for model_block in model.models:
        left = model_block["left"]
        right = model_block["right"]
        mask = (holdout["hold_days"] >= left) & (holdout["hold_days"] <= right)
        segment = holdout[mask].copy()
        sample_pct = float(len(segment) / len(holdout)) if len(holdout) else 0.0
        r2 = 0.0
        t_abs = 0.0
        if len(segment) >= 10 and _numeric(segment["hold_days"]).std() > 1e-9:
            lr = stats.linregress(_numeric(segment["hold_days"]), _numeric(segment["return_pct"]))
            r2 = float(lr.rvalue ** 2) if np.isfinite(lr.rvalue) else 0.0
            t_abs = float(abs(lr.slope / lr.stderr)) if lr.stderr and np.isfinite(lr.stderr) else 0.0
        failed = bool(sample_pct >= 0.10 and r2 < 0.05 and t_abs < 2.0)
        segment_failure = segment_failure or failed
        segment_tests.append({
            "left": round(float(left), 6),
            "right": round(float(right), 6),
            "rows": int(len(segment)),
            "sample_pct": round(sample_pct, 6),
            "r2": round(r2, 6),
            "abs_t_slope": round(t_abs, 6),
            "failed": failed,
        })
    passed = bool(
        (not np.isfinite(t_p) or t_p >= 0.05)
        and (not np.isfinite(levene_p) or levene_p >= 0.05)
        and not segment_failure
    )
    full_model = HoldDaysResidualizer.fit(train, bins) if passed else None
    return passed, {
        "passed": passed,
        "mean_residual": round(float(np.nanmean(residuals)), 6),
        "std_residual": round(float(np.nanstd(residuals)), 6),
        "ttest_p_value_mean_zero": round(float(t_p) if np.isfinite(t_p) else 1.0, 6),
        "levene_p_value_bucket_variance": round(float(levene_p) if np.isfinite(levene_p) else 1.0, 6),
        "fit_rows": int(len(fit_frame)),
        "holdout_rows": int(len(holdout)),
        "bins": int(bins),
        "segment_regression_tests": segment_tests,
        "segment_failure_rule": "fail if sample_pct >= 0.10 and r2 < 0.05 and abs_t_slope < 2.0",
    }, full_model


def train_reconstructed_ranker(
    train: pd.DataFrame,
    *,
    residualizer: HoldDaysResidualizer,
    params: dict[str, Any],
    num_boost_round: int,
    hold_decay_lambda: float,
) -> tuple[Any, RobustScaler, dict[str, Any]]:
    train = train.sort_values(["entry_date", "code"]).reset_index(drop=True)
    x_train = _feature_matrix(train, V2_FEATURES)
    scaler = RobustScaler()
    x_train_s = scaler.fit_transform(x_train)
    residualized = residualizer.residuals(train)
    labels = label_from_train_bins(residualized, residualized)
    hold = _numeric(train["hold_days"]).to_numpy(dtype=float)
    weights = np.exp(-hold_decay_lambda * np.clip(hold, 0.0, None))
    train_set = lgb.Dataset(
        x_train_s,
        label=labels,
        weight=weights,
        group=group_sizes_by(train),
    )
    model = lgb.train(params, train_set, num_boost_round=num_boost_round)
    return model, scaler, {
        "num_boost_round": int(num_boost_round),
        "target": "hold_days_residualized_return",
        "sample_weight": "exp(-lambda * hold_days)",
        "hold_decay_lambda": float(hold_decay_lambda),
        "weight_min": round(float(np.nanmin(weights)), 6),
        "weight_max": round(float(np.nanmax(weights)), 6),
        "weight_mean": round(float(np.nanmean(weights)), 6),
        "labels_not_model_inputs": ["return_pct", "hold_days", "exit_date", "exit_price"],
    }


def score_frame(model: Any, scaler: RobustScaler, frame: pd.DataFrame) -> np.ndarray:
    return model.predict(scaler.transform(_feature_matrix(frame, V2_FEATURES)))


def select_top_n(frame: pd.DataFrame, scores: np.ndarray, top_n: int) -> pd.DataFrame:
    out = frame.copy()
    out["score"] = scores
    out["_rank"] = out.groupby("entry_date")["score"].rank(ascending=False, method="first")
    return out[out["_rank"] <= top_n].sort_values(["entry_date", "_rank"]).copy()


def evaluate(
    frame: pd.DataFrame,
    scores: np.ndarray,
    *,
    output_dir: Path,
    prefix: str,
    top_n: int,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    target_position_pct: float,
    max_positions: int,
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> dict[str, Any]:
    selected = select_top_n(frame, scores, top_n)
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


def metric_delta(candidate: dict[str, Any], baseline: dict[str, Any], surface: str, metric: str) -> float:
    return round(
        float(candidate.get(surface, {}).get(metric, 0.0) - baseline.get(surface, {}).get(metric, 0.0)),
        6,
    )


def regime_delta_summary(frame: pd.DataFrame, baseline_scores: np.ndarray, reconstructed_scores: np.ndarray, top_n: int) -> dict[str, Any]:
    scored = frame[["entry_date", "code", "return_pct", "signal_day_regime_label"]].copy()
    scored["baseline_score"] = baseline_scores
    scored["reconstructed_score"] = reconstructed_scores
    rows = []
    for regime, grp in scored.groupby("signal_day_regime_label", dropna=False):
        base = select_top_n(grp.rename(columns={"baseline_score": "score"}), grp["baseline_score"].to_numpy(dtype=float), top_n)
        reco = select_top_n(grp.rename(columns={"reconstructed_score": "score"}), grp["reconstructed_score"].to_numpy(dtype=float), top_n)
        rows.append({
            "regime": str(regime),
            "baseline_avg_return_pct": round(float(_numeric(base["return_pct"]).mean()) if not base.empty else 0.0, 6),
            "reconstructed_avg_return_pct": round(float(_numeric(reco["return_pct"]).mean()) if not reco.empty else 0.0, 6),
            "baseline_trades": int(len(base)),
            "reconstructed_trades": int(len(reco)),
        })
    return {"rows": rows}


def run_fold(
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
    residualizer_bins: int,
    hold_decay_lambda: float,
) -> dict[str, Any]:
    name = str(fold["fold"])
    train, train_purge = filter_train_strict(panel, tuple(fold["train"]), fold["validation"][0])
    validation_raw = filter_entry_window(panel, tuple(fold["validation"]))
    validation = validation_raw[validation_raw["exit_date"] < pd.Timestamp(fold["test"][0])].copy()
    test = filter_entry_window(panel, tuple(fold["test"]))
    if train.empty or validation.empty or test.empty:
        raise ValueError(f"{name}: empty train/validation/test split")

    baseline_model, baseline_scaler, baseline_train_info = train_ranker_no_validation(
        train,
        params,
        num_boost_round,
    )
    residualizer_ok, residualizer_check, residualizer = residualizer_holdout_check(train, residualizer_bins)
    val_start = pd.Timestamp(fold["validation"][0])
    val_end = min(pd.Timestamp(fold["validation"][1]), validation["entry_date"].max())
    test_start = pd.Timestamp(fold["test"][0])
    test_end = min(pd.Timestamp(fold["test"][1]), test["entry_date"].max())

    baseline_val_scores = score_frame(baseline_model, baseline_scaler, validation)
    baseline_test_scores = score_frame(baseline_model, baseline_scaler, test)
    baseline_val = evaluate(
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

    block: dict[str, Any] = {
        "fold": name,
        "train_window": list(fold["train"]),
        "validation_window": list(fold["validation"]),
        "test_window": list(fold["test"]),
        "rows": {
            "train": int(len(train)),
            "validation_before_exit_purge": int(len(validation_raw)),
            "validation_after_exit_purge": int(len(validation)),
            "test": int(len(test)),
        },
        "train_info": {**baseline_train_info, **train_purge},
        "residualizer_holdout_check": residualizer_check,
        "baseline": {"validation": baseline_val, "test": baseline_test},
    }
    if not residualizer_ok or residualizer is None:
        block.update({
            "status": "RESIDUALIZER_STABILITY_STOP",
            "reconstructed": {},
            "deltas": {},
            "falsification": {
                "ex_sharpe_improved_by_0_1": False,
                "ex_cagr_not_worse": False,
                "mdd_not_worse_by_2pp": False,
                "no_val_to_test_degradation": False,
            },
            "falsification_passed": False,
        })
        return block

    reconstructed_model, reconstructed_scaler, reconstructed_train_info = train_reconstructed_ranker(
        train,
        residualizer=residualizer,
        params=params,
        num_boost_round=num_boost_round,
        hold_decay_lambda=hold_decay_lambda,
    )
    reco_val_scores = score_frame(reconstructed_model, reconstructed_scaler, validation)
    reco_test_scores = score_frame(reconstructed_model, reconstructed_scaler, test)
    reco_val = evaluate(
        validation,
        reco_val_scores,
        output_dir=output_dir,
        prefix=f"{name}.validation.label_reconstructed",
        top_n=top_n,
        start_ts=val_start,
        end_ts=val_end,
        target_position_pct=target_position_pct,
        max_positions=max_positions,
        commission_bp=commission_bp,
        stamp_pct=stamp_pct,
        slippage_pct=slippage_pct,
    )
    reco_test = evaluate(
        test,
        reco_test_scores,
        output_dir=output_dir,
        prefix=f"{name}.test.label_reconstructed",
        top_n=top_n,
        start_ts=test_start,
        end_ts=test_end,
        target_position_pct=target_position_pct,
        max_positions=max_positions,
        commission_bp=commission_bp,
        stamp_pct=stamp_pct,
        slippage_pct=slippage_pct,
    )
    val_sharpe_delta = metric_delta(reco_val, baseline_val, "executable_portfolio", "sharpe")
    test_sharpe_delta = metric_delta(reco_test, baseline_test, "executable_portfolio", "sharpe")
    val_cagr_delta = metric_delta(reco_val, baseline_val, "executable_portfolio", "cagr_pct")
    test_cagr_delta = metric_delta(reco_test, baseline_test, "executable_portfolio", "cagr_pct")
    test_mdd_delta = metric_delta(reco_test, baseline_test, "executable_portfolio", "max_dd_pct")
    falsification = {
        "ex_sharpe_improved_by_0_1": bool(test_sharpe_delta >= 0.1),
        "ex_cagr_not_worse": bool(test_cagr_delta >= 0.0),
        "mdd_not_worse_by_2pp": bool(test_mdd_delta >= -2.0),
        "no_val_to_test_degradation": bool(not ((val_sharpe_delta >= 0.0 and test_sharpe_delta < 0.0) or (val_cagr_delta >= 0.0 and test_cagr_delta < 0.0))),
    }
    block.update({
        "status": "VALIDATION_COMPLETED",
        "reconstructed_train_info": reconstructed_train_info,
        "reconstructed": {"validation": reco_val, "test": reco_test},
        "deltas": {
            "validation_ex_sharpe": val_sharpe_delta,
            "test_ex_sharpe": test_sharpe_delta,
            "validation_ex_cagr_pct": val_cagr_delta,
            "test_ex_cagr_pct": test_cagr_delta,
            "test_ex_max_dd_pct": test_mdd_delta,
            "test_signal_quality_cagr_pct": metric_delta(reco_test, baseline_test, "signal_quality", "cagr_pct"),
            "test_signal_quality_sharpe": metric_delta(reco_test, baseline_test, "signal_quality", "sharpe"),
        },
        "regime_stratified_test_return": regime_delta_summary(test, baseline_test_scores, reco_test_scores, top_n),
        "falsification": falsification,
        "falsification_passed": bool(all(falsification.values())),
    })
    return block


def summarize(folds: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for item in folds:
        base = item.get("baseline", {}).get("test", {}).get("executable_portfolio", {})
        reco = item.get("reconstructed", {}).get("test", {}).get("executable_portfolio", {})
        rows.append({
            "fold": item["fold"],
            "status": item["status"],
            "baseline_ex_cagr": base.get("cagr_pct", 0.0),
            "reconstructed_ex_cagr": reco.get("cagr_pct", 0.0),
            "baseline_ex_sharpe": base.get("sharpe", 0.0),
            "reconstructed_ex_sharpe": reco.get("sharpe", 0.0),
            "ex_sharpe_delta": item.get("deltas", {}).get("test_ex_sharpe", 0.0),
            "ex_cagr_delta": item.get("deltas", {}).get("test_ex_cagr_pct", 0.0),
            "mdd_delta": item.get("deltas", {}).get("test_ex_max_dd_pct", 0.0),
            "passed": item.get("falsification_passed", False),
        })
    frame = pd.DataFrame(rows)
    if frame.empty:
        return {}
    sharpe_pass = (frame["ex_sharpe_delta"] >= 0.1).sum()
    cagr_worse = (frame["ex_cagr_delta"] < 0.0).sum()
    mdd_worse = (frame["mdd_delta"] < -2.0).sum()
    promoted = bool(sharpe_pass >= 2 and cagr_worse < 2 and mdd_worse < 2)
    return {
        "folds": int(len(frame)),
        "completed_reconstructed_folds": int((frame["status"] == "VALIDATION_COMPLETED").sum()),
        "passed_folds": int(frame["passed"].sum()),
        "ex_sharpe_improve_0_1_folds": int(sharpe_pass),
        "ex_cagr_worse_folds": int(cagr_worse),
        "mdd_worse_over_2pp_folds": int(mdd_worse),
        "avg_ex_sharpe_delta": round(float(frame["ex_sharpe_delta"].mean()), 6),
        "avg_ex_cagr_delta": round(float(frame["ex_cagr_delta"].mean()), 6),
        "promotion_gate_passed": promoted,
        "not_promoted": not promoted,
        "rows": rows,
    }


def write_report(result: dict[str, Any], path: Path) -> None:
    lines = [
        "# Brick Label Reconstruction Phase 6",
        "",
        f"Created: {result['created_at']}",
        "",
        "## Boundary",
        "",
        f"- Handoff: `{result['data_boundary']['handoff_path']}`",
        f"- Candidate parquet: `{result['data_boundary']['candidate_path']}`",
        "- Production script untouched.",
        "- Market timing disabled.",
        "- `hold_days` and `return_pct` are labels/weights only, not inference inputs.",
        "- Split column: `entry_date`; train labels are purged before validation, validation labels before test.",
        "",
        "## Summary",
        "",
        "```json",
        json.dumps(result["summary"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## Fold Results",
        "",
        "| Fold | Status | Pass | EX Sharpe Delta | EX CAGR Delta | MDD Delta |",
        "| --- | --- | --- | ---: | ---: | ---: |",
    ]
    for item in result["folds"]:
        delta = item.get("deltas", {})
        lines.append(
            "| {fold} | {status} | {passed} | {sharpe:.4f} | {cagr:.2f}% | {mdd:.2f}% |".format(
                fold=item["fold"],
                status=item["status"],
                passed="yes" if item.get("falsification_passed") else "no",
                sharpe=delta.get("test_ex_sharpe", 0.0),
                cagr=delta.get("test_ex_cagr_pct", 0.0),
                mdd=delta.get("test_ex_max_dd_pct", 0.0),
            )
        )
    lines.extend([
        "",
        "## Residualizer Holdout Checks",
        "",
        "```json",
        json.dumps(
            {item["fold"]: item["residualizer_holdout_check"] for item in result["folds"]},
            ensure_ascii=False,
            indent=2,
        ),
        "```",
        "",
        "## Promotion Note",
        "",
        "Promotion requires executable NAV Sharpe improvement of at least 0.1 in at least two test folds, without executable CAGR worsening in at least two folds or max drawdown worsening by more than 2 percentage points in at least two folds.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = Path(args.candidate_path)
    panel, panel_stats = load_panel(candidate_path)
    panel_path = out_dir / "label_reconstruction_panel.parquet"
    audit_cols = [
        "code",
        "signal_date",
        "entry_date",
        "exit_date",
        "return_pct",
        "entry_price",
        "exit_price",
        "hold_days",
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
            target_position_pct=args.target_position_pct,
            max_positions=args.max_positions,
            commission_bp=args.commission,
            stamp_pct=args.stamp,
            slippage_pct=args.slippage,
            residualizer_bins=args.residualizer_bins,
            hold_decay_lambda=args.hold_decay_lambda,
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
            "train_role": "train baseline and reconstructed-label rankers",
            "validation_role": "check val-to-test degradation only",
            "test_role": "single unseen evaluation",
            "purge_rule": "train exit_date < validation_start; validation exit_date < test_start",
        },
        "panel": panel_stats,
        "compute_acceleration": acceleration,
        "features": {
            "ranker_features": V2_FEATURES,
            "new_inference_features": [],
            "training_target": "hold-days residualized return labels",
            "sample_weight": "exp(-lambda * hold_days)",
            "hold_decay_lambda": float(args.hold_decay_lambda),
            "residualizer_bins": int(args.residualizer_bins),
        },
        "folds": folds,
        "summary": summarize(folds),
    }
    result_path = out_dir / "brick_label_reconstruction_phase6_results.json"
    result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(result, out_dir / "brick_label_reconstruction_phase6_report.md")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick label reconstruction strict validation")
    parser.add_argument("--handoff-path", default="")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--candidate-path", default=str(DEFAULT_CANDIDATE_PATH))
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--num-boost-round", type=int, default=300)
    parser.add_argument("--max-folds", type=int, default=0)
    parser.add_argument("--residualizer-bins", type=int, default=4)
    parser.add_argument("--hold-decay-lambda", type=float, default=0.08)
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
