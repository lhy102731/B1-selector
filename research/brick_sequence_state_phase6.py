"""Brick sequence-state pre-screen and strict forward validation.

This research-only runner implements an APPROVED AG2-KBase handoff without
touching Brick production code. It computes sequence features from the same
signal-day BrickChartStrategy history, runs train/validation-only structural
pre-screens, and enters the fixed three-fold PWF only when every blocking
pre-screen passes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing as mp
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

from strategy.brick_chart_strategy import BrickChartStrategy  # noqa: E402
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
    _probe_lightgbm_gpu,
    build_lgb_params,
    score_frame,
    select_top_n,
    trade_metrics,
    train_ranker,
)
from brick_generated_daily_factor_sqnav_phase6 import (  # noqa: E402
    DEFAULT_CANDIDATE_PATH,
    load_candidates,
)
from brick_v2_rebuilt_dual_metrics import (  # noqa: E402
    executable_portfolio_metrics,
    filter_entry_window,
    filter_train_strict,
    signal_quality_nav_metrics,
)


DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / "brick_sequence_state_phase6"
DEFAULT_CACHE_DIR = ROOT / "data" / "raw_parquet"

SEQUENCE_FEATURES = [
    "brick_same_color_run_length",
    "brick_reversal_recency",
    "brick_run_length_ratio",
]
REQUIRED_FACTOR_NAMES = set(SEQUENCE_FEATURES)

FOLDS_3Y = [
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

FOLDS_4Y = [
    {
        **fold,
        "fold": f"{fold['fold']}_V2R",
        "train": (
            f"{int(fold['train'][0][:4]) - 1}-01-01",
            fold["train"][1],
        ),
    }
    for fold in FOLDS_3Y
]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)


def _sequence_state_from_brick(brick: np.ndarray) -> dict[str, np.ndarray]:
    """Compute the handoff's exact include/exclude sequence definitions."""
    values = np.asarray(brick, dtype=float)
    delta = np.diff(values, prepend=np.nan)
    colors = np.sign(delta)
    same_color_exclusive = np.full(len(values), np.nan, dtype=float)
    reversal_recency = np.full(len(values), np.nan, dtype=float)
    prior_run_length = np.full(len(values), np.nan, dtype=float)

    current_color = 0.0
    current_run = 0
    completed_run = np.nan
    for index, color in enumerate(colors):
        if not np.isfinite(color) or color == 0:
            continue
        if current_color == 0:
            current_color = float(color)
            current_run = 1
        elif color == current_color:
            current_run += 1
        else:
            completed_run = float(current_run)
            current_color = float(color)
            current_run = 1
        same_color_exclusive[index] = float(current_run - 1)
        reversal_recency[index] = float(current_run)
        prior_run_length[index] = completed_run

    ratio = reversal_recency / np.where(prior_run_length > 0, prior_run_length, np.nan)
    return {
        "color": colors,
        "same_color_exclusive": same_color_exclusive,
        "reversal_recency": reversal_recency,
        "prior_run_length": prior_run_length,
        "run_length_ratio_raw": ratio,
        "brick_height": np.abs(delta),
    }


def _sequence_worker(args: tuple[str, list[str], str]) -> list[dict[str, Any]]:
    code, wanted_dates, cache_dir_text = args
    cache_root = Path(cache_dir_text)
    path = cache_root / code[:2] / f"{code}.parquet"
    if not path.is_file():
        path = cache_root / f"{code}.parquet"
    if not path.is_file() or not wanted_dates:
        return []
    try:
        frame = pd.read_parquet(path, columns=["date", "open", "high", "low", "close", "volume"])
    except Exception:
        return []
    if frame.empty:
        return []
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    frame = frame.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    try:
        indicators = BrickChartStrategy().calculate_indicators(frame)
    except Exception:
        return []
    state = _sequence_state_from_brick(indicators["brick"].to_numpy(dtype=float))
    wanted = set(wanted_dates)
    rows: list[dict[str, Any]] = []
    for index, date in enumerate(pd.to_datetime(indicators["date"]).dt.strftime("%Y-%m-%d")):
        if date not in wanted:
            continue
        rows.append(
            {
                "code": code,
                "signal_date": date,
                "brick_same_color_run_length": state["same_color_exclusive"][index],
                "brick_reversal_recency": state["reversal_recency"][index],
                "brick_run_length_ratio_raw": state["run_length_ratio_raw"][index],
                "signal_brick_height": state["brick_height"][index],
                "sequence_color": state["color"][index],
                "strategy_brick_signal": bool(indicators["brick_signal"].iloc[index]),
            }
        )
    return rows


def build_sequence_panel(
    candidates: pd.DataFrame,
    *,
    cache_dir: Path,
    workers: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    date_map = (
        candidates.assign(signal_date_text=candidates["signal_date"].dt.strftime("%Y-%m-%d"))
        .groupby("code")["signal_date_text"]
        .apply(lambda values: sorted(set(values)))
        .to_dict()
    )
    tasks = [(str(code), dates, str(cache_dir)) for code, dates in date_map.items()]
    rows: list[dict[str, Any]] = []
    with mp.Pool(max(1, int(workers))) as pool:
        for batch in pool.imap_unordered(_sequence_worker, tasks, chunksize=20):
            rows.extend(batch)
    sequence = pd.DataFrame(rows)
    if sequence.empty:
        raise ValueError("no Brick sequence rows were produced")
    sequence["signal_date"] = pd.to_datetime(sequence["signal_date"], errors="coerce").dt.normalize()
    panel = candidates.merge(
        sequence,
        on=["code", "signal_date"],
        how="left",
        validate="many_to_one",
    )
    panel["brick_run_length_ratio"] = _numeric(panel["brick_run_length_ratio_raw"])
    for feature in [*SEQUENCE_FEATURES, "signal_brick_height"]:
        panel[feature] = _numeric(panel[feature])
    coverage = {
        feature: {
            "non_null_rows": int(panel[feature].notna().sum()),
            "coverage_pct": round(float(panel[feature].notna().mean() * 100.0), 6),
            "unique_values": int(panel[feature].nunique(dropna=True)),
            "std": round(float(panel[feature].std()) if panel[feature].notna().any() else 0.0, 8),
        }
        for feature in SEQUENCE_FEATURES
    }
    return panel, {
        "cache_dir": str(cache_dir.resolve()),
        "workers": int(workers),
        "candidate_rows": int(len(candidates)),
        "sequence_rows": int(len(sequence)),
        "merged_rows": int(len(panel)),
        "strategy_signal_mismatch_rows": int(
            panel["strategy_brick_signal"].fillna(False).eq(False).sum()
        ),
        "neutral_brick_rule": "zero-change days are not counted as bricks",
        "same_color_boundary": "signal brick excluded exactly as specified by handoff",
        "reversal_recency_boundary": "signal brick included exactly as specified by handoff",
        "coverage": coverage,
    }


def _cohen_d(left: np.ndarray, right: np.ndarray) -> float:
    left = np.asarray(left, dtype=float)
    right = np.asarray(right, dtype=float)
    left = left[np.isfinite(left)]
    right = right[np.isfinite(right)]
    if len(left) < 2 or len(right) < 2:
        return 0.0
    pooled = np.sqrt(
        ((len(left) - 1) * np.var(left, ddof=1) + (len(right) - 1) * np.var(right, ddof=1))
        / max(1, len(left) + len(right) - 2)
    )
    return float((np.mean(left) - np.mean(right)) / pooled) if pooled > 1e-12 else 0.0


def _bh_adjust(p_values: list[float]) -> list[float]:
    if not p_values:
        return []
    values = np.asarray(p_values, dtype=float)
    values[~np.isfinite(values)] = 1.0
    order = np.argsort(values)
    adjusted = np.ones(len(values), dtype=float)
    running = 1.0
    for reverse_rank, index in enumerate(order[::-1], start=1):
        rank = len(values) - reverse_rank + 1
        running = min(running, float(values[index] * len(values) / rank))
        adjusted[index] = min(1.0, running)
    return adjusted.tolist()


def _max_v2_correlation(frame: pd.DataFrame, feature: str) -> dict[str, Any]:
    best = {"feature": None, "pearson": 0.0, "abs_pearson": 0.0, "rows": 0}
    for control in V2_FEATURES:
        values = frame[[feature, control]].apply(_numeric).dropna()
        if len(values) < 100 or values[feature].std() <= 1e-12 or values[control].std() <= 1e-12:
            continue
        value = values[feature].corr(values[control])
        if np.isfinite(value) and abs(float(value)) > best["abs_pearson"]:
            best = {
                "feature": control,
                "pearson": round(float(value), 6),
                "abs_pearson": round(abs(float(value)), 6),
                "rows": int(len(values)),
            }
    return best


def _quintile_effect(frame: pd.DataFrame, feature: str) -> dict[str, Any]:
    values = frame[[feature, "return_pct"]].apply(_numeric).dropna()
    if len(values) < 100 or values[feature].nunique() < 5:
        return {
            "rows": int(len(values)),
            "buckets": 0,
            "cohen_d_q5_minus_q1": 0.0,
            "p_value": 1.0,
            "reason": "insufficient unique values for five buckets",
        }
    try:
        values["bucket"] = pd.qcut(values[feature], q=5, labels=False, duplicates="drop")
    except ValueError:
        return {
            "rows": int(len(values)),
            "buckets": 0,
            "cohen_d_q5_minus_q1": 0.0,
            "p_value": 1.0,
            "reason": "qcut failed",
        }
    buckets = int(values["bucket"].nunique())
    if buckets < 2:
        return {
            "rows": int(len(values)),
            "buckets": buckets,
            "cohen_d_q5_minus_q1": 0.0,
            "p_value": 1.0,
            "reason": "fewer than two buckets",
        }
    low = values.loc[values["bucket"] == values["bucket"].min(), "return_pct"].to_numpy(dtype=float)
    high = values.loc[values["bucket"] == values["bucket"].max(), "return_pct"].to_numpy(dtype=float)
    test = stats.ttest_ind(high, low, equal_var=False, nan_policy="omit")
    p_value = float(test.pvalue) if np.isfinite(test.pvalue) else 1.0
    return {
        "rows": int(len(values)),
        "buckets": buckets,
        "q1_rows": int(len(low)),
        "q5_rows": int(len(high)),
        "q1_mean_return_pct": round(float(np.mean(low)), 6),
        "q5_mean_return_pct": round(float(np.mean(high)), 6),
        "cohen_d_q5_minus_q1": round(_cohen_d(high, low), 6),
        "p_value": round(p_value, 8),
    }


def run_prescreen(panel: pd.DataFrame) -> dict[str, Any]:
    fold_results: list[dict[str, Any]] = []
    blockers: list[str] = []
    primary_direction_passes = 0
    for fold in FOLDS_3Y:
        development = filter_entry_window(
            panel,
            (fold["train"][0], fold["validation"][1]),
        )
        development = development[
            development["exit_date"] < pd.Timestamp(fold["test"][0])
        ].copy()
        diagnostics: list[dict[str, Any]] = []
        p_values: list[float] = []
        for feature in SEQUENCE_FEATURES:
            numeric = _numeric(development[feature])
            correlation = _max_v2_correlation(development, feature)
            quintile = _quintile_effect(development, feature)
            p_values.append(float(quintile.get("p_value", 1.0)))
            diagnostics.append(
                {
                    "feature": feature,
                    "non_null_rows": int(numeric.notna().sum()),
                    "coverage_pct": round(float(numeric.notna().mean() * 100.0), 6),
                    "unique_values": int(numeric.nunique(dropna=True)),
                    "std": round(float(numeric.std()) if numeric.notna().any() else 0.0, 8),
                    "max_v2_correlation": correlation,
                    "quintile_effect": quintile,
                }
            )
        q_values = _bh_adjust(p_values)
        for diagnostic, q_value in zip(diagnostics, q_values):
            diagnostic["quintile_effect"]["bh_fdr_q_value"] = round(float(q_value), 8)
            feature = diagnostic["feature"]
            if diagnostic["coverage_pct"] < 50.0:
                blockers.append(f"{fold['fold']}:{feature}:coverage_below_50pct")
            if diagnostic["unique_values"] < 2 or diagnostic["std"] <= 1e-12:
                blockers.append(f"{fold['fold']}:{feature}:constant_or_nonvarying")
            if diagnostic["max_v2_correlation"]["abs_pearson"] > 0.7:
                blockers.append(f"{fold['fold']}:{feature}:v2_correlation_above_0_7")
        primary = next(item for item in diagnostics if item["feature"] == "brick_same_color_run_length")
        effect = primary["quintile_effect"]
        direction_pass = bool(
            effect.get("cohen_d_q5_minus_q1", 0.0) < -0.05
            and effect.get("bh_fdr_q_value", 1.0) <= 0.5
        )
        primary_direction_passes += int(direction_pass)
        fold_results.append(
            {
                "fold": fold["fold"],
                "development_window": [fold["train"][0], fold["validation"][1]],
                "unseen_test_not_read": list(fold["test"]),
                "rows": int(len(development)),
                "diagnostics": diagnostics,
                "primary_direction_pass": direction_pass,
            }
        )
    if primary_direction_passes < 2:
        blockers.append(
            f"brick_same_color_run_length:expected_negative_direction_passed_{primary_direction_passes}_of_3"
        )
    blockers = sorted(set(blockers))
    return {
        "status": "PASS" if not blockers else "FAIL",
        "protocol": {
            "development_only": True,
            "folds": FOLDS_3Y,
            "correlation_stop_abs_pearson": 0.7,
            "coverage_stop_pct": 50.0,
            "primary_effect_rule": "negative Cohen d <= -0.05 with BH-FDR q <= 0.5 in at least 2/3 development folds",
        },
        "folds": fold_results,
        "primary_direction_passes": int(primary_direction_passes),
        "blockers": blockers,
    }


def _fit_ratio_residualizer(train: pd.DataFrame) -> tuple[float, float]:
    values = train[["brick_run_length_ratio_raw", "signal_brick_height"]].apply(_numeric).dropna()
    if len(values) < 100 or values["signal_brick_height"].std() <= 1e-12:
        return 0.0, float(values["brick_run_length_ratio_raw"].mean()) if len(values) else 0.0
    x = np.column_stack([np.ones(len(values)), values["signal_brick_height"].to_numpy(dtype=float)])
    y = values["brick_run_length_ratio_raw"].to_numpy(dtype=float)
    intercept, slope = np.linalg.lstsq(x, y, rcond=None)[0]
    return float(slope), float(intercept)


def _apply_ratio_residualizer(frame: pd.DataFrame, slope: float, intercept: float) -> pd.DataFrame:
    out = frame.copy()
    raw = _numeric(out["brick_run_length_ratio_raw"])
    height = _numeric(out["signal_brick_height"])
    out["brick_run_length_ratio"] = raw - (intercept + slope * height)
    return out


def _placebo(frame: pd.DataFrame, seed: int) -> pd.DataFrame:
    out = frame.copy()
    rng = np.random.default_rng(seed)
    for feature in SEQUENCE_FEATURES:
        shuffled = pd.Series(index=out.index, dtype=float)
        for _, group in out.groupby("signal_date", sort=False):
            shuffled.loc[group.index] = rng.permutation(_numeric(group[feature]).to_numpy(dtype=float))
        out[feature] = shuffled
    return out


def _evaluate_selection(
    frame: pd.DataFrame,
    scores: np.ndarray,
    *,
    output_dir: Path,
    prefix: str,
    top_n: int,
    target_position_pct: float,
    max_positions: int,
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> dict[str, Any]:
    selected = select_top_n(frame, scores, top_n).sort_values(["entry_date", "_rank"])
    selected.to_csv(output_dir / f"{prefix}.trades.csv", index=False, encoding="gbk")
    start = pd.Timestamp(frame["entry_date"].min())
    end = pd.Timestamp(frame["entry_date"].max())
    signal_quality, sq_nav = signal_quality_nav_metrics(
        selected, start, end, top_n, commission_bp, stamp_pct, slippage_pct
    )
    sq_nav.to_csv(output_dir / f"{prefix}.signal_quality.nav.csv", index=False, encoding="gbk")
    executable, ex_nav = executable_portfolio_metrics(
        selected,
        start,
        end,
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


def _run_fold(
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
    train, train_purge = filter_train_strict(panel, tuple(fold["train"]), fold["validation"][0])
    validation_raw = filter_entry_window(panel, tuple(fold["validation"]))
    validation = validation_raw[
        validation_raw["exit_date"] < pd.Timestamp(fold["test"][0])
    ].copy()
    test = filter_entry_window(panel, tuple(fold["test"]))
    if train.empty or validation.empty or test.empty:
        raise ValueError(f"{fold['fold']}: empty train/validation/test split")

    slope, intercept = _fit_ratio_residualizer(train)
    train = _apply_ratio_residualizer(train, slope, intercept)
    validation = _apply_ratio_residualizer(validation, slope, intercept)
    test = _apply_ratio_residualizer(test, slope, intercept)
    placebo_train = _placebo(train, seed=1000 + sum(map(ord, fold["fold"])))
    placebo_validation = _placebo(validation, seed=2000 + sum(map(ord, fold["fold"])))
    placebo_test = _placebo(test, seed=3000 + sum(map(ord, fold["fold"])))

    arms = {
        "baseline": (train, validation, test, V2_FEATURES),
        "sequence": (train, validation, test, [*V2_FEATURES, *SEQUENCE_FEATURES]),
        "placebo": (
            placebo_train,
            placebo_validation,
            placebo_test,
            [*V2_FEATURES, *SEQUENCE_FEATURES],
        ),
    }
    results: dict[str, Any] = {}
    for arm, (arm_train, arm_validation, arm_test, features) in arms.items():
        model, scaler, train_info = train_ranker(
            arm_train,
            arm_validation,
            features,
            params,
            num_boost_round,
        )
        scores = score_frame(model, scaler, arm_test, features)
        metrics = _evaluate_selection(
            arm_test,
            scores,
            output_dir=output_dir,
            prefix=f"{fold['fold']}.{arm}.top{top_n}",
            top_n=top_n,
            target_position_pct=target_position_pct,
            max_positions=max_positions,
            commission_bp=commission_bp,
            stamp_pct=stamp_pct,
            slippage_pct=slippage_pct,
        )
        results[arm] = {
            "features": features,
            "train_info": train_info,
            "test": metrics,
        }

    def ex_cagr(arm: str) -> float:
        return float(results[arm]["test"]["executable_portfolio"].get("cagr_pct", 0.0))

    return {
        "fold": fold["fold"],
        "train_window": list(fold["train"]),
        "validation_window": list(fold["validation"]),
        "test_window": list(fold["test"]),
        "rows": {
            "train": int(len(train)),
            "validation_before_exit_purge": int(len(validation_raw)),
            "validation_after_exit_purge": int(len(validation)),
            "test": int(len(test)),
        },
        "train_purge": train_purge,
        "ratio_residualization": {
            "fit_on_train_only": True,
            "target": "brick_run_length_ratio_raw",
            "control": "signal_brick_height",
            "slope": round(slope, 8),
            "intercept": round(intercept, 8),
        },
        "arms": results,
        "executable_cagr_deltas": {
            "sequence_minus_baseline": round(ex_cagr("sequence") - ex_cagr("baseline"), 6),
            "sequence_minus_placebo": round(ex_cagr("sequence") - ex_cagr("placebo"), 6),
        },
    }


def _summarize_pwf(three_year: list[dict[str, Any]], four_year: list[dict[str, Any]]) -> dict[str, Any]:
    three_base = np.array(
        [item["executable_cagr_deltas"]["sequence_minus_baseline"] for item in three_year],
        dtype=float,
    )
    three_placebo = np.array(
        [item["executable_cagr_deltas"]["sequence_minus_placebo"] for item in three_year],
        dtype=float,
    )
    four_base = np.array(
        [item["executable_cagr_deltas"]["sequence_minus_baseline"] for item in four_year],
        dtype=float,
    )
    rules = {
        "three_year_sequence_gt_baseline_2pct_folds": int((three_base > 2.0).sum()),
        "three_year_worst_sequence_minus_baseline_pct": round(float(three_base.min()), 6),
        "three_year_sequence_gt_placebo_1pct_folds": int((three_placebo > 1.0).sum()),
        "four_year_sequence_gt_baseline_0pct_folds": int((four_base > 0.0).sum()),
    }
    passed = bool(
        rules["three_year_sequence_gt_baseline_2pct_folds"] >= 2
        and rules["three_year_worst_sequence_minus_baseline_pct"] > 0.0
        and rules["three_year_sequence_gt_placebo_1pct_folds"] >= 2
        and rules["four_year_sequence_gt_baseline_0pct_folds"] >= 2
    )
    return {
        "status": "RESEARCH_CANDIDATE_PASS" if passed else "RESEARCH_REJECTED",
        "rules": rules,
        "average_test_performance": {
            "three_year_sequence_minus_baseline_executable_cagr_pct": round(float(three_base.mean()), 6),
            "three_year_sequence_minus_placebo_executable_cagr_pct": round(float(three_placebo.mean()), 6),
            "four_year_sequence_minus_baseline_executable_cagr_pct": round(float(four_base.mean()), 6),
        },
        "dispersion": {
            "three_year_sequence_minus_baseline_std": round(float(three_base.std(ddof=1)), 6),
            "three_year_sequence_minus_placebo_std": round(float(three_placebo.std(ddof=1)), 6),
            "four_year_sequence_minus_baseline_std": round(float(four_base.std(ddof=1)), 6),
        },
        "promotion_boundary": "research only; production requires explicit human confirmation",
    }


def _write_report(result: dict[str, Any], path: Path) -> None:
    lines = [
        "# Brick Sequence-State Phase 6",
        "",
        f"- Status: `{result['status']}`",
        f"- Generated at: `{result['generated_at']}`",
        f"- Handoff: `{result['handoff']['path']}`",
        f"- Candidate rows: `{result['panel']['rows']}`",
        f"- GPU backend: `{result['compute_acceleration'].get('selected_backend')}`",
        "- Production files modified: `false`",
        "",
        "## Feature Contract",
        "",
        "- `brick_same_color_run_length`: signal brick excluded.",
        "- `brick_reversal_recency`: signal brick included.",
        "- `brick_run_length_ratio`: current run divided by immediately prior run; residualization is fit on train only if PWF runs.",
        "- Zero-change days are not counted as bricks.",
        "",
        "## Pre-Screen",
        "",
        f"- Status: `{result['prescreen']['status']}`",
        f"- Blockers: `{json.dumps(result['prescreen']['blockers'], ensure_ascii=False)}`",
    ]
    if result.get("pwf"):
        lines += [
            "",
            "## PWF",
            "",
            f"- Status: `{result['pwf']['summary']['status']}`",
            f"- Rules: `{json.dumps(result['pwf']['summary']['rules'], ensure_ascii=False)}`",
        ]
    lines += [
        "",
        "## Boundary",
        "",
        "This is research evidence only. It does not change `backtest_brick_v2.py`, production parameters, or KBase source content.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    handoff_path = Path(args.handoff_path).resolve()
    document = load_handoff_document(handoff_path)
    factors = extract_factor_batch(document)
    factor_names = {str(item.get("name") or "").strip() for item in factors}
    if factor_names != REQUIRED_FACTOR_NAMES:
        raise ValueError(
            f"brick sequence runner requires {sorted(REQUIRED_FACTOR_NAMES)}, got {sorted(factor_names)}"
        )

    gpu = detect_nvidia_gpu()
    gpu_ok, gpu_error = _probe_lightgbm_gpu()
    if gpu.get("available") and not gpu_ok:
        raise RuntimeError(f"GPU is present but LightGBM GPU probe failed: {gpu_error}")
    acceleration = build_compute_acceleration_plan(
        workload="brick_sequence_state_phase6",
        gpu_info=gpu,
        gpu_backend_available=gpu_ok,
        cpu_fallback_reason=None if gpu_ok else "no compatible NVIDIA GPU detected",
    )
    acceleration["lightgbm_gpu_probe"] = {"ok": gpu_ok, "error": gpu_error}

    candidates, candidate_stats = load_candidates(Path(args.candidate_path))
    panel, sequence_stats = build_sequence_panel(
        candidates,
        cache_dir=Path(args.cache_dir),
        workers=args.workers,
    )
    panel_path = output_dir / "brick_sequence_state_panel.parquet"
    panel.to_parquet(panel_path, index=False)
    prescreen = run_prescreen(panel)
    (output_dir / "prescreen.json").write_text(
        json.dumps(prescreen, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    result: dict[str, Any] = {
        "schema_version": "brick.sequence_state_phase6.v1",
        "generated_at": datetime.now().astimezone().isoformat(),
        "status": "PRE_SCREEN_STOP" if prescreen["status"] != "PASS" else "PWF_RUNNING",
        "handoff": {
            "path": str(handoff_path),
            "sha256": _sha256(handoff_path),
            "source_boundary": (
                next(
                    item.get("output", {}).get("source_boundary")
                    for item in reversed((document.get("result") or {}).get("discovery", {}).get("transcript", []))
                    if item.get("stage") == "factor_engineer"
                )
            ),
            "factor_names": sorted(factor_names),
        },
        "compute_acceleration": acceleration,
        "candidate_source": candidate_stats,
        "sequence_construction": sequence_stats,
        "panel": {
            "path": str(panel_path),
            "rows": int(len(panel)),
            "sha256": _sha256(panel_path),
        },
        "prescreen": prescreen,
        "pwf": None,
        "production_modified": False,
    }

    if prescreen["status"] == "PASS":
        params = build_lgb_params(use_gpu=gpu_ok, num_threads=args.num_threads)
        common = {
            "output_dir": output_dir,
            "params": params,
            "num_boost_round": args.num_boost_round,
            "top_n": args.top_n,
            "target_position_pct": args.target_position_pct,
            "max_positions": args.max_positions,
            "commission_bp": args.commission_bp,
            "stamp_pct": args.stamp_pct,
            "slippage_pct": args.slippage_pct,
        }
        three_year = [_run_fold(panel, fold, **common) for fold in FOLDS_3Y]
        four_year = [_run_fold(panel, fold, **common) for fold in FOLDS_4Y]
        summary = _summarize_pwf(three_year, four_year)
        result["pwf"] = {
            "three_year_folds": three_year,
            "four_year_folds": four_year,
            "summary": summary,
        }
        result["status"] = summary["status"]

    results_path = output_dir / "brick_sequence_state_phase6_results.json"
    results_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_report(result, output_dir / "brick_sequence_state_phase6_report.md")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick sequence-state strict Phase 6 validation")
    parser.add_argument("--handoff-path", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--candidate-path", default=str(DEFAULT_CANDIDATE_PATH))
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--workers", type=int, default=max(1, min(12, mp.cpu_count() - 1)))
    parser.add_argument("--num-threads", type=int, default=max(1, mp.cpu_count() // 2))
    parser.add_argument("--num-boost-round", type=int, default=400)
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--target-position-pct", type=float, default=0.10)
    parser.add_argument("--max-positions", type=int, default=10)
    parser.add_argument("--commission-bp", type=float, default=3.0)
    parser.add_argument("--stamp-pct", type=float, default=0.05)
    parser.add_argument("--slippage-pct", type=float, default=0.10)
    return parser.parse_args()


def main() -> None:
    result = run(parse_args())
    print(
        json.dumps(
            {
                "status": result["status"],
                "output_dir": str(Path(result["panel"]["path"]).parent),
                "prescreen": result["prescreen"]["status"],
                "blockers": result["prescreen"]["blockers"],
                "pwf_summary": (result.get("pwf") or {}).get("summary"),
                "compute_acceleration": result["compute_acceleration"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    mp.freeze_support()
    main()
