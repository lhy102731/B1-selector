"""Brick pool-quality dynamic TopK Signal Quality NAV validation.

Research-only runner for AG2-KBase handoffs that propose daily candidate-pool
quality as a soft TopK control. It uses the frozen no-timing V2 Top3/Top5
Signal Quality NAV artifacts as the comparison anchor and tests a predeclared
dynamic TopK(3,5) overlay on the frozen Top5 ranked trades.
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

ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(RESEARCH_DIR))

from brick_erd_phase6 import _code_str, trade_metrics  # noqa: E402
from brick_v2_rebuilt_dual_metrics import (  # noqa: E402
    ROLLING_WINDOWS,
    signal_quality_nav_metrics,
    _nav_metrics_from_returns,
)


DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / "pool_quality_topk_phase6"
DEFAULT_CANDIDATE_PATH = (
    ROOT
    / "research_state"
    / "brick"
    / "v2_rebuilt_dual_metrics_20260709_parquet_notiming_top3"
    / "rebuilt_candidates_from_daily.parquet"
)
DEFAULT_FROZEN_TOP3_DIR = ROOT / "research_state" / "brick" / "v2_rebuilt_dual_metrics_20260709_parquet_notiming_top3"
DEFAULT_FROZEN_TOP5_DIR = ROOT / "research_state" / "brick" / "v2_rebuilt_dual_metrics_20260709_parquet_notiming_top5"


POOL_QUALITY_FEATURES = [
    "pullback_coherence_fraction",
    "gap_dispersion_std",
    "yellow_zone_lower_quartile_fraction",
    "composite_pool_quality_score",
]


def load_candidates(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"candidate parquet not found: {path}")
    df = pd.read_parquet(path)
    df = df.copy()
    df["code"] = df["code"].map(_code_str)
    for col in ["signal_date", "entry_date", "exit_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()
    for col in ["overnight_gap_pct", "entry_open_to_yellow_pct", "entry_open_to_ma5_pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["signal_date", "entry_date"])


def build_pool_panel(candidates: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for signal_date, grp in candidates.groupby("signal_date", sort=True):
        gap = grp["overnight_gap_pct"].replace([np.inf, -np.inf], np.nan)
        ma5 = grp["entry_open_to_ma5_pct"].replace([np.inf, -np.inf], np.nan)
        yellow = grp["entry_open_to_yellow_pct"].replace([np.inf, -np.inf], np.nan)
        valid = pd.DataFrame({"gap": gap, "ma5": ma5, "yellow": yellow}).dropna()
        if valid.empty:
            continue
        rows.append({
            "signal_date": signal_date,
            "entry_date": grp["entry_date"].min(),
            "pool_size": int(len(grp)),
            "pullback_coherence_fraction": float(((valid["gap"] <= 0) & valid["ma5"].between(-5.0, 2.0)).mean()),
            "gap_dispersion_std": float(valid["gap"].std(ddof=0)),
            "yellow_support_fraction": float((valid["yellow"] <= 0).mean()),
            "mean_overnight_gap_pct": float(valid["gap"].mean()),
        })
    panel = pd.DataFrame(rows)
    if panel.empty:
        raise ValueError("pool panel is empty")
    panel["signal_date"] = pd.to_datetime(panel["signal_date"]).dt.normalize()
    panel["entry_date"] = pd.to_datetime(panel["entry_date"]).dt.normalize()
    return panel.sort_values("signal_date").reset_index(drop=True)


def _train_percentile(train_values: pd.Series, values: pd.Series, *, inverse: bool = False) -> pd.Series:
    sample = pd.to_numeric(train_values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    values = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan)
    if sample.empty:
        return pd.Series(0.5, index=values.index)
    sorted_values = np.sort(sample.to_numpy(dtype=float))
    ranks = np.searchsorted(sorted_values, values.to_numpy(dtype=float), side="right") / len(sorted_values)
    out = pd.Series(ranks, index=values.index).fillna(0.5).clip(0.0, 1.0)
    return 1.0 - out if inverse else out


def add_window_quality_scores(pool: pd.DataFrame, train_window: tuple[str, str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    train_start, train_end = [pd.Timestamp(x) for x in train_window]
    train = pool[(pool["entry_date"] >= train_start) & (pool["entry_date"] <= train_end)].copy()
    if train.empty:
        raise ValueError(f"no pool rows in train window {train_window}")
    out = pool.copy()
    yellow_threshold = float(
        pd.to_numeric(train["yellow_support_fraction"], errors="coerce").quantile(0.25)
    )
    out["yellow_zone_lower_quartile_fraction"] = (
        pd.to_numeric(out["yellow_support_fraction"], errors="coerce") <= yellow_threshold
    ).astype(float)
    train = out[(out["entry_date"] >= train_start) & (out["entry_date"] <= train_end)].copy()
    out["_coherence_pct"] = _train_percentile(
        train["pullback_coherence_fraction"],
        out["pullback_coherence_fraction"],
    )
    out["_dispersion_pct"] = _train_percentile(
        train["gap_dispersion_std"],
        out["gap_dispersion_std"],
        inverse=True,
    )
    out["_yellow_pct"] = _train_percentile(
        train["yellow_zone_lower_quartile_fraction"],
        out["yellow_zone_lower_quartile_fraction"],
    )
    out["composite_pool_quality_score"] = out[["_coherence_pct", "_dispersion_pct", "_yellow_pct"]].mean(axis=1)
    threshold = float(train["composite_pool_quality_score"].median()) if "composite_pool_quality_score" in train else 0.5
    if not np.isfinite(threshold):
        threshold = 0.5
    meta = {
        "yellow_support_fraction_train_q25": round(yellow_threshold, 6),
        "topk_threshold_source": "train_median_composite_pool_quality_score",
        "topk_threshold": round(threshold, 6),
        "top5_rule": "composite_pool_quality_score >= train median and pool_size >= 5",
        "top3_rule": "otherwise",
        "component_formula": {
            "pullback_coherence_fraction": "fraction of signal_date candidates with overnight_gap_pct <= 0 and entry_open_to_ma5_pct in [-5, 2]",
            "gap_dispersion_std": "same-pool standard deviation of overnight_gap_pct; lower is better",
            "yellow_zone_lower_quartile_fraction": "binary pool flag from train lower-quartile yellow_support_fraction threshold",
            "composite_pool_quality_score": "mean of train-percentile coherence, inverse dispersion, and yellow-zone component",
        },
    }
    return out.drop(columns=["_coherence_pct", "_dispersion_pct", "_yellow_pct"]), meta


def load_frozen_trades(frozen_dir: Path, window_name: str) -> pd.DataFrame:
    path = frozen_dir / f"{window_name}.trades.csv"
    if not path.exists():
        raise FileNotFoundError(f"frozen trades not found: {path}")
    trades = pd.read_csv(path, encoding="gbk")
    trades["code"] = trades["code"].map(_code_str)
    for col in ["signal_date", "entry_date", "exit_date"]:
        trades[col] = pd.to_datetime(trades[col], errors="coerce").dt.normalize()
    if "_rank" not in trades.columns:
        trades["_rank"] = trades.groupby("entry_date")["score"].rank(ascending=False, method="first")
    return trades


def load_frozen_nav(frozen_dir: Path, window_name: str) -> dict[str, Any]:
    path = frozen_dir / f"{window_name}.signal_quality.nav.csv"
    nav = pd.read_csv(path)
    return {
        "nav_path": str(path.resolve()),
        "final_nav": round(float(nav["nav"].iloc[-1]), 6),
        "cum_return_pct": round(float((nav["nav"].iloc[-1] - 1.0) * 100.0), 6),
        "days": int(len(nav)),
        "ret_sum": round(float(pd.to_numeric(nav["ret"], errors="coerce").fillna(0.0).sum()), 6),
    }


def evaluate_dynamic_window(
    *,
    pool: pd.DataFrame,
    window: dict[str, Any],
    frozen_top3_dir: Path,
    frozen_top5_dir: Path,
    output_dir: Path,
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> dict[str, Any]:
    name = str(window["name"])
    scored_pool, score_meta = add_window_quality_scores(pool, tuple(window["train"]))
    test_start, requested_test_end = [pd.Timestamp(x) for x in window["test"]]
    test_pool = scored_pool[
        (scored_pool["entry_date"] >= test_start)
        & (scored_pool["entry_date"] <= requested_test_end)
    ][["signal_date", *POOL_QUALITY_FEATURES, "pool_size"]].copy()

    top5_trades = load_frozen_trades(frozen_top5_dir, name)
    merged = top5_trades.merge(test_pool, on="signal_date", how="left", validate="many_to_one")
    missing_pool = int(merged["composite_pool_quality_score"].isna().sum())
    merged["selected_k"] = np.where(
        (merged["composite_pool_quality_score"] >= score_meta["topk_threshold"])
        & (merged["pool_size"] >= 5),
        5,
        3,
    )
    dynamic = merged[merged["_rank"] <= merged["selected_k"]].copy()
    dynamic_path = output_dir / f"{name}_dynamic_pool_quality_topk.trades.csv"
    dynamic.to_csv(dynamic_path, index=False, encoding="gbk")

    effective_end = min(requested_test_end, dynamic["entry_date"].max())
    metrics, nav = signal_quality_nav_metrics(
        dynamic,
        test_start,
        effective_end,
        5,
        commission_bp,
        stamp_pct,
        slippage_pct,
    )
    nav_path = output_dir / f"{name}_dynamic_pool_quality_topk.signal_quality.nav.csv"
    nav.to_csv(nav_path, index=False, encoding="gbk")

    frozen_top3 = load_frozen_nav(frozen_top3_dir, name)
    frozen_top5 = load_frozen_nav(frozen_top5_dir, name)
    dynamic_final_nav = float(nav["nav"].iloc[-1]) if not nav.empty else 1.0
    k_by_day = dynamic.groupby("entry_date")["selected_k"].max()
    return {
        "name": name,
        "train_window": list(window["train"]),
        "test_window": list(window["test"]),
        "score_meta": score_meta,
        "missing_pool_rows": missing_pool,
        "dynamic_topk": {
            "trade": trade_metrics(dynamic),
            "signal_quality": metrics,
            "final_nav": round(dynamic_final_nav, 6),
            "nav_path": str(nav_path.resolve()),
            "trades_path": str(dynamic_path.resolve()),
            "top5_day_fraction": round(float((k_by_day == 5).mean()), 6) if len(k_by_day) else 0.0,
            "avg_selected_k_by_day": round(float(k_by_day.mean()), 6) if len(k_by_day) else 0.0,
        },
        "frozen_top3": frozen_top3,
        "frozen_top5": frozen_top5,
        "delta_dynamic_minus_frozen_top3_nav_points": round(float((dynamic_final_nav - frozen_top3["final_nav"]) * 100.0), 6),
        "delta_dynamic_minus_frozen_top5_nav_points": round(float((dynamic_final_nav - frozen_top5["final_nav"]) * 100.0), 6),
    }


def stitch_dynamic_nav(windows: list[dict[str, Any]], output_dir: Path) -> dict[str, Any]:
    returns: list[float] = []
    dates: list[pd.Timestamp] = []
    source_windows: list[str] = []
    for item in windows:
        path = Path(item["dynamic_topk"]["nav_path"])
        nav = pd.read_csv(path)
        nav["date"] = pd.to_datetime(nav["date"], errors="coerce").dt.normalize()
        nav = nav.dropna(subset=["date"]).sort_values("date")
        returns.extend(pd.to_numeric(nav["ret"], errors="coerce").fillna(0.0).to_numpy(dtype=float).tolist())
        dates.extend(nav["date"].tolist())
        source_windows.extend([item["name"]] * len(nav))
    metrics, stitched = _nav_metrics_from_returns(
        np.asarray(returns, dtype=float),
        dates,
        extra={
            "metric_surface": "signal_quality_dynamic_pool_quality_topk_stitched",
            "source_windows": int(len(windows)),
        },
    )
    if not stitched.empty:
        stitched["window"] = source_windows[:len(stitched)]
    out_path = output_dir / "rolling_oos_dynamic_pool_quality_topk_signal_quality_nav.csv"
    stitched.to_csv(out_path, index=False, encoding="gbk")
    metrics["nav_path"] = str(out_path.resolve())
    return metrics


def summarize_windows(windows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for item in windows:
        rows.append({
            "name": item["name"],
            "dynamic_final_nav": item["dynamic_topk"]["final_nav"],
            "frozen_top3_final_nav": item["frozen_top3"]["final_nav"],
            "frozen_top5_final_nav": item["frozen_top5"]["final_nav"],
            "delta_vs_top3": item["delta_dynamic_minus_frozen_top3_nav_points"],
            "delta_vs_top5": item["delta_dynamic_minus_frozen_top5_nav_points"],
            "top5_day_fraction": item["dynamic_topk"]["top5_day_fraction"],
        })
    frame = pd.DataFrame(rows)
    if frame.empty:
        return {}
    return {
        "windows": int(len(frame)),
        "avg_delta_vs_top3_nav_points": round(float(frame["delta_vs_top3"].mean()), 6),
        "avg_delta_vs_top5_nav_points": round(float(frame["delta_vs_top5"].mean()), 6),
        "beats_top3_windows": int((frame["delta_vs_top3"] > 0).sum()),
        "beats_top5_windows": int((frame["delta_vs_top5"] > 0).sum()),
        "avg_top5_day_fraction": round(float(frame["top5_day_fraction"].mean()), 6),
        "window_rows": rows,
    }


def write_report(result: dict[str, Any], path: Path) -> None:
    lines = [
        "# Brick Pool Quality Dynamic TopK Phase 6",
        "",
        f"Created: {result['created_at']}",
        "",
        "## Boundary",
        "",
        f"- Handoff: `{result['data_boundary']['handoff_path']}`",
        f"- Candidate parquet: `{result['data_boundary']['candidate_path']}`",
        f"- Frozen Top3 dir: `{result['data_boundary']['frozen_top3_dir']}`",
        f"- Frozen Top5 dir: `{result['data_boundary']['frozen_top5_dir']}`",
        "- Uses frozen Top5 ranking per window and selects Top3 or Top5 by train-calibrated pool-quality score.",
        "- This is a soft TopK overlay; it does not change production code and does not use market-timing deletion.",
        "",
        "## Stitched Rolling OOS",
        "",
        "| Strategy | Final NAV | CAGR | MaxDD | Sharpe |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    frozen = result["frozen_stitched"]
    dyn = result["dynamic_stitched"]
    lines.append(f"| Frozen Top3 | {frozen['top3_final_nav']:.4f} | {frozen['top3_cagr_pct']:.2f}% | n/a | n/a |")
    lines.append(f"| Frozen Top5 | {frozen['top5_final_nav']:.4f} | {frozen['top5_cagr_pct']:.2f}% | n/a | n/a |")
    lines.append(
        "| Dynamic TopK | {nav:.4f} | {cagr:.2f}% | {mdd:.2f}% | {sharpe:.3f} |".format(
            nav=1.0 + dyn.get("cum_return_pct", 0.0) / 100.0,
            cagr=dyn.get("cagr_pct", 0.0),
            mdd=dyn.get("max_dd_pct", 0.0),
            sharpe=dyn.get("sharpe", 0.0),
        )
    )
    lines.extend([
        "",
        "## Window Results",
        "",
        "| Window | Dynamic NAV | Frozen Top3 | Frozen Top5 | Delta vs Top3 | Delta vs Top5 | Top5 Day Fraction |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for row in result["summary"]["window_rows"]:
        lines.append(
            "| {name} | {dyn:.4f} | {t3:.4f} | {t5:.4f} | {d3:.2f} | {d5:.2f} | {frac:.2%} |".format(
                name=row["name"],
                dyn=row["dynamic_final_nav"],
                t3=row["frozen_top3_final_nav"],
                t5=row["frozen_top5_final_nav"],
                d3=row["delta_vs_top3"],
                d5=row["delta_vs_top5"],
                frac=row["top5_day_fraction"],
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
        "Promotion requires Dynamic TopK to beat frozen Top3 in most rolling windows and exceed frozen Top5 without relying on small-pool artifacts.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def load_stitched_baseline(path: Path) -> tuple[float, float]:
    nav = pd.read_csv(path / "signal_quality_nav_report" / "rolling_oos_signal_quality_nav.csv")
    final_nav = float(nav["nav"].iloc[-1])
    days = len(nav)
    cagr = (final_nav ** (252.0 / days) - 1.0) * 100.0 if final_nav > 0 and days else 0.0
    return round(final_nav, 6), round(cagr, 6)


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = Path(args.candidate_path)
    frozen_top3_dir = Path(args.frozen_top3_dir)
    frozen_top5_dir = Path(args.frozen_top5_dir)
    candidates = load_candidates(candidate_path)
    pool = build_pool_panel(candidates)
    pool.to_parquet(out_dir / "brick_pool_quality_panel.parquet", index=False)

    windows = [
        evaluate_dynamic_window(
            pool=pool,
            window=window,
            frozen_top3_dir=frozen_top3_dir,
            frozen_top5_dir=frozen_top5_dir,
            output_dir=out_dir,
            commission_bp=args.commission,
            stamp_pct=args.stamp,
            slippage_pct=args.slippage,
        )
        for window in ROLLING_WINDOWS
    ]
    dynamic_stitched = stitch_dynamic_nav(windows, out_dir)
    top3_nav, top3_cagr = load_stitched_baseline(frozen_top3_dir)
    top5_nav, top5_cagr = load_stitched_baseline(frozen_top5_dir)
    dynamic_nav = 1.0 + dynamic_stitched.get("cum_return_pct", 0.0) / 100.0
    result = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "data_boundary": {
            "handoff_path": str(Path(args.handoff_path).resolve()) if args.handoff_path else "",
            "candidate_path": str(candidate_path.resolve()),
            "frozen_top3_dir": str(frozen_top3_dir.resolve()),
            "frozen_top5_dir": str(frozen_top5_dir.resolve()),
            "candidate_rows": int(len(candidates)),
            "pool_days": int(len(pool)),
            "split_column": "entry_date",
            "pool_score_date_column": "signal_date",
        },
        "factor_features": POOL_QUALITY_FEATURES,
        "rolling_windows": windows,
        "dynamic_stitched": dynamic_stitched,
        "frozen_stitched": {
            "top3_final_nav": top3_nav,
            "top3_cagr_pct": top3_cagr,
            "top5_final_nav": top5_nav,
            "top5_cagr_pct": top5_cagr,
        },
        "stitched_delta_dynamic_minus_top3_nav_points": round(float((dynamic_nav - top3_nav) * 100.0), 6),
        "stitched_delta_dynamic_minus_top5_nav_points": round(float((dynamic_nav - top5_nav) * 100.0), 6),
        "summary": summarize_windows(windows),
    }
    result_path = out_dir / "brick_pool_quality_topk_phase6_results.json"
    result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(result, out_dir / "brick_pool_quality_topk_phase6_report.md")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick pool-quality dynamic TopK validation")
    parser.add_argument("--handoff-path", default="")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--candidate-path", default=str(DEFAULT_CANDIDATE_PATH))
    parser.add_argument("--frozen-top3-dir", default=str(DEFAULT_FROZEN_TOP3_DIR))
    parser.add_argument("--frozen-top5-dir", default=str(DEFAULT_FROZEN_TOP5_DIR))
    parser.add_argument("--commission", type=float, default=3.0)
    parser.add_argument("--stamp", type=float, default=0.05)
    parser.add_argument("--slippage", type=float, default=0.1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run(args)
    print(json.dumps({
        "output_dir": str(Path(args.output_dir).resolve()),
        "dynamic_stitched": result["dynamic_stitched"],
        "frozen_stitched": result["frozen_stitched"],
        "delta_vs_top3_nav_points": result["stitched_delta_dynamic_minus_top3_nav_points"],
        "delta_vs_top5_nav_points": result["stitched_delta_dynamic_minus_top5_nav_points"],
        "summary": result["summary"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
