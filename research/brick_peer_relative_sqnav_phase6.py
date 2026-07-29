"""Brick peer-relative factors with Signal Quality NAV validation.

Research-only runner for AG2-KBase handoffs that propose same-day candidate
pool percentile factors. It reuses the frozen rebuilt Brick V2 candidate
parquet, keeps the production script untouched, and compares V2 baseline versus
V2 plus peer-relative factors under the same train->test Signal Quality NAV
surface used by the current Top3/Top5 baseline.
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
    FIXED_WINDOWS,
    ROLLING_WINDOWS,
    filter_entry_window,
    filter_train_strict,
    rebuild_candidates,
    select_top_n,
    signal_quality_nav_metrics,
    summarize_windows,
    _nav_metrics_from_returns,
)


DEFAULT_OUTPUT_DIR = ROOT / "research_state" / "brick" / "peer_relative_sqnav_phase6"
DEFAULT_CANDIDATE_PATH = (
    ROOT
    / "research_state"
    / "brick"
    / "v2_rebuilt_dual_metrics_20260709_parquet_notiming_top3"
    / "rebuilt_candidates_from_daily.parquet"
)

PEER_RELATIVE_FEATURES = [
    "volume_contraction_ratio_peer_rank",
    "turnover_state_peer_rank",
    "ma_distance_peer_rank",
]

PEER_CONTROL_MAP = {
    "volume_contraction_ratio_peer_rank": ["vol_ratio_20", "vol_ratio_5"],
    "turnover_state_peer_rank": ["turnover_ratio_5", "turnover_ratio_20"],
    "ma_distance_peer_rank": ["close_to_ma20_pct", "entry_open_to_ma5_pct"],
}


def load_or_rebuild_candidates(args: argparse.Namespace, out_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    candidate_path = Path(args.candidate_path)
    if candidate_path.exists():
        df = pd.read_parquet(candidate_path)
        stats_block = {
            "source": "existing_rebuilt_candidate_parquet",
            "candidate_path": str(candidate_path.resolve()),
            "rebuilt_this_run": False,
        }
    elif args.rebuild_if_missing:
        df, stats_block = rebuild_candidates(
            start_date=args.start,
            end_date=args.end,
            entry_ma_source=args.entry_ma_source,
            use_market_timing=False,
            workers=args.workers,
            output_dir=out_dir,
        )
        stats_block["rebuilt_this_run"] = True
    else:
        raise FileNotFoundError(f"candidate parquet not found: {candidate_path}")

    df = df.copy()
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
            df[col] = pd.to_numeric(
                df[col].replace({True: 1, False: 0, "True": 1, "False": 0}),
                errors="coerce",
            )
    df = df.dropna(subset=["code", "signal_date", "entry_date", "exit_date", "return_pct"])
    df = df.sort_values(["entry_date", "code"]).reset_index(drop=True)
    stats_block.update({
        "rows": int(len(df)),
        "signal_days": int(df["signal_date"].nunique()),
        "entry_days": int(df["entry_date"].nunique()),
        "signal_start": df["signal_date"].min().strftime("%Y-%m-%d"),
        "signal_end": df["signal_date"].max().strftime("%Y-%m-%d"),
        "entry_start": df["entry_date"].min().strftime("%Y-%m-%d"),
        "entry_end": df["entry_date"].max().strftime("%Y-%m-%d"),
        "use_market_timing": False,
    })
    return df, stats_block


def _rank_with_default(grouped: pd.core.groupby.SeriesGroupBy, *, ascending: bool) -> pd.Series:
    ranked = grouped.rank(method="average", pct=True, ascending=ascending)
    return ranked.replace([np.inf, -np.inf], np.nan).fillna(0.5)


def add_peer_relative_features(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = df.copy()
    for col in [
        "vol_ratio_20",
        "vol_ratio_5",
        "turnover_ratio_5",
        "turnover_ratio_20",
        "close_to_ma20_pct",
        "entry_open_to_ma5_pct",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out["peer_pool_size"] = out.groupby("signal_date")["code"].transform("count").astype(int)
    out["volume_contraction_ratio_peer_rank"] = _rank_with_default(
        out.groupby("signal_date")["vol_ratio_20"],
        ascending=False,
    )
    turnover_base = "turnover_ratio_20" if "turnover_ratio_20" in out.columns else "turnover_ratio_5"
    out["turnover_state_peer_rank"] = _rank_with_default(
        out.groupby("signal_date")[turnover_base],
        ascending=True,
    )
    out["_abs_ma20_distance"] = out["close_to_ma20_pct"].abs()
    out["ma_distance_peer_rank"] = _rank_with_default(
        out.groupby("signal_date")["_abs_ma20_distance"],
        ascending=True,
    )

    stats_block = {
        "pool_definition": "all rebuilt Brick candidates sharing the same signal_date; no future outcome filtering",
        "rank_date_column": "signal_date",
        "model_group_column": "entry_date",
        "peer_pool_days": int(out["signal_date"].nunique()),
        "peer_pool_size_mean": round(float(out["peer_pool_size"].mean()), 6),
        "peer_pool_size_median": round(float(out["peer_pool_size"].median()), 6),
        "peer_pool_size_p10": round(float(out["peer_pool_size"].quantile(0.10)), 6),
        "peer_pool_size_p90": round(float(out["peer_pool_size"].quantile(0.90)), 6),
        "feature_definitions": {
            "volume_contraction_ratio_peer_rank": (
                "within-signal_date percentile of vol_ratio_20 with lower absolute ratio ranked higher"
            ),
            "turnover_state_peer_rank": (
                "within-signal_date percentile of turnover state with higher turnover ranked higher; polarity is tested by the model"
            ),
            "ma_distance_peer_rank": (
                "within-signal_date percentile of abs(close_to_ma20_pct) with farther MA20 distance ranked higher"
            ),
        },
        "forbidden_inputs": [
            "return_pct",
            "exit_date",
            "exit_price",
            "hold_days",
            "entry_date high/low/close",
            "post-09:25 intraday data",
        ],
    }
    return out.drop(columns=["_abs_ma20_distance"]), stats_block


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


def _spearman_pair(df: pd.DataFrame, left: str, right: str) -> dict[str, Any]:
    valid = df[[left, right]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(valid) < 100:
        return {"feature": right, "spearman": 0.0, "abs_spearman": 0.0, "n": int(len(valid))}
    if valid[left].std() <= 1e-12 or valid[right].std() <= 1e-12:
        return {"feature": right, "spearman": 0.0, "abs_spearman": 0.0, "n": int(len(valid))}
    corr = stats.spearmanr(valid[left], valid[right]).correlation
    corr = float(corr) if np.isfinite(corr) else 0.0
    return {
        "feature": right,
        "spearman": round(corr, 6),
        "abs_spearman": round(abs(corr), 6),
        "n": int(len(valid)),
    }


def build_correlation_diagnostics(train_probe: pd.DataFrame) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {}
    for factor in PEER_RELATIVE_FEATURES:
        pairs = [_spearman_pair(train_probe, factor, control) for control in PEER_CONTROL_MAP[factor]]
        max_pair = max(pairs, key=lambda item: item["abs_spearman"]) if pairs else {
            "feature": None,
            "spearman": 0.0,
            "abs_spearman": 0.0,
            "n": 0,
        }
        diagnostics[factor] = {
            "controls": pairs,
            "max_abs_control_corr": max_pair,
            "redundancy_failure_threshold_abs_spearman": 0.85,
            "redundancy_failure": bool(max_pair["abs_spearman"] > 0.85),
        }
    return diagnostics


def assign_pool_size_bucket(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    unique_sizes = out[["signal_date", "peer_pool_size"]].drop_duplicates()["peer_pool_size"]
    if unique_sizes.empty:
        out["pool_size_bucket"] = "unknown"
        return out
    q1 = float(unique_sizes.quantile(1.0 / 3.0))
    q2 = float(unique_sizes.quantile(2.0 / 3.0))
    out["pool_size_bucket"] = np.select(
        [out["peer_pool_size"] <= q1, out["peer_pool_size"] <= q2],
        ["small", "medium"],
        default="large",
    )
    return out


def pool_bucket_signal_quality(
    selected: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    top_n: int,
    commission_bp: float,
    stamp_pct: float,
    slippage_pct: float,
) -> dict[str, Any]:
    selected = assign_pool_size_bucket(selected)
    out: dict[str, Any] = {}
    for bucket in ["small", "medium", "large"]:
        bucket_trades = selected[selected["pool_size_bucket"] == bucket].copy()
        metrics, _ = signal_quality_nav_metrics(
            bucket_trades,
            start_ts,
            end_ts,
            top_n,
            commission_bp,
            stamp_pct,
            slippage_pct,
        )
        out[bucket] = {
            "trades": int(len(bucket_trades)),
            "signal_quality": metrics,
        }
    return out


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
    selected = assign_pool_size_bucket(selected)
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
    metrics = {
        "trade": trade_metrics(selected),
        "signal_quality": signal_quality,
        "nav_path": str(nav_path.resolve()),
        "trades_path": str(prefix.with_suffix(".trades.csv").resolve()),
    }
    if top_n == 5:
        metrics["pool_size_tertile_signal_quality"] = pool_bucket_signal_quality(
            selected,
            start_ts,
            end_ts,
            top_n,
            commission_bp,
            stamp_pct,
            slippage_pct,
        )
    return metrics, nav_df


def feature_importance_block(model: Any, features: list[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    importance = pd.DataFrame({
        "feature": features,
        "importance_gain": model.feature_importance(importance_type="gain"),
        "importance_split": model.feature_importance(importance_type="split"),
    }).sort_values("importance_gain", ascending=False)
    total_gain = float(importance["importance_gain"].sum())
    peer_gain = float(importance.loc[importance["feature"].isin(PEER_RELATIVE_FEATURES), "importance_gain"].sum())
    share = peer_gain / total_gain if total_gain > 0 else 0.0
    return importance.to_dict(orient="records"), {
        "total_gain": round(total_gain, 6),
        "peer_relative_gain": round(peer_gain, 6),
        "peer_relative_gain_share_pct": round(share * 100.0, 6),
        "low_importance_failure_threshold_pct": 5.0,
        "low_importance_failure": bool(share < 0.05),
    }


def run_window(
    df: pd.DataFrame,
    window: dict[str, Any],
    *,
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
        "v2_plus_peer_relative": [*V2_FEATURES, *PEER_RELATIVE_FEATURES],
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
    scores_by_model: dict[str, np.ndarray] = {}
    train_returns = train["return_pct"].to_numpy(dtype=float)
    for model_name, features in model_specs.items():
        model, scaler, train_info = train_ranker(train, features, params, num_boost_round)
        scores = score_frame(model, scaler, test, features)
        scores_by_model[model_name] = scores
        importance, importance_summary = feature_importance_block(model, features)
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

    block["deltas_v2_plus_peer_relative_minus_baseline"] = {}
    for top_n in top_ns:
        key = f"top{top_n}"
        base = block["models"]["v2_baseline"]["topn"][key]["signal_quality"]
        peer = block["models"]["v2_plus_peer_relative"]["topn"][key]["signal_quality"]
        block["deltas_v2_plus_peer_relative_minus_baseline"][key] = {
            metric: round(float(peer.get(metric, 0.0) - base.get(metric, 0.0)), 6)
            for metric in ["cum_return_pct", "cagr_pct", "max_dd_pct", "sharpe", "calmar", "daily_avg_ret_pct"]
        }
    base_rank = block["models"]["v2_baseline"]["rank_metrics"]
    peer_rank = block["models"]["v2_plus_peer_relative"]["rank_metrics"]
    block["rank_metric_deltas"] = {
        key: round(float(peer_rank.get(key, 0.0) - base_rank.get(key, 0.0)), 6)
        for key in ["ndcg_at_3", "ndcg_at_5", "ndcg_at_10"]
    }
    return block


def summarize_model_topn(windows: list[dict[str, Any]], model_name: str, top_n: int) -> dict[str, Any]:
    rows = []
    for item in windows:
        model = item.get("models", {}).get(model_name, {})
        top = model.get("topn", {}).get(f"top{top_n}", {})
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
    return summarize_windows([
        {
            "name": row["name"],
            "signal_quality": {
                "cagr_pct": row["cagr_pct"],
                "cum_return_pct": row["cum_return_pct"],
                "max_dd_pct": row["max_dd_pct"],
                "sharpe": row["sharpe"],
                "calmar": row["calmar"],
            },
            "trade": {
                "trades": row["trades"],
                "win_rate_pct": row["win_rate_pct"],
                "avg_return_pct": row["avg_return_pct"],
            },
        }
        for row in rows
    ], "signal_quality")


def summarize_deltas(windows: list[dict[str, Any]], top_n: int) -> dict[str, Any]:
    rows = []
    for item in windows:
        delta = item.get("deltas_v2_plus_peer_relative_minus_baseline", {}).get(f"top{top_n}", {})
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
        "top5_improvement_ge_2pct_folds": int((frame["cum_return_delta"] >= 2.0).sum()) if top_n == 5 else None,
        "top3_degrade_ge_1pct_folds": int((frame["cum_return_delta"] <= -1.0).sum()) if top_n == 3 else None,
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


def write_report(result: dict[str, Any], path: Path) -> None:
    lines = [
        "# Brick Peer-Relative Signal Quality NAV Phase 6",
        "",
        f"Created: {result['created_at']}",
        "",
        "## Boundary",
        "",
        f"- Handoff: `{result['data_boundary']['handoff_path']}`",
        f"- Candidate parquet: `{result['data_boundary']['candidate_path']}`",
        "- Candidate source: rebuilt daily-bar Brick V2 candidates; no legacy signal CSV is read.",
        "- Market timing: disabled.",
        f"- Split column: `{result['strict_forward_validation']['split_column']}`",
        f"- Train label purge: `{result['strict_forward_validation']['purge_rule']}`",
        f"- Peer pool: {result['peer_relative_construction']['pool_definition']}",
        "- Signal Quality NAV is an active selected-signal index, not a cash account.",
        "",
        "## Compute",
        "",
        f"- Backend: `{result['compute_acceleration']['selected_backend']}`",
        f"- GPU available: `{result['compute_acceleration']['gpu_available']}`",
        f"- LightGBM GPU probe: `{result['compute_acceleration']['lightgbm_gpu_probe']}`",
        "",
        "## Rolling OOS Stitched",
        "",
        "| Model | TopN | Final NAV | CAGR | MaxDD | Sharpe | Daily AvgRet |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key, metrics in result["rolling_oos_stitched"].items():
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
        "## Rolling Window Deltas",
        "",
        "| Window | Top3 CAGR Delta | Top3 CumRet Delta | Top5 CAGR Delta | Top5 CumRet Delta | NDCG@5 Delta |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ])
    for item in result["rolling_windows"]:
        d3 = item["deltas_v2_plus_peer_relative_minus_baseline"]["top3"]
        d5 = item["deltas_v2_plus_peer_relative_minus_baseline"]["top5"]
        lines.append(
            "| {name} | {d3c:.2f}% | {d3r:.2f}% | {d5c:.2f}% | {d5r:.2f}% | {ndcg:.4f} |".format(
                name=item["name"],
                d3c=d3.get("cagr_pct", 0.0),
                d3r=d3.get("cum_return_pct", 0.0),
                d5c=d5.get("cagr_pct", 0.0),
                d5r=d5.get("cum_return_pct", 0.0),
                ndcg=item["rank_metric_deltas"].get("ndcg_at_5", 0.0),
            )
        )
    lines.extend([
        "",
        "## Diagnostics",
        "",
        "```json",
        json.dumps({
            "correlation_diagnostics_train_2020_2022": result["correlation_diagnostics_train_2020_2022"],
            "summary": result["summary"],
        }, ensure_ascii=False, indent=2),
        "```",
        "",
        "## Promotion Note",
        "",
        "A peer-relative factor is not promotion-valid unless it improves Top5 capacity without materially degrading Top3, remains useful across rolling folds, and does not fail the redundancy or low-importance diagnostics.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    candidates, candidate_stats = load_or_rebuild_candidates(args, out_dir)
    panel, peer_stats = add_peer_relative_features(candidates)
    panel_path = out_dir / "brick_peer_relative_factor_panel.parquet"
    panel.to_parquet(panel_path, index=False)

    gpu_capability = detect_nvidia_gpu()
    acceleration = build_compute_acceleration_plan("ranker_training", gpu_capability)
    use_gpu = False
    gpu_probe_error = None
    if args.prefer_gpu and gpu_capability.available:
        use_gpu, gpu_probe_error = _probe_lightgbm_gpu()
    acceleration["lightgbm_gpu_probe"] = {"usable": bool(use_gpu), "error": gpu_probe_error}
    acceleration["selected_backend"] = "lightgbm_gpu" if use_gpu else "cpu"
    params = build_lgb_params(use_gpu=use_gpu, num_threads=args.threads)

    train_probe = panel[
        (panel["entry_date"] >= pd.Timestamp("2020-01-01"))
        & (panel["entry_date"] <= pd.Timestamp("2022-12-31"))
    ].copy()
    correlation_diagnostics = build_correlation_diagnostics(train_probe)

    top_ns = sorted(set(args.top_n))
    common = {
        "output_dir": out_dir,
        "params": params,
        "num_boost_round": args.num_boost_round,
        "top_ns": top_ns,
        "commission_bp": args.commission,
        "stamp_pct": args.stamp,
        "slippage_pct": args.slippage,
    }
    fixed = [run_window(panel, window, **common) for window in FIXED_WINDOWS]
    rolling = [run_window(panel, window, **common) for window in ROLLING_WINDOWS]

    rolling_oos: dict[str, Any] = {}
    for model_name in ["v2_baseline", "v2_plus_peer_relative"]:
        for top_n in top_ns:
            key = f"{model_name}_top{top_n}"
            rolling_oos[key] = stitch_rolling_nav(rolling, model_name, top_n, out_dir)

    result = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "strict_forward_validation": {
            "fixed_windows": FIXED_WINDOWS,
            "rolling_windows": ROLLING_WINDOWS,
            "split_column": "entry_date",
            "purge_rule": "train exit_date must be before each test_start",
            "validation_used": False,
            "test_years_unseen_by_each_window_model": True,
        },
        "data_boundary": {
            "handoff_path": str(Path(args.handoff_path).resolve()) if args.handoff_path else "",
            "candidate_path": str(Path(args.candidate_path).resolve()),
            "factor_panel": str(panel_path.resolve()),
            "candidate_rows": int(len(panel)),
            "candidate_signal_start": panel["signal_date"].min().strftime("%Y-%m-%d"),
            "candidate_signal_end": panel["signal_date"].max().strftime("%Y-%m-%d"),
            "entry_open_feature_formula": (
                "overnight_gap_pct uses entry_date open versus signal_date close; "
                "entry_open_to_yellow_pct and entry_open_to_ma5_pct use entry_date open versus signal-day yellow/MA5."
            ),
        },
        "candidate_source": candidate_stats,
        "peer_relative_construction": peer_stats,
        "compute_acceleration": acceleration,
        "features": {
            "v2_baseline": V2_FEATURES,
            "v2_plus_peer_relative": [*V2_FEATURES, *PEER_RELATIVE_FEATURES],
        },
        "correlation_diagnostics_train_2020_2022": correlation_diagnostics,
        "fixed_windows": fixed,
        "rolling_windows": rolling,
        "rolling_oos_stitched": rolling_oos,
        "summary": {
            "fixed": {
                model: {
                    f"top{top_n}": summarize_model_topn(fixed, model, top_n)
                    for top_n in top_ns
                }
                for model in ["v2_baseline", "v2_plus_peer_relative"]
            },
            "rolling": {
                model: {
                    f"top{top_n}": summarize_model_topn(rolling, model, top_n)
                    for top_n in top_ns
                }
                for model in ["v2_baseline", "v2_plus_peer_relative"]
            },
            "delta_v2_plus_peer_relative_minus_baseline": {
                "fixed": {f"top{top_n}": summarize_deltas(fixed, top_n) for top_n in top_ns},
                "rolling": {f"top{top_n}": summarize_deltas(rolling, top_n) for top_n in top_ns},
            },
        },
    }
    results_path = out_dir / "brick_peer_relative_sqnav_phase6_results.json"
    results_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(result, out_dir / "brick_peer_relative_sqnav_phase6_report.md")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brick peer-relative SQ NAV strict validation")
    parser.add_argument("--handoff-path", default="")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--candidate-path", default=str(DEFAULT_CANDIDATE_PATH))
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--entry-ma-source", default="t0", choices=["t0", "t1_open"])
    parser.add_argument("--rebuild-if-missing", action="store_true")
    parser.add_argument("--top-n", type=int, nargs="+", default=[3, 5])
    parser.add_argument("--num-boost-round", type=int, default=300)
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
        "candidate_rows": result["data_boundary"]["candidate_rows"],
        "rolling_oos_stitched": result["rolling_oos_stitched"],
        "summary": result["summary"]["delta_v2_plus_peer_relative_minus_baseline"]["rolling"],
        "compute_acceleration": result["compute_acceleration"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
