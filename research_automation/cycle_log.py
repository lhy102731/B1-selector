"""v4.1 — cycle_log writer.

Persists a per-experiment cycle log under:
  research_state/<subject>/cycle_log_<cycle_id>.yaml

The Research Director (autonomous_runner._collect_recent_events) reads these
to compute info_gain_zero_streak and max_surprise_last_5.

This is the minimal viable version (no LLM call):
  prediction      = baseline metrics (the natural null prediction)
  actual          = experiment's actual metrics
  surprise        = max |actual - prediction| / max(|prediction|, eps) per metric
  info_gain_score = heuristic from promotion_status (0..4)

A proper version, once Statistician and Research_Historian agents are wired,
would replace `prediction` with the Statistician's locked prediction and
`info_gain_score` with the Historian's structured assessment. Until then,
this null-baseline version is enough to drive Director triggers.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


# Metrics tracked in the surprise calculation. Fixed set — agents cannot extend.
_SURPRISE_METRICS = ("return", "max_drawdown", "sharpe", "profit_factor",
                     "trades", "win_rate")

# Promotion-status -> info_gain heuristic (used until Research_Historian is live).
_INFO_GAIN_FROM_STATUS = {
    "champion":   4,
    "promote":    4,
    "VERIFIED":   3,
    "PARTIAL":    2,
    "OPEN":       1,
    "FAILED":     0,
    "ABANDONED":  0,
    "duplicate":  0,
    None:         0,
}


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _compute_surprise(prediction: dict, actual: dict) -> dict:
    """Return per-metric surprise + the max across the fixed metric set."""
    per_metric: dict[str, float] = {}
    eps = 1e-6
    for k in _SURPRISE_METRICS:
        p = _safe_float(prediction.get(k))
        a = _safe_float(actual.get(k))
        if p is None or a is None:
            continue
        denom = max(abs(p), eps)
        per_metric[k] = abs(a - p) / denom
    max_score = max(per_metric.values()) if per_metric else 0.0
    surprise_metric = max(per_metric, key=per_metric.get) if per_metric else None
    return {
        "per_metric": {k: round(v, 4) for k, v in per_metric.items()},
        "max_surprise_score": round(max_score, 4),
        "surprise_metric": surprise_metric,
    }


def _baseline_to_dict(baseline) -> dict:
    """Turn a StandardMetrics into the dict shape we use for prediction."""
    if baseline is None:
        return {}
    extra = (getattr(baseline, "extra", None) or {})
    return {
        "return": extra.get("total_return"),
        "max_drawdown": getattr(baseline, "max_drawdown", None),
        "sharpe": getattr(baseline, "sharpe", None),
        "profit_factor": extra.get("profit_factor"),
        "trades": getattr(baseline, "trades", None),
        "win_rate": getattr(baseline, "win_rate", None),
    }


def _actual_from_entry(entry: dict) -> dict:
    """Pull the actual metrics from a candidate_pool entry (autonomous_runner._drain output)."""
    m = entry.get("metrics") or {}
    return {
        "return":        m.get("total_return") or m.get("return"),
        "max_drawdown":  m.get("max_drawdown"),
        "sharpe":        m.get("sharpe"),
        "profit_factor": m.get("profit_factor"),
        "trades":        m.get("trades"),
        "win_rate":      m.get("win_rate"),
    }


def _info_gain_from_entry(entry: dict) -> int:
    """Heuristic info_gain_score (0..4) from promotion status."""
    status = entry.get("promotion_status")
    return _INFO_GAIN_FROM_STATUS.get(status, 1)


def write_cycle_log(subject: str, cycle_id: str, round_n: int,
                    entry: dict, baseline,
                    strategy_state: dict | None = None) -> Path:
    """Write one cycle_log file for one experiment.

    Returns the path written to. Always returns even if subject has no KB —
    the file is the input for Research_Director triggers regardless of KB
    presence.

    Naming: cycle_log_<cycle_id>_r<round>_<experiment_id>.yaml
    """
    state_dir = Path("research_state") / subject
    state_dir.mkdir(parents=True, exist_ok=True)
    exp_id = entry.get("experiment_id", "unknown")
    safe_exp_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in str(exp_id))
    path = state_dir / f"cycle_log_{cycle_id}_r{round_n:02d}_{safe_exp_id}.yaml"

    prediction = _baseline_to_dict(baseline)
    actual = _actual_from_entry(entry)
    surprise = _compute_surprise(prediction, actual)
    ig = _info_gain_from_entry(entry)

    payload = {
        "schema_version": "1.0",
        "subject": subject,
        "cycle_id": cycle_id,
        "round": round_n,
        "experiment_id": exp_id,
        "strategy_state_at_start": strategy_state or {},
        "proposal": {
            "hypothesis": entry.get("hypothesis"),
            "params": entry.get("params"),
            "code_change": entry.get("code_change"),
            "scope": (entry.get("metrics", {}) or {}).get("scope"),
        },
        "prediction": {
            "metrics": prediction,
            "basis": "champion_baseline (v4.1 null prediction)",
            "locked_at": datetime.now(timezone.utc).isoformat(),
            "by_agent": "autopilot_baseline",
        },
        "actual": {
            "metrics": actual,
            "promotion_status": entry.get("promotion_status"),
            "experiment_status": entry.get("_experiment_status"),
        },
        "surprise": surprise,
        "info_gain": {
            "info_gain_score": ig,
            "basis": "heuristic_from_promotion_status (v4.1 placeholder)",
            "novelty": (
                "overturn" if ig == 4 else
                "refinement" if ig == 3 else
                "minor" if ig == 2 else
                "trivial" if ig == 1 else
                "duplicate"
            ),
        },
    }

    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return path


def list_recent_logs(subject: str, n: int = 5) -> list[Path]:
    """Return up to n most-recent cycle_log files."""
    state_dir = Path("research_state") / subject
    if not state_dir.exists():
        return []
    return sorted(state_dir.glob("cycle_log_*.yaml"))[-n:]
