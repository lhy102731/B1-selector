"""v4.2 — Research Coverage Map (governance layer).

NOT a passive dashboard. ACTIVE governance:
- Auto-written by autonomous_runner._drain after every experiment
- Read by Research_Director on every invocation
- Triggers Pipeline_Controller to fire Director early when a category
  has coverage_score < 0.30 while another has rolling capital_share > 0.75

Files written:
  research_state/<subject>/coverage_map.yaml
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


CATEGORIES = ("architecture", "factor", "dimension", "kgpr")
STATUSES   = ("UNTOUCHED", "PARTIAL", "EXPLORED", "CONFIRMED", "ARCHIVED")


# Default coverage taxonomy. Subject seeds override starting status.
DEFAULT_TAXONOMY = {
    "architecture": [
        "brick_geometry", "constraint_geometry", "adaptive_brick",
        "multi_brick", "state_machine", "hierarchy", "regime_driven",
    ],
    "factor": [
        "trend", "momentum", "volatility", "acceleration", "asymmetry",
        "persistence", "entropy", "liquidity", "relative_strength",
    ],
    "dimension": [
        "cross_timeframe", "cross_asset", "regime", "market_microstructure",
        "participation", "breadth", "concentration", "structural_break",
    ],
    "kgpr": [
        "robustness", "sensitivity", "interaction", "saturation",
        "threshold", "stability",
    ],
}

# Subject-specific starting status. Based on Phase 9-15 closure.
SUBJECT_SEEDS = {
    "b1_v3": {
        "architecture": {
            "brick_geometry":      "EXPLORED",
            "constraint_geometry": "CONFIRMED",   # Phase 14B closed this
        },
        "factor": {
            "trend":      "EXPLORED",
            "momentum":   "EXPLORED",
            "volatility": "EXPLORED",
        },
        "dimension": {},   # Phase 14A confirmed: all UNTOUCHED
        "kgpr": {
            "robustness":  "EXPLORED",   # Phase 10
            "sensitivity": "EXPLORED",   # Phase 9
            "interaction": "EXPLORED",   # Phase 11/14B
        },
    },
}


def _path(subject: str) -> Path:
    p = Path("research_state") / subject
    p.mkdir(parents=True, exist_ok=True)
    return p / "coverage_map.yaml"


def _build_seed(subject: str) -> dict:
    seed = SUBJECT_SEEDS.get(subject, {})
    out = {
        "schema_version": "1.0",
        "subject": subject,
        "coverage": {},
        "coverage_score": {},
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    for cat, items in DEFAULT_TAXONOMY.items():
        cat_seed = seed.get(cat, {})
        out["coverage"][cat] = {
            "items": [
                {
                    "id": item,
                    "status": cat_seed.get(item, "UNTOUCHED"),
                    "last_touched_cycle": None,
                    "evidence": [],
                }
                for item in items
            ]
        }
    out["coverage_score"] = compute_scores(out["coverage"])
    return out


def load(subject: str) -> dict:
    p = _path(subject)
    if not p.exists():
        data = _build_seed(subject)
        save(subject, data)
        return data
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or _build_seed(subject)
    except Exception:
        data = _build_seed(subject)
    data["coverage_score"] = compute_scores(data.get("coverage", {}))
    return data


def save(subject: str, data: dict) -> None:
    p = _path(subject)
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    p.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
                 encoding="utf-8")


def compute_scores(coverage: dict) -> dict:
    """coverage_score = (EXPLORED + CONFIRMED) / total per category."""
    scored = {}
    for cat in CATEGORIES:
        items = (coverage.get(cat) or {}).get("items") or []
        if not items:
            scored[cat] = 0.0
            continue
        explored = sum(1 for it in items
                       if it.get("status") in ("EXPLORED", "CONFIRMED"))
        scored[cat] = round(explored / len(items), 4)
    return scored


# ---------------------------------------------------------------- tagging

_FACTOR_KEYWORDS = {
    "trend":             ["trend", "ma", "moving_avg", "moving average",
                          "white_line", "yellow_line"],
    "momentum":          ["momentum", "rsi", "macd", "dif", "dea", "kdj",
                          "j_max", "j_min"],
    "volatility":        ["volatility", "atr", "stddev", "vol_"],
    "acceleration":      ["accel", "acceleration"],
    "persistence":       ["persistence", "duration", "k_lt_d_days"],
    "liquidity":         ["turnover", "liquid"],
    "relative_strength": ["relative_strength", "rs_", "cs_", "cross_sectional_rank"],
    "asymmetry":         ["asymmetry", "skew"],
    "entropy":           ["entropy"],
}

_KGPR_KEYWORDS = {
    "robustness":  ["robust", "robustness", "window"],
    "sensitivity": ["sensitivity"],
    "interaction": ["interaction", "joint", "pair_removal", "remove_pair"],
    "threshold":   ["threshold"],
    "saturation":  ["saturation"],
    "stability":   ["stability", "stable"],
}

_DIM_KEYWORDS = {
    "cross_timeframe":       ["timeframe", "multi-frame", "multi_timeframe",
                              "weekly", "monthly"],
    "regime":                ["regime", "market_state"],
    "breadth":               ["breadth", "universe", "advance_decline"],
    "cross_asset":           ["cross-asset", "multi-asset", "cross_asset"],
    "market_microstructure": ["microstructure", "tick", "intraday"],
    "concentration":         ["concentration"],
    "structural_break":      ["structural_break", "regime_shift"],
    "participation":         ["participation"],
}

_ARCH_KEYWORDS = {
    "constraint_geometry": ["constraint_geometry", "hub_constraint",
                             "interaction_matrix", "wave_qualified"],
    "regime_driven":       ["regime_driven", "regime_machine"],
    "adaptive_brick":      ["adaptive_brick", "adaptive_geometry"],
    "state_machine":       ["state_machine"],
    "hierarchy":            ["hierarchy", "hierarchical"],
}


def infer_tags(entry: dict) -> list[tuple[str, str]]:
    """Heuristic: map a candidate entry to (category, item) tags."""
    tags: list[tuple[str, str]] = []
    h = (entry.get("hypothesis") or "").lower()
    params = entry.get("params") or {}
    code_change = entry.get("code_change") or {}

    # Factor tags
    for item, kws in _FACTOR_KEYWORDS.items():
        if any(kw in h for kw in kws):
            tags.append(("factor", item))
        # Also match against param names
        if any(any(kw in pname.lower() for kw in kws) for pname in params.keys()):
            tags.append(("factor", item))

    # KGPR tags — match against hypothesis or default to "threshold" for any param sweep
    matched_kgpr = False
    for item, kws in _KGPR_KEYWORDS.items():
        if any(kw in h for kw in kws):
            tags.append(("kgpr", item))
            matched_kgpr = True
    if params and not matched_kgpr:
        tags.append(("kgpr", "threshold"))   # default for any param sweep

    # Dimension tags
    for item, kws in _DIM_KEYWORDS.items():
        if any(kw in h for kw in kws):
            tags.append(("dimension", item))

    # Architecture tags
    for item, kws in _ARCH_KEYWORDS.items():
        if any(kw in h for kw in kws):
            tags.append(("architecture", item))
    if code_change:
        sym = (code_change.get("symbol") or "").lower()
        if any(k in sym for k in ("wave_", "surge_", "generator")):
            tags.append(("architecture", "constraint_geometry"))

    # Deduplicate while preserving order
    seen = set()
    out = []
    for tag in tags:
        if tag not in seen:
            seen.add(tag)
            out.append(tag)
    return out


def _advance_status(current: str, info_gain: int) -> str:
    """State transition: UNTOUCHED → PARTIAL → EXPLORED → CONFIRMED.

    Aggressiveness controlled by info_gain (0..4).
    """
    if current == "ARCHIVED":
        return current
    if current == "UNTOUCHED":
        return "PARTIAL"
    if current == "PARTIAL" and info_gain >= 2:
        return "EXPLORED"
    if current == "EXPLORED" and info_gain >= 3:
        return "CONFIRMED"
    return current


def update_from_entry(subject: str, entry: dict,
                     cycle_id: str | None = None,
                     info_gain: int = 0) -> dict:
    """Tag the entry's output to coverage map. Returns updated coverage data."""
    data = load(subject)
    tags = infer_tags(entry)
    if not tags:
        return data

    for category, item_id in tags:
        items = (data["coverage"].get(category) or {}).get("items") or []
        for it in items:
            if it["id"] == item_id:
                current = it.get("status", "UNTOUCHED")
                it["status"] = _advance_status(current, info_gain)
                it["last_touched_cycle"] = cycle_id
                it.setdefault("evidence", []).append({
                    "exp": entry.get("experiment_id"),
                    "ig": info_gain,
                    "at": cycle_id,
                })
                it["evidence"] = it["evidence"][-5:]
                break

    data["coverage_score"] = compute_scores(data["coverage"])
    save(subject, data)
    return data


# ---------------------------------------------------------------- queries

def lowest_coverage_area(subject: str) -> tuple[str, float] | None:
    data = load(subject)
    scores = data.get("coverage_score", {})
    if not scores:
        return None
    return min(scores.items(), key=lambda kv: kv[1])


def highest_coverage_area(subject: str) -> tuple[str, float] | None:
    data = load(subject)
    scores = data.get("coverage_score", {})
    if not scores:
        return None
    return max(scores.items(), key=lambda kv: kv[1])


def priority_candidates(subject: str,
                        threshold_score: float = 0.30,
                        threshold_share: float = 0.15) -> list[dict]:
    """Items in low-coverage AND low-capital areas — allocation candidates for Director."""
    from .capital_tracker import summary_for_director as cap_summary
    cap = cap_summary(subject)
    cov = load(subject)
    per_chan = cap.get("per_channel", {})

    candidates = []
    for cat, score in cov.get("coverage_score", {}).items():
        chan_share = per_chan.get(cat, {}).get("capital_share", 0)
        if score < threshold_score and chan_share < threshold_share:
            items = (cov["coverage"].get(cat) or {}).get("items") or []
            untouched = [it["id"] for it in items if it.get("status") == "UNTOUCHED"]
            candidates.append({
                "category": cat,
                "coverage_score": score,
                "capital_share": round(chan_share, 4),
                "untouched_items": untouched[:5],
            })
    return candidates


def check_coverage_imbalance(subject: str) -> dict | None:
    """Trigger condition: coverage<0.30 in some category AND rolling concentration>0.75 elsewhere."""
    cands = priority_candidates(subject)
    if not cands:
        return None
    from .capital_tracker import _rolling_concentration
    rolling = _rolling_concentration(subject)
    high = [(c, s) for c, s in rolling.items() if s > 0.75]
    if not high:
        return None
    return {
        "low_coverage_areas": cands,
        "high_concentration_channels": high,
        "rule": "category_coverage_below_0.30_while_other_above_0.75",
    }


def summary_for_director(subject: str) -> dict:
    """Compact summary passed to Research_Director's research_context."""
    data = load(subject)
    low = lowest_coverage_area(subject)
    high = highest_coverage_area(subject)
    return {
        "coverage_score": data.get("coverage_score", {}),
        "lowest_coverage_area": low,
        "highest_coverage_area": high,
        "priority_candidates": priority_candidates(subject),
    }
