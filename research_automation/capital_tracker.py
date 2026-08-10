"""capital_tracker.py - analytics-only research capital projection.

This module is NOT a governance or control layer. It is a read-only
analytics projection:

- pure projection: load / infer_channel / compute_metrics /
  _rolling_concentration / summary_for_director / estimate_cost /
  category_spend_estimate / aggregate_to_json only read input files or
  in-memory records and return projections;
- no state writes: save(), record_experiment(), record_round() and
  _append_event() fail closed with RuntimeError. Importing the module,
  loading a missing subject, or calling any projection never creates
  directories or files;
- no triggering: the tracker no longer claims to fire Pipeline_Controller
  or any other control surface. Governance decisions belong to the P6
  control plane only.

Legacy read locations (kept read-only for analytics):
  research_state/<subject>/capital_tracker.yaml
  research_state/<subject>/capital_events.jsonl
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import yaml


CHANNELS = ("architecture", "factor", "dimension", "kgpr", "maintenance")

# Estimated LLM tokens per agent invocation. Used only as a secondary signal;
# the PRIMARY capital measure is experiments_per_channel (mode-agnostic).
EST_TOKENS_PER_AGENT_CALL = 3000

# Analytics thresholds. The tracker reports these; it never enforces them.
ROLLING_WINDOW = 20
CONCENTRATION_THRESHOLD = 0.70
OVERFUND_SHARE = 0.40
UNDERFUND_SHARE = 0.10

WRITE_FORBIDDEN_MESSAGE = (
    "capital_tracker is analytics-only: state writes, event appends and "
    "governance triggering are forbidden outside the P6 control plane."
)


def _state_dir(subject: str) -> Path:
    # Read-only path resolution: never creates directories or writes.
    return Path("research_state") / subject


def _capital_path(subject: str) -> Path:
    return _state_dir(subject) / "capital_tracker.yaml"


def _events_path(subject: str) -> Path:
    return _state_dir(subject) / "capital_events.jsonl"


def _empty_record(subject: str) -> dict:
    return {
        "schema_version": "1.0",
        "subject": subject,
        "total_cycles": 0,
        "total_experiments": 0,
        "agent_usage": {},                       # agent_id -> {experiments, tokens}
        "channel_usage": {c: {"experiments": 0, "tokens": 0,
                              "info_gain_total": 0} for c in CHANNELS},
        "llm_usage": {},                         # profile -> {calls, tokens}
        "research_return": {c: {"information_gain": 0} for c in CHANNELS},
        "metrics": {},
        "updated_at": None,
    }


def load(subject: str) -> dict:
    """Read the tracker record if present; otherwise return an empty record.

    Read-only: a missing file never triggers a write.
    """
    p = _capital_path(subject)
    if not p.exists():
        return _empty_record(subject)
    try:
        return yaml.safe_load(p.read_text(encoding="utf-8")) or _empty_record(subject)
    except Exception:
        return _empty_record(subject)


def save(subject: str, data: dict) -> None:
    raise RuntimeError(WRITE_FORBIDDEN_MESSAGE)


def infer_channel(entry: dict) -> str:
    """Heuristic: map a candidate entry to one of CHANNELS.

    Override mechanism: if entry["channel"] is set, use that.
    Otherwise infer from code_change / params / hypothesis text.
    """
    if entry.get("channel") in CHANNELS:
        return entry["channel"]

    code_change = entry.get("code_change") or {}
    if code_change:
        sym = (code_change.get("symbol") or "").lower()
        if any(k in sym for k in ("wave_", "surge_", "structure", "generator")):
            return "architecture"
        if any(k in sym for k in ("ma", "rsi", "macd", "atr", "vol_",
                                   "factor", "j_", "k_lt_d")):
            return "factor"
        return "factor"

    params = entry.get("params") or {}
    hypothesis = (entry.get("hypothesis") or "").lower()

    if any(k in hypothesis for k in ("regime", "timeframe", "breadth",
                                       "cross-section", "cross_section")):
        return "dimension"
    if any(k in hypothesis for k in ("alpha family", "new factor",
                                       "factor discovery")):
        return "factor"
    if any(k in hypothesis for k in ("architecture", "generator", "wave")):
        return "architecture"
    if any(k in hypothesis for k in ("maintenance", "regime_refresh", "drift")):
        return "maintenance"

    return "kgpr"   # parameter sweeps are knowledge-generating param research by default

def _append_event(subject: str, ev: dict) -> None:
    raise RuntimeError(WRITE_FORBIDDEN_MESSAGE)


def record_experiment(subject: str, cycle_id: str, round_n: int,
                      entry: dict,
                      agents_used: Iterable[str] | None = None,
                      llm_profiles_used: Iterable[str] | None = None,
                      info_gain: int = 0) -> dict:
    """Fail closed: recording experiments would write state and events.

    The P6 control plane owns capital-state persistence; this analytics-only
    module must never write.
    """
    raise RuntimeError(WRITE_FORBIDDEN_MESSAGE)


def record_round(subject: str, cycle_id: str, round_n: int) -> dict:
    """Fail closed: recording rounds would write state."""
    raise RuntimeError(WRITE_FORBIDDEN_MESSAGE)


def _rolling_concentration(subject: str, window: int = ROLLING_WINDOW) -> dict[str, float]:
    """Share of the last window experiments per channel (read from event log)."""
    p = _events_path(subject)
    if not p.exists():
        return {ch: 0.0 for ch in CHANNELS}
    lines = [ln for ln in p.read_text(encoding="utf-8").strip().split("\n") if ln]
    last = lines[-window:] if len(lines) > window else lines
    counts: Counter = Counter()
    for line in last:
        try:
            ev = json.loads(line)
            counts[ev.get("channel", "kgpr")] += 1
        except Exception:
            pass
    total = sum(counts.values()) or 1
    return {ch: counts.get(ch, 0) / total for ch in CHANNELS}


def compute_metrics(subject: str, data: dict) -> dict:
    """Compute capital_share, knowledge_yield, ROI, waste, violations.

    Pure projection over the in-memory record; never writes.
    """
    # PRIMARY measure: experiments (mode-agnostic)
    total_exp = sum(c.get("experiments", 0) for c in data["channel_usage"].values())
    total_exp = max(total_exp, 1)
    total_tok = sum(c.get("tokens", 0) for c in data["channel_usage"].values())
    total_tok = max(total_tok, 1)

    per_channel = {}
    for ch in CHANNELS:
        cu = data["channel_usage"].get(ch, {})
        exp = cu.get("experiments", 0)
        tok = cu.get("tokens", 0)
        ig = data["research_return"].get(ch, {}).get("information_gain", 0)
        capital_share = exp / total_exp
        token_share = tok / total_tok if total_tok else 0
        knowledge_yield = (ig / max(exp, 1)) if exp else 0
        waste_index = (exp / max(ig, 1)) if ig > 0 else (
            float("inf") if exp > 0 else 0.0)
        per_channel[ch] = {
            "capital_share": round(capital_share, 4),
            "token_share":   round(token_share, 4),
            "knowledge_yield": round(knowledge_yield, 4),
            "research_roi":  round(knowledge_yield, 4),  # alias
            "waste_index": (
                round(waste_index, 2) if waste_index != float("inf") else None
            ),
            "experiments": exp,
            "tokens": tok,
            "info_gain_total": ig,
        }

    rolling = _rolling_concentration(subject)
    violations = []
    for ch, share in rolling.items():
        if share > CONCENTRATION_THRESHOLD:
            violations.append({
                "rule": f"no_channel_above_{int(CONCENTRATION_THRESHOLD*100)}pct_in_rolling_{ROLLING_WINDOW}",
                "channel": ch,
                "share": round(share, 4),
                "threshold": CONCENTRATION_THRESHOLD,
            })

    # ROI ranking - only channels that actually have info_gain
    rois = [(ch, m["research_roi"]) for ch, m in per_channel.items()
            if m["info_gain_total"] > 0]
    rois.sort(key=lambda x: -x[1])
    top_roi = rois[0][0] if rois else None
    lowest_roi = rois[-1][0] if rois else None

    yields = [m["knowledge_yield"] for m in per_channel.values()
              if m["knowledge_yield"] > 0]
    mean_yield = (sum(yields) / len(yields)) if yields else 0

    overfunded = [ch for ch, m in per_channel.items()
                  if m["capital_share"] > OVERFUND_SHARE
                  and (m["knowledge_yield"] < mean_yield or m["info_gain_total"] == 0)]
    underfunded = [ch for ch, m in per_channel.items()
                   if m["capital_share"] < UNDERFUND_SHARE
                   and m["experiments"] == 0]

    return {
        "per_channel": per_channel,
        "rolling_20_cycle_concentration": {k: round(v, 4) for k, v in rolling.items()},
        "top_roi_channel": top_roi,
        "lowest_roi_channel": lowest_roi,
        "overfunded_channels": overfunded,
        "underfunded_channels": underfunded,
        "violations": violations,
    }

def check_concentration_violation(subject: str) -> dict | None:
    """Return violation dict if any channel > threshold in rolling window.

    Analytics-only: reports the projection, never triggers an action.
    """
    rolling = _rolling_concentration(subject)
    for ch, share in rolling.items():
        if share > CONCENTRATION_THRESHOLD:
            return {
                "channel": ch,
                "share": round(share, 4),
                "window": ROLLING_WINDOW,
                "threshold": CONCENTRATION_THRESHOLD,
            }
    return None


def summary_for_director(subject: str) -> dict:
    """Compact read-only summary for a Director research_context."""
    data = load(subject)
    m = data.get("metrics", {})
    return {
        "total_experiments": data.get("total_experiments", 0),
        "total_cycles": data.get("total_cycles", 0),
        "per_channel": m.get("per_channel", {}),
        "rolling_20_concentration": m.get("rolling_20_cycle_concentration", {}),
        "top_roi_channel": m.get("top_roi_channel"),
        "lowest_roi_channel": m.get("lowest_roi_channel"),
        "overfunded": m.get("overfunded_channels", []),
        "underfunded": m.get("underfunded_channels", []),
        "violations": m.get("violations", []),
    }


# ============================================================
# Analytics-only cost projection
# ============================================================
# Per-profile LLM pricing in USD / 1M tokens. Multiplier-only model: actual
# spend = (tokens_in / 1e6) * in_price + (tokens_out / 1e6) * out_price.
# Values are conservative public-list estimates; override via env if you
# have better ones. Used ONLY for cost projection - never for gating.
LLM_PRICING_USD_PER_MTOKEN = {
    "deepseekv4":    {"in": 0.55, "out": 2.18},
    "glm51":         {"in": 0.14, "out": 0.55},
    "doubao":        {"in": 0.12, "out": 0.45},   # Seed 2.0 Pro alias
    "kimi":          {"in": 0.24, "out": 0.95},
    "minimax":       {"in": 0.20, "out": 0.80},
    "mimo":          {"in": 0.12, "out": 0.42},
    "gemini35flash": {"in": 0.075, "out": 0.30},
    "gpt55":         {"in": 1.25, "out": 7.50},
    "grok43":        {"in": 5.00, "out": 15.00},
}

# Conservative split between in/out tokens per agent invocation (default 30/70).
DEFAULT_TOKEN_SPLIT = {"in_pct": 0.30, "out_pct": 0.70}


def estimate_cost(profile: str, tokens: int) -> float | None:
    """Estimate USD cost for tokens consumed by profile.

    Uses the 30/70 in/out split assumption. Returns None if profile is
    unknown - caller should treat None as 0.
    """
    pricing = LLM_PRICING_USD_PER_MTOKEN.get(profile)
    if not pricing:
        return None
    in_tok = tokens * DEFAULT_TOKEN_SPLIT["in_pct"]
    out_tok = tokens * DEFAULT_TOKEN_SPLIT["out_pct"]
    return round((in_tok / 1e6) * pricing["in"]
                 + (out_tok / 1e6) * pricing["out"], 6)


def _iter_events(subject: str) -> Iterable[dict]:
    p = _events_path(subject)
    if not p.exists():
        return
    with p.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def aggregate_to_json(subject: str) -> dict:
    """Aggregate all capital_events into an in-memory analytics projection.

    Per-agent projected fields:
      tokens_in / tokens_out / cost : estimated (default in:out=30:70 split)
      accepted_proposals / rejected_proposals / confirmed_results / disproven_results
      info_gain_total
      info_gain_per_1k_tokens
      cost_per_info_gain_point
      last_10_cycles_active

    Also aggregates llm_usage + channel_usage returns.

    This is a pure aggregation pass. It returns the projection dict and
    NEVER writes agent_performance.json or any other file.
    """
    # Accumulators keyed by agent_id + by profile + by channel
    by_agent: dict[str, dict] = {}
    by_profile: dict[str, dict] = {}
    by_channel: dict[str, dict] = {}
    info_gain_total = 0

    seen_cycles: set[str] = set()
    cycle_per_agent: dict[str, set[str]] = {}

    for ev in _iter_events(subject):
        cycle_id = ev.get("cycle_id") or "unknown"
        seen_cycles.add(cycle_id)
        ig = int(ev.get("info_gain") or 0)
        info_gain_total += ig

        # Per agent (estimating tokens_in/out via the split; we attribute
        # the per-call estimate already added by record_experiment)
        for a in ev.get("agents") or []:
            ent = by_agent.setdefault(a, {
                "tokens_in": 0, "tokens_out": 0, "cost_total_usd": 0.0,
                "experiments": 0,
                "accepted_proposals": 0, "rejected_proposals": 0,
                "confirmed_results": 0, "disproven_results": 0,
                "info_gain_total": 0, "cycles_active": set(),
            })
            ent["experiments"] += 1
            ent["info_gain_total"] += ig
            ent["cycles_active"].add(cycle_id)
            # We don't track exactly which profile this agent ran under here;
            # that info is logged separately in llm_profiles. We approximate
            # by summing agent_usage from the yaml file below.

        for prof in ev.get("llm_profiles") or []:
            lk = by_profile.setdefault(prof, {
                "calls": 0, "tokens": 0, "cost_total_usd": 0.0,
                "info_gain_total": 0,
            })
            lk["calls"] += 1
            # token estimate: EST_TOKENS_PER_AGENT_CALL per call (rough)
            lk["tokens"] += EST_TOKENS_PER_AGENT_CALL
            lk["info_gain_total"] += ig

        ch = ev.get("channel") or "kgpr"
        ck = by_channel.setdefault(ch, {
            "experiments": 0, "info_gain_total": 0,
            "agents_seen": set(),
        })
        ck["experiments"] += 1
        ck["info_gain_total"] += ig
        for a in ev.get("agents") or []:
            ck["agents_seen"].add(a)

    # Now read the yaml tracker to get the authoritative per-agent token counts
    # (record_experiment accumulates these; events alone don't carry in/out).
    data = load(subject)
    au_yaml = data.get("agent_usage") or {}

    # Cost & efficiency per agent
    agent_rows = []
    for agent_id, ent in by_agent.items():
        yaml_ent = au_yaml.get(agent_id, {})
        tokens_total = int(yaml_ent.get("tokens") or ent.get("experiments", 0) * EST_TOKENS_PER_AGENT_CALL)
        # split tokens in/out 30/70 (default)
        in_tok = int(tokens_total * DEFAULT_TOKEN_SPLIT["in_pct"])
        out_tok = tokens_total - in_tok
        ent["tokens_in"] = in_tok
        ent["tokens_out"] = out_tok
        # lookup profile from ResearchConfig if available; fall back to Unknown
        profile = None
        try:
            from ag2_research.config import ResearchConfig
            tpl = ResearchConfig().get_agent(agent_id)
            if tpl:
                profile = tpl.get("profile") or ResearchConfig().default_profile
        except Exception:
            pass
        ent["profile"] = profile or "unknown"
        ent["cost_total_usd"] = estimate_cost(profile, tokens_total) or 0.0
        ig_total = ent["info_gain_total"]
        ent["info_gain_per_1k_tokens"] = (
            round(ig_total / (tokens_total / 1000), 4) if tokens_total > 0 else 0.0
        )
        ent["cost_per_info_gain_point"] = (
            round(ent["cost_total_usd"] / ig_total, 4) if ig_total > 0 else None
        )
        # status breakdown - defaulted; refined later when annotated events exist
        ent.setdefault("accepted_proposals", ent["experiments"])
        ent["cycles_active"] = sorted(ent["cycles_active"])[-10:]
        # remove the set before serializing
        agent_rows.append({k: v for k, v in ent.items() if k != "cycles_active_set"})

    # Sort by info_gain_total descending - most impactful agents first
    agent_rows.sort(key=lambda r: -r["info_gain_total"])

    # Per-profile cost rollup
    profile_rows = []
    for prof, en in by_profile.items():
        en["cost_total_usd"] = estimate_cost(prof, en["tokens"]) or 0.0
        en["info_gain_per_1k_tokens"] = (
            round(en["info_gain_total"] / (en["tokens"] / 1000), 4)
            if en["tokens"] > 0 else 0.0
        )
        en["cost_per_info_gain_point"] = (
            round(en["cost_total_usd"] / en["info_gain_total"], 4)
            if en["info_gain_total"] > 0 else None
        )
        en["agents_seen"] = sorted(en.get("agents_seen", set())) if en.get("agents_seen") else []
        profile_rows.append({"profile": prof, **en})

    profile_rows.sort(key=lambda r: -r["cost_total_usd"])

    # Per-channel roll up
    channel_rows = []
    for ch, en in by_channel.items():
        en["agents_seen"] = sorted(en["agents_seen"])
        channel_rows.append({"channel": ch, **en})

    return {
        "schema_version": "1.0",
        "subject": subject,
        "as_of_cycles_seen": len(seen_cycles),
        "as_of_info_gain_total": info_gain_total,
        "agents": agent_rows,
        "llm_profiles": profile_rows,
        "channels": channel_rows,
        "pricing_note": (
            "USD estimates use conservative per-1M-token pricing in "
            "LLM_PRICING_USD_PER_MTOKEN; token in/out split 30/70 default. "
            "Update env / constants for production figures."
        ),
    }


# Bucketed category budget projection: warn if any category over target.
CATEGORY_BUDGETS_USD = {
    # Soft monthly budget per category - triggers a softok warning, not a block
    "architecture":    25.0,
    "factor":          40.0,
    "dimension":       35.0,
    "kgpr":            30.0,
    "maintenance":     10.0,
}


def category_spend_estimate(subject: str,
                            per_event_tokens: int | None = None) -> dict:
    """Return per-category estimated USD spend over last N events using jsonl.

    Pure projection. The warning is informational; nothing is enforced.
    """
    per_event_tokens = per_event_tokens or EST_TOKENS_PER_AGENT_CALL
    spend = {c: 0.0 for c in CHANNELS}

    for ev in _iter_events(subject):
        ch = ev.get("channel") or "kgpr"
        # Use first llm profile of each event as a price proxy (conservative)
        profiles = ev.get("llm_profiles") or []
        if not profiles:
            continue
        prof = profiles[0]
        c = estimate_cost(prof, per_event_tokens) or 0.0
        spend[ch] = round(spend.get(ch, 0.0) + c, 6)

    warnings = []
    for ch, amt in spend.items():
        cap = CATEGORY_BUDGETS_USD.get(ch, float("inf"))
        if amt > cap:
            warnings.append({
                "channel": ch,
                "spent_usd_est": amt,
                "budget_usd": cap,
                "ratio": round(amt / cap, 4) if cap else None,
                "rule": "category_over_soft_budget",
            })

    return {
        "subject": subject,
        "spend_est": spend,
        "warnings": warnings,
        "budget": CATEGORY_BUDGETS_USD,
    }
