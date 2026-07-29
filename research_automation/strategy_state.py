"""v4.1 — Strategy lifecycle state machine reader.

Reads research_state/<subject>/strategy_state.yaml. If absent, returns a
default exploring/discovery state — newer strategies are assumed unexplored
until proven otherwise.

Used by Pipeline_Controller (via autonomous_runner._round_memory_packet)
and Research_Director (via _invoke_director's governance context) to decide
channel allocation per cycle.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import yaml


State = Literal["exploring", "architecture_locked", "maintenance"]
Mode  = Literal["discovery", "execution", "maintenance"]

VALID_STATES = {"exploring", "architecture_locked", "maintenance"}
VALID_MODES  = {"discovery", "execution", "maintenance"}


def _path(subject: str) -> Path:
    return Path("research_state") / subject / "strategy_state.yaml"


# Default allocation per state (matches AG2_V4_DESIGN.md section 2).
DEFAULT_ALLOCATION = {
    "exploring": {
        "architecture": 0.40, "factor": 0.25, "dimension": 0.20,
        "kgpr": 0.10, "maintenance": 0.05,
    },
    "architecture_locked": {
        "architecture": 0.05, "factor": 0.25, "dimension": 0.40,
        "kgpr": 0.25, "maintenance": 0.05,
    },
    "maintenance": {
        "architecture": 0.00, "factor": 0.05, "dimension": 0.10,
        "kgpr": 0.25, "maintenance": 0.60,
    },
}


def _default(subject: str) -> dict:
    return {
        "schema_version": "1.0",
        "subject": subject,
        "state": "exploring",
        "mode": "discovery",
        "last_transition": datetime.now(timezone.utc).isoformat(),
        "transition_history": [],
        "confidence": {"architecture_locked_confidence": "low"},
        "allocation_target": dict(DEFAULT_ALLOCATION["exploring"]),
        "allocation_actual_last_5_cycles": {},
        "discovery_debt": {
            "open_questions_count": 0,
            "open_questions_high_priority": 0,
        },
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def load(subject: str) -> dict:
    """Return merged state. Falls back to default exploring state silently."""
    p = _path(subject)
    if not p.exists():
        return _default(subject)
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return _default(subject)
    data.setdefault("subject", subject)
    if data.get("state") not in VALID_STATES:
        data["state"] = "exploring"
    if data.get("mode") not in VALID_MODES:
        data["mode"] = "discovery"
    data.setdefault("allocation_target",
                    dict(DEFAULT_ALLOCATION[data["state"]]))
    return data


def save(subject: str, data: dict) -> Path:
    """Persist state. Writes parent dirs. Used by Director (and manual ops)."""
    p = _path(subject)
    p.parent.mkdir(parents=True, exist_ok=True)
    data["subject"] = subject
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    p.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
                 encoding="utf-8")
    return p


def get_allocation(subject: str) -> dict:
    """Return the per-channel allocation actually to use this cycle.

    Director overrides live in allocation_target; absence -> state default.
    """
    data = load(subject)
    return data.get("allocation_target") or dict(DEFAULT_ALLOCATION[data["state"]])


def apply_director_delta(subject: str, state_delta: dict,
                         mode_delta: dict | None = None,
                         allocation_delta: dict | None = None,
                         reason: str = "") -> dict:
    """Apply a Research_Director decision delta. Adds a transition_history entry."""
    data = load(subject)
    now = datetime.now(timezone.utc).isoformat()

    if state_delta and state_delta.get("state") in VALID_STATES:
        new_state = state_delta["state"]
        if new_state != data["state"]:
            data["transition_history"].append({
                "at": now, "from": data["state"], "to": new_state,
                "reason": reason,
            })
            data["state"] = new_state
            data["allocation_target"] = dict(DEFAULT_ALLOCATION[new_state])

    if mode_delta and mode_delta.get("mode") in VALID_MODES:
        data["mode"] = mode_delta["mode"]

    if allocation_delta:
        data["allocation_target"].update(allocation_delta)

    if state_delta.get("confidence"):
        data["confidence"].update(state_delta["confidence"])

    data["discovery_debt"] = state_delta.get("discovery_debt",
        data.get("discovery_debt",
                 {"open_questions_count": 0, "open_questions_high_priority": 0}))
    data["last_transition"] = now

    save(subject, data)
    return data


def read_latest_director_decision(subject: str) -> dict | None:
    """Return the most recent director_decisions.yaml entry, or None.

    Used by Pipeline_Controller at Step 0: the director's last decision is
    embedded in the memory_packet as director_directives.
    """
    p = Path("research_state") / subject / "director_decisions.yaml"
    if not p.exists():
        return None
    try:
        entries = yaml.safe_load(p.read_text(encoding="utf-8")) or []
    except Exception:
        return None
    return entries[-1] if entries else None


def summary_for_director(subject: str) -> dict:
    """Compact state summary injected into Research_Director's research_context
    alongside the v4.2 governance summary (Capital/Coverage).

    Tells the Director: what state are we in, what mode, what allocation is
    in force, how much discovery debt remains, and the Open Questions list.
    """
    data = load(subject)

    oq_path = Path("research_state") / subject / "open_questions.yaml"
    open_questions = []
    if oq_path.exists():
        try:
            oq = yaml.safe_load(oq_path.read_text(encoding="utf-8")) or {}
            open_questions = [
                {
                    "id": q["id"], "status": q["status"],
                    "priority": q.get("priority", "medium"),
                    "text": q["text"],
                    "blocking_for_mode": q.get("blocking_for_mode", []),
                }
                for q in oq.get("questions", [])
                if q.get("status") in ("OPEN", "PARTIALLY_EXPLORED")
            ]
        except Exception:
            pass

    return {
        "subject": subject,
        "state": data["state"],
        "mode": data["mode"],
        "confidence": data["confidence"],
        "allocation_target_in_force": data["allocation_target"],
        "discovery_debt": data["discovery_debt"],
        "open_questions": open_questions,
    }