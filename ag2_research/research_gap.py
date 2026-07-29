"""Build source-only KBase discovery requests from compiled project state."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable


def _texts(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        values = []
        for item in value:
            values.extend(_texts(item))
        return values
    if isinstance(value, dict):
        values = []
        for key in ("primary", "secondary", "item", "condition", "name", "title"):
            if key in value:
                values.extend(_texts(value[key]))
        return values
    return [str(value).strip()] if str(value).strip() else []


def _records(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, list):
        yield from (item for item in value if isinstance(item, dict))
    elif isinstance(value, dict):
        yielded = False
        for key in (
            "factors", "components", "directions", "items", "experiments",
            "lessons", "entries",
        ):
            if isinstance(value.get(key), list):
                yielded = True
                yield from (item for item in value[key] if isinstance(item, dict))
        if not yielded and value:
            yield value


def _label(record: dict[str, Any]) -> str:
    for key in ("factor", "direction", "name", "title", "id", "component"):
        text = str(record.get(key) or "").strip()
        if text:
            return text
    return ""


def _stable_id(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_research_gap_request(project_state: dict[str, Any]) -> dict[str, Any]:
    """Describe what KBase must investigate without claiming an unseen factor."""
    if project_state.get("schema_version") != "ag2.project_state_packet.v1":
        raise ValueError("unsupported project_state schema")

    coverage: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def add(label: str, status: str, evidence: str, detail: str = "") -> None:
        clean = str(label or "").strip()
        key = (clean.lower(), status)
        if not clean or key in seen:
            return
        seen.add(key)
        coverage.append({
            "dimension": clean,
            "status": status,
            "evidence": evidence,
            "detail": detail,
        })

    memory = project_state.get("memory") or {}
    snapshot = memory.get("snapshot") or {}
    handoff = memory.get("handoff") or {}
    registry = memory.get("registry") or {}
    for item in _texts(snapshot.get("next_priority")):
        add(item, "covered_shallow", "snapshot.next_priority")
    for item in _texts(handoff.get("active_focus")):
        add(item, "covered_shallow", "handoff.active_focus")
    for item in _texts(snapshot.get("rejected_directions")):
        add(item, "covered_failed", "snapshot.rejected_directions")
    for item in _texts(snapshot.get("frozen_directions")):
        add(item, "excluded", "snapshot.frozen_directions")
    for entry in registry.get("experiments", []):
        label = _label(entry)
        status = str(entry.get("status") or "").upper()
        mapped = {
            "OPEN": "covered_shallow",
            "FAILED": "covered_failed",
            "ABANDONED": "covered_failed",
            "VERIFIED": "covered_validated",
        }.get(status)
        if mapped:
            add(label, mapped, f"registry:{entry.get('id') or 'unknown'}", str(entry.get("short_result") or ""))

    headline = (project_state.get("project_kb") or {}).get("headline") or {}
    for item in _texts(headline.get("primary_open_directions")):
        add(item, "covered_shallow", "project_kb.headline.primary_open_directions")

    artifacts = (project_state.get("project_kb") or {}).get("artifacts") or {}
    for key, value in artifacts.items():
        lower = str(key).lower()
        if "forbidden" in lower:
            mapped_status = "excluded"
        elif "archived" in lower or "dead" in lower or "redundant" in lower:
            mapped_status = "covered_failed"
        else:
            continue
        for record in _records(value):
            add(_label(record), mapped_status, f"project_kb.artifacts.{key}", str(record.get("reason") or record.get("status") or ""))

    for item in (project_state.get("research_history") or {}).get("current_priority_items", []):
        add(str(item), "covered_shallow", "research_history.current_priority_items")

    open_coverage = [item for item in coverage if item["status"] == "covered_shallow"]
    candidate_gaps = [
        {
            "gap_id": _stable_id({"dimension": item["dimension"], "evidence": item["evidence"]})[:16],
            "label": item["dimension"],
            "status": "deepen_existing",
            "reason": "Project evidence marks this area open or only partially resolved.",
            "project_evidence": item["evidence"],
        }
        for item in open_coverage[:12]
    ]
    candidate_gaps.append({
        "gap_id": "orthogonal-unseen-scan",
        "label": "KBase 中与当前项目覆盖机制距离较远的来源分支",
        "status": "unseen_scan_required",
        "reason": "Absence from project memory is not proof of novelty; KBase must search and compare before classification.",
        "project_evidence": "project_state_packet",
    })

    request: dict[str, Any] = {
        "schema_version": "ag2.research_gap_request.v1",
        "strategy_id": project_state["strategy_id"],
        "optimization_request": project_state["optimization_request"],
        "project_state_fingerprint": project_state["project_state_fingerprint"],
        "catalog_version": (project_state.get("kbase_release") or {}).get("catalog_version"),
        "semantic_release_fingerprint": (project_state.get("kbase_release") or {}).get("bundle_fingerprint"),
        "project_coverage": coverage,
        "candidate_gaps": candidate_gaps,
        "discovery_requirements": [
            "Search source-backed branches that deepen covered_shallow dimensions.",
            "Deliberately inspect at least one sparse or mechanism-distant branch.",
            "Separate independent voices from reposts, compilations, and narrated adaptations.",
            "Return disagreements, conditions, sequences, limits, and missing evidence.",
            "Do not call an unseen_scan_required branch novel until compared with project coverage.",
        ],
        "kbase_boundary": {
            "allowed_outputs": [
                "source statements", "source context", "conditions", "sequences",
                "disagreements", "limits", "missing evidence", "coverage comparison",
            ],
            "forbidden_outputs": [
                "hypothesis", "mechanism", "factor", "proxy", "formula", "parameter",
                "experiment queue", "backtest design", "production recommendation",
            ],
        },
    }
    request["request_id"] = _stable_id(request)
    return request
