"""Persist and read KBase discovery handoffs for downstream AG2 research."""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from research_automation.control_plane.contracts import SideEffect
from research_automation.control_plane.sink_guard import (
    AuthorizedPathMutation,
    ExecutionInvocation,
)
from research_automation.control_plane.stores import AuthorityReader, TaskExecutionLease


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_HANDOFF_DIR = PROJECT_ROOT / "research_state" / "kbase_discovery_handoffs"


def save_discovery_handoff(
    result: dict[str, Any],
    *,
    topic: str,
    strategy_id: str,
    output_dir: str | Path | None = None,
    created_at: datetime | None = None,
    lease: TaskExecutionLease | None = None,
    invocation: ExecutionInvocation | None = None,
    execution_lease: TaskExecutionLease | None = None,
    execution_invocation: ExecutionInvocation | None = None,
    authority_reader: AuthorityReader | None = None,
    repository_root: str | Path | None = None,
) -> Path:
    """Atomically persist a complete discovery result outside the source KBase."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", strategy_id or ""):
        raise ValueError("strategy_id may contain only letters, numbers, '_' and '-'")
    destination = Path(output_dir or DEFAULT_HANDOFF_DIR).resolve()
    if created_at is None:
        created_at = datetime.now(timezone.utc)
    if not isinstance(created_at, datetime):
        raise TypeError("created_at must be a datetime")
    status = str(result.get("status") or "UNKNOWN").upper()
    safe_status = re.sub(r"[^A-Z0-9_-]+", "_", status).strip("_") or "UNKNOWN"
    document = {
        "handoff_type": "kbase_discovery",
        "schema_version": 1,
        "created_at": created_at.isoformat(),
        "strategy_id": strategy_id,
        "topic": topic,
        "status": status,
        "result": result,
    }
    filename = (
        f"discovery_{strategy_id}_{safe_status}_"
        f"{created_at.strftime('%Y%m%dT%H%M%S%fZ')}.yaml"
    )
    target = destination / filename
    temporary_path = destination / f".{filename}.tmp"
    AuthorizedPathMutation(
        authority_reader=authority_reader or AuthorityReader(),
        repository_root=repository_root or PROJECT_ROOT,
    ).authorize(
        lease if lease is not None else execution_lease,
        invocation if invocation is not None else execution_invocation,
        operation="KBASE_WRITE",
        effect=SideEffect.WRITE_KBASE,
        module="ag2_research.discovery_handoff",
        callable_name="save_discovery_handoff",
        paths=(destination, target, temporary_path),
    )
    if target.exists():
        raise FileExistsError(f"discovery handoff already exists: {target}")
    destination.mkdir(parents=True, exist_ok=True)
    try:
        with temporary_path.open("x", encoding="utf-8", newline="\n") as handle:
            yaml.safe_dump(document, handle, allow_unicode=True, sort_keys=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, target)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
    return target


def extract_discovery_transcript(document: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Return the discovery transcript from legacy or roundtable handoff shapes."""
    if not isinstance(document, dict):
        return None
    result = document.get("result")
    if not isinstance(result, dict):
        return None
    transcript = result.get("transcript")
    if isinstance(transcript, list):
        return transcript
    discovery = result.get("discovery")
    if isinstance(discovery, dict) and isinstance(discovery.get("transcript"), list):
        return discovery["transcript"]
    return None


def extract_stage_outputs(transcript: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Map stage name to its structured output, ignoring malformed entries."""
    return {
        step.get("stage"): step.get("output")
        for step in transcript
        if isinstance(step, dict)
        and isinstance(step.get("stage"), str)
        and isinstance(step.get("output"), dict)
    }


def load_latest_approved_discovery(
    strategy_id: str,
    *,
    handoff_dir: str | Path | None = None,
) -> dict[str, Any] | None:
    """Return the newest structurally complete APPROVED handoff, or ``None``."""
    root = Path(handoff_dir or DEFAULT_HANDOFF_DIR).resolve()
    if not root.is_dir():
        return None
    candidates = sorted(root.glob(f"discovery_{strategy_id}_APPROVED_*.yaml"), reverse=True)
    for path in candidates:
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, UnicodeError, yaml.YAMLError):
            continue
        if not isinstance(document, dict) or document.get("handoff_type") != "kbase_discovery":
            continue
        if document.get("status") != "APPROVED" or document.get("strategy_id") != strategy_id:
            continue
        transcript = extract_discovery_transcript(document)
        if not transcript:
            continue
        outputs = extract_stage_outputs(transcript)
        factor_output = outputs.get("factor_engineer") or {}
        if not (factor_output.get("factor_batch") or factor_output.get("research_mechanism")):
            continue
        return {
            "path": str(path),
            "created_at": document.get("created_at"),
            "topic": document.get("topic"),
            "source_brief": outputs.get("source_librarian"),
            "alpha_discovery": outputs.get("alpha_hunter"),
            "factor_handoff": factor_output,
        }
    return None


def render_discovery_context(strategy_id: str, *, handoff_dir: str | Path | None = None) -> str:
    """Render a bounded, explicit project-side handoff for an AG2 proposer."""
    handoff = load_latest_approved_discovery(strategy_id, handoff_dir=handoff_dir)
    if not handoff:
        return ""
    payload = {
        "handoff_path": handoff["path"],
        "topic": handoff["topic"],
        "alpha_discovery": handoff["alpha_discovery"],
        "factor_handoff": handoff["factor_handoff"],
    }
    return (
        "\nLATEST APPROVED KBASE DISCOVERY HANDOFF\n"
        "This is inspiration, not validation. Do not map a new factor onto an existing "
        "parameter unless the mapping is exact and stated.\n"
        + yaml.safe_dump(payload, allow_unicode=True, sort_keys=False)
    )
