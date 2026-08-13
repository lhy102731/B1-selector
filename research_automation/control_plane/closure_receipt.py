"""Authoritative phase-gate closure receipt builder (CR-010 F-01).

The legacy activation scripts hand-wrote closure receipt JSON with a LOCAL
timezone offset while the Authority DB stores the canonical UTC ``closed_at``
from ``_close_phase_gate``.  That mixed time base makes the declared closure
time mechanically compare BEFORE the gate ``created_at`` even though the
wall-clock order was correct.

This builder reads the AUTHORITATIVE closure row from the Authority store
(UTC) and emits a receipt whose ``closed_at`` is canonical UTC (``Z``).  It
also enforces the causality contract: ``closed_at > gate.created_at``, so a
receipt that cannot prove the gate was closed after it was built is refused.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path

from .contracts import Phase, canonical_json
from .gates import parse_gate_report_v1_bytes
from .stores import AuthorityReader, _utc_text


class ClosureReceiptError(RuntimeError):
    """Base error for closure receipt building."""


def build_closure_receipt(
    *,
    root_secret: str,
    phase: str,
    attempt_id: str,
    gate_report_path: str | Path,
    repository_root: str | Path,
) -> dict[str, object]:
    """Build an authoritative UTC closure receipt from the Authority row.

    Requires the gate report (committed) and the already-closed Authority
    row for (phase, attempt).  The emitted receipt binds:
    - gate_ref / gate_report_sha256 from the committed gate report;
    - closure_id / grant_id / verdict / closed_at from the Authority row;
    - ``closed_at`` is canonical UTC and mechanically > gate ``created_at``.
    """
    root = Path(repository_root).resolve(strict=True)
    gate_path = Path(gate_report_path).resolve()
    if not str(gate_path).startswith(str(root)):
        raise ClosureReceiptError("gate report must live under the repository")
    try:
        raw = gate_path.read_bytes()
    except OSError as error:
        raise ClosureReceiptError("gate report is unreadable") from error
    report = parse_gate_report_v1_bytes(raw)
    if report["phase"] != phase or report["attempt_id"] != attempt_id:
        raise ClosureReceiptError(
            "gate report phase/attempt does not match the request"
        )
    gate_created = datetime.fromisoformat(
        str(report["created_at"]).replace("Z", "+00:00")
    )
    if gate_created.tzinfo is None or gate_created.utcoffset() != timezone.utc:
        raise ClosureReceiptError("gate report created_at must be canonical UTC")

    reader = AuthorityReader()
    closure = reader.phase_gate_closure(Phase(phase), attempt_id)
    if closure is None:
        raise ClosureReceiptError(
            "no authoritative closure row exists for this phase/attempt"
        )
    closed_at = closure.closed_at
    if closed_at.tzinfo is None:
        closed_at = closed_at.replace(tzinfo=timezone.utc)
    closed_utc = closed_at.astimezone(timezone.utc)
    if closed_utc <= gate_created:
        raise ClosureReceiptError(
            "closure closed_at must be after the gate created_at "
            "(causality contract violated)"
        )
    if closure.gate_report_sha256 != str(report["gate_report_sha256"]):
        raise ClosureReceiptError(
            "closure row binds a different gate report hash"
        )
    gate_ref = str(gate_path.relative_to(root)).replace("\\", "/")
    tickets: list[str] = []
    snapshot = report.get("authority_snapshot")
    if isinstance(snapshot, Mapping):
        succeeded = snapshot.get("succeeded_ticket_ids")
        if isinstance(succeeded, Sequence):
            tickets = [str(item) for item in succeeded]
    receipt = {
        "schema": "control_plane.phase_gate_closure_receipt.v1",
        "phase": phase,
        "attempt_id": attempt_id,
        "closure_id": closure.closure_id,
        "grant_id": closure.grant_id,
        "gate_ref": gate_ref,
        "gate_report_sha256": closure.gate_report_sha256,
        "closed_at": _utc_text(closed_utc),
        "verdict": closure.verdict,
        "tickets": tickets,
        "trust_root": (
            "CR-010 F-01: closed_at is canonical UTC read from the "
            "authoritative Authority closure row; mechanically provable "
            "closed_at > gate.created_at."
        ),
    }
    # Parse/validate round-trip to guarantee the emitted bytes are canonical.
    canonical_json(receipt)
    return receipt


def serialize_receipt(receipt: Mapping[str, object]) -> str:
    return canonical_json(receipt) + "\n"


__all__ = [
    "ClosureReceiptError",
    "build_closure_receipt",
    "serialize_receipt",
]
