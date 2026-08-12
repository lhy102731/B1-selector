"""Lease-bound Operational terminal audit and Campaign CLOSED (P8R3 T5).

The terminal audit event and the COMPLETED -> CLOSED transition commit in a
single Operational transaction.  The happy-path writer accepts only a real
P8 TaskExecutionLease with a matching fixed-claim binding; the recovery
writer accepts only a FinalEvalRecoveryLease whose effect set excludes
OPEN_HOLDOUT/evaluate/reissue.  Caller-supplied ids, raw lease hashes and
other object types are rejected.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

from .contracts import canonical_json
from .sqlite_uow import _SqliteUnitOfWork
from .stores import (
    FinalEvalRecoveryLease,
    SideEffect,
    TaskExecutionLease,
    _operational_spec,
)


class FinalEvalClosureError(RuntimeError):
    """Base error for the final evaluation closure writer."""


class FinalEvalClosureRejected(FinalEvalClosureError):
    """The closure request failed binding/lease validation."""


class FinalEvalClosureConflict(FinalEvalClosureError):
    """A second terminal event for the same campaign conflicts."""


_FIXED_CLAIM_FIELDS = frozenset(
    {
        "campaign_id",
        "request_sha256",
        "result_object_sha256",
        "result_claim_sha256",
        "verdict",
        "evidence_ref",
    }
)


def _validate_claim_binding(
    claim: Mapping[str, object],
    *,
    campaign_id: str,
    request_sha256: str,
) -> None:
    if not isinstance(claim, Mapping):
        raise FinalEvalClosureRejected("fixed claim must be a mapping")
    if set(claim) != _FIXED_CLAIM_FIELDS:
        raise FinalEvalClosureRejected("fixed claim has an invalid field set")
    if str(claim.get("campaign_id")) != campaign_id:
        raise FinalEvalClosureRejected("fixed claim campaign mismatch")
    if str(claim.get("request_sha256")) != request_sha256:
        raise FinalEvalClosureRejected("fixed claim request mismatch")
    for field in ("result_object_sha256", "result_claim_sha256"):
        value = str(claim.get(field))
        if (
            len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
        ):
            raise FinalEvalClosureRejected(
                f"fixed claim {field} must be a SHA-256 digest"
            )


def _require_effect_allowed(lease, effect: SideEffect) -> None:
    if effect not in lease.allowed_side_effects:
        raise FinalEvalClosureRejected(
            f"lease does not allow effect {effect.value}"
        )


def close_completed_campaign(
    *,
    lease: TaskExecutionLease,
    campaign_id: str,
    fixed_claim: Mapping[str, object],
    clock=None,
) -> dict[str, object]:
    """Append the terminal audit and CLOSED transition in one transaction.

    Only a real P8 TaskExecutionLease with WRITE_CONTROL_PLANE is accepted;
    the fixed claim must match the campaign/request binding exactly.  A
    second terminal event for the same campaign conflicts.
    """
    if not isinstance(lease, TaskExecutionLease):
        raise FinalEvalClosureRejected("closure requires a real TaskExecutionLease")
    if not isinstance(lease, FinalEvalRecoveryLease):
        _require_effect_allowed(lease, SideEffect.WRITE_CONTROL_PLANE)
    _validate_claim_binding(
        fixed_claim,
        campaign_id=campaign_id,
        request_sha256=str(fixed_claim.get("request_sha256")),
    )
    now = (clock() if clock else datetime.now(timezone.utc)).astimezone(
        timezone.utc
    )
    terminal_event_id = (
        f"terminal-{campaign_id}-"
        + str(fixed_claim.get("result_claim_sha256"))[:16]
    )

    def commit(connection) -> dict[str, object]:
        existing = connection.execute(
            "SELECT event_id FROM campaign_events "
            "WHERE event_type = 'CAMPAIGN_CLOSED' AND campaign_id = ?",
            (campaign_id,),
        ).fetchone()
        if existing is not None:
            raise FinalEvalClosureConflict(
                "campaign already has a terminal CLOSED event"
            )
        import hashlib
        import json as _json

        payload = {
            "schema_version": "control_plane.final_eval_terminal_audit.v1",
            "campaign_id": campaign_id,
            "request_sha256": fixed_claim["request_sha256"],
            "result_object_sha256": fixed_claim["result_object_sha256"],
            "result_claim_sha256": fixed_claim["result_claim_sha256"],
            "verdict": fixed_claim["verdict"],
            "evidence_ref": fixed_claim["evidence_ref"],
            "promotion": "MANUAL_ONLY",
            "closed_at": now.isoformat(),
        }
        payload_json = _json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        )
        payload_sha256 = hashlib.sha256(
            payload_json.encode("utf-8")
        ).hexdigest()
        connection.execute(
            """INSERT INTO campaign_events
            (event_id, namespace, campaign_id, cycle_id, aggregate_type,
             aggregate_id, event_type, payload_json, payload_sha256,
             occurred_at)
            VALUES (?, 'formal', ?, NULL, 'campaign', ?, 'CAMPAIGN_CLOSED',
                    ?, ?, ?)""",
            (
                terminal_event_id,
                campaign_id,
                campaign_id,
                payload_json,
                payload_sha256,
                now.isoformat(),
            ),
        )
        return payload

    return _SqliteUnitOfWork(_operational_spec())._write(commit)


__all__ = [
    "FinalEvalClosureConflict",
    "FinalEvalClosureError",
    "FinalEvalClosureRejected",
    "close_completed_campaign",
]
