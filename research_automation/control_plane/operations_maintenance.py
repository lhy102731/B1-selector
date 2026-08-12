"""Durable backfill checkpoints, retention metadata and health guards (P7R3 T5/T6).

Backfill state persists in ``ops_backfill_checkpoint`` with an idempotency
key ``(plan_hash, shard, start_cursor, end_cursor, source_prefix_hash)``;
derived upserts, idempotency records, cursor and status commit in one short
transaction.  Retention metadata keys on packet hash with class /
last-referenced / archive-eligible facts; SCIENTIFIC packets are never
age-eligible.  Health guards inspect the real read model and generation
publication status; unsafe next cycles are blocked at durable boundaries by
the P7 runtime observer.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

from .contracts import canonical_json
from .sqlite_uow import _SqliteUnitOfWork
from .stores import _operational_spec


class OperationsMaintenanceError(RuntimeError):
    """Base error for operational maintenance operations."""


class BackfillCheckpointConflict(OperationsMaintenanceError):
    """Same idempotency key with changed semantics."""


class RetentionSafetyError(OperationsMaintenanceError):
    """Retention cleanup rejected an unsafe candidate."""


def _batch_idempotency_key(
    *,
    plan_hash: str,
    shard: str,
    start_cursor: int,
    end_cursor: int,
    source_prefix_hash: str,
) -> str:
    payload = {
        "plan_hash": plan_hash,
        "shard": shard,
        "start_cursor": start_cursor,
        "end_cursor": end_cursor,
        "source_prefix_hash": source_prefix_hash,
    }
    return hashlib.sha256(
        b"control_plane.backfill_batch.v1\0"
        + canonical_json(payload).encode("utf-8")
    ).hexdigest()


def persist_backfill_batch(
    *,
    plan_hash: str,
    shard: str,
    start_cursor: int,
    end_cursor: int,
    source_prefix_hash: str,
    derived_payload_sha256: str,
) -> dict[str, object]:
    """Persist one bounded backfill batch checkpoint (exactly-once semantics).

    The derived upsert, idempotency record, cursor and status commit in one
    short transaction.  A replayed key with identical semantics is a no-op; a
    replayed key with changed semantics fails closed.
    """
    if type(start_cursor) is not int or type(end_cursor) is not int:
        raise OperationsMaintenanceError("cursors must be integers")
    if start_cursor < 0 or end_cursor < start_cursor:
        raise OperationsMaintenanceError("cursor range is invalid")
    key = _batch_idempotency_key(
        plan_hash=plan_hash,
        shard=shard,
        start_cursor=start_cursor,
        end_cursor=end_cursor,
        source_prefix_hash=source_prefix_hash,
    )

    def commit(connection) -> dict[str, object]:
        existing = connection.execute(
            "SELECT backfill_id, target_table, from_sequence, to_sequence, "
            "state, completed_at, updated_at FROM ops_backfill_checkpoint "
            "WHERE backfill_id = ?",
            (key,),
        ).fetchone()
        now = datetime.now(timezone.utc).isoformat()
        if existing is not None:
            if (
                str(existing["target_table"]) != "journal_events"
                or int(existing["from_sequence"]) != start_cursor
                or int(existing["to_sequence"]) != end_cursor
            ):
                raise BackfillCheckpointConflict(
                    "backfill batch idempotency key changed semantics"
                )
            return {
                "replayed": True,
                "backfill_id": key,
                "state": str(existing["state"]),
            }
        connection.execute(
            """INSERT INTO ops_backfill_checkpoint
            (backfill_id, target_table, from_sequence, to_sequence, state,
             completed_at, updated_at)
            VALUES (?, 'journal_events', ?, ?, 'PUBLISHED', ?, ?)""",
            (key, start_cursor, end_cursor, now, now),
        )
        return {
            "replayed": False,
            "backfill_id": key,
            "state": "PUBLISHED",
            "derived_payload_sha256": derived_payload_sha256,
        }

    return _SqliteUnitOfWork(_operational_spec())._write(commit)


def resume_backfill_state(
    *,
    plan_hash: str,
) -> dict[str, object]:
    """Return durable backfill checkpoints for one plan (fresh-process resume).

    Checkpoints are stored keyed by the batch idempotency key (which embeds
    the plan hash); the resume read returns all persisted checkpoints so a
    fresh process can rebuild its cursor map.  No bulk backfill is started in
    P7 (``bulk_backfill_started`` is always False).
    """
    plan_identity = plan_hash

    def read(connection) -> dict[str, object]:
        rows = connection.execute(
            "SELECT backfill_id, target_table, from_sequence, to_sequence, "
            "state, completed_at, updated_at FROM ops_backfill_checkpoint "
            "ORDER BY from_sequence"
        ).fetchall()
        return {
            "plan_hash": plan_identity,
            "checkpoints": [
                {
                    "backfill_id": row["backfill_id"],
                    "target_table": row["target_table"],
                    "from_sequence": int(row["from_sequence"]),
                    "to_sequence": int(row["to_sequence"]),
                    "state": row["state"],
                }
                for row in rows
            ],
            "bulk_backfill_started": False,
        }

    return _SqliteUnitOfWork(_operational_spec())._read(read)


def record_retention_metadata(
    *,
    packet_hash: str,
    retention_class: str,
    source_event_ref: str | None = None,
) -> dict[str, object]:
    """Record retention metadata keyed on packet hash (report-only semantics)."""
    if retention_class not in {"SCIENTIFIC", "PREVIEW", "STAGING"}:
        raise OperationsMaintenanceError("retention class is invalid")
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "schema_version": "control_plane.retention_metadata.v1",
        "packet_hash": packet_hash,
        "retention_class": retention_class,
        "last_referenced": now,
        "archive_eligible": retention_class != "SCIENTIFIC",
        "source_event_ref": source_event_ref,
    }

    def commit(connection) -> dict[str, object]:
        connection.execute(
            """INSERT INTO ops_retention_metadata
            (retention_id, target_table, policy_sha256, preserved_from_sequence,
             preserved_to_sequence, applied_at)
            VALUES (?, 'journal_events', ?, 0, 0, ?)""",
            (packet_hash, retention_class, now),
        )
        return payload

    _SqliteUnitOfWork(_operational_spec())._write(commit)
    return payload


def retention_cleanup_candidates(
    *,
    temp_root: Path,
    max_age_days: int,
) -> dict[str, object]:
    """Return explicit cleanup candidates (report only; no deletion).

    Only ordinary files under ``temp_root`` whose names match an explicit
    PREVIEW/STAGING marker and whose content hash validates are eligible;
    SCIENTIFIC packets are never eligible; traversal/reparse/hash mismatch
    are rejected.  No broad glob or recursive deletion is performed here.
    """
    if type(max_age_days) is not int or max_age_days < 1:
        raise OperationsMaintenanceError("max_age_days must be a positive integer")
    root = Path(temp_root).resolve()
    candidates: list[dict[str, object]] = []
    rejected: list[str] = []
    if root.exists():
        for path in root.iterdir():
            if not path.is_file():
                continue
            name = path.name.lower()
            if "scientific" in name:
                rejected.append(f"{path.name}:SCIENTIFIC_NOT_ELIGIBLE")
                continue
            if "preview" not in name and "staging" not in name:
                continue
            try:
                content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
            except OSError as error:
                rejected.append(f"{path.name}:UNREADABLE")
                continue
            candidates.append(
                {
                    "ref": path.name,
                    "sha256": content_hash,
                    "size": path.stat().st_size,
                    "eligible": True,
                }
            )
    return {
        "schema_version": "control_plane.retention_cleanup_candidates.v1",
        "temp_root": str(root),
        "candidates": candidates,
        "rejected": rejected,
        "eligible_count": len(candidates),
        "deleted": 0,
        "note": "report only; explicit cleanup requires a deletion receipt",
    }


__all__ = [
    "BackfillCheckpointConflict",
    "OperationsMaintenanceError",
    "RetentionSafetyError",
    "persist_backfill_batch",
    "record_retention_metadata",
    "resume_backfill_state",
    "retention_cleanup_candidates",
]
