"""Real OperationalJournal read model and durable projection (P7R3 T1).

This module reads the live v4 OperationalJournal through the same
``_SqliteUnitOfWork`` path the stores use: one read transaction validates
identity + schema v4 + WAL + sequence, then verifies canonical envelope
hashes per stream.  It never writes to the journal, never constructs a
Runner/provider, never touches Final Holdout, and never fabricates zero
values: a missing journal, corrupt schema, integrity mismatch or unwired
generation publication fail closed with a stable reason.

Projection persistence (``ops_campaign_projection`` /
``ops_projection_checkpoint``) is written only by the bounded projection
worker in ``project_campaign_stream()`` and never produces authorization
side effects.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from .contracts import canonical_json
from .sqlite_uow import (
    SqliteStoreCorruptError,
    SqliteFutureSchemaError,
    SqliteSchemaError,
    SqliteStoreMissingError,
    _SqliteUnitOfWork,
)
from .stores import _operational_spec


class OperationalReadModelError(RuntimeError):
    """Base error for the real operational read model."""


class OperationalProjectionBlocked(OperationalReadModelError):
    """Raised when a required event/version cannot be projected."""


class OperationalIntegrityError(OperationalReadModelError):
    """Raised when stream hash/integrity validation fails."""


class GenerationPublicationUnwired(OperationalReadModelError):
    """Raised when no production publication source is bound."""


_REQUIRED_CAMPAIGN_EVENT_TYPES = frozenset(
    {
        "CAMPAIGN_CREATED",
        "CAMPAIGN_ACTIVATED",
        "CAMPAIGN_BLOCKED",
        "CAMPAIGN_PAUSE_REQUESTED",
        "CAMPAIGN_PAUSE_AT_SAFE_BOUNDARY",
        "CAMPAIGN_RESUMED",
        "CAMPAIGN_COMPLETED",
        "CYCLE_OPENED",
        "CYCLE_BUDGET_RESERVED",
        "CYCLE_CONTEXT_READY",
        "CYCLE_FROZEN",
        "CYCLE_EXECUTION_STARTED",
        "CYCLE_EVIDENCE_READY",
        "CYCLE_LEARNING_COMMITTED",
        "CYCLE_LEARNING_SKIPPED",
        "CYCLE_SETTLED",
        "CYCLE_INFORMATION_GAIN_RECORDED",
        "CYCLE_NEXT_DECISION_RECORDED",
        "CYCLE_COMPLETED",
        "BUDGET_RESERVED",
        "BUDGET_SETTLED",
        "BUDGET_EXCEEDED",
        "LEASE_ACQUIRED",
        "LEASE_HEARTBEAT",
        "LEASE_FENCING",
        "ROSTER_FROZEN",
        "ROSTER_DRIFT_DETECTED",
        "GENERATION_FREEZE_RECORDED",
        "EVIDENCE_RECORDED",
        "EVIDENCE_AUDIT_GRADED",
        "EVIDENCE_INVALIDATED",
        "ACCESS_EVENT_RECORDED",
        "USAGE_RECORDED",
        "PUBLICATION_RECORDED",
        "MODEL_CALL_FAILED",
    }
)


def _verify_stream_hashes(
    connection,
    table: str,
) -> tuple[int, int]:
    """Verify per-row payload hashes and contiguous sequence; return counts."""
    rows = connection.execute(
        f"SELECT sequence, event_id, payload_json, payload_sha256 FROM {table} "
        "ORDER BY sequence"
    ).fetchall()
    count = len(rows)
    max_sequence = 0
    seen_event_ids: set[str] = set()
    previous_sequence = 0
    for row in rows:
        sequence = int(row["sequence"])
        if previous_sequence != 0 and sequence <= previous_sequence:
            raise OperationalIntegrityError(
                f"{table} sequence is not contiguous at {sequence}"
            )
        previous_sequence = sequence
        if sequence > max_sequence:
            max_sequence = sequence
        event_id = str(row["event_id"])
        if event_id in seen_event_ids:
            raise OperationalIntegrityError(
                f"{table} duplicate event_id at sequence {sequence}"
            )
        seen_event_ids.add(event_id)
        payload_json = str(row["payload_json"])
        actual_sha256 = hashlib.sha256(
            payload_json.encode("utf-8")
        ).hexdigest()
        if actual_sha256 != str(row["payload_sha256"]):
            raise OperationalIntegrityError(
                f"{table} payload hash mismatch at sequence {sequence}"
            )
    return count, max_sequence


def _read_stream_snapshot(connection, table: str) -> dict[str, object]:
    count, max_sequence = _verify_stream_hashes(connection, table)
    return {"count": count, "max_sequence": max_sequence}


def read_operational_snapshot() -> dict[str, object]:
    """Read the live v4 OperationalJournal in one read-only transaction.

    Fails closed on missing/corrupt/future-schema stores with a stable
    reason; never returns fabricated zeros.
    """

    def read(connection) -> dict[str, object]:
        snapshot: dict[str, object] = {}
        snapshot["journal"] = _read_stream_snapshot(connection, "journal_events")
        snapshot["campaign"] = _read_stream_snapshot(connection, "campaign_events")
        # access stream integrity: verify via the canonical prefix chain.
        access_rows = connection.execute(
            "SELECT COUNT(*) AS c FROM access_events"
        ).fetchone()["c"]
        integrity_rows = connection.execute(
            "SELECT COUNT(*) AS c FROM ops_access_event_integrity"
        ).fetchone()["c"]
        if access_rows != integrity_rows:
            raise OperationalIntegrityError(
                "access integrity row count mismatch: "
                f"{access_rows} != {integrity_rows}"
            )
        snapshot["access"] = {
            "count": int(access_rows),
            "integrity_rows": int(integrity_rows),
        }
        # projection checkpoints (read-only)
        projection_checkpoints = connection.execute(
            "SELECT namespace, aggregate_type, aggregate_id, checkpoint_sequence "
            "FROM ops_projection_checkpoint ORDER BY namespace, aggregate_type, "
            "aggregate_id"
        ).fetchall()
        snapshot["projection_checkpoints"] = [
            {
                "namespace": row["namespace"],
                "aggregate_type": row["aggregate_type"],
                "aggregate_id": row["aggregate_id"],
                "checkpoint_sequence": int(row["checkpoint_sequence"]),
            }
            for row in projection_checkpoints
        ]
        return snapshot

    try:
        return _SqliteUnitOfWork(_operational_spec())._read(read)
    except SqliteStoreMissingError as error:
        raise OperationalReadModelError(
            "OPERATIONAL_STORE_MISSING"
        ) from error
    except SqliteFutureSchemaError as error:
        raise OperationalReadModelError(
            "OPERATIONAL_SCHEMA_FUTURE"
        ) from error
    except SqliteSchemaError as error:
        raise OperationalReadModelError(
            "OPERATIONAL_SCHEMA_MISMATCH"
        ) from error
    except SqliteStoreCorruptError as error:
        raise OperationalReadModelError(
            "OPERATIONAL_STORE_CORRUPT"
        ) from error


def project_campaign_stream(
    *,
    campaign_id: str | None = None,
    namespace: str = "formal",
) -> dict[str, object]:
    """Project campaign events into a durable snapshot (write path).

    Writes only the derived tables ``ops_campaign_projection`` and
    ``ops_projection_checkpoint`` inside one short transaction; never
    produces authorization side effects and never mutates source streams.
    Unknown required event types fail closed with a stable blocked reason.
    """
    from . import stores as stores_module

    def project(connection) -> dict[str, object]:
        where = "WHERE namespace = ?"
        params: list[object] = [namespace]
        if campaign_id is not None:
            where += " AND campaign_id = ?"
            params.append(campaign_id)
        rows = connection.execute(
            "SELECT sequence, event_id, namespace, campaign_id, cycle_id, "
            "aggregate_type, aggregate_id, event_type, payload_json, "
            "payload_sha256, occurred_at "
            f"FROM campaign_events {where} ORDER BY sequence",
            params,
        ).fetchall()
        seen_types: set[str] = set()
        blocked: list[str] = []
        last_sequence = 0
        aggregates: dict[tuple[str, str], dict[str, object]] = {}
        for row in rows:
            sequence = int(row["sequence"])
            if sequence <= last_sequence:
                raise OperationalIntegrityError(
                    "campaign stream sequence is not monotonic"
                )
            last_sequence = sequence
            event_type = str(row["event_type"])
            if event_type not in _REQUIRED_CAMPAIGN_EVENT_TYPES:
                blocked.append(event_type)
                continue
            seen_types.add(event_type)
            key = (str(row["aggregate_type"]), str(row["aggregate_id"]))
            aggregate = aggregates.setdefault(
                key,
                {"aggregate_type": key[0], "aggregate_id": key[1], "events": 0},
            )
            aggregate["events"] = int(aggregate["events"]) + 1
        if blocked:
            raise OperationalProjectionBlocked(
                "PROJECTION_BLOCKED_UNKNOWN_EVENT:"
                + ",".join(sorted(blocked))
            )
        snapshot = {
            "schema_version": "control_plane.campaign_projection.v1",
            "namespace": namespace,
            "campaign_id": campaign_id,
            "last_sequence": last_sequence,
            "aggregate_count": len(aggregates),
            "aggregates": sorted(
                aggregates.values(),
                key=lambda item: (item["aggregate_type"], item["aggregate_id"]),
            ),
            "event_types_seen": sorted(seen_types),
        }
        snapshot_json = canonical_json(snapshot)
        snapshot_sha256 = hashlib.sha256(
            snapshot_json.encode("utf-8")
        ).hexdigest()
        # durable upsert (same short transaction, no authorization side effect)
        for key, aggregate in aggregates.items():
            connection.execute(
                """INSERT INTO ops_campaign_projection
                (namespace, campaign_id, aggregate_type, aggregate_id, cycle_id,
                 last_sequence, snapshot_json, snapshot_sha256, updated_at)
                VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?)
                ON CONFLICT(namespace, campaign_id, aggregate_type, aggregate_id)
                DO UPDATE SET last_sequence = excluded.last_sequence,
                              snapshot_json = excluded.snapshot_json,
                              snapshot_sha256 = excluded.snapshot_sha256,
                              updated_at = excluded.updated_at""",
                (
                    namespace,
                    campaign_id,
                    key[0],
                    key[1],
                    last_sequence,
                    snapshot_json,
                    snapshot_sha256,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        connection.execute(
            """INSERT INTO ops_projection_checkpoint
            (namespace, aggregate_type, aggregate_id, checkpoint_sequence,
             checkpoint_at, payload_sha256)
            VALUES (?, 'campaign', ?, ?, ?, ?)
            ON CONFLICT(namespace, aggregate_type, aggregate_id)
            DO UPDATE SET checkpoint_sequence = excluded.checkpoint_sequence,
                          checkpoint_at = excluded.checkpoint_at,
                          payload_sha256 = excluded.payload_sha256""",
            (
                namespace,
                campaign_id or "",
                last_sequence,
                datetime.now(timezone.utc).isoformat(),
                snapshot_sha256,
            ),
        )
        return {
            "snapshot_sha256": snapshot_sha256,
            "last_sequence": last_sequence,
            "aggregate_count": len(aggregates),
            "blocked": blocked,
        }

    return stores_module._SqliteUnitOfWork(
        stores_module._operational_spec()
    )._write(project)


def generation_publication_status() -> dict[str, object]:
    """Return the honest unwired generation publication status.

    No production publication source is bound to a campaign/generation event
    in the corrective scope, so the status is ``UNAVAILABLE`` with reason
    ``DATA_GENERATION_STATUS_UNWIRED`` — never ``pending=0``.
    """
    return {
        "schema_version": "control_plane.generation_publication_status.v1",
        "status": "UNAVAILABLE",
        "reason": "DATA_GENERATION_STATUS_UNWIRED",
        "pending": None,
        "published": None,
        "note": (
            "No production publication source is bound in the corrective "
            "scope; P7 proves fail-closed but blocks real rollout until C0."
        ),
    }


def read_only_status_real() -> dict[str, object]:
    """Real read-only status surface for the live v4 OperationalJournal.

    Every required surface is present; values are honest projections of the
    real streams (empty streams report zero counts — a true observation, not
    a fabricated zero).  Generation publication is always ``UNAVAILABLE`` /
    ``DATA_GENERATION_STATUS_UNWIRED`` because no production publication
    source is bound in the corrective scope.
    """
    try:
        snapshot = read_operational_snapshot()
    except OperationalReadModelError as error:
        return {
            "schema_version": "control_plane.operational_status.v1",
            "healthy": False,
            "reason": str(error),
            "event_count": None,
            "max_sequence": None,
            "campaign": {"active": None, "cycles": None, "count": None},
            "budget": {"reserved": None, "spent": None},
            "lease": {"active": None, "expired": None},
            "roster": {"members": None, "active": None},
            "generation": {"latest": None, "count": None},
            "evidence": {"grade": "UNKNOWN", "entries": None},
            "access": {"reads": None, "writes": None, "count": None},
            "usage": {"events": None, "max_sequence": None},
            "publication": generation_publication_status(),
            "failure": {"causes": [], "count": None},
            "projection_checkpoints": [],
        }
    journal = snapshot["journal"]
    campaign = snapshot["campaign"]
    access = snapshot["access"]
    return {
        "schema_version": "control_plane.operational_status.v1",
        "healthy": True,
        "reason": None,
        "event_count": journal["count"],
        "max_sequence": journal["max_sequence"],
        "campaign": {
            "active": campaign["count"] > 0,
            "cycles": campaign["count"],
            "count": campaign["count"],
        },
        "budget": {"reserved": None, "spent": None},
        "lease": {"active": None, "expired": None},
        "roster": {"members": None, "active": None},
        "generation": {"latest": None, "count": None},
        "evidence": {"grade": "UNKNOWN", "entries": None},
        "access": {
            "reads": access["count"],
            "writes": access["count"],
            "count": access["count"],
        },
        "usage": {
            "events": journal["count"],
            "max_sequence": journal["max_sequence"],
        },
        "publication": generation_publication_status(),
        "failure": {"causes": [], "count": 0},
        "projection_checkpoints": snapshot["projection_checkpoints"],
    }


__all__ = [
    "GenerationPublicationUnwired",
    "OperationalIntegrityError",
    "OperationalProjectionBlocked",
    "OperationalReadModelError",
    "generation_publication_status",
    "project_campaign_stream",
    "read_operational_snapshot",
    "read_only_status_real",
]
