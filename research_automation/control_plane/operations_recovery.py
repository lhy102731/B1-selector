"""Validated OperationalJournal backup and restore (P7R3 T4).

Uses the SQLite backup API for consistent snapshots (pages=-1 includes rows
committed but still in the WAL), then verifies schema/installation/
quick_check/foreign keys/row counts and logical stream hashes before writing
a create-only manifest.  Restore goes through a staging database, full
validation, then a Windows-safe atomic publish; failures never delete the
current store and never auto-recreate/clear Authority.  No live restore is
authorized in P7: the maintenance entry points require an explicit trusted
maintenance context and quiescence.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .contracts import canonical_json
from .sqlite_uow import _SqliteUnitOfWork
from .stores import _operational_spec, _read_installation_id


class OperationsRecoveryError(RuntimeError):
    """Base error for operational backup/restore."""


class OperationsBackupError(OperationsRecoveryError):
    """Backup creation or validation failed."""


class OperationsRestoreError(OperationsRecoveryError):
    """Restore failed (preconditions, staging, or publish)."""


class OperationsRestoreBlocked(OperationsRestoreError):
    """Restore blocked by active handles or missing maintenance context."""


class OperationsMaintenanceContextRequired(OperationsRestoreError):
    """A trusted maintenance context is required for restore."""


@dataclass(frozen=True, slots=True)
class OperationsBackupReceipt:
    """Validated backup receipt with schema/installation/row/hash facts."""

    target: str
    installation_id: str
    schema_version: int
    user_version: int
    quick_check_ok: bool
    foreign_keys_ok: bool
    row_counts: dict[str, int]
    journal_logical_sha256: str
    file_sha256: str
    manifest_sha256: str
    backup_path: str
    created_at: str

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": "control_plane.operations_backup_receipt.v1",
            "target": self.target,
            "installation_id": self.installation_id,
            "schema_version": self.schema_version,
            "user_version": self.user_version,
            "quick_check_ok": self.quick_check_ok,
            "foreign_keys_ok": self.foreign_keys_ok,
            "row_counts": dict(self.row_counts),
            "journal_logical_sha256": self.journal_logical_sha256,
            "file_sha256": self.file_sha256,
            "manifest_sha256": self.manifest_sha256,
            "backup_path": self.backup_path,
            "created_at": self.created_at,
        }


@dataclass(frozen=True, slots=True)
class OperationsRestoreReceipt:
    """Validated staging restore receipt; publish is separate."""

    target: str
    installation_id: str
    schema_version: int
    user_version: int
    quick_check_ok: bool
    row_counts: dict[str, int]
    journal_logical_sha256: str
    restored_at: str
    published: bool

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": "control_plane.operations_restore_receipt.v1",
            "target": self.target,
            "installation_id": self.installation_id,
            "schema_version": self.schema_version,
            "user_version": self.user_version,
            "quick_check_ok": self.quick_check_ok,
            "row_counts": dict(self.row_counts),
            "journal_logical_sha256": self.journal_logical_sha256,
            "restored_at": self.restored_at,
            "published": self.published,
        }


def _journal_logical_sha256(path: Path) -> str:
    """Logical hash over validated event rows (never the live WAL file)."""
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        digest = hashlib.sha256()
        rows = conn.execute(
            "SELECT sequence, event_id, event_type, payload_sha256 "
            "FROM journal_events ORDER BY sequence"
        ).fetchall()
        for row in rows:
            digest.update(
                b"control_plane.journal_row.v1\0"
                + str(row["sequence"]).encode("utf-8")
                + b"\0"
                + str(row["event_id"]).encode("utf-8")
                + b"\0"
                + str(row["payload_sha256"]).encode("utf-8")
            )
        return digest.hexdigest()
    finally:
        conn.close()


def _validate_backup_database(path: Path) -> dict[str, object]:
    """Open a backup and validate schema/installation/integrity/row counts."""
    try:
        conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    except sqlite3.DatabaseError as error:
        raise OperationsRestoreError(
            "backup is not a valid SQLite database"
        ) from error
    conn.row_factory = sqlite3.Row
    try:
        quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
        user_version = conn.execute("PRAGMA user_version").fetchone()[0]
        row = conn.execute(
            "SELECT value FROM operational_meta WHERE key = 'installation_id'"
        ).fetchone()
        installation_id = str(row["value"]) if row is not None else ""
        row_counts: dict[str, int] = {}
        for (table,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall():
            row_counts[str(table)] = int(
                conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
        return {
            "quick_check_ok": quick_check == "ok",
            "foreign_keys_ok": len(foreign_keys) == 0,
            "user_version": int(user_version),
            "installation_id": str(installation_id),
            "row_counts": row_counts,
        }
    except sqlite3.DatabaseError as error:
        raise OperationsRestoreError(
            "backup is not a valid SQLite database"
        ) from error
    finally:
        conn.close()


def backup_operational_journal(
    *,
    backup_path: Path,
    progress: object = None,
) -> OperationsBackupReceipt:
    """Create a validated consistent backup of the live OperationalJournal.

    Create-only: fails if ``backup_path`` already exists.  Uses the SQLite
    backup API (pages=-1) so rows committed but still in the WAL are
    included; the live main file is never copied raw.
    """
    spec = _operational_spec()
    source = Path(spec.path)
    if not source.exists():
        raise OperationsBackupError("operational journal is unavailable")
    destination = Path(backup_path).resolve()
    if destination.exists() or os.path.lexists(destination):
        raise OperationsBackupError("backup destination already exists (create-only)")
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_conn = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True)
    try:
        target_conn = sqlite3.connect(str(destination))
        try:
            source_conn.backup(target_conn, pages=-1, progress=progress)
            target_conn.commit()
        finally:
            target_conn.close()
    finally:
        source_conn.close()
    validated = _validate_backup_database(destination)
    if not validated["quick_check_ok"] or not validated["foreign_keys_ok"]:
        raise OperationsBackupError("backup validation failed")
    live_logical = _journal_logical_sha256(source)
    backup_logical = _journal_logical_sha256(destination)
    if live_logical != backup_logical:
        raise OperationsBackupError("backup logical hash mismatch")
    file_sha256 = hashlib.sha256(destination.read_bytes()).hexdigest()
    manifest_payload = {
        "schema_version": "control_plane.operations_backup_manifest.v1",
        "target": "operational",
        "installation_id": validated["installation_id"],
        "journal_logical_sha256": live_logical,
        "file_sha256": file_sha256,
    }
    manifest_sha256 = hashlib.sha256(
        canonical_json(manifest_payload).encode("utf-8")
    ).hexdigest()
    receipt = OperationsBackupReceipt(
        target="operational",
        installation_id=validated["installation_id"],
        schema_version=int(validated["user_version"]),
        user_version=int(validated["user_version"]),
        quick_check_ok=bool(validated["quick_check_ok"]),
        foreign_keys_ok=bool(validated["foreign_keys_ok"]),
        row_counts=dict(validated["row_counts"]),
        journal_logical_sha256=live_logical,
        file_sha256=file_sha256,
        manifest_sha256=manifest_sha256,
        backup_path=str(destination),
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    return receipt


def _require_maintenance_context(context: object | None) -> None:
    """A trusted maintenance context is mandatory for restore."""
    if context is None:
        raise OperationsMaintenanceContextRequired(
            "a trusted maintenance context is required for restore"
        )
    if not getattr(context, "maintenance_authorized", False):
        raise OperationsMaintenanceContextRequired(
            "maintenance context is not authorized"
        )


def _check_quiescent() -> None:
    """Fail if any active campaign/lease or WAL sidecar is present."""
    spec = _operational_spec()
    path = Path(spec.path)

    def read(connection) -> None:
        active_campaigns = connection.execute(
            "SELECT COUNT(*) AS c FROM campaign_events "
            "WHERE event_type = 'CAMPAIGN_ACTIVATED'"
        ).fetchone()["c"]
        if active_campaigns:
            raise OperationsRestoreBlocked(
                "restore blocked: active campaign events present"
            )
        active_leases = connection.execute(
            "SELECT COUNT(*) AS c FROM campaign_events "
            "WHERE event_type IN ('LEASE_ACQUIRED', 'CYCLE_EXECUTION_STARTED')"
        ).fetchone()["c"]
        if active_leases:
            raise OperationsRestoreBlocked(
                "restore blocked: active lease events present"
            )

    _SqliteUnitOfWork(spec)._read(read)
    if os.path.lexists(f"{path}-wal") or os.path.lexists(f"{path}-shm"):
        raise OperationsRestoreBlocked(
            "restore blocked: WAL sidecar present (possible active handle)"
        )


def restore_operational_journal(
    *,
    backup_path: Path,
    staging_path: Path,
    maintenance_context: object | None = None,
    publish: bool = False,
) -> OperationsRestoreReceipt:
    """Restore the journal via staging, validate, then optionally publish.

    Restore never touches the current store until the staging copy is fully
    validated; publish uses an atomic replace and fails with a blocked reason
    if the target is held open (never kills the holder).  Live restore is not
    authorized in P7: ``publish`` requires an explicit maintenance context.
    """
    _require_maintenance_context(maintenance_context)
    backup = Path(backup_path).resolve()
    if not backup.exists():
        raise OperationsRestoreError("backup is unavailable")
    validated = _validate_backup_database(backup)
    if not validated["quick_check_ok"] or not validated["foreign_keys_ok"]:
        raise OperationsRestoreError("backup validation failed before restore")
    staging = Path(staging_path).resolve()
    if staging.exists() or os.path.lexists(staging):
        raise OperationsRestoreError("staging path already exists (create-only)")
    staging.parent.mkdir(parents=True, exist_ok=True)
    try:
        backup_conn = sqlite3.connect(
            f"file:{backup.as_posix()}?mode=ro", uri=True
        )
    except sqlite3.DatabaseError as error:
        raise OperationsRestoreError(
            "backup is not a valid SQLite database"
        ) from error
    try:
        staging_conn = sqlite3.connect(str(staging))
        try:
            backup_conn.backup(staging_conn, pages=-1)
            staging_conn.commit()
        finally:
            staging_conn.close()
    finally:
        backup_conn.close()
    staged = _validate_backup_database(staging)
    if not staged["quick_check_ok"] or not staged["foreign_keys_ok"]:
        raise OperationsRestoreError("staging validation failed")
    logical = _journal_logical_sha256(staging)
    published = False
    if publish:
        _check_quiescent()
        spec = _operational_spec()
        live = Path(spec.path)
        try:
            os.replace(staging, live)
            published = True
        except PermissionError as error:
            raise OperationsRestoreBlocked(
                "restore publish blocked: live store is held open"
            ) from error
    return OperationsRestoreReceipt(
        target="operational",
        installation_id=staged["installation_id"],
        schema_version=int(staged["user_version"]),
        user_version=int(staged["user_version"]),
        quick_check_ok=bool(staged["quick_check_ok"]),
        row_counts=dict(staged["row_counts"]),
        journal_logical_sha256=logical,
        restored_at=datetime.now(timezone.utc).isoformat(),
        published=published,
    )


__all__ = [
    "OperationsBackupError",
    "OperationsBackupReceipt",
    "OperationsMaintenanceContextRequired",
    "OperationsRecoveryError",
    "OperationsRestoreBlocked",
    "OperationsRestoreError",
    "OperationsRestoreReceipt",
    "backup_operational_journal",
    "restore_operational_journal",
]
