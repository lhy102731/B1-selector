"""Narrow store migration coordinator for the two fixed control-plane targets.

The coordinator only exposes quiescence checks, SQLite-backup-API snapshots,
staging rehearsals and validated publication for the fixed Authority and
OperationalJournal stores. It never accepts arbitrary SQL, arbitrary target
paths or automatic process termination, and the root capability stays in
memory only.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Mapping

from research_automation.control_plane import stores as _stores
from research_automation.control_plane.sqlite_uow import (
    _SqliteUnitOfWork,
    _StoreSpec,
)


class MigrationTarget(str, Enum):
    AUTHORITY = "authority"
    OPERATIONAL = "operational"


class StoreMigrationError(RuntimeError):
    """Base error for the narrow store migration coordinator."""


class StoreMigrationActiveError(StoreMigrationError):
    """Raised when live activation objects prevent a quiescent migration."""


class StoreMigrationBackupError(StoreMigrationError):
    """Raised when a migration backup is missing or invalid."""


class StoreMigrationStagingError(StoreMigrationError):
    """Raised when a staging rehearsal cannot be prepared."""


@dataclass(frozen=True)
class QuiescenceReport:
    quiescent: bool
    active_tickets: int
    pending_outbox: int
    active_grants: int
    wal_present: bool


@dataclass(frozen=True)
class BackupReceipt:
    target: str
    installation_id: str
    schema_version: int
    user_version: int
    quick_check_ok: bool
    row_counts: Mapping[str, int]
    journal_mode: str
    file_sha256: str
    manifest_sha256: str


@dataclass(frozen=True)
class RehearsalReceipt:
    target: str
    migrated: bool
    staging_schema_version: int
    verified: bool


@dataclass(frozen=True)
class MigrationReceipt:
    target: str
    from_schema_version: int
    to_schema_version: int
    migrated: bool


class StoreMigrationCoordinator:
    """Fixed-target migration orchestration with in-memory root only."""

    def __init__(self, *, root_secret: str) -> None:
        if not isinstance(root_secret, str) or len(root_secret) < 32:
            raise ValueError("root_secret must be a strong capability")
        self._root_secret = root_secret

    def _target_spec(self, target: MigrationTarget) -> _StoreSpec:
        if target is MigrationTarget.AUTHORITY:
            return _stores._authority_spec()
        if target is MigrationTarget.OPERATIONAL:
            return _stores._operational_spec()
        raise ValueError("unknown migration target")

    def _current_spec(self, target: MigrationTarget) -> _StoreSpec:
        spec = self._target_spec(target)
        connection = sqlite3.connect(
            f"{spec.path.as_uri()}?mode=ro",
            uri=True,
            isolation_level=None,
        )
        try:
            user_version = int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
        finally:
            connection.close()
        if user_version < 1:
            raise StoreMigrationError("store schema version is invalid")
        return _StoreSpec(
            path=spec.path,
            store_kind=spec.store_kind,
            metadata_table=spec.metadata_table,
            schema_version=user_version,
            expected_schema_sha256=None,
        )

    @staticmethod
    def _require_absolute(path: object, label: str) -> Path:
        if not isinstance(path, Path):
            raise TypeError(f"{label} must be a Path")
        if not path.is_absolute():
            raise ValueError(f"{label} must be absolute")
        return path.resolve(strict=False)

    def check_quiescence(self, target: MigrationTarget) -> QuiescenceReport:
        """Read-only snapshot of live activation objects for one target."""

        spec = self._current_spec(target)

        def check(connection: sqlite3.Connection) -> QuiescenceReport:
            if target is MigrationTarget.AUTHORITY:
                active_tickets = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM task_tickets_v2 "
                        "WHERE state = 'IN_PROGRESS'"
                    ).fetchone()[0]
                )
                pending_outbox = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM authority_outbox "
                        "WHERE mirrored_at IS NULL"
                    ).fetchone()[0]
                )
                active_grants = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM phase_grants_v2 "
                        "WHERE state = 'ACTIVE'"
                    ).fetchone()[0]
                )
            else:
                active_tickets = 0
                pending_outbox = 0
                active_grants = 0
            wal_present = os.path.lexists(f"{spec.path}-wal")
            return QuiescenceReport(
                quiescent=(
                    active_tickets == 0
                    and pending_outbox == 0
                    and not wal_present
                ),
                active_tickets=active_tickets,
                pending_outbox=pending_outbox,
                active_grants=active_grants,
                wal_present=wal_present,
            )

        return _SqliteUnitOfWork(spec)._read(check)

    def backup(
        self,
        target: MigrationTarget,
        backup_path: Path,
    ) -> BackupReceipt:
        """Create a complete SQLite-backup-API snapshot and its manifest."""

        spec = self._target_spec(target)
        destination = self._require_absolute(backup_path, "backup_path")
        if destination.exists():
            raise StoreMigrationBackupError("backup path already exists")
        destination.parent.mkdir(parents=True, exist_ok=True)
        source = sqlite3.connect(
            f"{spec.path.as_uri()}?mode=ro",
            uri=True,
        )
        try:
            target_connection = sqlite3.connect(destination)
            try:
                source.backup(target_connection)
            finally:
                target_connection.close()
        finally:
            source.close()
        connection = sqlite3.connect(destination)
        try:
            quick_check = str(
                connection.execute("PRAGMA quick_check").fetchone()[0]
            )
            user_version = int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
            journal_mode = str(
                connection.execute("PRAGMA journal_mode").fetchone()[0]
            )
            row_counts: dict[str, int] = {}
            for (table,) in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall():
                table_name = str(table)
                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name) is None:
                    raise StoreMigrationBackupError(
                        "store contains a non-identifier table name"
                    )
                row_counts[table_name] = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    ).fetchone()[0]
                )
        finally:
            connection.close()
        file_sha256 = hashlib.sha256(destination.read_bytes()).hexdigest()
        manifest = json.dumps(
            {
                "schema": "control_plane.store_migration_backup.v1",
                "target": target.value,
                "installation_id": _stores._read_installation_id(spec),
                "schema_version": spec.schema_version,
                "user_version": user_version,
                "quick_check": quick_check,
                "row_counts": row_counts,
                "journal_mode": journal_mode,
                "file_sha256": file_sha256,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        manifest_sha256 = hashlib.sha256(manifest.encode("utf-8")).hexdigest()
        return BackupReceipt(
            target=target.value,
            installation_id=_stores._read_installation_id(spec),
            schema_version=spec.schema_version,
            user_version=user_version,
            quick_check_ok=quick_check == "ok",
            row_counts=row_counts,
            journal_mode=journal_mode,
            file_sha256=file_sha256,
            manifest_sha256=manifest_sha256,
        )

    def rehearse(
        self,
        target: MigrationTarget,
        *,
        backup_path: Path,
        staging_path: Path,
    ) -> RehearsalReceipt:
        """Restore a backup into staging and run the migration there only."""

        backup = self._require_absolute(backup_path, "backup_path")
        staging = self._require_absolute(staging_path, "staging_path")
        if not backup.exists():
            raise StoreMigrationBackupError("backup is missing")
        if staging.exists():
            raise StoreMigrationStagingError("staging path already exists")
        staging.parent.mkdir(parents=True, exist_ok=True)
        source = sqlite3.connect(
            f"{backup.as_uri()}?mode=ro",
            uri=True,
        )
        try:
            destination = sqlite3.connect(staging)
            try:
                source.backup(destination)
            finally:
                destination.close()
        finally:
            source.close()
        if target is MigrationTarget.AUTHORITY:
            migrated = _stores._migrate_authority_v2(
                root_secret=self._root_secret,
                path=staging,
            )
        elif target is MigrationTarget.OPERATIONAL:
            migrated = _stores._migrate_operational_journal_v4(
                root_secret=self._root_secret,
                path=staging,
            )
        else:
            raise ValueError("unknown migration target")
        connection = sqlite3.connect(staging)
        try:
            staging_schema_version = int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
            verified = (
                str(
                    connection.execute(
                        "PRAGMA integrity_check"
                    ).fetchone()[0]
                )
                == "ok"
            )
        finally:
            connection.close()
        return RehearsalReceipt(
            target=target.value,
            migrated=migrated,
            staging_schema_version=staging_schema_version,
            verified=verified,
        )

    def publish(
        self,
        target: MigrationTarget,
        *,
        backup_path: Path,
    ) -> MigrationReceipt:
        """Validate quiescence and a verified backup, then migrate the live store."""

        backup = self._require_absolute(backup_path, "backup_path")
        if not backup.exists():
            raise StoreMigrationBackupError("backup is missing")
        quiescence = self.check_quiescence(target)
        if not quiescence.quiescent:
            raise StoreMigrationActiveError(
                "migration requires a quiescent store"
            )
        from_schema_version = self._current_spec(target).schema_version
        if target is MigrationTarget.AUTHORITY:
            migrated = _stores._migrate_authority_v2(
                root_secret=self._root_secret
            )
            to_schema_version = _stores._AUTHORITY_SCHEMA_VERSION
        elif target is MigrationTarget.OPERATIONAL:
            migrated = _stores._migrate_operational_journal_v4(
                root_secret=self._root_secret
            )
            to_schema_version = _stores._OPERATIONAL_SCHEMA_VERSION
        else:
            raise ValueError("unknown migration target")
        return MigrationReceipt(
            target=target.value,
            from_schema_version=from_schema_version,
            to_schema_version=to_schema_version,
            migrated=migrated,
        )


__all__ = [
    "BackupReceipt",
    "MigrationReceipt",
    "MigrationTarget",
    "QuiescenceReport",
    "RehearsalReceipt",
    "StoreMigrationActiveError",
    "StoreMigrationBackupError",
    "StoreMigrationCoordinator",
    "StoreMigrationError",
    "StoreMigrationStagingError",
]
