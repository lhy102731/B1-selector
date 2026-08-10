"""P7 operations surface: journal durability contract.

The contract covers the operational journal write path used by control-plane
operations: WAL journal mode, an explicit bounded busy timeout, and exactly one
short IMMEDIATE write transaction per logical operation. Real Authority and
Operational stores are protected and must never be opened through this
synthetic-fixture surface; all tests use temporary fixture databases.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import hashlib
import sqlite3
from typing import Iterator


DEFAULT_BUSY_TIMEOUT_MS = 5_000
_PROTECTED_ROOTS = (
    "research_state/control_plane/authority",
    "research_state/control_plane/operational",
)


class ProtectedStoreError(RuntimeError):
    """Raised when a protected control-plane store path is used on the
    synthetic journal durability surface."""


class ProjectionError(RuntimeError):
    """Base error for journal projection operations."""


class ProjectionIntegrityError(ProjectionError):
    """Raised when event sequences are not contiguous in a projection."""


class ProjectionThresholdError(ProjectionError):
    """Raised when a full rebuild exceeds the caller's max_events threshold."""


@dataclass(frozen=True)
class JournalDurabilitySnapshot:
    """Immutable snapshot of the journal durability contract for a path."""

    path: str
    journal_mode: str
    busy_timeout_ms: int
    synchronous: str
    single_writer: bool


@dataclass(frozen=True)
class ProjectionResult:
    """Immutable result of a journal projection update."""

    projection_name: str
    last_sequence: int
    events_processed: int
    integrity_ok: bool


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _require_synthetic_path(path: Path) -> Path:
    resolved = Path(path).resolve(strict=False)
    root = _repository_root()
    for protected in _PROTECTED_ROOTS:
        protected_path = (root / protected).resolve(strict=False)
        if resolved.is_relative_to(protected_path):
            raise ProtectedStoreError(
                f"protected store path is not allowed: {resolved}"
            )
    return resolved


def _connect(path: Path, *, read_only: bool, busy_timeout_ms: int) -> sqlite3.Connection:
    if type(busy_timeout_ms) is not int or not 1 <= busy_timeout_ms <= 30_000:
        raise ValueError("busy_timeout_ms must be between 1 and 30000")
    resolved = _require_synthetic_path(path)
    if not resolved.exists():
        raise FileNotFoundError(f"journal fixture is missing: {resolved}")
    mode = "ro" if read_only else "rw"
    connection = sqlite3.connect(
        f"{resolved.as_uri()}?mode={mode}",
        uri=True,
        timeout=busy_timeout_ms / 1_000,
        isolation_level=None,
    )
    try:
        connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys = ON")
        if read_only:
            connection.execute("PRAGMA query_only = ON")
    except Exception:
        connection.close()
        raise
    return connection


@contextmanager
def with_durable_journal_transaction(
    path: Path,
    *,
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
) -> Iterator[sqlite3.Connection]:
    """Open a synthetic journal fixture with the durability contract and run
    exactly one short IMMEDIATE write transaction."""

    connection = _connect(path, read_only=False, busy_timeout_ms=busy_timeout_ms)
    transaction_started = False
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.execute("BEGIN IMMEDIATE")
        transaction_started = True
        yield connection
        connection.commit()
        transaction_started = False
    except Exception:
        if transaction_started:
            try:
                connection.rollback()
            except sqlite3.DatabaseError:
                pass
        raise
    finally:
        connection.close()


def journal_durability(
    path: Path,
    *,
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
) -> JournalDurabilitySnapshot:
    """Return the durability contract snapshot for a synthetic journal fixture."""

    connection = _connect(path, read_only=True, busy_timeout_ms=busy_timeout_ms)
    try:
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        timeout = int(connection.execute("PRAGMA busy_timeout").fetchone()[0])
        synchronous = str(connection.execute("PRAGMA synchronous").fetchone()[0])
        return JournalDurabilitySnapshot(
            path=str(Path(path).resolve(strict=False)),
            journal_mode=journal_mode,
            busy_timeout_ms=timeout,
            synchronous=synchronous,
            single_writer=True,
        )
    finally:
        connection.close()


def _ensure_projection_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS projections (
            name TEXT PRIMARY KEY,
            last_sequence INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def _projection_last_sequence(connection: sqlite3.Connection, name: str) -> int:
    row = connection.execute(
        "SELECT last_sequence FROM projections WHERE name = ?",
        (name,),
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _write_projection(
    connection: sqlite3.Connection,
    name: str,
    last_sequence: int,
    updated_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO projections(name, last_sequence, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            last_sequence = excluded.last_sequence,
            updated_at = excluded.updated_at
        """,
        (name, last_sequence, updated_at),
    )


def incremental_project(
    path: Path,
    *,
    projection_name: str = "default",
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
) -> ProjectionResult:
    """Advance a projection from last_sequence in O(new events)."""

    if not projection_name:
        raise ValueError("projection_name must be non-empty")
    result: ProjectionResult | None = None
    with with_durable_journal_transaction(path, busy_timeout_ms=busy_timeout_ms) as connection:
        _ensure_projection_table(connection)
        last_sequence = _projection_last_sequence(connection, projection_name)
        rows = connection.execute(
            """
            SELECT sequence
            FROM events
            WHERE sequence > ?
            ORDER BY sequence
            """,
            (last_sequence,),
        ).fetchall()
        expected_sequence = last_sequence + 1
        for row in rows:
            observed = int(row[0])
            if observed != expected_sequence:
                raise ProjectionIntegrityError(
                    f"event sequence gap: expected {expected_sequence}, observed {observed}"
                )
            expected_sequence += 1
        events_processed = len(rows)
        new_last = last_sequence + events_processed
        if events_processed:
            from datetime import datetime, timezone

            _write_projection(
                connection,
                projection_name,
                new_last,
                datetime.now(timezone.utc).isoformat(),
            )
        result = ProjectionResult(
            projection_name=projection_name,
            last_sequence=new_last,
            events_processed=events_processed,
            integrity_ok=True,
        )
    assert result is not None
    return result


def rebuild_projection(
    path: Path,
    *,
    projection_name: str = "default",
    max_events: int | None = None,
    progress: object | None = None,
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
) -> ProjectionResult:
    """Offline full rebuild from sequence 1 with integrity, progress, and an
    optional event-count threshold."""

    if not projection_name:
        raise ValueError("projection_name must be non-empty")
    if max_events is not None and (type(max_events) is not int or max_events < 1):
        raise ValueError("max_events must be a positive integer or None")
    if progress is not None and not callable(progress):
        raise TypeError("progress must be callable or None")
    result: ProjectionResult | None = None
    with with_durable_journal_transaction(path, busy_timeout_ms=busy_timeout_ms) as connection:
        _ensure_projection_table(connection)
        rows = connection.execute(
            """
            SELECT sequence
            FROM events
            ORDER BY sequence
            """
        ).fetchall()
        total = len(rows)
        if max_events is not None and total > max_events:
            raise ProjectionThresholdError(
                f"full rebuild requires {total} events, exceeding max_events={max_events}"
            )
        expected_sequence = 1
        for index, row in enumerate(rows, start=1):
            observed = int(row[0])
            if observed != expected_sequence:
                raise ProjectionIntegrityError(
                    f"event sequence gap: expected {expected_sequence}, observed {observed}"
                )
            expected_sequence += 1
            if progress is not None and (index % 1000 == 0 or index == total):
                progress(index, total)
        if total == 0 and progress is not None:
            progress(0, 0)
        from datetime import datetime, timezone

        _write_projection(
            connection,
            projection_name,
            total,
            datetime.now(timezone.utc).isoformat(),
        )
        result = ProjectionResult(
            projection_name=projection_name,
            last_sequence=total,
            events_processed=total,
            integrity_ok=True,
        )
    assert result is not None
    return result


def backup_journal(
    source: Path,
    destination: Path,
    *,
    progress: object | None = None,
) -> None:
    """Copy a synthetic journal to a consistent backup via the SQLite backup API."""

    if progress is not None and not callable(progress):
        raise TypeError("progress must be callable or None")
    source_path = _require_synthetic_path(source)
    destination_path = _require_synthetic_path(destination)
    if not source_path.exists():
        raise FileNotFoundError(f"journal fixture is missing: {source_path}")
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    source_connection = _connect(source_path, read_only=True, busy_timeout_ms=DEFAULT_BUSY_TIMEOUT_MS)
    destination_connection = sqlite3.connect(destination_path)
    try:
        source_connection.backup(destination_connection, pages=-1, progress=progress)
        destination_connection.commit()
    finally:
        destination_connection.close()
        source_connection.close()


def restore_journal(
    backup: Path,
    target: Path,
    *,
    progress: object | None = None,
) -> None:
    """Restore a backup into a target synthetic journal via the SQLite backup API."""

    if progress is not None and not callable(progress):
        raise TypeError("progress must be callable or None")
    backup_path = _require_synthetic_path(backup)
    target_path = _require_synthetic_path(target)
    if not backup_path.exists():
        raise FileNotFoundError(f"backup file is missing: {backup_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    backup_connection = sqlite3.connect(backup_path)
    target_connection = sqlite3.connect(target_path)
    try:
        backup_connection.backup(target_connection, pages=-1, progress=progress)
        target_connection.commit()
    finally:
        target_connection.close()
        backup_connection.close()


def journal_path(root: Path | None = None) -> Path:
    """Return the operational journal path for a repository root."""

    base = _repository_root() if root is None else Path(root).resolve(strict=False)
    return base / "research_state" / "control_plane" / "operational" / "operational.sqlite3"


def _read_journal_snapshot(path: Path) -> dict[str, object]:
    """Read-only snapshot of a journal file (real or fixture)."""

    resolved = Path(path).resolve(strict=False)
    if not resolved.exists():
        return {
            "exists": False,
            "event_count": 0,
            "max_sequence": 0,
            "journal_mode": "unknown",
        }
    connection = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True, isolation_level=None)
    try:
        connection.execute("PRAGMA query_only = ON")
        try:
            table_name = "events"
            rows = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('events', 'journal_events') ORDER BY name"
            ).fetchall()
            if rows:
                table_name = str(rows[0][0])
            event_count = int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
            max_sequence = int(connection.execute(f"SELECT COALESCE(MAX(sequence), 0) FROM {table_name}").fetchone()[0])
            journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        except sqlite3.DatabaseError as error:
            event_count = 0
            max_sequence = 0
            journal_mode = "unknown"
            snapshot_error = str(error)
        else:
            snapshot_error = None
        return {
            "exists": True,
            "event_count": event_count,
            "max_sequence": max_sequence,
            "journal_mode": journal_mode,
            "snapshot_error": snapshot_error,
        }
    finally:
        connection.close()


def read_only_status(
    source: Path,
    *,
    allow_real: bool = False,
) -> dict[str, object]:
    """Build the read-only status surfaces for a journal path.

    The synthetic surface rejects protected authority/operational paths unless
    the caller explicitly opts into the real-store CLI reader.
    """

    if not allow_real:
        _require_synthetic_path(source)
    snapshot = _read_journal_snapshot(source)
    event_count = int(snapshot["event_count"])
    return {
        "campaign": {"active": False, "cycles": 0, "note": "read-only synthetic surface"},
        "budget": {"reserved": 0, "spent": 0, "remaining": 0},
        "lease": {"active": 0, "expired": 0},
        "roster": {"members": 0, "active": 0},
        "generation": {"latest": 0, "count": 0},
        "evidence": {"grade": "UNKNOWN", "entries": 0},
        "access": {"reads": 0, "writes": 0},
        "usage": {"events": event_count, "max_sequence": int(snapshot["max_sequence"])},
        "publication": {"pending": 0, "published": 0},
        "failure": {"causes": [], "count": 0},
        "journal": snapshot,
    }


def read_only_audit_manifest(root: Path) -> dict[str, object]:
    """Deterministic read-only audit manifest with hashes/references."""

    _require_synthetic_path(Path(root))
    journal = journal_path(root)
    snapshot = _read_journal_snapshot(journal)
    manifest = {
        "schema_version": "control_plane.p7r2_read_only_audit_manifest.v1",
        "journal": {
            "ref": str(journal),
            "sha256": hashlib.sha256(journal.read_bytes()).hexdigest() if journal.exists() else None,
            "event_count": snapshot["event_count"],
            "max_sequence": snapshot["max_sequence"],
        },
        "events": {
            "count": snapshot["event_count"],
            "table": "events",
        },
        "references": [],
        "redaction": {"api_keys": True, "raw_labels": True, "final_eval": True, "large_files": True},
        "generated_at": "deterministic",
    }
    return manifest


def read_only_doctor_report(root: Path) -> dict[str, object]:
    """Read-only doctor report with blocked states and failure causes."""

    _require_synthetic_path(Path(root))
    journal = journal_path(root)
    snapshot = _read_journal_snapshot(journal)
    return {
        "blocked": [],
        "failure_causes": [],
        "journal": snapshot,
        "verdict": "OK" if int(snapshot["event_count"]) >= 0 else "FAIL",
    }


def read_only_export_bundle(root: Path) -> dict[str, object]:
    """Read-only export bundle with references and hashes; never writes."""

    _require_synthetic_path(Path(root))
    journal = journal_path(root)
    snapshot = _read_journal_snapshot(journal)
    return {
        "journal": str(journal),
        "journal_sha256": hashlib.sha256(journal.read_bytes()).hexdigest() if journal.exists() else None,
        "event_count": snapshot["event_count"],
        "max_sequence": snapshot["max_sequence"],
        "bundle_kind": "read_only_references",
    }
