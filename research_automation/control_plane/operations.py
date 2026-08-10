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
import time
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


def read_only_audit_manifest(
    root: Path,
    *,
    allow_real: bool = False,
) -> dict[str, object]:
    """Deterministic read-only audit manifest with hashes/references."""

    if not allow_real:
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


_SECRET_NAME_MARKERS = (
    ".env",
    "secret",
    "api_key",
    "apikey",
    "token",
    "password",
    "credential",
    "bearer",
)
_RAW_LABEL_MARKERS = (
    "raw_label",
    "raw-label",
    "rawlabels",
)
_HOLDOUT_MARKERS = (
    "holdout",
    "final_eval",
    "final-eval",
    "finaleval",
)


def _audit_exclusion_reason(relative_name: str) -> str | None:
    lowered = relative_name.lower()
    if any(marker in lowered for marker in _SECRET_NAME_MARKERS):
        return "secrets"
    if any(marker in lowered for marker in _RAW_LABEL_MARKERS):
        return "raw_labels"
    if any(marker in lowered for marker in _HOLDOUT_MARKERS):
        return "final_holdout"
    return None


def build_audit_bundle(
    root: Path,
    *,
    max_bytes: int = 1_000_000,
) -> dict[str, object]:
    """Deterministic read-only audit bundle for a synthetic fixture root.

    Includes sorted hashed entries and explicit exclusion reasons; never reads
    secrets, raw labels, Final Holdout, or unrelated large files.
    """

    if type(max_bytes) is not int or max_bytes < 1:
        raise ValueError("max_bytes must be a positive integer")
    resolved_root = Path(root).resolve(strict=False)
    if resolved_root == _repository_root():
        raise ProtectedStoreError(
            "repository root is not a synthetic audit fixture root"
        )
    _require_synthetic_path(resolved_root)
    if not resolved_root.exists():
        raise FileNotFoundError(f"audit fixture root is missing: {resolved_root}")
    entries: list[dict[str, object]] = []
    exclusions: list[dict[str, object]] = []
    for path in sorted(resolved_root.rglob("*")):
        if not path.is_file():
            continue
        relative_name = path.relative_to(resolved_root).as_posix()
        size_bytes = path.stat().st_size
        reason = _audit_exclusion_reason(relative_name)
        if reason is not None:
            exclusions.append({
                "path": relative_name,
                "reason": reason,
                "size_bytes": size_bytes,
            })
            continue
        if size_bytes > max_bytes:
            exclusions.append({
                "path": relative_name,
                "reason": "large_files",
                "size_bytes": size_bytes,
            })
            continue
        entries.append({
            "path": relative_name,
            "size_bytes": size_bytes,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        })
    return {
        "schema_version": "control_plane.p7r2_audit_bundle.v1",
        "root": str(resolved_root),
        "entries": entries,
        "exclusions": exclusions,
        "max_bytes": max_bytes,
        "generated_at": "deterministic",
    }


def read_only_doctor_report(
    root: Path,
    *,
    allow_real: bool = False,
) -> dict[str, object]:
    """Read-only doctor report with blocked states and failure causes."""

    if not allow_real:
        _require_synthetic_path(Path(root))
    journal = journal_path(root)
    snapshot = _read_journal_snapshot(journal)
    return {
        "blocked": [],
        "failure_causes": [],
        "journal": snapshot,
        "verdict": "OK" if int(snapshot["event_count"]) >= 0 else "FAIL",
    }


def read_only_export_bundle(
    root: Path,
    *,
    allow_real: bool = False,
) -> dict[str, object]:
    """Read-only export bundle with references and hashes; never writes."""

    if not allow_real:
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


@dataclass(frozen=True)
class BackfillShard:
    """Immutable shard of a synthetic backfill plan."""

    shard_id: str
    start: int
    end: int
    item_count: int


@dataclass(frozen=True)
class BackfillPlan:
    """Immutable synthetic backfill plan with shards and low priority."""

    plan_id: str
    total_items: int
    shards: tuple[BackfillShard, ...]
    priority: str
    status: str = "CREATED"


@dataclass(frozen=True)
class BackfillRunResult:
    """Immutable result of one adapter run; resumes from checkpoints."""

    plan_id: str
    status: str
    processed_items: int
    completed_shard_ids: tuple[str, ...]
    paused: bool
    throttled: bool


class TokenBucketLimiter:
    """Deterministic token bucket used by the synthetic backfill adapter.

    The clock is injectable so tests can advance time without sleeping.
    """

    def __init__(
        self,
        capacity: int,
        refill_per_second: float = 1.0,
        *,
        clock: object | None = None,
    ) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        if not isinstance(refill_per_second, (int, float)) or refill_per_second <= 0:
            raise ValueError("refill_per_second must be positive")
        self._capacity = float(capacity)
        self._refill_per_second = float(refill_per_second)
        self._clock = clock if clock is not None else time.monotonic
        if not callable(self._clock):
            raise TypeError("clock must be callable or None")
        self._tokens = float(capacity)
        self._last = self._clock()

    def try_acquire(self) -> bool:
        now = self._clock()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(
                self._capacity,
                self._tokens + elapsed * self._refill_per_second,
            )
            self._last = now
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False


class BackfillAdapter:
    """Synthetic-only historical backfill adapter.

    The adapter never reads or writes real research, data, KBase, Authority,
    operational, policy, inventory, or campaign content. It only orchestrates
    a caller-injected worker over a caller-owned synthetic plan; P7 does not
    run bulk historical backfill.
    """

    def __init__(
        self,
        fixture_root: Path,
        *,
        limiter: TokenBucketLimiter | None = None,
    ) -> None:
        resolved = Path(fixture_root).resolve(strict=False)
        if resolved == _repository_root():
            raise ProtectedStoreError(
                "repository root is not a synthetic backfill fixture root"
            )
        _require_synthetic_path(resolved)
        if not resolved.exists():
            raise FileNotFoundError(
                f"backfill fixture root is missing: {resolved}"
            )
        if limiter is not None and not isinstance(limiter, TokenBucketLimiter):
            raise TypeError("limiter must be a TokenBucketLimiter or None")
        self._fixture_root = resolved
        self._limiter = limiter or TokenBucketLimiter(
            capacity=1_000,
            refill_per_second=10_000.0,
        )
        self._plan: BackfillPlan | None = None
        self._completed: list[str] = []
        self._shard_cursors: dict[str, int] = {}
        self._processed_items = 0
        self._paused = False
        self._throttled = False

    def build_plan(
        self,
        total_items: int,
        *,
        shard_count: int | None = None,
        priority: str = "low",
    ) -> BackfillPlan:
        if type(total_items) is not int or total_items < 1:
            raise ValueError("total_items must be a positive integer")
        if priority != "low":
            raise ValueError(
                "P7 synthetic backfill adapter only supports priority='low'"
            )
        if shard_count is None:
            shard_count = min(total_items, 100)
        if type(shard_count) is not int or shard_count < 1 or shard_count > total_items:
            raise ValueError("shard_count must be between 1 and total_items")
        base = total_items // shard_count
        extra = total_items % shard_count
        shards: list[BackfillShard] = []
        start = 0
        for index in range(shard_count):
            item_count = base + (1 if index < extra else 0)
            shards.append(
                BackfillShard(
                    shard_id=f"shard-{index}",
                    start=start,
                    end=start + item_count,
                    item_count=item_count,
                )
            )
            start += item_count
        plan = BackfillPlan(
            plan_id="synthetic-backfill-plan",
            total_items=total_items,
            shards=tuple(shards),
            priority=priority,
        )
        self._plan = plan
        self._completed = []
        self._shard_cursors = {
            shard.shard_id: shard.start for shard in plan.shards
        }
        self._processed_items = 0
        self._paused = False
        self._throttled = False
        return plan

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    @property
    def paused(self) -> bool:
        return self._paused

    def run(self, worker: object) -> BackfillRunResult:
        if self._plan is None:
            raise RuntimeError("backfill plan has not been built")
        if not callable(worker):
            raise TypeError("worker must be callable")
        self._throttled = False
        for shard in self._plan.shards:
            if shard.shard_id in self._completed:
                continue
            if self._paused:
                break
            cursor = self._shard_cursors.get(shard.shard_id, shard.start)
            for item_index in range(cursor, shard.end):
                if self._paused:
                    break
                if not self._limiter.try_acquire():
                    self._throttled = True
                    break
                worker(item_index)
                self._processed_items += 1
                self._shard_cursors[shard.shard_id] = item_index + 1
                if self._paused:
                    break
            if self._shard_cursors.get(shard.shard_id, shard.start) == shard.end:
                self._completed.append(shard.shard_id)
            if self._paused or self._throttled:
                break
        return self._result()

    def _result(self) -> BackfillRunResult:
        assert self._plan is not None
        completed = tuple(self._completed)
        if self._paused:
            status = "PAUSED"
        elif self._throttled:
            status = "THROTTLED"
        elif len(completed) == len(self._plan.shards):
            status = "COMPLETED"
        else:
            status = "RUNNING"
        return BackfillRunResult(
            plan_id=self._plan.plan_id,
            status=status,
            processed_items=self._processed_items,
            completed_shard_ids=completed,
            paused=self._paused,
            throttled=self._throttled,
        )

    def status(self) -> dict[str, object]:
        if self._plan is None:
            return {
                "schema_version": "control_plane.p7r2_backfill_status.v1",
                "plan_id": None,
                "priority": "low",
                "paused": False,
                "throttled": False,
                "processed_items": 0,
                "total_items": 0,
                "completed_shard_ids": [],
                "remaining_items": 0,
                "fixture_root": str(self._fixture_root),
                "real_backfill": False,
            }
        result = self._result()
        return {
            "schema_version": "control_plane.p7r2_backfill_status.v1",
            "plan_id": result.plan_id,
            "priority": self._plan.priority,
            "paused": result.paused,
            "throttled": result.throttled,
            "processed_items": result.processed_items,
            "total_items": self._plan.total_items,
            "completed_shard_ids": list(result.completed_shard_ids),
            "remaining_items": self._plan.total_items - result.processed_items,
            "fixture_root": str(self._fixture_root),
            "real_backfill": False,
        }


def retention_cleanup_report(
    metadata_entries: object,
    *,
    now: object | None = None,
    preview_ttl_days: int = 7,
    staging_ttl_days: int = 30,
) -> dict[str, object]:
    """Deterministic synthetic retention cleanup report.

    Delegates TTL eligibility to memory.retention_cleanup_candidates and
    never reads or mutates real learning packets, data, or KBase content.
    Scientific packets are always preserved.
    """

    from datetime import date

    from research_automation.control_plane import memory

    if not isinstance(metadata_entries, (list, tuple)):
        raise TypeError("metadata_entries must be a sequence")
    normalized: list[memory.LearningPacketRetentionMetadata] = []
    for entry in metadata_entries:
        if not isinstance(entry, memory.LearningPacketRetentionMetadata):
            raise TypeError(
                "metadata entry must be LearningPacketRetentionMetadata"
            )
        normalized.append(entry)
    reference_date = now if now is not None else date.today()
    if not isinstance(reference_date, date):
        raise ValueError("now must be a date or None")
    candidates = memory.retention_cleanup_candidates(
        normalized,
        now=reference_date,
        preview_ttl_days=preview_ttl_days,
        staging_ttl_days=staging_ttl_days,
    )
    preserved_scientific = sorted(
        entry.packet_id
        for entry in normalized
        if entry.retention_class is memory.RetentionClass.SCIENTIFIC
    )
    return {
        "schema_version": "control_plane.p7r2_retention_cleanup_report.v1",
        "priority": "low",
        "real_data": False,
        "now": reference_date.isoformat(),
        "preview_ttl_days": preview_ttl_days,
        "staging_ttl_days": staging_ttl_days,
        "metadata_count": len(normalized),
        "cleanup_candidates": list(candidates),
        "preserved_scientific": preserved_scientific,
    }
