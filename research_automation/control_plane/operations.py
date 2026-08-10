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


@dataclass(frozen=True)
class JournalDurabilitySnapshot:
    """Immutable snapshot of the journal durability contract for a path."""

    path: str
    journal_mode: str
    busy_timeout_ms: int
    synchronous: str
    single_writer: bool


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
