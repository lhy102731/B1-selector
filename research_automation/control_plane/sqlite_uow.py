"""Small SQLite transaction kernel shared by trusted control-plane stores."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
from typing import Callable, TypeVar


_T = TypeVar("_T")
_SQL_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_FORBIDDEN_ACTIONS = frozenset({sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH})


class SqliteUnitOfWorkError(RuntimeError):
    """Base error for one control-plane SQLite operation."""


class SqliteStoreMissingError(SqliteUnitOfWorkError):
    """Raised when a runtime operation would create a missing store."""


class SqliteStoreBusyError(SqliteUnitOfWorkError):
    """Raised after the bounded SQLite lock wait expires."""


class SqliteStoreCorruptError(SqliteUnitOfWorkError):
    """Raised when SQLite reports malformed store bytes."""


class SqliteSchemaError(SqliteUnitOfWorkError):
    """Raised when a store does not match its sealed schema identity."""


class SqliteFutureSchemaError(SqliteSchemaError):
    """Raised when a runtime is older than the store schema."""


class SqliteReadOnlyError(SqliteUnitOfWorkError):
    """Raised when a read snapshot attempts a write."""


class SqliteStatementForbiddenError(SqliteUnitOfWorkError):
    """Raised when SQLite denies a forbidden cross-store statement."""


@dataclass(frozen=True)
class _StoreSpec:
    path: Path
    store_kind: str
    metadata_table: str
    schema_version: int

    def __post_init__(self) -> None:
        resolved = Path(self.path).resolve(strict=False)
        if not resolved.is_absolute():
            raise ValueError("store path must be absolute")
        if not self.store_kind or not _SQL_IDENTIFIER.fullmatch(
            self.metadata_table
        ):
            raise ValueError("invalid store schema specification")
        if type(self.schema_version) is not int or self.schema_version < 1:
            raise ValueError("schema_version must be a positive integer")
        object.__setattr__(self, "path", resolved)


def _sqlite_authorizer(
    action_code: int,
    _parameter_one: str | None,
    _parameter_two: str | None,
    _database_name: str | None,
    _trigger_name: str | None,
) -> int:
    if action_code in _FORBIDDEN_ACTIONS:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def _translate_sqlite_error(
    error: sqlite3.DatabaseError,
    *,
    read_only: bool,
) -> SqliteUnitOfWorkError:
    message = str(error).lower()
    if "locked" in message or "busy" in message:
        return SqliteStoreBusyError("control-plane store is busy")
    if "not a database" in message or "malformed" in message:
        return SqliteStoreCorruptError("control-plane store is corrupt")
    if "not authorized" in message or "authorization denied" in message:
        return SqliteStatementForbiddenError(
            "cross-store SQLite statements are forbidden"
        )
    if read_only and ("readonly" in message or "read-only" in message):
        return SqliteReadOnlyError("read snapshot cannot modify its store")
    return SqliteUnitOfWorkError("control-plane SQLite operation failed")


class _SqliteUnitOfWork:
    """Internal single-store connection and transaction boundary."""

    def __init__(
        self,
        spec: _StoreSpec,
        *,
        busy_timeout_ms: int = 2_000,
    ) -> None:
        if not isinstance(spec, _StoreSpec):
            raise TypeError("spec must be a StoreSpec")
        if type(busy_timeout_ms) is not int or not 1 <= busy_timeout_ms <= 30_000:
            raise ValueError("busy_timeout_ms must be between 1 and 30000")
        self._spec = spec
        self._busy_timeout_ms = busy_timeout_ms

    def _connect(self, *, read_only: bool) -> sqlite3.Connection:
        if not os.path.lexists(self._spec.path):
            raise SqliteStoreMissingError("pre-provisioned store is required")
        mode = "ro" if read_only else "rw"
        database_uri = self._spec.path.as_uri()
        try:
            connection = sqlite3.connect(
                f"{database_uri}?mode={mode}",
                uri=True,
                timeout=self._busy_timeout_ms / 1_000,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            connection.set_authorizer(_sqlite_authorizer)
            connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
            connection.execute("PRAGMA foreign_keys = ON")
            if read_only:
                connection.execute("PRAGMA query_only = ON")
            return connection
        except sqlite3.DatabaseError as error:
            raise _translate_sqlite_error(error, read_only=read_only) from error

    def _validate_schema(self, connection: sqlite3.Connection) -> None:
        try:
            user_version = int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
            metadata = {
                str(row["key"]): str(row["value"])
                for row in connection.execute(
                    f"SELECT key, value FROM {self._spec.metadata_table}"
                )
            }
        except sqlite3.DatabaseError as error:
            raise _translate_sqlite_error(error, read_only=True) from error
        if user_version > self._spec.schema_version:
            raise SqliteFutureSchemaError("future store schema is not supported")
        if (
            user_version != self._spec.schema_version
            or metadata.get("schema_version") != str(self._spec.schema_version)
            or metadata.get("store_kind") != self._spec.store_kind
        ):
            raise SqliteSchemaError("control-plane store schema identity mismatch")

    def _read(
        self,
        operation: Callable[[sqlite3.Connection], _T],
    ) -> _T:
        connection = self._connect(read_only=True)
        try:
            self._validate_schema(connection)
            return operation(connection)
        except SqliteUnitOfWorkError:
            raise
        except sqlite3.DatabaseError as error:
            raise _translate_sqlite_error(error, read_only=True) from error
        finally:
            connection.close()


__all__ = [
    "SqliteFutureSchemaError",
    "SqliteReadOnlyError",
    "SqliteSchemaError",
    "SqliteStatementForbiddenError",
    "SqliteStoreBusyError",
    "SqliteStoreCorruptError",
    "SqliteStoreMissingError",
    "SqliteUnitOfWorkError",
]
