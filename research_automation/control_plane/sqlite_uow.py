"""Small SQLite transaction kernel shared by trusted control-plane stores."""

from __future__ import annotations

import hashlib
import json
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
    expected_schema_sha256: str | None = None

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
        if (
            self.expected_schema_sha256 is not None
            and re.fullmatch(r"[0-9a-f]{64}", self.expected_schema_sha256)
            is None
        ):
            raise ValueError("expected_schema_sha256 must be a SHA-256 digest")
        object.__setattr__(self, "path", resolved)


def _schema_sha256(connection: sqlite3.Connection) -> str:
    records = [
        {
            "type": str(row[0]),
            "name": str(row[1]),
            "table": str(row[2]),
            "sql": None if row[3] is None else str(row[3]),
        }
        for row in connection.execute(
            """
            SELECT type, name, tbl_name, sql
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type, name
            """
        )
    ]
    canonical = json.dumps(
        records,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(b"control_plane.sqlite_schema.v1\0" + canonical).hexdigest()


def _schema_sha256_for_statements(statements: tuple[str, ...]) -> str:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    try:
        connection.execute("BEGIN IMMEDIATE")
        for statement in statements:
            connection.execute(statement)
        digest = _schema_sha256(connection)
        connection.rollback()
        return digest
    finally:
        connection.close()


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

    def _validate_schema(
        self,
        connection: sqlite3.Connection,
        *,
        read_only: bool,
    ) -> None:
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
            raise _translate_sqlite_error(error, read_only=read_only) from error
        if user_version > self._spec.schema_version:
            raise SqliteFutureSchemaError("future store schema is not supported")
        if (
            user_version != self._spec.schema_version
            or metadata.get("schema_version") != str(self._spec.schema_version)
            or metadata.get("store_kind") != self._spec.store_kind
        ):
            raise SqliteSchemaError("control-plane store schema identity mismatch")
        if (
            self._spec.expected_schema_sha256 is not None
            and _schema_sha256(connection)
            != self._spec.expected_schema_sha256
        ):
            raise SqliteSchemaError("control-plane store schema structure mismatch")

    def _read(
        self,
        operation: Callable[[sqlite3.Connection], _T],
    ) -> _T:
        connection = self._connect(read_only=True)
        transaction_started = False
        try:
            connection.execute("BEGIN")
            transaction_started = True
            self._validate_schema(connection, read_only=True)
            result = operation(connection)
            connection.rollback()
            transaction_started = False
            return result
        except SqliteUnitOfWorkError:
            if transaction_started:
                self._rollback_without_masking(connection)
            raise
        except sqlite3.DatabaseError as error:
            if transaction_started:
                self._rollback_without_masking(connection)
            raise _translate_sqlite_error(error, read_only=True) from error
        except Exception:
            if transaction_started:
                self._rollback_without_masking(connection)
            raise
        finally:
            connection.close()

    def _write(
        self,
        operation: Callable[[sqlite3.Connection], _T],
    ) -> _T:
        connection = self._connect(read_only=False)
        transaction_started = False
        try:
            connection.execute("BEGIN IMMEDIATE")
            transaction_started = True
            self._validate_schema(connection, read_only=False)
            result = operation(connection)
            connection.commit()
            transaction_started = False
            return result
        except SqliteUnitOfWorkError:
            if transaction_started:
                self._rollback_without_masking(connection)
            raise
        except sqlite3.DatabaseError as error:
            if transaction_started:
                self._rollback_without_masking(connection)
            raise _translate_sqlite_error(error, read_only=False) from error
        except Exception:
            if transaction_started:
                self._rollback_without_masking(connection)
            raise
        finally:
            connection.close()

    @staticmethod
    def _rollback_without_masking(connection: sqlite3.Connection) -> None:
        try:
            connection.rollback()
        except sqlite3.DatabaseError:
            pass


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
