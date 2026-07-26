from __future__ import annotations

import hashlib
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.sqlite_uow import (
    SqliteFutureSchemaError,
    SqliteReadOnlyError,
    SqliteStatementForbiddenError,
    SqliteStoreCorruptError,
    SqliteStoreBusyError,
    SqliteStoreMissingError,
    SqliteUnitOfWorkError,
    _SqliteUnitOfWork,
    _StoreSpec,
)


class SqliteUnitOfWorkTests(unittest.TestCase):
    def test_read_snapshot_cannot_write_the_store(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap()

            before = hashlib.sha256(authority_path.read_bytes()).hexdigest()
            unit_of_work = _SqliteUnitOfWork(
                _StoreSpec(
                    path=authority_path,
                    store_kind="AUTHORITY_STORE",
                    metadata_table="authority_meta",
                    schema_version=1,
                ),
                busy_timeout_ms=50,
            )

            with self.assertRaises(SqliteReadOnlyError):
                unit_of_work._read(
                    lambda connection: connection.execute(
                        "CREATE TABLE forbidden_write(value TEXT)"
                    )
                )

            after = hashlib.sha256(authority_path.read_bytes()).hexdigest()
            self.assertEqual(after, before)

    def test_write_immediate_commits_one_store_transaction(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap()

            unit_of_work = _SqliteUnitOfWork(
                _StoreSpec(
                    path=authority_path,
                    store_kind="AUTHORITY_STORE",
                    metadata_table="authority_meta",
                    schema_version=1,
                ),
                busy_timeout_ms=50,
            )

            def write_value(connection) -> None:
                connection.execute(
                    "CREATE TABLE committed_value(value TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO committed_value(value) VALUES ('committed')"
                )

            unit_of_work._write(write_value)

            value = unit_of_work._read(
                lambda connection: connection.execute(
                    "SELECT value FROM committed_value"
                ).fetchone()[0]
            )
            self.assertEqual(value, "committed")

    def test_attach_is_denied_before_another_database_is_created(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            attached_path = root / "forbidden-attached.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap()

            unit_of_work = _SqliteUnitOfWork(
                _StoreSpec(
                    path=authority_path,
                    store_kind="AUTHORITY_STORE",
                    metadata_table="authority_meta",
                    schema_version=1,
                ),
                busy_timeout_ms=50,
            )

            with self.assertRaises(SqliteStatementForbiddenError):
                unit_of_work._read(
                    lambda connection: connection.execute(
                        "ATTACH DATABASE ? AS forbidden_store",
                        (str(attached_path),),
                    )
                )

            self.assertFalse(attached_path.exists())

    def test_missing_store_fails_without_recreating_it(self) -> None:
        with TemporaryDirectory() as tmp:
            missing_path = Path(tmp) / "missing-authority.sqlite3"
            unit_of_work = _SqliteUnitOfWork(
                _StoreSpec(
                    path=missing_path,
                    store_kind="AUTHORITY_STORE",
                    metadata_table="authority_meta",
                    schema_version=1,
                ),
                busy_timeout_ms=50,
            )

            with self.assertRaises(SqliteStoreMissingError):
                unit_of_work._read(lambda _connection: None)

            self.assertFalse(missing_path.exists())

    def test_corrupt_store_is_translated_without_exposing_sqlite_details(self) -> None:
        with TemporaryDirectory() as tmp:
            corrupt_path = Path(tmp) / "corrupt-authority.sqlite3"
            corrupt_path.write_bytes(b"not a sqlite database")
            unit_of_work = _SqliteUnitOfWork(
                _StoreSpec(
                    path=corrupt_path,
                    store_kind="AUTHORITY_STORE",
                    metadata_table="authority_meta",
                    schema_version=1,
                ),
                busy_timeout_ms=50,
            )

            with self.assertRaisesRegex(
                SqliteStoreCorruptError,
                "control-plane store is corrupt",
            ) as caught:
                unit_of_work._read(lambda _connection: None)

            self.assertNotIn("not a database", str(caught.exception).lower())

    def test_future_store_schema_fails_closed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap()
            connection = sqlite3.connect(authority_path)
            try:
                connection.execute("PRAGMA user_version = 2")
                connection.commit()
            finally:
                connection.close()

            unit_of_work = _SqliteUnitOfWork(
                _StoreSpec(
                    path=authority_path,
                    store_kind="AUTHORITY_STORE",
                    metadata_table="authority_meta",
                    schema_version=1,
                ),
                busy_timeout_ms=50,
            )

            with self.assertRaises(SqliteFutureSchemaError):
                unit_of_work._read(lambda _connection: None)

    def test_locked_store_uses_a_bounded_wait_and_domain_error(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap()

            lock_connection = sqlite3.connect(
                authority_path,
                isolation_level=None,
            )
            lock_connection.execute("BEGIN EXCLUSIVE")
            try:
                unit_of_work = _SqliteUnitOfWork(
                    _StoreSpec(
                        path=authority_path,
                        store_kind="AUTHORITY_STORE",
                        metadata_table="authority_meta",
                        schema_version=1,
                    ),
                    busy_timeout_ms=25,
                )
                started = time.monotonic()

                with self.assertRaises(SqliteStoreBusyError):
                    unit_of_work._write(lambda _connection: None)

                elapsed = time.monotonic() - started
                self.assertLess(elapsed, 0.5)
            finally:
                lock_connection.rollback()
                lock_connection.close()

    def test_foreign_key_failure_rolls_back_the_whole_transaction(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap()

            unit_of_work = _SqliteUnitOfWork(
                _StoreSpec(
                    path=authority_path,
                    store_kind="AUTHORITY_STORE",
                    metadata_table="authority_meta",
                    schema_version=1,
                ),
                busy_timeout_ms=50,
            )

            def violate_foreign_key(connection) -> None:
                connection.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
                connection.execute(
                    """
                    CREATE TABLE child(
                        parent_id INTEGER NOT NULL REFERENCES parent(id)
                    )
                    """
                )
                connection.execute("INSERT INTO child(parent_id) VALUES (7)")

            with self.assertRaises(SqliteUnitOfWorkError):
                unit_of_work._write(violate_foreign_key)

            created_tables = unit_of_work._read(
                lambda connection: connection.execute(
                    """
                    SELECT COUNT(*) FROM sqlite_master
                    WHERE type = 'table' AND name IN ('parent', 'child')
                    """
                ).fetchone()[0]
            )
            self.assertEqual(created_tables, 0)


if __name__ == "__main__":
    unittest.main()
