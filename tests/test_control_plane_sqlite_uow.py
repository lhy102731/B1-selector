from __future__ import annotations

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.sqlite_uow import (
    SqliteReadOnlyError,
    SqliteStatementForbiddenError,
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


if __name__ == "__main__":
    unittest.main()
