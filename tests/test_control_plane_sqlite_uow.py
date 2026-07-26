from __future__ import annotations

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.sqlite_uow import (
    SqliteReadOnlyError,
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


if __name__ == "__main__":
    unittest.main()
