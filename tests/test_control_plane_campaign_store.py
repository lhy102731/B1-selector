from __future__ import annotations

import hashlib
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"


class OperationalCampaignMigrationTests(unittest.TestCase):
    def test_v2_migration_adds_campaign_events_without_touching_authority(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                original_schema = stores_module._OPERATIONAL_SCHEMA
                original_version = stores_module._OPERATIONAL_SCHEMA_VERSION
                try:
                    stores_module._OPERATIONAL_SCHEMA = (
                        stores_module._OPERATIONAL_SCHEMA_V2
                    )
                    stores_module._OPERATIONAL_SCHEMA_VERSION = 2
                    stores_module._expected_schema_sha256.cache_clear()
                    stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                finally:
                    stores_module._OPERATIONAL_SCHEMA = original_schema
                    stores_module._OPERATIONAL_SCHEMA_VERSION = original_version
                    stores_module._expected_schema_sha256.cache_clear()

                authority_before = hashlib.sha256(
                    authority_path.read_bytes()
                ).hexdigest()
                self.assertTrue(
                    stores_module._migrate_operational_journal_v3(
                        root_secret=ROOT_SECRET
                    )
                )
                self.assertFalse(
                    stores_module._migrate_operational_journal_v3(
                        root_secret=ROOT_SECRET
                    )
                )
                self.assertEqual(
                    hashlib.sha256(authority_path.read_bytes()).hexdigest(),
                    authority_before,
                )
                connection = sqlite3.connect(operational_path)
                try:
                    table = connection.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE type = 'table' AND name = 'campaign_events'"
                    ).fetchone()
                    version = connection.execute(
                        "PRAGMA user_version"
                    ).fetchone()[0]
                finally:
                    connection.close()
                self.assertIsNotNone(table)
                self.assertEqual(version, 3)


if __name__ == "__main__":
    unittest.main()
