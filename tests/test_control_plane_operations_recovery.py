"""Tests for validated OperationalJournal backup and restore (P7R3 T4)."""

from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane.operations_recovery import (
    OperationsBackupError,
    OperationsMaintenanceContextRequired,
    OperationsRestoreBlocked,
    OperationsRestoreError,
    backup_operational_journal,
    restore_operational_journal,
)
from research_automation.control_plane import stores as stores_module
from tests.test_control_plane_campaign_store import (
    NOW,
    ROOT_SECRET,
    _authorized_campaign,
)


class _MaintenanceContext:
    maintenance_authorized = True


class BackupContractTests(unittest.TestCase):
    def test_backup_uses_sqlite_api_and_includes_wal_committed_rows(self) -> None:
        campaign_id = "campaign-recovery-wal"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            # Write one event but leave it in the WAL (no checkpoint).
            conn = sqlite3.connect(str(root / "operational.sqlite3"))
            conn.execute("BEGIN IMMEDIATE")
            import json as _json
            import hashlib as _hashlib

            payload_json = _json.dumps(
                {"x": 1}, sort_keys=True, separators=(",", ":")
            )
            payload_sha256 = _hashlib.sha256(
                payload_json.encode("utf-8")
            ).hexdigest()
            conn.execute(
                """INSERT INTO campaign_events
                (event_id, namespace, campaign_id, cycle_id, aggregate_type,
                 aggregate_id, event_type, payload_json, payload_sha256,
                 occurred_at)
                VALUES (?, 'formal', ?, NULL, 'campaign', ?, 'CAMPAIGN_CREATED',
                        ?, ?, ?)""",
                (
                    f"evt-wal-{campaign_id}",
                    campaign_id,
                    campaign_id,
                    payload_json,
                    payload_sha256,
                    NOW.isoformat(),
                ),
            )
            conn.commit()
            # do NOT checkpoint; the row lives in the WAL
            conn.close()

            backup = root / "operational.backup"
            receipt = backup_operational_journal(backup_path=backup)
            self.assertTrue(receipt.quick_check_ok)
            self.assertTrue(receipt.foreign_keys_ok)
            self.assertGreaterEqual(receipt.row_counts["campaign_events"], 1)
            self.assertEqual(receipt.target, "operational")
            self.assertTrue(receipt.manifest_sha256)

    def test_backup_is_create_only(self) -> None:
        with _authorized_campaign("campaign-recovery-createonly") as (root, _, journal):
            backup = root / "operational.backup"
            backup_operational_journal(backup_path=backup)
            with self.assertRaises(OperationsBackupError):
                backup_operational_journal(backup_path=backup)

    def test_restore_requires_maintenance_context(self) -> None:
        with _authorized_campaign("campaign-recovery-ctx") as (root, _, journal):
            backup = root / "operational.backup"
            backup_operational_journal(backup_path=backup)
            with self.assertRaises(OperationsMaintenanceContextRequired):
                restore_operational_journal(
                    backup_path=backup,
                    staging_path=root / "operational.staging",
                    maintenance_context=None,
                )

    def test_restore_to_staging_validates_and_does_not_publish(self) -> None:
        with _authorized_campaign("campaign-recovery-staging") as (root, _, journal):
            backup = root / "operational.backup"
            backup_operational_journal(backup_path=backup)
            staging = root / "operational.staging"
            receipt = restore_operational_journal(
                backup_path=backup,
                staging_path=staging,
                maintenance_context=_MaintenanceContext(),
                publish=False,
            )
            self.assertFalse(receipt.published)
            self.assertTrue(receipt.quick_check_ok)
            self.assertTrue(staging.exists())
            # current store untouched
            self.assertTrue((root / "operational.sqlite3").exists())

    def test_restore_rejects_corrupt_backup(self) -> None:
        with _authorized_campaign("campaign-recovery-corrupt") as (root, _, journal):
            backup = root / "operational.backup"
            backup.write_bytes(b"not a sqlite database")
            with self.assertRaises(OperationsRestoreError):
                restore_operational_journal(
                    backup_path=backup,
                    staging_path=root / "operational.staging",
                    maintenance_context=_MaintenanceContext(),
                )

    def test_restore_rejects_missing_backup(self) -> None:
        with _authorized_campaign("campaign-recovery-missing") as (root, _, journal):
            with self.assertRaises(OperationsRestoreError):
                restore_operational_journal(
                    backup_path=root / "does-not-exist.backup",
                    staging_path=root / "operational.staging",
                    maintenance_context=_MaintenanceContext(),
                )

    def test_failed_staging_keeps_current_store_unchanged(self) -> None:
        with _authorized_campaign("campaign-recovery-failinj") as (root, _, journal):
            backup = root / "operational.backup"
            backup_operational_journal(backup_path=backup)
            original = (root / "operational.sqlite3").read_bytes()
            staging = root / "operational.staging"
            # inject failure: pre-create staging (create-only conflict)
            staging.write_bytes(b"occupied")
            with self.assertRaises(OperationsRestoreError):
                restore_operational_journal(
                    backup_path=backup,
                    staging_path=staging,
                    maintenance_context=_MaintenanceContext(),
                )
            self.assertEqual((root / "operational.sqlite3").read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
