"""P0-CR-008 slice B/C: narrow store migration coordinator contracts."""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane import store_migration


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"


class StoreMigrationCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        root = Path(self.temporary.name)
        self.authority_path = root / "authority.sqlite3"
        self.operational_path = root / "operational.sqlite3"
        self.paths = patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=self.authority_path,
            _OPERATIONAL_STORE_PATH=self.operational_path,
        )
        self.paths.start()
        stores_module._expected_schema_sha256.cache_clear()
        stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
        self.coordinator = store_migration.StoreMigrationCoordinator(
            root_secret=ROOT_SECRET
        )

    def tearDown(self) -> None:
        self.paths.stop()
        stores_module._expected_schema_sha256.cache_clear()
        self.temporary.cleanup()

    def _provision_authority_v1(self) -> None:
        installation_id = (
            stores_module.AuthorityReader().read_identity().installation_id
        )
        self.authority_path.unlink(missing_ok=True)
        for suffix in ("-journal", "-wal", "-shm"):
            Path(f"{self.authority_path}{suffix}").unlink(missing_ok=True)
        original_schema = stores_module._AUTHORITY_SCHEMA
        original_version = stores_module._AUTHORITY_SCHEMA_VERSION
        try:
            stores_module._AUTHORITY_SCHEMA = stores_module._AUTHORITY_SCHEMA_V1
            stores_module._AUTHORITY_SCHEMA_VERSION = 1
            stores_module._expected_schema_sha256.cache_clear()
            stores_module._provision_store(
                self.authority_path,
                store_kind="AUTHORITY_STORE",
                metadata_table="authority_meta",
                installation_id=installation_id,
                root_capability_sha256=stores_module._root_secret_sha256(
                    ROOT_SECRET
                ),
            )
        finally:
            stores_module._AUTHORITY_SCHEMA = original_schema
            stores_module._AUTHORITY_SCHEMA_VERSION = original_version
            stores_module._expected_schema_sha256.cache_clear()

    def test_backup_creates_complete_snapshot_and_manifest(self) -> None:
        backup_path = Path(self.temporary.name) / "authority.backup.sqlite3"
        receipt = self.coordinator.backup(
            store_migration.MigrationTarget.AUTHORITY,
            backup_path,
        )
        self.assertTrue(backup_path.exists())
        self.assertEqual(
            receipt.installation_id,
            stores_module.AuthorityReader().read_identity().installation_id,
        )
        self.assertEqual(receipt.schema_version, 2)
        self.assertTrue(receipt.quick_check_ok)
        self.assertEqual(
            receipt.file_sha256,
            hashlib.sha256(backup_path.read_bytes()).hexdigest(),
        )
        connection = sqlite3.connect(backup_path)
        try:
            integrity = connection.execute(
                "PRAGMA integrity_check"
            ).fetchone()[0]
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(integrity, "ok")
        self.assertEqual(version, 2)

    def test_rehearsal_restores_backup_into_staging_without_touching_live(
        self,
    ) -> None:
        self._provision_authority_v1()
        backup_path = Path(self.temporary.name) / "authority.backup.sqlite3"
        staging_path = Path(self.temporary.name) / "authority.staging.sqlite3"
        self.coordinator.backup(
            store_migration.MigrationTarget.AUTHORITY,
            backup_path,
        )
        live_version_before = self._authority_user_version()
        rehearsal = self.coordinator.rehearse(
            store_migration.MigrationTarget.AUTHORITY,
            backup_path=backup_path,
            staging_path=staging_path,
        )
        self.assertTrue(rehearsal.migrated)
        self.assertEqual(rehearsal.staging_schema_version, 2)
        connection = sqlite3.connect(staging_path)
        try:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                "AND name = 'final_eval_authorizations_v1'"
            ).fetchone()
        finally:
            connection.close()
        self.assertIsNotNone(table)
        self.assertEqual(self._authority_user_version(), live_version_before)

    def test_publish_rejects_active_objects(self) -> None:
        actor = stores_module.Actor(
            "p0-runner", "automation", "invocation-migration-tests"
        )
        authority = stores_module._AuthorityStore(
            root_secret=ROOT_SECRET
        )
        identity = stores_module.AuthorityIdentity("a" * 64, "b" * 64, "c" * 64)
        envelope = authority._provision_authorization(
            phase=stores_module.Phase.P0,
            attempt_id="p0-migration-attempt",
            actor=actor,
            identity=identity,
            expires_at=datetime(
                2027, 1, 1, tzinfo=timezone.utc
            ),
            allowed_side_effects=(
                stores_module.SideEffect.WRITE_CONTROL_PLANE,
            ),
        )
        grant = authority.claim_authorization(
            envelope,
            expected_phase=stores_module.Phase.P0,
            expected_attempt_id="p0-migration-attempt",
            actor=actor,
            identity=identity,
        )
        ticket = authority._issue_task_ticket(
            grant,
            {
                "task_id": "P0-MIGRATION-ACTIVE-GUARD",
                "objective": "prove active objects block migration",
                "dependencies": [],
                "idempotency_key": "p0-migration-active-guard-001",
                "task_spec_ref": (
                    "research_state/control_plane/p0/task_specs/guard.json"
                ),
                "task_spec_sha256": "c" * 64,
                "requirements": {
                    "required_test_receipt_ids": [],
                    "required_review_receipt_ids": [],
                    "required_evidence_ids": [],
                },
                "allowed_files": [
                    "research_state/control_plane/p0/task_specs/"
                ],
                "forbidden_files": ["data/"],
                "baseline_ref": (
                    "research_state/control_plane/p0/baselines/guard.json"
                ),
                "baseline_sha256": "d" * 64,
                "input_evidence_refs": [],
            },
            allowed_side_effects=(
                stores_module.SideEffect.WRITE_CONTROL_PLANE,
            ),
        )
        authority._begin_task(ticket)
        backup_path = Path(self.temporary.name) / "authority.backup.sqlite3"
        self.coordinator.backup(
            store_migration.MigrationTarget.AUTHORITY,
            backup_path,
        )
        with self.assertRaises(store_migration.StoreMigrationActiveError):
            self.coordinator.publish(
                store_migration.MigrationTarget.AUTHORITY,
                backup_path=backup_path,
            )

    def test_publish_migrates_and_writes_receipt(self) -> None:
        self._provision_authority_v1()
        backup_path = Path(self.temporary.name) / "authority.backup.sqlite3"
        self.coordinator.backup(
            store_migration.MigrationTarget.AUTHORITY,
            backup_path,
        )
        receipt = self.coordinator.publish(
            store_migration.MigrationTarget.AUTHORITY,
            backup_path=backup_path,
        )
        self.assertEqual(receipt.target, "authority")
        self.assertEqual(receipt.from_schema_version, 1)
        self.assertEqual(receipt.to_schema_version, 2)
        self.assertTrue(receipt.migrated)
        self.assertEqual(self._authority_user_version(), 2)
        connection = sqlite3.connect(self.authority_path)
        try:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                "AND name = 'final_eval_authorizations_v1'"
            ).fetchone()
        finally:
            connection.close()
        self.assertIsNotNone(table)

    def test_rollback_boundary_keeps_backup_immutable(self) -> None:
        self._provision_authority_v1()
        backup_path = Path(self.temporary.name) / "authority.backup.sqlite3"
        self.coordinator.backup(
            store_migration.MigrationTarget.AUTHORITY,
            backup_path,
        )
        backup_bytes = backup_path.read_bytes()
        staging_path = Path(self.temporary.name) / "authority.staging.sqlite3"
        self.coordinator.rehearse(
            store_migration.MigrationTarget.AUTHORITY,
            backup_path=backup_path,
            staging_path=staging_path,
        )
        self.assertEqual(backup_path.read_bytes(), backup_bytes)
        with self.assertRaises(store_migration.StoreMigrationBackupError):
            self.coordinator.publish(
                store_migration.MigrationTarget.AUTHORITY,
                backup_path=Path(self.temporary.name) / "missing.backup.sqlite3",
            )
        self.assertEqual(self._authority_user_version(), 1)
        self.assertEqual(backup_path.read_bytes(), backup_bytes)

    def test_rejects_arbitrary_targets_and_paths(self) -> None:
        with self.assertRaises(ValueError):
            self.coordinator.backup(
                "BOGUS_TARGET",
                Path(self.temporary.name) / "bogus.backup.sqlite3",
            )
        with self.assertRaises(ValueError):
            self.coordinator.backup(
                store_migration.MigrationTarget.AUTHORITY,
                Path("relative-backup.sqlite3"),
            )
        with self.assertRaises(TypeError):
            self.coordinator.backup(
                store_migration.MigrationTarget.AUTHORITY,
                "not-a-path",
            )

    def _authority_user_version(self) -> int:
        connection = sqlite3.connect(self.authority_path)
        try:
            return int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
