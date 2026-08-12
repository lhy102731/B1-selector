"""Tests for the real OperationalJournal read model and projection (P7R3 T1)."""

from __future__ import annotations

import json
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane.operations_projection import (
    OperationalIntegrityError,
    OperationalProjectionBlocked,
    OperationalReadModelError,
    generation_publication_status,
    project_campaign_stream,
    read_operational_snapshot,
    read_only_status_real,
)
from research_automation.control_plane import stores as stores_module
from tests.test_control_plane_campaign_store import (
    NOW,
    ROOT_SECRET,
    _authorized_campaign,
)


class RealReadModelTests(unittest.TestCase):
    def _bootstrap(self):
        import tempfile

        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        patch_obj = patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
            _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
        )
        patch_obj.start()
        self.addCleanup(patch_obj.stop)
        self.addCleanup(temporary.cleanup)
        stores_module._expected_schema_sha256.cache_clear()
        stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
        return root

    def test_read_operational_snapshot_reports_real_journal_stream(self) -> None:
        self._bootstrap()
        snapshot = read_operational_snapshot()
        self.assertIn("journal", snapshot)
        self.assertIn("campaign", snapshot)
        self.assertIn("access", snapshot)
        self.assertGreaterEqual(snapshot["journal"]["count"], 0)
        self.assertIn("projection_checkpoints", snapshot)

    def test_missing_journal_fails_closed_without_zero_snapshot(self) -> None:
        import tempfile

        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        self.addCleanup(temporary.cleanup)
        patch_obj = patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
            _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
        )
        patch_obj.start()
        self.addCleanup(patch_obj.stop)
        with self.assertRaises(OperationalReadModelError) as caught:
            read_operational_snapshot()
        self.assertIn("OPERATIONAL_STORE_MISSING", str(caught.exception))

    def test_corrupt_journal_fails_closed(self) -> None:
        import tempfile

        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        self.addCleanup(temporary.cleanup)
        operational = root / "operational.sqlite3"
        operational.write_bytes(b"this is not a sqlite database")
        patch_obj = patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
            _OPERATIONAL_STORE_PATH=operational,
        )
        patch_obj.start()
        self.addCleanup(patch_obj.stop)
        with self.assertRaises(OperationalReadModelError):
            read_operational_snapshot()

    def test_generation_publication_is_unwired_never_pending_zero(self) -> None:
        status = generation_publication_status()
        self.assertEqual(status["status"], "UNAVAILABLE")
        self.assertEqual(status["reason"], "DATA_GENERATION_STATUS_UNWIRED")
        self.assertIsNone(status["pending"])

    def test_read_only_status_real_is_healthy_on_bootstrapped_store(self) -> None:
        self._bootstrap()
        status = read_only_status_real()
        self.assertTrue(status["healthy"])
        self.assertEqual(status["reason"], None)
        self.assertEqual(
            status["generation_publication"]["status"],
            "UNAVAILABLE",
        )

    def test_read_only_status_real_fails_closed_on_missing_store(self) -> None:
        import tempfile

        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        self.addCleanup(temporary.cleanup)
        patch_obj = patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
            _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
        )
        patch_obj.start()
        self.addCleanup(patch_obj.stop)
        status = read_only_status_real()
        self.assertFalse(status["healthy"])
        self.assertEqual(status["event_count"], None)
        self.assertIn("OPERATIONAL_STORE_MISSING", status["reason"])


class ProjectionWriterTests(unittest.TestCase):
    def test_project_campaign_stream_writes_derived_tables(self) -> None:
        campaign_id = "campaign-projection-t1"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            # Insert one campaign event directly into the real stream.
            conn = sqlite3.connect(str(root / "operational.sqlite3"))
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            payload = {"hypothesis": "bounded"}
            payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            payload_sha256 = __import__("hashlib").sha256(
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
                    f"evt-{campaign_id}",
                    campaign_id,
                    campaign_id,
                    payload_json,
                    payload_sha256,
                    NOW.isoformat(),
                ),
            )
            conn.commit()
            conn.close()

            result = project_campaign_stream(
                campaign_id=campaign_id,
                namespace="formal",
            )
            self.assertEqual(result["last_sequence"], 1)
            self.assertEqual(result["aggregate_count"], 1)
            self.assertEqual(result["blocked"], [])

            # checkpoints are durable
            check = read_operational_snapshot()
            self.assertTrue(check["projection_checkpoints"])

    def test_unknown_event_type_fails_closed_with_blocked_reason(self) -> None:
        campaign_id = "campaign-projection-blocked"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            conn = sqlite3.connect(str(root / "operational.sqlite3"))
            conn.execute("BEGIN IMMEDIATE")
            payload_json = json.dumps(
                {"x": 1}, sort_keys=True, separators=(",", ":")
            )
            payload_sha256 = __import__("hashlib").sha256(
                payload_json.encode("utf-8")
            ).hexdigest()
            conn.execute(
                """INSERT INTO campaign_events
                (event_id, namespace, campaign_id, cycle_id, aggregate_type,
                 aggregate_id, event_type, payload_json, payload_sha256,
                 occurred_at)
                VALUES (?, 'formal', ?, NULL, 'campaign', ?, 'UNKNOWN_EVENT',
                        ?, ?, ?)""",
                (
                    f"evt-unknown-{campaign_id}",
                    campaign_id,
                    campaign_id,
                    payload_json,
                    payload_sha256,
                    NOW.isoformat(),
                ),
            )
            conn.commit()
            conn.close()
            with self.assertRaises(OperationalProjectionBlocked) as caught:
                project_campaign_stream(
                    campaign_id=campaign_id,
                    namespace="formal",
                )
            self.assertIn("UNKNOWN_EVENT", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
