"""Tests for durable backfill, retention and health maintenance (P7R3 T5/T6)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_automation.control_plane.operations_maintenance import (
    OperationsMaintenanceError,
    persist_backfill_batch,
    record_retention_metadata,
    resume_backfill_state,
    retention_cleanup_candidates,
)
from research_automation.control_plane import stores as stores_module
from tests.test_control_plane_campaign_store import (
    ROOT_SECRET,
    _authorized_campaign,
)


class BackfillPersistenceTests(unittest.TestCase):
    def test_backfill_batch_persists_and_replays_idempotently(self) -> None:
        with _authorized_campaign("campaign-backfill-1") as (root, _, journal):
            first = persist_backfill_batch(
                plan_hash="a" * 64,
                shard="shard-0",
                start_cursor=0,
                end_cursor=99,
                source_prefix_hash="b" * 64,
                derived_payload_sha256="c" * 64,
            )
            self.assertFalse(first["replayed"])
            replay = persist_backfill_batch(
                plan_hash="a" * 64,
                shard="shard-0",
                start_cursor=0,
                end_cursor=99,
                source_prefix_hash="b" * 64,
                derived_payload_sha256="c" * 64,
            )
            self.assertTrue(replay["replayed"])

    def test_backfill_distinct_batches_coexist(self) -> None:
        with _authorized_campaign("campaign-backfill-2") as (root, _, journal):
            persist_backfill_batch(
                plan_hash="d" * 64,
                shard="shard-0",
                start_cursor=0,
                end_cursor=99,
                source_prefix_hash="e" * 64,
                derived_payload_sha256="f" * 64,
            )
            # A distinct cursor range produces a distinct idempotency key and
            # persists independently (exactly-once per batch).
            second = persist_backfill_batch(
                plan_hash="d" * 64,
                shard="shard-0",
                start_cursor=100,
                end_cursor=199,
                source_prefix_hash="e" * 64,
                derived_payload_sha256="f" * 64,
            )
            self.assertFalse(second["replayed"])
            state = resume_backfill_state(plan_hash="d" * 64)
            self.assertEqual(len(state["checkpoints"]), 2)

    def test_backfill_state_resumes_in_fresh_process(self) -> None:
        with _authorized_campaign("campaign-backfill-3") as (root, _, journal):
            persist_backfill_batch(
                plan_hash="g" * 64,
                shard="shard-0",
                start_cursor=0,
                end_cursor=49,
                source_prefix_hash="h" * 64,
                derived_payload_sha256="i" * 64,
            )
            # A "fresh process" is a new read transaction (same store).
            state = resume_backfill_state(plan_hash="g" * 64)
            self.assertEqual(len(state["checkpoints"]), 1)
            self.assertEqual(
                state["checkpoints"][0]["from_sequence"],
                0,
            )
            self.assertFalse(state["bulk_backfill_started"])

    def test_backfill_rejects_invalid_cursors(self) -> None:
        with _authorized_campaign("campaign-backfill-4") as (root, _, journal):
            with self.assertRaises(OperationsMaintenanceError):
                persist_backfill_batch(
                    plan_hash="j" * 64,
                    shard="s",
                    start_cursor=10,
                    end_cursor=5,
                    source_prefix_hash="k" * 64,
                    derived_payload_sha256="l" * 64,
                )


class RetentionMetadataTests(unittest.TestCase):
    def test_scientific_is_never_archive_eligible(self) -> None:
        with _authorized_campaign("campaign-retention-1") as (root, _, journal):
            payload = record_retention_metadata(
                packet_hash="m" * 64,
                retention_class="SCIENTIFIC",
            )
            self.assertFalse(payload["archive_eligible"])

    def test_preview_is_archive_eligible(self) -> None:
        with _authorized_campaign("campaign-retention-2") as (root, _, journal):
            payload = record_retention_metadata(
                packet_hash="n" * 64,
                retention_class="PREVIEW",
            )
            self.assertTrue(payload["archive_eligible"])

    def test_invalid_class_rejected(self) -> None:
        with _authorized_campaign("campaign-retention-3") as (root, _, journal):
            with self.assertRaises(OperationsMaintenanceError):
                record_retention_metadata(
                    packet_hash="o" * 64,
                    retention_class="UNKNOWN",
                )


class ExplicitCleanupTests(unittest.TestCase):
    def test_cleanup_candidates_only_preview_staging_ordinary_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "preview-abc.json").write_text("{}", encoding="utf-8")
            (root / "staging-def.json").write_text("{}", encoding="utf-8")
            (root / "scientific-ghi.json").write_text("{}", encoding="utf-8")
            (root / "random.txt").write_text("x", encoding="utf-8")
            result = retention_cleanup_candidates(
                temp_root=root,
                max_age_days=30,
            )
            self.assertEqual(result["eligible_count"], 2)
            refs = {c["ref"] for c in result["candidates"]}
            self.assertEqual(
                refs,
                {"preview-abc.json", "staging-def.json"},
            )
            self.assertTrue(
                any("SCIENTIFIC_NOT_ELIGIBLE" in r for r in result["rejected"])
            )
            self.assertEqual(result["deleted"], 0)

    def test_cleanup_rejects_invalid_max_age(self) -> None:
        with self.assertRaises(OperationsMaintenanceError):
            retention_cleanup_candidates(
                temp_root=Path("."),
                max_age_days=0,
            )


if __name__ == "__main__":
    unittest.main()
