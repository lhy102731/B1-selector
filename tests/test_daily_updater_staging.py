from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path

from research_automation.data_generation.contracts import (
    GENERATION_MANIFEST_V1,
    GenerationManifest,
)
from research_automation.data_generation.daily_staging import (
    DailyBarUpdate,
    DailyPublishStatus,
    DailyStagingValidationError,
    DailyUpdaterStagingAdapter,
)
from research_automation.data_generation.generation import (
    GenerationPublicationPendingError,
)
from research_automation.data_generation.market_data import (
    BarAvailability,
    FetchFailure,
    MarketSessionKey,
    NoBarConfirmation,
)


def _manifest(cutoff: str = "2026-07-30") -> GenerationManifest:
    return GenerationManifest(
        schema_version=GENERATION_MANIFEST_V1,
        csv_cutoff=cutoff,
        trading_calendar_identity=f"calendar-cn-a-share-{cutoff}",
        point_in_time_universe_identity=f"pit-universe-{cutoff}",
        adjustment_scheme="hfq-v1",
        missing_data_policy="four-state-v1",
        cache_manifest_references=("raw-parquet-production-parent",),
    )


def _present(code: str = "000001", session: date = date(2026, 7, 30)) -> DailyBarUpdate:
    csv_bytes = (
        f"date,open,high,low,close,volume\n{session.isoformat()},10,11,9,10.5,100\n"
    ).encode("gbk")
    return DailyBarUpdate(
        key=MarketSessionKey(code, session),
        relative_path=f"00/{code}.csv",
        status="updated",
        csv_bytes=csv_bytes,
    )


class DailyUpdaterStagingTests(unittest.TestCase):
    def test_stage_writes_delta_bytes_and_binds_parent_and_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            adapter = DailyUpdaterStagingAdapter(Path(td) / "staging")
            staged = adapter.stage(_manifest(), [_present()])

            self.assertEqual(DailyPublishStatus.STAGED, staged.status)
            self.assertIsNone(staged.parent_generation_id)
            self.assertTrue(staged.candidate_path.is_dir())
            data_path = staged.candidate_path / "delta" / "00" / "000001.csv"
            self.assertEqual(_present().csv_bytes, data_path.read_bytes())
            delta = json.loads(
                (staged.candidate_path / "daily_delta_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            binding = json.loads(
                (staged.candidate_path / "candidate_binding.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(staged.generation_id, binding["candidate_generation_id"])
            self.assertEqual(staged.delta_manifest_sha256, binding["delta_manifest_sha256"])
            self.assertEqual(
                hashlib.sha256(
                    (staged.candidate_path / "daily_delta_manifest.json").read_bytes()
                ).hexdigest(),
                staged.delta_manifest_sha256,
            )
            self.assertEqual("PRESENT", delta["entries"][0]["availability"])

    def test_no_today_bar_without_typed_confirmation_cannot_publish(self) -> None:
        update = DailyBarUpdate(
            key=MarketSessionKey("000001", date(2026, 7, 30)),
            relative_path="00/000001.csv",
            status="no_today_bar",
        )
        with tempfile.TemporaryDirectory() as td:
            adapter = DailyUpdaterStagingAdapter(Path(td) / "staging")
            with self.assertRaises(DailyStagingValidationError):
                adapter.stage(_manifest(), [update])

    def test_no_today_bar_with_matching_confirmation_is_recorded_as_suspension(self) -> None:
        key = MarketSessionKey("000001", date(2026, 7, 30))
        update = DailyBarUpdate(
            key=key,
            relative_path="00/000001.csv",
            status="no_today_bar",
            no_bar_confirmation=NoBarConfirmation(
                key=key,
                source_id="calendar-provider",
                evidence_ref="fixture/no-bar/000001-20260730",
            ),
        )
        with tempfile.TemporaryDirectory() as td:
            adapter = DailyUpdaterStagingAdapter(Path(td) / "staging")
            staged = adapter.stage(_manifest(), [update])
            self.assertEqual(
                BarAvailability.NO_BAR_CONFIRMED,
                staged.observations[0].availability,
            )

    def test_fetch_failure_and_unknown_bar_are_fail_closed(self) -> None:
        key = MarketSessionKey("000001", date(2026, 7, 30))
        failure = DailyBarUpdate(
            key=key,
            relative_path="00/000001.csv",
            status="fetch_failed",
            fetch_failure=FetchFailure(
                key=key,
                source_id="provider",
                error_ref="fixture/error/1",
            ),
        )
        with tempfile.TemporaryDirectory() as td:
            adapter = DailyUpdaterStagingAdapter(Path(td) / "staging")
            with self.assertRaises(DailyStagingValidationError):
                adapter.stage(_manifest(), [failure])

    def test_validation_failure_does_not_create_current(self) -> None:
        bad = DailyBarUpdate(
            key=MarketSessionKey("000001", date(2026, 7, 30)),
            relative_path="../escape.csv",
            status="updated",
            csv_bytes=b"bad",
        )
        with tempfile.TemporaryDirectory() as td:
            adapter = DailyUpdaterStagingAdapter(Path(td) / "staging")
            with self.assertRaises(DailyStagingValidationError):
                adapter.stage(_manifest(), [bad])
            self.assertFalse((Path(td) / "staging" / "generations" / "current").exists())

    def test_published_delta_is_atomic_and_reused_on_pending_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            adapter = DailyUpdaterStagingAdapter(
                Path(td) / "staging",
                lock_timeout_seconds=0.05,
            )
            first = adapter.stage(_manifest("2026-07-30"), [_present()])
            self.assertEqual(DailyPublishStatus.PUBLISHED, adapter.publish(first).status)
            staged = adapter.stage(
                _manifest("2026-07-31"),
                [_present("000002", date(2026, 7, 31))],
            )
            lease = adapter.publisher.acquire_read_lease(
                expected_generation_id=first.generation_id,
            )
            result = adapter.publish(staged)
            self.assertEqual(DailyPublishStatus.PUBLISH_PENDING, result.status)
            self.assertTrue(adapter.publisher.pending_publication())
            with self.assertRaises(GenerationPublicationPendingError):
                adapter.publisher.acquire_read_lease(
                    expected_generation_id=first.generation_id,
                )
            lease.release()
            recovered = adapter.resume_pending()
            self.assertIsNotNone(recovered)
            self.assertEqual(DailyPublishStatus.PUBLISHED, recovered.status)
            self.assertEqual(staged.generation_id, adapter.publisher.read_current().generation_id)

    def test_publish_rechecks_delta_bytes_before_moving_current(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            adapter = DailyUpdaterStagingAdapter(Path(td) / "staging")
            staged = adapter.stage(_manifest(), [_present()])
            path = staged.candidate_path / "delta" / "00" / "000001.csv"
            path.write_bytes(b"tampered")
            with self.assertRaises(DailyStagingValidationError):
                adapter.publish(staged)
            self.assertFalse((Path(td) / "staging" / "generations" / "current").exists())

    def test_top_level_candidate_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            adapter = DailyUpdaterStagingAdapter(root / "staging")
            staged = adapter.stage(_manifest(), [_present()])
            manifest = staged.candidate_path / "manifest.json"
            outside = root / "outside-manifest.json"
            outside.write_bytes(manifest.read_bytes())
            manifest.unlink()
            try:
                os.symlink(outside, manifest)
            except OSError as error:
                self.skipTest(f"symlink creation is unavailable: {error}")
            with self.assertRaises(DailyStagingValidationError):
                adapter.publish(staged)

    def test_generation_publisher_revalidates_delta_inside_publication_lock(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            adapter = DailyUpdaterStagingAdapter(Path(td) / "staging")
            first = adapter.stage(_manifest("2026-07-30"), [_present()])
            self.assertEqual(DailyPublishStatus.PUBLISHED, adapter.publish(first).status)
            staged = adapter.stage(
                _manifest("2026-07-31"),
                [_present("000002", date(2026, 7, 31))],
            )
            calls = 0
            csv_path = staged.candidate_path / "delta" / "00" / "000002.csv"

            def validator(path: Path, manifest: GenerationManifest) -> None:
                nonlocal calls
                if manifest.generation_id != staged.generation_id:
                    return
                calls += 1
                adapter._validate_candidate(
                    path,
                    expected_generation_id=staged.generation_id,
                    expected_parent_id=staged.parent_generation_id,
                    expected_delta_hash=staged.delta_manifest_sha256,
                )
                if calls == 1:
                    csv_path.write_bytes(b"date,open,high,low,close,volume\n2026-07-31,1,1,1,1,1\n")

            with self.assertRaises(DailyStagingValidationError):
                adapter.publisher.publish(
                    staged.staged_generation,
                    candidate_validator=validator,
                )
            self.assertEqual(first.generation_id, adapter.publisher.read_current().generation_id)


if __name__ == "__main__":
    unittest.main()
