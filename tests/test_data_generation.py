from __future__ import annotations

import json
import multiprocessing
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.data_generation.contracts import (
    GENERATION_MANIFEST_V1,
    GenerationManifest,
    generation_contract_registry,
)
from research_automation.data_generation.generation import (
    GenerationPublicationPendingError,
    GenerationPublisher,
)
from research_automation.foundations.contract_registry import ContractValidationError


def _hold_generation_read_lease(
    root: str,
    generation_id: str,
    ready: object,
    release: object,
) -> None:
    publisher = GenerationPublisher(Path(root), lock_timeout_seconds=2.0)
    lease = publisher.acquire_read_lease(
        expected_generation_id=generation_id,
    )
    ready.set()
    release.wait(10)
    lease.release()


def _exit_with_generation_read_lease(
    root: str,
    generation_id: str,
    connection: object,
) -> None:
    publisher = GenerationPublisher(Path(root), lock_timeout_seconds=2.0)
    lease = publisher.acquire_read_lease(
        expected_generation_id=generation_id,
    )
    connection.send(lease.fencing_token)
    connection.close()
    os._exit(0)


class GenerationManifestContractTests(unittest.TestCase):
    def test_active_generation_lease_records_pending_until_retry_can_publish(self) -> None:
        first = GenerationManifest(
            schema_version=GENERATION_MANIFEST_V1,
            csv_cutoff="2026-07-28",
            trading_calendar_identity="calendar-cn-a-share-20260728",
            point_in_time_universe_identity="pit-universe-20260728",
            adjustment_scheme="qfq-v1",
            missing_data_policy="four-state-v1",
            cache_manifest_references=("raw-parquet-production-20260728",),
        )
        second = first.model_copy(update={"csv_cutoff": "2026-07-29"})

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "generations"
            publisher = GenerationPublisher(root, lock_timeout_seconds=0.05)
            publisher.publish(publisher.stage(first))
            lease = publisher.acquire_read_lease(
                expected_generation_id=first.generation_id,
            )
            staged = publisher.stage(second)

            with self.assertRaises(GenerationPublicationPendingError):
                publisher.publish(staged)

            pending = publisher.pending_publication()
            self.assertIsNotNone(pending)
            assert pending is not None
            self.assertEqual(second.generation_id, pending.candidate_generation_id)
            with self.assertRaises(GenerationPublicationPendingError):
                publisher.acquire_read_lease(
                    expected_generation_id=first.generation_id,
                )
            self.assertEqual(first.generation_id, publisher.read(lease).generation_id)
            self.assertEqual(first.generation_id, publisher.read_current().generation_id)

            lease.release()
            published = publisher.publish(staged)

            self.assertEqual(second.generation_id, published.generation_id)
            self.assertIsNone(publisher.pending_publication())

    def test_recovery_clears_pending_after_publish_committed_before_cleanup(self) -> None:
        first = GenerationManifest(
            schema_version=GENERATION_MANIFEST_V1,
            csv_cutoff="2026-07-28",
            trading_calendar_identity="calendar-cn-a-share-20260728",
            point_in_time_universe_identity="pit-universe-20260728",
            adjustment_scheme="qfq-v1",
            missing_data_policy="four-state-v1",
            cache_manifest_references=("raw-parquet-production-20260728",),
        )
        second = first.model_copy(update={"csv_cutoff": "2026-07-29"})

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "generations"
            publisher = GenerationPublisher(root)
            publisher.publish(publisher.stage(first))
            staged = publisher.stage(second)
            pending_path = root / ".publish_pending.json"
            real_unlink = Path.unlink

            def fail_pending_cleanup(path: Path, *args: object, **kwargs: object) -> None:
                if path == pending_path:
                    raise PermissionError("injected pending cleanup crash")
                real_unlink(path, *args, **kwargs)

            with patch.object(Path, "unlink", new=fail_pending_cleanup):
                with self.assertRaisesRegex(
                    PermissionError,
                    "injected pending cleanup crash",
                ):
                    publisher.publish(staged)

            self.assertEqual(second.generation_id, publisher.read_current().generation_id)
            self.assertIsNotNone(publisher.pending_publication())

            recovered = publisher.recover_pending_publication()

            self.assertEqual(second.generation_id, recovered.generation_id)
            self.assertIsNone(publisher.pending_publication())

    def test_pending_recovery_finishes_committed_release_transaction_first(self) -> None:
        first = GenerationManifest(
            schema_version=GENERATION_MANIFEST_V1,
            csv_cutoff="2026-07-27",
            trading_calendar_identity="calendar-cn-a-share-20260727",
            point_in_time_universe_identity="pit-universe-20260727",
            adjustment_scheme="qfq-v1",
            missing_data_policy="four-state-v1",
            cache_manifest_references=("raw-parquet-production-20260727",),
        )
        second = first.model_copy(update={"csv_cutoff": "2026-07-28"})
        third = first.model_copy(update={"csv_cutoff": "2026-07-29"})

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "generations"
            publisher = GenerationPublisher(root)
            publisher.publish(publisher.stage(first))
            publisher.publish(publisher.stage(second))
            staged = publisher.stage(third)
            real_replace = os.replace

            def fail_previous_archive(source: object, target: object) -> None:
                source_path = Path(source)
                target_path = Path(target)
                if (
                    source_path.name == "previous"
                    and source_path.parent.name.startswith(".promotion.")
                    and target_path.parent == root / "archive"
                ):
                    raise PermissionError("injected release cleanup crash")
                real_replace(source, target)

            with patch(
                "research_automation.foundations.immutable_release.os.replace",
                side_effect=fail_previous_archive,
            ):
                with self.assertRaisesRegex(
                    PermissionError,
                    "injected release cleanup crash",
                ):
                    publisher.publish(staged)

            self.assertTrue(any(root.glob(".promotion.*.tmp")))
            self.assertIsNotNone(publisher.pending_publication())

            recovered = publisher.recover_pending_publication()

            self.assertEqual(third.generation_id, recovered.generation_id)
            self.assertFalse(any(root.glob(".promotion.*.tmp")))
            self.assertIsNone(publisher.pending_publication())

    def test_cross_process_read_lease_prevents_generation_publication(self) -> None:
        first = GenerationManifest(
            schema_version=GENERATION_MANIFEST_V1,
            csv_cutoff="2026-07-28",
            trading_calendar_identity="calendar-cn-a-share-20260728",
            point_in_time_universe_identity="pit-universe-20260728",
            adjustment_scheme="qfq-v1",
            missing_data_policy="four-state-v1",
            cache_manifest_references=("raw-parquet-production-20260728",),
        )
        second = first.model_copy(update={"csv_cutoff": "2026-07-29"})

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "generations"
            publisher = GenerationPublisher(root, lock_timeout_seconds=0.05)
            publisher.publish(publisher.stage(first))
            staged = publisher.stage(second)
            context = multiprocessing.get_context("spawn")
            ready = context.Event()
            release = context.Event()
            process = context.Process(
                target=_hold_generation_read_lease,
                args=(str(root), first.generation_id, ready, release),
            )
            process.start()
            try:
                self.assertTrue(ready.wait(5), "read-lease subprocess did not start")
                with self.assertRaises(GenerationPublicationPendingError):
                    publisher.publish(staged)
                self.assertEqual(
                    first.generation_id,
                    publisher.read_current().generation_id,
                )
            finally:
                release.set()
                process.join(10)
            self.assertFalse(process.is_alive())
            self.assertEqual(0, process.exitcode)
            process.close()

            published = publisher.publish(staged)

            self.assertEqual(second.generation_id, published.generation_id)

    def test_process_exit_releases_lease_and_next_generation_fences_its_token(self) -> None:
        first = GenerationManifest(
            schema_version=GENERATION_MANIFEST_V1,
            csv_cutoff="2026-07-28",
            trading_calendar_identity="calendar-cn-a-share-20260728",
            point_in_time_universe_identity="pit-universe-20260728",
            adjustment_scheme="qfq-v1",
            missing_data_policy="four-state-v1",
            cache_manifest_references=("raw-parquet-production-20260728",),
        )
        second = first.model_copy(update={"csv_cutoff": "2026-07-29"})

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "generations"
            publisher = GenerationPublisher(root, lock_timeout_seconds=1.0)
            publisher.publish(publisher.stage(first))
            staged = publisher.stage(second)
            context = multiprocessing.get_context("spawn")
            parent_connection, child_connection = context.Pipe(duplex=False)
            process = context.Process(
                target=_exit_with_generation_read_lease,
                args=(str(root), first.generation_id, child_connection),
            )
            process.start()
            child_connection.close()
            self.assertTrue(parent_connection.poll(5), "lease token was not reported")
            old_token = parent_connection.recv()
            parent_connection.close()
            process.join(10)
            self.assertFalse(process.is_alive())
            self.assertEqual(0, process.exitcode)
            process.close()

            publisher.publish(staged)
            next_lease = publisher.acquire_read_lease(
                expected_generation_id=second.generation_id,
            )
            try:
                self.assertGreater(next_lease.fencing_token, old_token)
            finally:
                next_lease.release()

    def test_generation_manifest_strictly_binds_source_and_cache_identities(self) -> None:
        payload = {
            "schema_version": GENERATION_MANIFEST_V1,
            "csv_cutoff": "2026-07-28",
            "trading_calendar_identity": "calendar-cn-a-share-20260728",
            "point_in_time_universe_identity": "pit-universe-20260728",
            "adjustment_scheme": "qfq-v1",
            "missing_data_policy": "four-state-v1",
            "cache_manifest_references": [
                "raw-parquet-production-20260728",
                "indicators-production-20260728",
            ],
        }

        registry = generation_contract_registry()
        parsed = registry.parse_mapping(GENERATION_MANIFEST_V1, payload)

        self.assertIsInstance(parsed, GenerationManifest)
        self.assertEqual(parsed.model_dump(mode="json"), payload)
        invalid = {**payload, "csv_cutoff": 20260728}
        with self.assertRaises(ContractValidationError):
            registry.parse_mapping(GENERATION_MANIFEST_V1, invalid)

    def test_generation_id_is_stable_and_content_addressed(self) -> None:
        baseline = GenerationManifest(
            schema_version=GENERATION_MANIFEST_V1,
            csv_cutoff="2026-07-28",
            trading_calendar_identity="calendar-cn-a-share-20260728",
            point_in_time_universe_identity="pit-universe-20260728",
            adjustment_scheme="qfq-v1",
            missing_data_policy="four-state-v1",
            cache_manifest_references=("raw-parquet-production-20260728",),
        )
        same = GenerationManifest.model_validate(
            baseline.model_dump(mode="python"),
            strict=True,
        )
        changed = baseline.model_copy(
            update={"trading_calendar_identity": "calendar-cn-a-share-20260729"}
        )

        self.assertEqual(baseline.generation_id, same.generation_id)
        self.assertNotEqual(baseline.generation_id, changed.generation_id)
        self.assertRegex(baseline.generation_id, r"^[0-9a-f]{64}$")

    def test_current_changes_only_after_a_strict_manifest_is_valid(self) -> None:
        manifest = GenerationManifest(
            schema_version=GENERATION_MANIFEST_V1,
            csv_cutoff="2026-07-28",
            trading_calendar_identity="calendar-cn-a-share-20260728",
            point_in_time_universe_identity="pit-universe-20260728",
            adjustment_scheme="qfq-v1",
            missing_data_policy="four-state-v1",
            cache_manifest_references=("raw-parquet-production-20260728",),
        )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "generations"
            publisher = GenerationPublisher(root)
            staged = publisher.stage(manifest)
            self.assertFalse((root / "current").exists())

            published = publisher.publish(staged)

            self.assertEqual(manifest.generation_id, published.generation_id)
            next_manifest = manifest.model_copy(update={"csv_cutoff": "2026-07-29"})
            invalid = publisher.stage(next_manifest)
            manifest_path = invalid.path / "manifest.json"
            document = json.loads(manifest_path.read_text(encoding="utf-8"))
            document["unexpected"] = True
            manifest_path.write_text(json.dumps(document), encoding="utf-8")

            with self.assertRaises(ContractValidationError):
                publisher.publish(invalid)
            self.assertEqual(
                manifest.generation_id,
                publisher.read_current().generation_id,
            )

    def test_manifest_rejects_blank_identity_bindings(self) -> None:
        payload = {
            "schema_version": GENERATION_MANIFEST_V1,
            "csv_cutoff": "2026-07-28",
            "trading_calendar_identity": " ",
            "point_in_time_universe_identity": "pit-universe-20260728",
            "adjustment_scheme": "qfq-v1",
            "missing_data_policy": "four-state-v1",
            "cache_manifest_references": ["raw-parquet-production-20260728"],
        }

        with self.assertRaises(ContractValidationError):
            generation_contract_registry().parse_mapping(
                GENERATION_MANIFEST_V1,
                payload,
            )

    def test_manifest_rejects_noncanonical_or_duplicate_cache_references(self) -> None:
        payload = {
            "schema_version": GENERATION_MANIFEST_V1,
            "csv_cutoff": "2026-07-28",
            "trading_calendar_identity": "calendar-cn-a-share-20260728",
            "point_in_time_universe_identity": "pit-universe-20260728",
            "adjustment_scheme": "qfq-v1",
            "missing_data_policy": "four-state-v1",
            "cache_manifest_references": ["raw-parquet-production-20260728"],
        }

        invalid_references = (
            [" "],
            ["raw-parquet-production-20260728", "raw-parquet-production-20260728"],
        )
        for references in invalid_references:
            with self.subTest(references=references):
                with self.assertRaises(ContractValidationError):
                    generation_contract_registry().parse_mapping(
                        GENERATION_MANIFEST_V1,
                        {**payload, "cache_manifest_references": references},
                    )

    def test_manifest_rejects_a_noncanonical_csv_cutoff_date(self) -> None:
        payload = {
            "schema_version": GENERATION_MANIFEST_V1,
            "csv_cutoff": "2026-07-28",
            "trading_calendar_identity": "calendar-cn-a-share-20260728",
            "point_in_time_universe_identity": "pit-universe-20260728",
            "adjustment_scheme": "qfq-v1",
            "missing_data_policy": "four-state-v1",
            "cache_manifest_references": ["raw-parquet-production-20260728"],
        }

        for cutoff in ("2026-7-28", "20260728", "2026-02-30"):
            with self.subTest(cutoff=cutoff):
                with self.assertRaises(ContractValidationError):
                    generation_contract_registry().parse_mapping(
                        GENERATION_MANIFEST_V1,
                        {**payload, "csv_cutoff": cutoff},
                    )


if __name__ == "__main__":
    unittest.main()
