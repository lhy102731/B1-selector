from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_automation.data_generation.contracts import (
    GENERATION_MANIFEST_V1,
    GenerationManifest,
    generation_contract_registry,
)
from research_automation.data_generation.generation import GenerationPublisher
from research_automation.foundations.contract_registry import ContractValidationError


class GenerationManifestContractTests(unittest.TestCase):
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
