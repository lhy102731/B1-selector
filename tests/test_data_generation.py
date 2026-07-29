from __future__ import annotations

import unittest

from research_automation.data_generation.contracts import (
    GENERATION_MANIFEST_V1,
    GenerationManifest,
    generation_contract_registry,
)
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


if __name__ == "__main__":
    unittest.main()
