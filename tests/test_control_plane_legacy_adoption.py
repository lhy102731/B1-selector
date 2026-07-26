from __future__ import annotations

import unittest
from pathlib import Path

from research_automation.control_plane.legacy_adoption import (
    P0R1_T1_RAW_SHA256,
    P0R1_T2_RAW_SHA256,
    LegacyAdoptionError,
    classify_known_p0r1_t1,
    classify_known_p0r1_t2,
)


ROOT = Path(__file__).resolve().parents[1]
T1_REPORT = (
    ROOT
    / "research_state"
    / "control_plane"
    / "p0"
    / "task_report_p0_t1_contract_authority.json"
)
T2_REPORT = (
    ROOT
    / "research_state"
    / "control_plane"
    / "p0"
    / "task_report_p0_t2_inventory_import.json"
)


class P0R1LegacyAdoptionTests(unittest.TestCase):
    def test_t1_requires_the_exact_known_source_bytes_before_parsing(self) -> None:
        raw = T1_REPORT.read_bytes()

        snapshot = classify_known_p0r1_t1(raw)

        self.assertEqual(snapshot.raw_sha256, P0R1_T1_RAW_SHA256)
        self.assertEqual(snapshot.schema_version, "control_plane.task_report.v1")
        self.assertEqual(snapshot.source_result, "PASS")
        self.assertEqual(snapshot.source_status, "PASS")
        self.assertEqual(snapshot.source_gate_status, "NOT_COMPUTED")
        self.assertEqual(snapshot.adoption_status, "REVALIDATION_REQUIRED")
        self.assertEqual(snapshot.known_total_tokens, 58734)
        self.assertEqual(snapshot.unknown_usage_count, 1)
        self.assertEqual(snapshot.usage_owner_source_id, "P0R1-T1")
        self.assertFalse(snapshot.count_in_target_total)
        self.assertFalse(snapshot.execution_eligible)

        tampered = b"!" + raw[1:]
        with self.assertRaisesRegex(
            LegacyAdoptionError,
            "P0R1 T1 raw SHA-256 mismatch",
        ):
            classify_known_p0r1_t1(tampered)

    def test_t2_preserves_blocked_status_and_provisional_artifacts(self) -> None:
        raw = T2_REPORT.read_bytes()

        snapshot = classify_known_p0r1_t2(raw)

        self.assertEqual(snapshot.raw_sha256, P0R1_T2_RAW_SHA256)
        self.assertEqual(
            snapshot.schema_version,
            "control_plane.p0_task_report.v1",
        )
        self.assertEqual(snapshot.source_result, "GREEN_CURRENT_SNAPSHOT")
        self.assertEqual(snapshot.source_status, "BLOCKED_BY_PLAN_REVISION")
        self.assertEqual(
            snapshot.inventory_disposition,
            "INITIAL_PROVISIONAL_ONLY",
        )
        self.assertFalse(snapshot.entry_policy_final_gate_eligible)
        self.assertEqual(
            snapshot.missing_source_fields,
            ("authorization_ref", "started_at"),
        )
        self.assertEqual(snapshot.known_total_tokens, 58734)
        self.assertEqual(snapshot.unknown_usage_count, 1)
        self.assertEqual(snapshot.usage_owner_source_id, "P0R1-T1")
        self.assertFalse(snapshot.count_in_target_total)
        self.assertFalse(snapshot.execution_eligible)

        with self.assertRaisesRegex(
            LegacyAdoptionError,
            "P0R1 T2 raw SHA-256 mismatch",
        ):
            classify_known_p0r1_t2(b"!" + raw[1:])


if __name__ == "__main__":
    unittest.main()
