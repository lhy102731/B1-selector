from __future__ import annotations

import unittest
from pathlib import Path

from research_automation.control_plane.legacy_adoption import (
    P0R1_T1_RAW_SHA256,
    P0R1_T2_RAW_SHA256,
    LegacyAdoptionError,
    classify_known_p0r1_t1,
    classify_known_p0r1_t2,
    derive_ordered_p0r1_file_expectations,
    revalidate_ordered_p0r1_files,
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

    def test_file_expectations_use_t1_then_t2_last_writer_semantics(self) -> None:
        expectations = derive_ordered_p0r1_file_expectations(
            T1_REPORT.read_bytes(),
            T2_REPORT.read_bytes(),
        )
        by_path = {expectation.path: expectation for expectation in expectations}

        self.assertEqual(len(expectations), 13)
        self.assertEqual(
            by_path["research_automation/control_plane/contracts.py"].source_id,
            "P0R1-T1",
        )
        self.assertEqual(
            by_path["research_automation/control_plane/entry_guard.py"].source_id,
            "P0R1-T2",
        )
        self.assertEqual(
            by_path["research_automation/control_plane/entry_guard.py"].expected_sha256,
            "bdf3d2ad902ea890c3d8a1f4413f022b283d28b33ed9d7537427e022c862413c",
        )
        self.assertEqual(
            by_path["tests/test_control_plane_entry_guard.py"].source_id,
            "P0R1-T2",
        )

    def test_ordered_file_revalidation_never_promotes_legacy_sources(self) -> None:
        t1_raw = T1_REPORT.read_bytes()
        t2_raw = T2_REPORT.read_bytes()
        expectations = derive_ordered_p0r1_file_expectations(t1_raw, t2_raw)
        current_hashes = {
            expectation.path: expectation.expected_sha256
            for expectation in expectations
        }

        ready = revalidate_ordered_p0r1_files(
            t1_raw,
            t2_raw,
            current_hashes,
        )

        self.assertEqual(ready.source_order, ("P0R1-T1", "P0R1-T2"))
        self.assertEqual(
            ready.overwritten_paths,
            (
                "research_automation/control_plane/entry_guard.py",
                "tests/test_control_plane_entry_guard.py",
            ),
        )
        self.assertEqual(ready.code_delta_status, "MATCH")
        self.assertEqual(
            ready.adoption_status,
            "READY_FOR_REQUIRED_TEST_REVALIDATION",
        )
        self.assertTrue(ready.ready_for_test_revalidation)
        self.assertFalse(ready.execution_eligible)
        self.assertFalse(ready.legacy_gate_eligible)
        self.assertFalse(ready.p0r1_evidence_directly_counted)
        self.assertEqual(ready.known_total_tokens, 58734)
        self.assertEqual(ready.unknown_usage_count, 1)
        self.assertFalse(ready.count_in_target_total)

        mismatched_hashes = dict(current_hashes)
        mismatched_hashes["research_automation/control_plane/contracts.py"] = (
            "0" * 64
        )
        blocked = revalidate_ordered_p0r1_files(
            t1_raw,
            t2_raw,
            mismatched_hashes,
        )

        self.assertEqual(blocked.code_delta_status, "MISMATCH")
        self.assertEqual(blocked.adoption_status, "BLOCKED_BY_FILE_DELTA")
        self.assertFalse(blocked.ready_for_test_revalidation)
        checks = {check.path: check for check in blocked.file_checks}
        self.assertEqual(
            checks["research_automation/control_plane/contracts.py"].status,
            "MISMATCH",
        )

        missing_hashes = dict(current_hashes)
        del missing_hashes["research_automation/control_plane/contracts.py"]
        missing = revalidate_ordered_p0r1_files(
            t1_raw,
            t2_raw,
            missing_hashes,
        )
        checks = {check.path: check for check in missing.file_checks}
        self.assertEqual(
            checks["research_automation/control_plane/contracts.py"].status,
            "MISSING",
        )
        self.assertFalse(missing.ready_for_test_revalidation)


if __name__ == "__main__":
    unittest.main()
