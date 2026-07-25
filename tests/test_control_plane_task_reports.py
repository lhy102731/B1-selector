from __future__ import annotations

import copy
import unittest

from research_automation.control_plane.task_reports import (
    TaskReportBuildError,
    TaskReportValidationError,
    build_task_report_v2,
    task_report_v2_payload_sha256,
    validate_task_report_v2,
)


class TaskReportV2TracerTests(unittest.TestCase):
    def _complete_report(self) -> dict[str, object]:
        report: dict[str, object] = {
            "schema_version": "control_plane.task_report.v2",
            "plan_version": "V3.4.2-P0R2",
            "phase": "P0",
            "task_id": "P0R2-T1-TASK-REPORT-TRACER",
            "attempt_id": "p0r2-attempt-001",
            "authorization_ref": "p0r2-natural-20260726-001",
            "identity_binding": {
                "plan_hash": "1" * 64,
                "scope_hash": "2" * 64,
                "instruction_policy_hash": "3" * 64,
            },
            "objective": "Reject a task report whose hashed payload was changed.",
            "dependencies": [],
            "idempotency_key": "p0r2-t1-task-report-tracer-001",
            "task_spec_ref": "research_state/control_plane/p0r2/task_specs/task-report-tracer.json",
            "task_spec_sha256": "7" * 64,
            "requirements": {
                "required_test_receipt_ids": ["task-report-unit-tests"],
                "required_review_receipt_ids": ["task-report-controller-review"],
                "required_evidence_ids": [],
            },
            "allowed_files": [
                "research_automation/control_plane/task_reports.py",
                "tests/test_control_plane_task_reports.py",
            ],
            "forbidden_files": ["data/", "strategy/"],
            "baseline_ref": "research_state/control_plane/p0r2/implementation_baseline_v342_p0r2.json",
            "baseline_sha256": "4" * 64,
            "input_evidence_refs": [],
            "test_receipts": [
                {
                    "receipt_id": "task-report-unit-tests",
                    "command": "python -m unittest tests.test_control_plane_task_reports -v",
                    "exit_code": 0,
                    "result": "PASS",
                }
            ],
            "review_receipts": [
                {
                    "receipt_id": "task-report-controller-review",
                    "reviewer_id": "primary-codex",
                    "result": "PASS",
                    "exit_code": 0,
                }
            ],
            "review_findings": [],
            "changed_files": [
                {
                    "path": "research_automation/control_plane/task_reports.py",
                    "change_type": "ADD",
                    "baseline_sha256": None,
                    "current_sha256": "5" * 64,
                }
            ],
            "unexpected_changes": [],
            "external_invocations": [],
            "side_effect_summary": {
                "observed": ["WRITE_SCOPED_CODE_TEST_PLAN_AND_CONTROL_STATE"],
                "unauthorized": [],
            },
            "ticket_state": "SUCCEEDED",
            "outcome": "PASS",
            "reason_codes": [],
            "started_at": "2026-07-26T06:40:00+08:00",
            "completed_at": "2026-07-26T06:41:00+08:00",
        }
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)
        return report

    def test_tampering_after_payload_hash_is_rejected(self) -> None:
        report = self._complete_report()
        validate_task_report_v2(report)

        tampered = copy.deepcopy(report)
        changed_files = tampered["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["current_sha256"] = "6" * 64

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "report_payload_sha256 mismatch",
        ):
            validate_task_report_v2(tampered)

    def test_unknown_top_level_field_is_rejected_even_with_a_valid_hash(self) -> None:
        report = self._complete_report()
        report["caller_selected_verdict"] = "PASS"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(TaskReportValidationError, "unknown fields"):
            validate_task_report_v2(report)

    def test_missing_required_field_is_rejected_even_with_a_valid_hash(self) -> None:
        report = self._complete_report()
        del report["changed_files"]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(TaskReportValidationError, "missing fields: changed_files"):
            validate_task_report_v2(report)

    def test_changed_files_must_always_be_an_array(self) -> None:
        report = self._complete_report()
        report["changed_files"] = {
            "research_automation/control_plane/task_reports.py": "ADD"
        }
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "changed_files must be a list",
        ):
            validate_task_report_v2(report)

    def test_identity_binding_requires_exact_named_sha256_digests(self) -> None:
        report = self._complete_report()
        identity_binding = report["identity_binding"]
        self.assertIsInstance(identity_binding, dict)
        identity_binding["plan_hash"] = "public-plan-name"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "identity_binding.plan_hash must be a lowercase SHA-256 digest",
        ):
            validate_task_report_v2(report)

    def test_outcome_is_a_closed_v2_value(self) -> None:
        report = self._complete_report()
        report["outcome"] = "GREEN_CURRENT_SNAPSHOT"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "outcome must be PASS, FAIL, BLOCKED, or IN_DOUBT",
        ):
            validate_task_report_v2(report)

    def test_phase_is_closed_to_the_declared_control_plane_phases(self) -> None:
        report = self._complete_report()
        report["phase"] = "P9"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(TaskReportValidationError, "phase must be P0 through P8"):
            validate_task_report_v2(report)

    def test_builder_owns_outcome_and_missing_required_test_blocks_pass(self) -> None:
        complete = self._complete_report()
        draft = {
            key: value
            for key, value in complete.items()
            if key
            not in {
                "schema_version",
                "outcome",
                "reason_codes",
                "report_payload_sha256",
            }
        }
        draft.update(
            {
                "task_spec_ref": "research_state/control_plane/p0r2/task_specs/task-report-tracer.json",
                "task_spec_sha256": "7" * 64,
                "requirements": {
                    "required_test_receipt_ids": ["required-unit-test"],
                    "required_review_receipt_ids": [],
                    "required_evidence_ids": [],
                },
                "review_receipts": [],
                "ticket_state": "SUCCEEDED",
            }
        )

        caller_selected = dict(draft)
        caller_selected["outcome"] = "PASS"
        with self.assertRaisesRegex(TaskReportBuildError, "computed fields"):
            build_task_report_v2(caller_selected)

        report = build_task_report_v2(draft)

        self.assertEqual(report["outcome"], "BLOCKED")
        self.assertEqual(
            report["reason_codes"],
            ["MISSING_REQUIRED_TEST_RECEIPT:required-unit-test"],
        )
        validate_task_report_v2(report)

    def test_failed_required_test_mechanically_derives_fail(self) -> None:
        complete = self._complete_report()
        draft = {
            key: copy.deepcopy(value)
            for key, value in complete.items()
            if key
            not in {
                "schema_version",
                "outcome",
                "reason_codes",
                "report_payload_sha256",
            }
        }
        test_receipts = draft["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        receipt = test_receipts[0]
        self.assertIsInstance(receipt, dict)
        receipt["result"] = "FAIL"
        receipt["exit_code"] = 1

        report = build_task_report_v2(draft)

        self.assertEqual(report["outcome"], "FAIL")
        self.assertEqual(
            report["reason_codes"],
            ["REQUIRED_TEST_FAILED:task-report-unit-tests"],
        )
        validate_task_report_v2(report)

    def test_unexpected_change_mechanically_derives_fail(self) -> None:
        complete = self._complete_report()
        draft = {
            key: copy.deepcopy(value)
            for key, value in complete.items()
            if key
            not in {
                "schema_version",
                "outcome",
                "reason_codes",
                "report_payload_sha256",
            }
        }
        draft["unexpected_changes"] = ["strategy/unified_b1_strategy.py"]

        report = build_task_report_v2(draft)

        self.assertEqual(report["outcome"], "FAIL")
        self.assertEqual(
            report["reason_codes"],
            ["UNEXPECTED_CHANGE:strategy/unified_b1_strategy.py"],
        )
        validate_task_report_v2(report)

    def test_unauthorized_side_effect_mechanically_derives_fail(self) -> None:
        complete = self._complete_report()
        draft = {
            key: copy.deepcopy(value)
            for key, value in complete.items()
            if key
            not in {
                "schema_version",
                "outcome",
                "reason_codes",
                "report_payload_sha256",
            }
        }
        side_effect_summary = draft["side_effect_summary"]
        self.assertIsInstance(side_effect_summary, dict)
        side_effect_summary["unauthorized"] = ["RUN_RESEARCH"]

        report = build_task_report_v2(draft)

        self.assertEqual(report["outcome"], "FAIL")
        self.assertEqual(
            report["reason_codes"],
            ["UNAUTHORIZED_SIDE_EFFECT:RUN_RESEARCH"],
        )
        validate_task_report_v2(report)

    def test_missing_required_evidence_blocks_pass(self) -> None:
        complete = self._complete_report()
        draft = {
            key: copy.deepcopy(value)
            for key, value in complete.items()
            if key
            not in {
                "schema_version",
                "outcome",
                "reason_codes",
                "report_payload_sha256",
            }
        }
        requirements = draft["requirements"]
        self.assertIsInstance(requirements, dict)
        requirements["required_evidence_ids"] = ["required-baseline-evidence"]

        report = build_task_report_v2(draft)

        self.assertEqual(report["outcome"], "BLOCKED")
        self.assertEqual(
            report["reason_codes"],
            ["MISSING_REQUIRED_EVIDENCE:required-baseline-evidence"],
        )
        validate_task_report_v2(report)

    def test_rehash_cannot_hide_an_outcome_derivation_mismatch(self) -> None:
        forged = self._complete_report()
        test_receipts = forged["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        receipt = test_receipts[0]
        self.assertIsInstance(receipt, dict)
        receipt["result"] = "FAIL"
        receipt["exit_code"] = 1
        forged["report_payload_sha256"] = task_report_v2_payload_sha256(forged)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "outcome does not match mechanical derivation",
        ):
            validate_task_report_v2(forged)


if __name__ == "__main__":
    unittest.main()
