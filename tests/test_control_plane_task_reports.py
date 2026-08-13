from __future__ import annotations

import copy
import json
import unittest

from research_automation.control_plane.task_reports import (
    MAX_TASK_REPORT_V2_BYTES,
    MAX_TASK_REPORT_V2_DEPTH,
    TaskReportBuildError,
    TaskReportValidationError,
    build_task_report_v2,
    parse_task_report_v2_bytes,
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
            "ticket_id": "ticket-p0r2-t1-task-report-001",
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
                    "findings_sha256": (
                        "350369ce69c8bf1abffbfb6645d504dd6061c3b6b645c1357"
                        "dab54fb43b3377b"
                    ),
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

    def _reported_external_invocation(self) -> dict[str, object]:
        return {
            "invocation_id": "review-doubao-attempt-001",
            "invocation_ref": "research_state/control_plane/p0r2/invocations/review-doubao-attempt-001.json",
            "invocation_sha256": "a" * 64,
            "usage": {
                "status": "REPORTED",
                "input_tokens": 1200,
                "output_tokens": 300,
                "total_tokens": 1500,
            },
        }

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

    def test_review_receipt_binds_its_exact_findings(self) -> None:
        report = self._complete_report()
        validate_task_report_v2(report)
        report["review_findings"] = [
            {
                "finding_id": "finding-001",
                "review_receipt_id": "task-report-controller-review",
                "severity": "NON_BLOCKING",
                "status": "RESOLVED",
                "summary": "A resolved review observation.",
                "resolution": "Verified by the trusted reviewer.",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "findings_sha256",
        ):
            validate_task_report_v2(report)

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
                "unexpected_changes",
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

        caller_selected = dict(draft)
        caller_selected["unexpected_changes"] = []
        with self.assertRaisesRegex(
            TaskReportBuildError,
            "computed fields: unexpected_changes",
        ):
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
                "unexpected_changes",
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
                "unexpected_changes",
                "report_payload_sha256",
            }
        }
        changed_files = draft["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["path"] = "strategy/unified_b1_strategy.py"

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
                "unexpected_changes",
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
                "unexpected_changes",
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

    def test_byte_parser_rejects_duplicate_json_keys(self) -> None:
        report = self._complete_report()
        encoded = json.dumps(
            report,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        duplicate = (encoded[:-1] + ',"outcome":"PASS"}').encode("utf-8")

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "duplicate JSON key: outcome",
        ):
            parse_task_report_v2_bytes(duplicate)

    def test_byte_parser_rejects_oversized_input_before_json_parsing(self) -> None:
        oversized = b"{" + (b"x" * MAX_TASK_REPORT_V2_BYTES) + b"}"

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "exceeds 65536 byte limit",
        ):
            parse_task_report_v2_bytes(oversized)

    def test_byte_parser_rejects_excessive_nesting_with_a_typed_error(self) -> None:
        depth = MAX_TASK_REPORT_V2_DEPTH + 1
        nested = ((b'{"x":' * depth) + b"null" + (b"}" * depth))

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "exceeds 64 level nesting limit",
        ):
            parse_task_report_v2_bytes(nested)

    def test_duplicate_test_receipt_ids_are_rejected(self) -> None:
        report = self._complete_report()
        test_receipts = report["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        test_receipts.append(copy.deepcopy(test_receipts[0]))
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "test_receipts must not contain duplicate receipt_id values",
        ):
            validate_task_report_v2(report)

    def test_test_receipt_rejects_unknown_nested_fields(self) -> None:
        report = self._complete_report()
        test_receipts = report["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        receipt = test_receipts[0]
        self.assertIsInstance(receipt, dict)
        receipt["caller_note"] = "not part of the receipt contract"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"test_receipts\[0\] contains unknown fields: caller_note",
        ):
            validate_task_report_v2(report)

    def test_test_receipt_rejects_missing_nested_fields(self) -> None:
        report = self._complete_report()
        test_receipts = report["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        receipt = test_receipts[0]
        self.assertIsInstance(receipt, dict)
        del receipt["command"]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"test_receipts\[0\] is missing fields: command",
        ):
            validate_task_report_v2(report)

    def test_test_receipt_rejects_bool_as_exit_code(self) -> None:
        report = self._complete_report()
        test_receipts = report["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        receipt = test_receipts[0]
        self.assertIsInstance(receipt, dict)
        receipt["exit_code"] = False
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"test_receipts\[0\]\.exit_code must be an exact integer",
        ):
            validate_task_report_v2(report)

    def test_test_receipt_items_must_be_objects(self) -> None:
        report = self._complete_report()
        report["test_receipts"] = ["task-report-unit-tests"]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"test_receipts\[0\] must be a mapping",
        ):
            validate_task_report_v2(report)

    def test_test_receipt_id_must_be_a_non_empty_string(self) -> None:
        report = self._complete_report()
        test_receipts = report["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        receipt = test_receipts[0]
        self.assertIsInstance(receipt, dict)
        receipt["receipt_id"] = ""
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"test_receipts\[0\]\.receipt_id must be a non-empty string",
        ):
            validate_task_report_v2(report)

    def test_test_receipt_command_must_be_a_non_empty_string(self) -> None:
        report = self._complete_report()
        test_receipts = report["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        receipt = test_receipts[0]
        self.assertIsInstance(receipt, dict)
        receipt["command"] = ""
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"test_receipts\[0\]\.command must be a non-empty string",
        ):
            validate_task_report_v2(report)

    def test_malformed_receipts_are_rejected_before_ticket_outcome_precedence(self) -> None:
        report = self._complete_report()
        report["ticket_state"] = "IN_DOUBT"
        report["outcome"] = "IN_DOUBT"
        report["reason_codes"] = ["TICKET_IN_DOUBT"]
        test_receipts = report["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        test_receipts.append(copy.deepcopy(test_receipts[0]))
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "test_receipts must not contain duplicate receipt_id values",
        ):
            validate_task_report_v2(report)

    def test_test_receipt_result_is_closed_to_pass_or_fail(self) -> None:
        report = self._complete_report()
        test_receipts = report["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        receipt = test_receipts[0]
        self.assertIsInstance(receipt, dict)
        receipt["result"] = "GREEN"
        report["outcome"] = "FAIL"
        report["reason_codes"] = [
            "REQUIRED_TEST_FAILED:task-report-unit-tests"
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"test_receipts\[0\]\.result must be PASS or FAIL",
        ):
            validate_task_report_v2(report)

    def test_review_receipt_rejects_unknown_nested_fields(self) -> None:
        report = self._complete_report()
        review_receipts = report["review_receipts"]
        self.assertIsInstance(review_receipts, list)
        receipt = review_receipts[0]
        self.assertIsInstance(receipt, dict)
        receipt["caller_note"] = "not part of the receipt contract"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_receipts\[0\] contains unknown fields: caller_note",
        ):
            validate_task_report_v2(report)

    def test_review_receipt_rejects_bool_as_exit_code(self) -> None:
        report = self._complete_report()
        review_receipts = report["review_receipts"]
        self.assertIsInstance(review_receipts, list)
        receipt = review_receipts[0]
        self.assertIsInstance(receipt, dict)
        receipt["exit_code"] = False
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_receipts\[0\]\.exit_code must be an exact integer",
        ):
            validate_task_report_v2(report)

    def test_passing_receipt_requires_zero_exit_code(self) -> None:
        report = self._complete_report()
        test_receipts = report["test_receipts"]
        self.assertIsInstance(test_receipts, list)
        receipt = test_receipts[0]
        self.assertIsInstance(receipt, dict)
        receipt["exit_code"] = 1
        report["outcome"] = "FAIL"
        report["reason_codes"] = [
            "REQUIRED_TEST_FAILED:task-report-unit-tests"
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"test_receipts\[0\] PASS requires exit_code 0",
        ):
            validate_task_report_v2(report)

    def test_test_receipts_must_be_an_array_even_when_none_are_required(self) -> None:
        report = self._complete_report()
        requirements = report["requirements"]
        self.assertIsInstance(requirements, dict)
        requirements["required_test_receipt_ids"] = []
        report["test_receipts"] = {}
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "test_receipts must be a list",
        ):
            validate_task_report_v2(report)

    def test_evidence_ref_rejects_unknown_nested_fields(self) -> None:
        report = self._complete_report()
        report["input_evidence_refs"] = [
            {
                "evidence_id": "implementation-baseline",
                "evidence_ref": "research_state/control_plane/p0r2/implementation_baseline_v342_p0r2.json",
                "evidence_sha256": "8" * 64,
                "status": "VERIFIED",
                "caller_note": "not part of the evidence contract",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"input_evidence_refs\[0\] contains unknown fields: caller_note",
        ):
            validate_task_report_v2(report)

    def test_input_evidence_refs_must_be_an_array(self) -> None:
        report = self._complete_report()
        report["input_evidence_refs"] = {}
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "input_evidence_refs must be a list",
        ):
            validate_task_report_v2(report)

    def test_input_evidence_ref_items_must_be_objects(self) -> None:
        report = self._complete_report()
        report["input_evidence_refs"] = ["implementation-baseline"]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"input_evidence_refs\[0\] must be a mapping",
        ):
            validate_task_report_v2(report)

    def test_evidence_ref_requires_a_strict_sha256(self) -> None:
        report = self._complete_report()
        report["input_evidence_refs"] = [
            {
                "evidence_id": "implementation-baseline",
                "evidence_ref": "research_state/control_plane/p0r2/implementation_baseline_v342_p0r2.json",
                "evidence_sha256": "NOT-A-DIGEST",
                "status": "VERIFIED",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"input_evidence_refs\[0\]\.evidence_sha256 must be a lowercase SHA-256 digest",
        ):
            validate_task_report_v2(report)

    def test_evidence_id_must_be_a_non_empty_string(self) -> None:
        report = self._complete_report()
        report["input_evidence_refs"] = [
            {
                "evidence_id": "",
                "evidence_ref": "research_state/control_plane/p0r2/implementation_baseline_v342_p0r2.json",
                "evidence_sha256": "8" * 64,
                "status": "VERIFIED",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"input_evidence_refs\[0\]\.evidence_id must be a non-empty string",
        ):
            validate_task_report_v2(report)

    def test_evidence_ref_must_be_a_non_empty_string(self) -> None:
        report = self._complete_report()
        report["input_evidence_refs"] = [
            {
                "evidence_id": "implementation-baseline",
                "evidence_ref": "",
                "evidence_sha256": "8" * 64,
                "status": "VERIFIED",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"input_evidence_refs\[0\]\.evidence_ref must be a non-empty string",
        ):
            validate_task_report_v2(report)

    def test_evidence_status_is_closed(self) -> None:
        report = self._complete_report()
        report["input_evidence_refs"] = [
            {
                "evidence_id": "implementation-baseline",
                "evidence_ref": "research_state/control_plane/p0r2/implementation_baseline_v342_p0r2.json",
                "evidence_sha256": "8" * 64,
                "status": "STALE",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"input_evidence_refs\[0\]\.status must be VERIFIED, INVALID, or IN_DOUBT",
        ):
            validate_task_report_v2(report)

    def test_duplicate_evidence_ids_are_rejected(self) -> None:
        report = self._complete_report()
        evidence = {
            "evidence_id": "implementation-baseline",
            "evidence_ref": "research_state/control_plane/p0r2/implementation_baseline_v342_p0r2.json",
            "evidence_sha256": "8" * 64,
            "status": "VERIFIED",
        }
        report["input_evidence_refs"] = [evidence, copy.deepcopy(evidence)]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "input_evidence_refs must not contain duplicate evidence_id values",
        ):
            validate_task_report_v2(report)

    def test_required_invalid_evidence_mechanically_derives_fail(self) -> None:
        complete = self._complete_report()
        draft = {
            key: copy.deepcopy(value)
            for key, value in complete.items()
            if key
            not in {
                "schema_version",
                "outcome",
                "reason_codes",
                "unexpected_changes",
                "report_payload_sha256",
            }
        }
        requirements = draft["requirements"]
        self.assertIsInstance(requirements, dict)
        requirements["required_evidence_ids"] = ["implementation-baseline"]
        draft["input_evidence_refs"] = [
            {
                "evidence_id": "implementation-baseline",
                "evidence_ref": "research_state/control_plane/p0r2/implementation_baseline_v342_p0r2.json",
                "evidence_sha256": "8" * 64,
                "status": "INVALID",
            }
        ]

        report = build_task_report_v2(draft)

        self.assertEqual(report["outcome"], "FAIL")
        self.assertEqual(
            report["reason_codes"],
            ["REQUIRED_EVIDENCE_INVALID:implementation-baseline"],
        )

    def test_required_in_doubt_evidence_mechanically_derives_in_doubt(self) -> None:
        complete = self._complete_report()
        draft = {
            key: copy.deepcopy(value)
            for key, value in complete.items()
            if key
            not in {
                "schema_version",
                "outcome",
                "reason_codes",
                "unexpected_changes",
                "report_payload_sha256",
            }
        }
        requirements = draft["requirements"]
        self.assertIsInstance(requirements, dict)
        requirements["required_evidence_ids"] = ["implementation-baseline"]
        draft["input_evidence_refs"] = [
            {
                "evidence_id": "implementation-baseline",
                "evidence_ref": "research_state/control_plane/p0r2/implementation_baseline_v342_p0r2.json",
                "evidence_sha256": "8" * 64,
                "status": "IN_DOUBT",
            }
        ]

        report = build_task_report_v2(draft)

        self.assertEqual(report["outcome"], "IN_DOUBT")
        self.assertEqual(
            report["reason_codes"],
            ["REQUIRED_EVIDENCE_IN_DOUBT:implementation-baseline"],
        )

    def test_review_findings_must_be_an_array(self) -> None:
        report = self._complete_report()
        report["review_findings"] = {}
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "review_findings must be a list",
        ):
            validate_task_report_v2(report)

    def test_review_finding_rejects_unknown_nested_fields(self) -> None:
        report = self._complete_report()
        report["review_findings"] = [
            {
                "finding_id": "finding-001",
                "review_receipt_id": "task-report-controller-review",
                "severity": "NON_BLOCKING",
                "status": "RESOLVED",
                "summary": "A bounded review note.",
                "resolution": "Confirmed by the controller.",
                "caller_note": "not part of the finding contract",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_findings\[0\] contains unknown fields: caller_note",
        ):
            validate_task_report_v2(report)

    def test_review_finding_severity_is_closed(self) -> None:
        report = self._complete_report()
        report["review_findings"] = [
            {
                "finding_id": "finding-001",
                "review_receipt_id": "task-report-controller-review",
                "severity": "CRITICAL",
                "status": "RESOLVED",
                "summary": "A bounded review note.",
                "resolution": "Confirmed by the controller.",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_findings\[0\]\.severity must be BLOCKING or NON_BLOCKING",
        ):
            validate_task_report_v2(report)

    def test_review_finding_status_is_closed(self) -> None:
        report = self._complete_report()
        report["review_findings"] = [
            {
                "finding_id": "finding-001",
                "review_receipt_id": "task-report-controller-review",
                "severity": "NON_BLOCKING",
                "status": "IGNORED",
                "summary": "A bounded review note.",
                "resolution": None,
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_findings\[0\]\.status must be OPEN or RESOLVED",
        ):
            validate_task_report_v2(report)

    def test_review_finding_id_must_be_a_non_empty_string(self) -> None:
        report = self._complete_report()
        report["review_findings"] = [
            {
                "finding_id": "",
                "review_receipt_id": "task-report-controller-review",
                "severity": "NON_BLOCKING",
                "status": "RESOLVED",
                "summary": "A bounded review note.",
                "resolution": "Confirmed by the controller.",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_findings\[0\]\.finding_id must be a non-empty string",
        ):
            validate_task_report_v2(report)

    def test_review_finding_summary_must_be_a_non_empty_string(self) -> None:
        report = self._complete_report()
        report["review_findings"] = [
            {
                "finding_id": "finding-001",
                "review_receipt_id": "task-report-controller-review",
                "severity": "NON_BLOCKING",
                "status": "RESOLVED",
                "summary": "",
                "resolution": "Confirmed by the controller.",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_findings\[0\]\.summary must be a non-empty string",
        ):
            validate_task_report_v2(report)

    def test_review_finding_receipt_id_must_be_a_non_empty_string(self) -> None:
        report = self._complete_report()
        report["review_findings"] = [
            {
                "finding_id": "finding-001",
                "review_receipt_id": "",
                "severity": "NON_BLOCKING",
                "status": "RESOLVED",
                "summary": "A bounded review note.",
                "resolution": "Confirmed by the controller.",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_findings\[0\]\.review_receipt_id must be a non-empty string",
        ):
            validate_task_report_v2(report)

    def test_duplicate_review_finding_ids_are_rejected(self) -> None:
        report = self._complete_report()
        finding = {
            "finding_id": "finding-001",
            "review_receipt_id": "task-report-controller-review",
            "severity": "NON_BLOCKING",
            "status": "RESOLVED",
            "summary": "A bounded review note.",
            "resolution": "Confirmed by the controller.",
        }
        report["review_findings"] = [finding, copy.deepcopy(finding)]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "review_findings must not contain duplicate finding_id values",
        ):
            validate_task_report_v2(report)

    def test_open_review_finding_requires_null_resolution(self) -> None:
        report = self._complete_report()
        report["review_findings"] = [
            {
                "finding_id": "finding-001",
                "review_receipt_id": "task-report-controller-review",
                "severity": "NON_BLOCKING",
                "status": "OPEN",
                "summary": "A bounded review note.",
                "resolution": "Premature resolution text.",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_findings\[0\] OPEN requires resolution null",
        ):
            validate_task_report_v2(report)

    def test_resolved_review_finding_requires_resolution_text(self) -> None:
        report = self._complete_report()
        report["review_findings"] = [
            {
                "finding_id": "finding-001",
                "review_receipt_id": "task-report-controller-review",
                "severity": "NON_BLOCKING",
                "status": "RESOLVED",
                "summary": "A bounded review note.",
                "resolution": None,
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_findings\[0\]\.resolution must be a non-empty string",
        ):
            validate_task_report_v2(report)

    def test_review_finding_must_reference_an_existing_review_receipt(self) -> None:
        report = self._complete_report()
        report["review_findings"] = [
            {
                "finding_id": "finding-001",
                "review_receipt_id": "unknown-review",
                "severity": "NON_BLOCKING",
                "status": "RESOLVED",
                "summary": "A bounded review note.",
                "resolution": "Confirmed by the controller.",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_findings\[0\] references unknown review receipt: unknown-review",
        ):
            validate_task_report_v2(report)

    def test_passing_review_cannot_have_an_open_blocking_finding(self) -> None:
        report = self._complete_report()
        report["review_findings"] = [
            {
                "finding_id": "finding-001",
                "review_receipt_id": "task-report-controller-review",
                "severity": "BLOCKING",
                "status": "OPEN",
                "summary": "A blocking issue is still unresolved.",
                "resolution": None,
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"review_findings\[0\] conflicts with PASS review receipt",
        ):
            validate_task_report_v2(report)

    def test_changed_file_rejects_unknown_nested_fields(self) -> None:
        report = self._complete_report()
        changed_files = report["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["caller_note"] = "not part of the changed-file contract"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"changed_files\[0\] contains unknown fields: caller_note",
        ):
            validate_task_report_v2(report)

    def test_changed_file_change_type_is_closed(self) -> None:
        report = self._complete_report()
        changed_files = report["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["change_type"] = "RENAME"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"changed_files\[0\]\.change_type must be ADD, MODIFY, or DELETE",
        ):
            validate_task_report_v2(report)

    def test_added_file_requires_null_baseline_hash(self) -> None:
        report = self._complete_report()
        changed_files = report["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["baseline_sha256"] = "9" * 64
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"changed_files\[0\] ADD requires baseline_sha256 null",
        ):
            validate_task_report_v2(report)

    def test_added_file_requires_a_current_sha256(self) -> None:
        report = self._complete_report()
        changed_files = report["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["current_sha256"] = None
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"changed_files\[0\]\.current_sha256 must be a lowercase SHA-256 digest",
        ):
            validate_task_report_v2(report)

    def test_modified_file_requires_a_baseline_sha256(self) -> None:
        report = self._complete_report()
        changed_files = report["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["change_type"] = "MODIFY"
        changed_file["baseline_sha256"] = None
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"changed_files\[0\]\.baseline_sha256 must be a lowercase SHA-256 digest",
        ):
            validate_task_report_v2(report)

    def test_modified_file_requires_a_current_sha256(self) -> None:
        report = self._complete_report()
        changed_files = report["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["change_type"] = "MODIFY"
        changed_file["baseline_sha256"] = "9" * 64
        changed_file["current_sha256"] = None
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"changed_files\[0\]\.current_sha256 must be a lowercase SHA-256 digest",
        ):
            validate_task_report_v2(report)

    def test_deleted_file_requires_a_baseline_sha256(self) -> None:
        report = self._complete_report()
        changed_files = report["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["change_type"] = "DELETE"
        changed_file["baseline_sha256"] = None
        changed_file["current_sha256"] = None
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"changed_files\[0\]\.baseline_sha256 must be a lowercase SHA-256 digest",
        ):
            validate_task_report_v2(report)

    def test_deleted_file_requires_null_current_hash(self) -> None:
        report = self._complete_report()
        changed_files = report["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["change_type"] = "DELETE"
        changed_file["baseline_sha256"] = "9" * 64
        changed_file["current_sha256"] = "5" * 64
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"changed_files\[0\] DELETE requires current_sha256 null",
        ):
            validate_task_report_v2(report)

    def test_changed_file_path_must_be_repository_relative_posix(self) -> None:
        invalid_paths = [
            "",
            "research_automation\\control_plane\\task_reports.py",
            "/absolute/file.py",
            "//server/share/file.py",
            "C:/repo/file.py",
            "C:repo/file.py",
            "dir/:stream",
            "dir/*.py",
            "dir/?.py",
            " dir/file.py",
            "dir/file.py ",
            "../escape.py",
            "dir/../file.py",
            "./file.py",
            "dir//file.py",
            "dir/",
            "dir/\x00file.py",
            "dir/\x1ffile.py",
        ]
        for invalid_path in invalid_paths:
            with self.subTest(path=repr(invalid_path)):
                report = self._complete_report()
                changed_files = report["changed_files"]
                self.assertIsInstance(changed_files, list)
                changed_file = changed_files[0]
                self.assertIsInstance(changed_file, dict)
                changed_file["path"] = invalid_path
                report["report_payload_sha256"] = task_report_v2_payload_sha256(
                    report
                )

                with self.assertRaisesRegex(
                    TaskReportValidationError,
                    r"changed_files\[0\]\.path must be a repository-relative POSIX file path",
                ):
                    validate_task_report_v2(report)

    def test_duplicate_changed_file_paths_are_rejected(self) -> None:
        report = self._complete_report()
        changed_files = report["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_files.append(copy.deepcopy(changed_files[0]))
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "changed_files must not contain duplicate path values",
        ):
            validate_task_report_v2(report)

    def test_rehash_cannot_hide_a_mechanically_unexpected_change(self) -> None:
        report = self._complete_report()
        changed_files = report["changed_files"]
        self.assertIsInstance(changed_files, list)
        changed_file = changed_files[0]
        self.assertIsInstance(changed_file, dict)
        changed_file["path"] = "strategy/unified_b1_strategy.py"
        report["unexpected_changes"] = []
        report["outcome"] = "PASS"
        report["reason_codes"] = []
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "unexpected_changes do not match mechanical derivation",
        ):
            validate_task_report_v2(report)

    def test_scope_file_rules_must_be_repository_relative_posix(self) -> None:
        invalid_rules = [
            "",
            "dir\\file.py",
            "/absolute",
            "C:/repo",
            "../escape",
            "dir//",
            "dir/*",
            "dir/?",
            " dir/file.py",
            "dir/file.py ",
            "dir/\x1f",
        ]
        for field_name in ("allowed_files", "forbidden_files"):
            for invalid_rule in invalid_rules:
                with self.subTest(field=field_name, rule=repr(invalid_rule)):
                    report = self._complete_report()
                    report[field_name] = [invalid_rule]
                    report["report_payload_sha256"] = task_report_v2_payload_sha256(
                        report
                    )

                    with self.assertRaisesRegex(
                        TaskReportValidationError,
                        rf"{field_name}\[0\] must be a repository-relative POSIX path or directory prefix",
                    ):
                        validate_task_report_v2(report)

    def test_forbidden_files_accepts_a_canonical_windows_directory_prefix(self) -> None:
        report = self._complete_report()
        report["forbidden_files"] = ["D:/KBase/"]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        validate_task_report_v2(report)

    def test_allowed_files_rejects_a_windows_absolute_directory_prefix(self) -> None:
        report = self._complete_report()
        report["allowed_files"] = ["D:/KBase/"]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"allowed_files\[0\] must be a repository-relative POSIX path or directory prefix",
        ):
            validate_task_report_v2(report)

    def test_forbidden_files_rejects_noncanonical_windows_absolute_rules(self) -> None:
        invalid_rules = [
            "d:/KBase/",
            "D:/KBase",
            "D:/KBase/../Data/",
            "D:/KBase//Data/",
            "D:/KBase./",
            "D:/KBase /",
            "D:\\KBase\\",
            "//server/share/",
            "D:relative/",
            "D:/KBase/*/",
            "D:/KBase/:stream/",
            "D://",
        ]
        for invalid_rule in invalid_rules:
            with self.subTest(rule=repr(invalid_rule)):
                report = self._complete_report()
                report["forbidden_files"] = [invalid_rule]
                report["report_payload_sha256"] = task_report_v2_payload_sha256(
                    report
                )

                with self.assertRaisesRegex(
                    TaskReportValidationError,
                    r"forbidden_files\[0\] must be a repository-relative POSIX path or directory prefix",
                ):
                    validate_task_report_v2(report)

    def test_task_report_timestamps_must_be_timezone_aware(self) -> None:
        for field_name in ("started_at", "completed_at"):
            with self.subTest(field=field_name):
                report = self._complete_report()
                report[field_name] = "2026-07-26T06:40:00"
                report["report_payload_sha256"] = task_report_v2_payload_sha256(
                    report
                )

                with self.assertRaisesRegex(
                    TaskReportValidationError,
                    rf"{field_name} must be a timezone-aware ISO-8601 timestamp",
                ):
                    validate_task_report_v2(report)

    def test_task_report_completion_cannot_precede_start(self) -> None:
        report = self._complete_report()
        report["started_at"] = "2026-07-26T07:00:00+08:00"
        report["completed_at"] = "2026-07-26T06:59:59+08:00"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "completed_at must not precede started_at",
        ):
            validate_task_report_v2(report)

    def test_top_level_artifact_refs_must_be_repository_relative_posix(self) -> None:
        for field_name in ("task_spec_ref", "baseline_ref"):
            with self.subTest(field=field_name):
                report = self._complete_report()
                report[field_name] = "../outside.json"
                report["report_payload_sha256"] = task_report_v2_payload_sha256(
                    report
                )

                with self.assertRaisesRegex(
                    TaskReportValidationError,
                    rf"{field_name} must be a repository-relative POSIX file path",
                ):
                    validate_task_report_v2(report)

    def test_evidence_artifact_ref_must_be_repository_relative_posix(self) -> None:
        report = self._complete_report()
        report["input_evidence_refs"] = [
            {
                "evidence_id": "implementation-baseline",
                "evidence_ref": "../outside.json",
                "evidence_sha256": "8" * 64,
                "status": "VERIFIED",
            }
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"input_evidence_refs\[0\]\.evidence_ref must be a repository-relative POSIX file path",
        ):
            validate_task_report_v2(report)

    def test_baseline_hash_must_be_a_strict_sha256(self) -> None:
        report = self._complete_report()
        report["baseline_sha256"] = "NOT-A-DIGEST"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "baseline_sha256 must be a lowercase SHA-256 digest",
        ):
            validate_task_report_v2(report)

    def test_top_level_identity_and_objective_fields_must_be_non_empty(self) -> None:
        field_names = (
            "plan_version",
            "task_id",
            "attempt_id",
            "authorization_ref",
            "objective",
            "idempotency_key",
        )
        for field_name in field_names:
            with self.subTest(field=field_name):
                report = self._complete_report()
                report[field_name] = ""
                report["report_payload_sha256"] = task_report_v2_payload_sha256(
                    report
                )

                with self.assertRaisesRegex(
                    TaskReportValidationError,
                    rf"{field_name} must be a non-empty string",
                ):
                    validate_task_report_v2(report)

    def test_ticket_state_is_closed_to_terminal_values(self) -> None:
        report = self._complete_report()
        report["ticket_state"] = "PENDING"
        report["outcome"] = "BLOCKED"
        report["reason_codes"] = ["TICKET_NOT_SUCCEEDED"]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "ticket_state must be SUCCEEDED, FAILED, or IN_DOUBT",
        ):
            validate_task_report_v2(report)

    def test_dependencies_must_be_a_unique_string_array(self) -> None:
        report = self._complete_report()
        report["dependencies"] = ["P0R2-T0", "P0R2-T0"]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "dependencies must not contain duplicates",
        ):
            validate_task_report_v2(report)

    def test_side_effect_summary_is_validated_before_ticket_precedence(self) -> None:
        report = self._complete_report()
        report["ticket_state"] = "IN_DOUBT"
        report["outcome"] = "IN_DOUBT"
        report["reason_codes"] = ["TICKET_IN_DOUBT"]
        side_effect_summary = report["side_effect_summary"]
        self.assertIsInstance(side_effect_summary, dict)
        side_effect_summary["caller_note"] = "not part of the summary contract"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "side_effect_summary contains unknown fields: caller_note",
        ):
            validate_task_report_v2(report)

    def test_floating_point_values_are_rejected_everywhere(self) -> None:
        report = self._complete_report()
        report["external_invocations"] = [{"latency_seconds": 1.5}]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"external_invocations\[0\]\.latency_seconds must not be a floating-point value",
        ):
            validate_task_report_v2(report)

    def test_closed_scalar_fields_translate_wrong_types(self) -> None:
        cases = (
            ("phase", "phase must be P0 through P8"),
            ("outcome", "outcome must be PASS, FAIL, BLOCKED, or IN_DOUBT"),
            (
                "ticket_state",
                "ticket_state must be SUCCEEDED, FAILED, or IN_DOUBT",
            ),
        )
        for field_name, message in cases:
            with self.subTest(field=field_name):
                report = self._complete_report()
                report[field_name] = []
                report["report_payload_sha256"] = task_report_v2_payload_sha256(
                    report
                )

                with self.assertRaisesRegex(TaskReportValidationError, message):
                    validate_task_report_v2(report)

    def test_identity_strings_reject_blank_or_surrounding_whitespace(self) -> None:
        for value in ("   ", " task-id", "task-id "):
            with self.subTest(value=repr(value)):
                report = self._complete_report()
                report["task_id"] = value
                report["report_payload_sha256"] = task_report_v2_payload_sha256(
                    report
                )

                with self.assertRaisesRegex(
                    TaskReportValidationError,
                    "task_id must be a non-empty string without surrounding whitespace",
                ):
                    validate_task_report_v2(report)

    def test_string_arrays_reject_whitespace_only_items(self) -> None:
        report = self._complete_report()
        report["dependencies"] = ["   "]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "dependencies must be a list of non-empty strings",
        ):
            validate_task_report_v2(report)

    def test_external_invocations_must_be_an_array(self) -> None:
        report = self._complete_report()
        report["external_invocations"] = {}
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "external_invocations must be a list",
        ):
            validate_task_report_v2(report)

    def test_external_invocation_rejects_unknown_nested_fields(self) -> None:
        report = self._complete_report()
        invocation = self._reported_external_invocation()
        invocation["caller_note"] = "not part of the invocation contract"
        report["external_invocations"] = [invocation]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"external_invocations\[0\] contains unknown fields: caller_note",
        ):
            validate_task_report_v2(report)

    def test_external_invocation_requires_a_strict_sha256(self) -> None:
        report = self._complete_report()
        invocation = self._reported_external_invocation()
        invocation["invocation_sha256"] = "NOT-A-DIGEST"
        report["external_invocations"] = [invocation]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"external_invocations\[0\]\.invocation_sha256 must be a lowercase SHA-256 digest",
        ):
            validate_task_report_v2(report)

    def test_external_invocation_ref_must_be_repository_relative_posix(self) -> None:
        report = self._complete_report()
        invocation = self._reported_external_invocation()
        invocation["invocation_ref"] = "../outside.json"
        report["external_invocations"] = [invocation]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"external_invocations\[0\]\.invocation_ref must be a repository-relative POSIX file path",
        ):
            validate_task_report_v2(report)

    def test_duplicate_external_invocation_ids_are_rejected(self) -> None:
        report = self._complete_report()
        invocation = self._reported_external_invocation()
        report["external_invocations"] = [
            invocation,
            copy.deepcopy(invocation),
        ]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "external_invocations must not contain duplicate invocation_id values",
        ):
            validate_task_report_v2(report)

    def test_usage_summary_rejects_unknown_nested_fields(self) -> None:
        report = self._complete_report()
        invocation = self._reported_external_invocation()
        usage = invocation["usage"]
        self.assertIsInstance(usage, dict)
        usage["fixed_estimate"] = 3000
        report["external_invocations"] = [invocation]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"external_invocations\[0\]\.usage contains unknown fields: fixed_estimate",
        ):
            validate_task_report_v2(report)

    def test_usage_status_is_closed(self) -> None:
        report = self._complete_report()
        invocation = self._reported_external_invocation()
        usage = invocation["usage"]
        self.assertIsInstance(usage, dict)
        usage["status"] = "APPROXIMATE"
        report["external_invocations"] = [invocation]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"external_invocations\[0\]\.usage\.status must be REPORTED, ESTIMATED, or UNKNOWN",
        ):
            validate_task_report_v2(report)

    def test_unknown_usage_requires_all_token_values_null(self) -> None:
        report = self._complete_report()
        invocation = self._reported_external_invocation()
        usage = invocation["usage"]
        self.assertIsInstance(usage, dict)
        usage["status"] = "UNKNOWN"
        report["external_invocations"] = [invocation]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"external_invocations\[0\]\.usage UNKNOWN requires token values null",
        ):
            validate_task_report_v2(report)

    def test_known_usage_rejects_bool_as_token_count(self) -> None:
        for status in ("REPORTED", "ESTIMATED"):
            with self.subTest(status=status):
                report = self._complete_report()
                invocation = self._reported_external_invocation()
                usage = invocation["usage"]
                self.assertIsInstance(usage, dict)
                usage["status"] = status
                usage["total_tokens"] = False
                report["external_invocations"] = [invocation]
                report["report_payload_sha256"] = task_report_v2_payload_sha256(
                    report
                )

                with self.assertRaisesRegex(
                    TaskReportValidationError,
                    r"external_invocations\[0\]\.usage\.total_tokens must be a nonnegative exact integer",
                ):
                    validate_task_report_v2(report)

    def test_known_usage_total_matches_reported_components(self) -> None:
        report = self._complete_report()
        invocation = self._reported_external_invocation()
        usage = invocation["usage"]
        self.assertIsInstance(usage, dict)
        usage["total_tokens"] = 1499
        report["external_invocations"] = [invocation]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        with self.assertRaisesRegex(
            TaskReportValidationError,
            r"external_invocations\[0\]\.usage\.total_tokens must equal input_tokens plus output_tokens",
        ):
            validate_task_report_v2(report)

    def test_unknown_usage_stays_null_without_changing_outcome(self) -> None:
        report = self._complete_report()
        invocation = self._reported_external_invocation()
        invocation["usage"] = {
            "status": "UNKNOWN",
            "input_tokens": None,
            "output_tokens": None,
            "total_tokens": None,
        }
        report["external_invocations"] = [invocation]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        validate_task_report_v2(report)

        self.assertEqual(report["outcome"], "PASS")

    def test_known_usage_preserves_total_when_components_are_unavailable(self) -> None:
        report = self._complete_report()
        invocation = self._reported_external_invocation()
        invocation["usage"] = {
            "status": "REPORTED",
            "input_tokens": None,
            "output_tokens": None,
            "total_tokens": 1500,
        }
        report["external_invocations"] = [invocation]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        validate_task_report_v2(report)

    def test_task_report_carries_ticket_identity_for_authority_lookup(self) -> None:
        report = self._complete_report()
        report["ticket_id"] = "ticket-p0r2-t1-task-report-001"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)

        validate_task_report_v2(report)

    def test_builder_translates_malformed_drafts_to_build_errors(self) -> None:
        complete = self._complete_report()
        draft = {
            key: copy.deepcopy(value)
            for key, value in complete.items()
            if key
            not in {
                "schema_version",
                "outcome",
                "reason_codes",
                "unexpected_changes",
                "report_payload_sha256",
            }
        }
        draft["external_invocations"] = {}

        with self.assertRaisesRegex(
            TaskReportBuildError,
            "external_invocations must be a list",
        ):
            build_task_report_v2(draft)

    def test_builder_cannot_emit_a_report_larger_than_the_parser_limit(self) -> None:
        complete = self._complete_report()
        draft = {
            key: copy.deepcopy(value)
            for key, value in complete.items()
            if key
            not in {
                "schema_version",
                "outcome",
                "reason_codes",
                "unexpected_changes",
                "report_payload_sha256",
            }
        }
        draft["objective"] = "x" * MAX_TASK_REPORT_V2_BYTES

        with self.assertRaisesRegex(
            TaskReportBuildError,
            "task report exceeds 65536 byte limit",
        ):
            build_task_report_v2(draft)

    def test_direct_mapping_validator_rejects_cycles_without_recursing(self) -> None:
        report = self._complete_report()
        cyclic: list[object] = []
        cyclic.append(cyclic)
        report["dependencies"] = cyclic

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "task report must not contain cyclic containers",
        ):
            validate_task_report_v2(report)

    def test_direct_mapping_uses_the_same_depth_limit_as_byte_parser(self) -> None:
        report = self._complete_report()
        nested: object = None
        for _ in range(MAX_TASK_REPORT_V2_DEPTH - 1):
            nested = [nested]
        report["dependencies"] = nested

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "task report exceeds 64 level nesting limit",
        ):
            validate_task_report_v2(report)

    def test_direct_mapping_rejects_non_string_keys_with_a_typed_error(self) -> None:
        report = self._complete_report()
        side_effect_summary = report["side_effect_summary"]
        self.assertIsInstance(side_effect_summary, dict)
        side_effect_summary[1] = "invalid JSON key"

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "task report mappings require string keys",
        ):
            validate_task_report_v2(report)


class TestReceiptContractTests(unittest.TestCase):
    """CR-010 F-05: full-contract test receipts + legacy compatibility."""

    def _legacy_report(self) -> dict[str, object]:
        report = TaskReportV2TracerTests()._complete_report()
        return report

    def _contract(self) -> dict[str, object]:
        return {
            "executable": "C:\python\python.exe",
            "cwd": "D:\workspace\a-share-quant-selector-main",
            "runtime_version": "Python 3.13",
            "lock_hash": "a" * 64,
            "candidate_commit": "b" * 64,
            "candidate_tree": "c" * 64,
            "started_at_utc": "2026-08-14T00:00:00Z",
            "completed_at_utc": "2026-08-14T00:05:00Z",
            "stdout_ref": "research_state/control_plane/full_discovery.log",
            "stdout_sha256": "d" * 64,
            "stderr_ref": "research_state/control_plane/full_discovery.err",
            "stderr_sha256": "e" * 64,
        }

    def test_legacy_receipt_without_contract_still_validates(self) -> None:
        report = self._legacy_report()
        for receipt in report["test_receipts"]:
            self.assertNotIn("candidate_commit", receipt)
        validate_task_report_v2(report)  # must not raise

    def test_new_style_receipt_with_full_contract_validates(self) -> None:
        report = self._legacy_report()
        contract = self._contract()
        receipt = report["test_receipts"][0]
        receipt.update(contract)
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)
        validate_task_report_v2(report)  # must not raise

    def test_new_style_receipt_missing_contract_fields_rejected(self) -> None:
        report = self._legacy_report()
        contract = self._contract()
        receipt = report["test_receipts"][0]
        receipt.update(contract)
        del receipt["stdout_sha256"]
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)
        with self.assertRaisesRegex(
            TaskReportValidationError,
            "test_receipts\[0\] is missing fields: stdout_sha256",
        ):
            validate_task_report_v2(report)

    def test_new_style_receipt_unknown_contract_field_rejected(self) -> None:
        report = self._legacy_report()
        contract = self._contract()
        receipt = report["test_receipts"][0]
        receipt.update(contract)
        receipt["bogus_extra"] = "x"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)
        with self.assertRaisesRegex(
            TaskReportValidationError,
            "test_receipts\[0\] contains unknown fields: bogus_extra",
        ):
            validate_task_report_v2(report)

    def test_new_style_receipt_non_utc_timestamp_rejected(self) -> None:
        report = self._legacy_report()
        contract = self._contract()
        receipt = report["test_receipts"][0]
        receipt.update(contract)
        receipt["started_at_utc"] = "2026-08-14T08:00:00+08:00"
        report["report_payload_sha256"] = task_report_v2_payload_sha256(report)
        with self.assertRaisesRegex(
            TaskReportValidationError,
            "started_at_utc must be normalized to UTC",
        ):
            validate_task_report_v2(report)

    def test_builder_stamps_contract_from_receipt_contract_block(self) -> None:
        draft = TaskReportV2TracerTests()._complete_report()
        draft.pop("report_payload_sha256", None)
        draft.pop("schema_version", None)
        draft.pop("outcome", None)
        draft.pop("reason_codes", None)
        draft.pop("unexpected_changes", None)
        receipt = draft["test_receipts"][0]
        receipt["candidate_commit"] = "b" * 64  # mark new-style
        draft["receipt_contract"] = self._contract()
        report = build_task_report_v2(draft)
        stamped = report["test_receipts"][0]
        self.assertEqual(stamped["executable"], self._contract()["executable"])
        self.assertEqual(stamped["candidate_tree"], "c" * 64)
        self.assertEqual(stamped["stdout_sha256"], "d" * 64)

    def test_builder_keeps_legacy_receipts_untouched(self) -> None:
        draft = TaskReportV2TracerTests()._complete_report()
        draft.pop("report_payload_sha256", None)
        draft.pop("schema_version", None)
        draft.pop("outcome", None)
        draft.pop("reason_codes", None)
        draft.pop("unexpected_changes", None)
        report = build_task_report_v2(draft)
        for receipt in report["test_receipts"]:
            self.assertNotIn("candidate_commit", receipt)
            self.assertNotIn("executable", receipt)


if __name__ == "__main__":
    unittest.main()
