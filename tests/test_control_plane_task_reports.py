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


if __name__ == "__main__":
    unittest.main()
