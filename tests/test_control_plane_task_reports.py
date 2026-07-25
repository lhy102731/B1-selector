from __future__ import annotations

import copy
import unittest

from research_automation.control_plane.task_reports import (
    TaskReportValidationError,
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
                    "command": "python -m unittest tests.test_control_plane_task_reports -v",
                    "exit_code": 0,
                    "result": "PASS",
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
            "outcome": "PASS",
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
        assert isinstance(changed_files, list)
        assert isinstance(changed_files[0], dict)
        changed_files[0]["current_sha256"] = "6" * 64

        with self.assertRaisesRegex(
            TaskReportValidationError,
            "report_payload_sha256 mismatch",
        ):
            validate_task_report_v2(tampered)


if __name__ == "__main__":
    unittest.main()
