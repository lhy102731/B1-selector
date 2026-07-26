from __future__ import annotations

import unittest
from datetime import datetime, timezone

from research_automation.control_plane.gates import (
    GateBuildError,
    PhaseGateBuilder,
    validate_gate_report,
)


class PhaseGateBuilderTests(unittest.TestCase):
    def _passing_draft(self) -> dict[str, object]:
        artifact = {
            "ref": "research_state/control_plane/p0r2/evidence.json",
            "sha256": "d" * 64,
        }
        return {
            "plan_version": "V3.4.2-P0R2",
            "phase": "P0",
            "attempt_id": "p0r2-attempt-001",
            "identity_binding": {
                "plan_hash": "a" * 64,
                "scope_hash": "b" * 64,
                "instruction_policy_hash": "c" * 64,
            },
            "task_reports": [
                {
                    "report_ref": (
                        "research_state/control_plane/p0r2/reports/task.json"
                    ),
                    "report_sha256": "1" * 64,
                    "ticket_id": "ticket-001",
                    "outcome": "PASS",
                }
            ],
            "implementation_baseline": dict(artifact),
            "code_freeze_manifest": dict(artifact),
            "final_inventory": dict(artifact),
            "reviewed_entry_policy": dict(artifact),
            "scheduler_inventory": {
                **artifact,
                "status": "VERIFIED",
            },
            "test_receipts": [
                {
                    "receipt_id": "gate-tests",
                    "command": "python -m unittest tests.test_control_plane_gates",
                    "exit_code": 0,
                    "result": "PASS",
                }
            ],
            "authority_snapshot": {
                "open_ticket_ids": [],
                "failed_ticket_ids": [],
                "in_doubt_ticket_ids": [],
                "pending_outbox_count": 0,
            },
            "side_effect_summary": {
                "observed": [],
                "unauthorized": [],
            },
            "file_delta_summary": {
                "changed_files": [],
                "unexpected_changes": [],
            },
            "unresolved_risks": [],
        }

    def test_caller_cannot_supply_computed_gate_fields(self) -> None:
        builder = PhaseGateBuilder()

        for field_name, value in (
            ("verdict", "PASS"),
            ("reason_codes", []),
            ("auto_advance", True),
            ("created_at", "2026-07-26T08:00:00Z"),
            ("gate_report_sha256", "a" * 64),
        ):
            with self.subTest(field_name=field_name):
                with self.assertRaisesRegex(
                    GateBuildError,
                    "computed fields",
                ):
                    builder.build({field_name: value})

    def test_builder_computes_a_hashed_non_advancing_pass_candidate(self) -> None:
        now = datetime(2026, 7, 26, 8, 0, tzinfo=timezone.utc)
        report = PhaseGateBuilder(clock=lambda: now).build(
            self._passing_draft()
        )

        self.assertEqual(report["verdict"], "PASS")
        self.assertEqual(report["reason_codes"], [])
        self.assertFalse(report["auto_advance"])
        self.assertEqual(report["created_at"], "2026-07-26T08:00:00Z")
        self.assertEqual(len(report["gate_report_sha256"]), 64)
        validate_gate_report(report)


if __name__ == "__main__":
    unittest.main()
