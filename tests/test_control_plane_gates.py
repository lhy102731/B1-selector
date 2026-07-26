from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import Actor, Phase, SideEffect
from research_automation.control_plane.gates import (
    GateAuthorityMismatchError,
    GateBuildError,
    PhaseGateBuilder,
    PhaseGateVerifier,
    validate_gate_report,
)
from research_automation.control_plane.stores import AuthorityIdentity


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"


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
                "active_grant_ids": ["grant-001"],
                "open_ticket_ids": [],
                "succeeded_ticket_ids": ["ticket-001"],
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

    def test_verifier_requeries_the_authority_snapshot(self) -> None:
        now = datetime(2026, 7, 26, 8, 15, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-gate-verify")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )
        task_spec = {
            "task_id": "P0R2-T3-GATE-VERIFY",
            "objective": "Re-query authority before accepting a gate.",
            "dependencies": [],
            "idempotency_key": "p0r2-gate-verify-001",
            "task_spec_ref": "research_state/control_plane/p0r2/task_specs/gate.json",
            "task_spec_sha256": "e" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_automation/control_plane/gates.py"],
            "forbidden_files": ["data/"],
            "baseline_ref": "research_state/control_plane/p0r2/baseline.json",
            "baseline_sha256": "f" * 64,
            "input_evidence_refs": [],
        }

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=datetime(2026, 7, 26, 9, 15, tzinfo=timezone.utc),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                ticket = authority._issue_task_ticket(
                    grant,
                    task_spec,
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                lease = authority._begin_task(ticket)
                authority._finish_task(
                    lease,
                    outcome="SUCCEEDED",
                    evidence_ref="evidence/gate-verify.json",
                )
                journal = stores_module._OperationalJournal(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                stores_module._mirror_authority_outbox(
                    authority,
                    journal,
                    limit=100,
                )

                reader = stores_module.AuthorityReader()
                snapshot = reader.phase_gate_snapshot(
                    Phase.P0,
                    "p0r2-attempt-001",
                )
                self.assertEqual(
                    snapshot.to_report_dict(),
                    {
                        "active_grant_ids": [grant.grant_id],
                        "open_ticket_ids": [],
                        "succeeded_ticket_ids": [ticket.ticket_id],
                        "failed_ticket_ids": [],
                        "in_doubt_ticket_ids": [],
                        "pending_outbox_count": 0,
                    },
                )
                draft = self._passing_draft()
                draft["task_reports"] = [
                    {
                        "report_ref": (
                            "research_state/control_plane/p0r2/reports/gate.json"
                        ),
                        "report_sha256": "1" * 64,
                        "ticket_id": ticket.ticket_id,
                        "outcome": "PASS",
                    }
                ]
                draft["authority_snapshot"] = snapshot.to_report_dict()
                report = PhaseGateBuilder(clock=lambda: now).build(draft)
                self.assertEqual(report["verdict"], "PASS")
                PhaseGateVerifier(authority_reader=reader).verify(report)

                forged_draft = dict(draft)
                forged_snapshot = snapshot.to_report_dict()
                forged_snapshot["active_grant_ids"] = ["grant-forged"]
                forged_draft["authority_snapshot"] = forged_snapshot
                forged_report = PhaseGateBuilder(clock=lambda: now).build(
                    forged_draft
                )
                with self.assertRaises(GateAuthorityMismatchError):
                    PhaseGateVerifier(authority_reader=reader).verify(
                        forged_report
                    )


if __name__ == "__main__":
    unittest.main()
