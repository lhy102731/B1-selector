from __future__ import annotations

import hashlib
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.cli import main as gate_cli_main
from research_automation.control_plane.contracts import (
    Actor,
    Phase,
    SideEffect,
    canonical_json,
)
from research_automation.control_plane.gates import (
    GateAuthorityMismatchError,
    GateBuildError,
    GateEvidenceError,
    GateValidationError,
    PhaseGateBuilder,
    PhaseGateCloser,
    PhaseGateVerifier,
    parse_gate_report_v1_bytes,
    validate_gate_report,
)
from research_automation.control_plane.stores import (
    AuthorityIdentity,
    AuthorityReader,
    PhaseGateClosureConflictError,
)
from research_automation.control_plane.task_reports import (
    build_task_report_v2,
    parse_task_report_v2_bytes,
)


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"


@dataclass(frozen=True, slots=True)
class _TrustedGateFixture:
    root: Path
    snapshot: dict[str, object]
    active_grant_id: str
    ticket_id: str
    draft: dict[str, object]
    report: dict[str, object]
    reader: AuthorityReader
    verifier: PhaseGateVerifier
    closer: PhaseGateCloser
    task_report_path: Path
    task_report_bytes: bytes
    artifact_paths: dict[str, Path]


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

    def test_builder_translates_malformed_nested_drafts(self) -> None:
        draft = self._passing_draft()
        draft["task_reports"] = [{}]

        with self.assertRaises(GateBuildError):
            PhaseGateBuilder().build(draft)

    def test_gate_report_byte_parser_rejects_duplicate_keys(self) -> None:
        raw = b'{"schema_version":"first","schema_version":"second"}'

        with self.assertRaisesRegex(
            GateValidationError,
            "duplicate JSON key: schema_version",
        ):
            parse_gate_report_v1_bytes(raw)

    def test_gate_artifact_refs_reject_windows_path_ambiguity(self) -> None:
        for invalid_ref in (
            "C:/outside.json",
            "C:drive-relative.json",
            "artifacts/policy.json:stream",
            "artifacts/*.json",
            "artifacts/policy?.json",
            "artifacts/policy\x1f.json",
        ):
            with self.subTest(invalid_ref=repr(invalid_ref)):
                draft = self._passing_draft()
                artifact = dict(draft["implementation_baseline"])
                artifact["ref"] = invalid_ref
                draft["implementation_baseline"] = artifact

                with self.assertRaises(GateBuildError):
                    PhaseGateBuilder().build(draft)

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

    def test_gate_requires_exactly_one_active_grant(self) -> None:
        cases = (
            ([], "ACTIVE_GRANT_COUNT:0"),
            (["grant-001", "grant-002"], "ACTIVE_GRANT_COUNT:2"),
        )
        for active_grant_ids, expected_reason in cases:
            with self.subTest(active_grant_ids=active_grant_ids):
                draft = self._passing_draft()
                authority_snapshot = dict(draft["authority_snapshot"])
                authority_snapshot["active_grant_ids"] = active_grant_ids
                draft["authority_snapshot"] = authority_snapshot

                report = PhaseGateBuilder().build(draft)

                self.assertEqual(report["verdict"], "FAIL")
                self.assertIn(expected_reason, report["reason_codes"])

    def test_gate_closes_task_reports_over_known_authority_tickets(self) -> None:
        cases = (
            ([], ["ticket-001"], "MISSING_TASK_REPORT:ticket-001"),
            (
                [
                    {
                        "report_ref": "reports/unknown.json",
                        "report_sha256": "1" * 64,
                        "ticket_id": "ticket-unknown",
                        "outcome": "PASS",
                    }
                ],
                [],
                "UNKNOWN_TASK_REPORT:ticket-unknown",
            ),
        )
        for task_reports, succeeded_ticket_ids, expected_reason in cases:
            with self.subTest(expected_reason=expected_reason):
                draft = self._passing_draft()
                draft["task_reports"] = task_reports
                authority_snapshot = dict(draft["authority_snapshot"])
                authority_snapshot["succeeded_ticket_ids"] = (
                    succeeded_ticket_ids
                )
                draft["authority_snapshot"] = authority_snapshot

                report = PhaseGateBuilder().build(draft)

                self.assertEqual(report["verdict"], "FAIL")
                self.assertIn(expected_reason, report["reason_codes"])

    @contextmanager
    def _trusted_gate_fixture(self) -> Iterator[_TrustedGateFixture]:
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
                finished = authority._finish_task(
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
                snapshot_dict = snapshot.to_report_dict()
                draft = self._passing_draft()
                task_report = build_task_report_v2(
                    {
                        "plan_version": "V3.4.2-P0R2",
                        "phase": "P0",
                        "task_id": task_spec["task_id"],
                        "attempt_id": "p0r2-attempt-001",
                        "authorization_ref": envelope.authorization_ref,
                        "ticket_id": ticket.ticket_id,
                        "identity_binding": {
                            "plan_hash": identity.plan_hash,
                            "scope_hash": identity.scope_hash,
                            "instruction_policy_hash": (
                                identity.instruction_policy_hash
                            ),
                        },
                        "objective": task_spec["objective"],
                        "dependencies": task_spec["dependencies"],
                        "idempotency_key": task_spec["idempotency_key"],
                        "task_spec_ref": task_spec["task_spec_ref"],
                        "task_spec_sha256": task_spec["task_spec_sha256"],
                        "requirements": task_spec["requirements"],
                        "allowed_files": task_spec["allowed_files"],
                        "forbidden_files": task_spec["forbidden_files"],
                        "baseline_ref": task_spec["baseline_ref"],
                        "baseline_sha256": task_spec["baseline_sha256"],
                        "input_evidence_refs": task_spec[
                            "input_evidence_refs"
                        ],
                        "test_receipts": [],
                        "review_receipts": [],
                        "review_findings": [],
                        "changed_files": [],
                        "external_invocations": [],
                        "side_effect_summary": {
                            "observed": [],
                            "unauthorized": [],
                        },
                        "ticket_state": "SUCCEEDED",
                        "started_at": finished.started_at.isoformat(),
                        "completed_at": finished.completed_at.isoformat(),
                    }
                )
                task_report_bytes = canonical_json(task_report).encode("utf-8")
                task_report_ref = "reports/gate.json"
                task_report_path = root / task_report_ref
                task_report_path.parent.mkdir(parents=True)
                task_report_path.write_bytes(task_report_bytes)
                draft["task_reports"] = [
                    {
                        "report_ref": task_report_ref,
                        "report_sha256": hashlib.sha256(
                            task_report_bytes
                        ).hexdigest(),
                        "ticket_id": ticket.ticket_id,
                        "outcome": "PASS",
                    }
                ]
                artifact_paths: dict[str, Path] = {}
                for field_name in (
                    "implementation_baseline",
                    "code_freeze_manifest",
                    "final_inventory",
                    "reviewed_entry_policy",
                    "scheduler_inventory",
                ):
                    artifact_ref = f"artifacts/{field_name}.json"
                    artifact_path = root / artifact_ref
                    artifact_path.parent.mkdir(parents=True, exist_ok=True)
                    artifact_bytes = canonical_json(
                        {"artifact_type": field_name}
                    ).encode("utf-8")
                    artifact_path.write_bytes(artifact_bytes)
                    artifact_paths[field_name] = artifact_path
                    draft[field_name] = {
                        "ref": artifact_ref,
                        "sha256": hashlib.sha256(
                            artifact_bytes
                        ).hexdigest(),
                    }
                scheduler_inventory = draft["scheduler_inventory"]
                self.assertIsInstance(scheduler_inventory, dict)
                scheduler_inventory["status"] = "VERIFIED"
                draft["authority_snapshot"] = snapshot_dict
                report = PhaseGateBuilder(clock=lambda: now).build(draft)
                verifier = PhaseGateVerifier(
                    authority_reader=reader,
                    repository_root=root,
                )
                closer = PhaseGateCloser(
                    root_secret=ROOT_SECRET,
                    authority_reader=reader,
                    repository_root=root,
                    clock=lambda: now,
                )
                yield _TrustedGateFixture(
                    root=root,
                    snapshot=snapshot_dict,
                    active_grant_id=grant.grant_id,
                    ticket_id=ticket.ticket_id,
                    draft=draft,
                    report=report,
                    reader=reader,
                    verifier=verifier,
                    closer=closer,
                    task_report_path=task_report_path,
                    task_report_bytes=task_report_bytes,
                    artifact_paths=artifact_paths,
                )

    def test_verifier_requeries_the_authority_snapshot(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            self.assertEqual(
                fixture.snapshot,
                {
                    "active_grant_ids": [fixture.active_grant_id],
                    "open_ticket_ids": [],
                    "succeeded_ticket_ids": [fixture.ticket_id],
                    "failed_ticket_ids": [],
                    "in_doubt_ticket_ids": [],
                    "pending_outbox_count": 0,
                },
            )
            self.assertEqual(fixture.report["verdict"], "PASS")
            fixture.verifier.verify(fixture.report)

            forged_draft = dict(fixture.draft)
            forged_snapshot = dict(fixture.snapshot)
            forged_snapshot["active_grant_ids"] = ["grant-forged"]
            forged_draft["authority_snapshot"] = forged_snapshot
            forged_report = PhaseGateBuilder().build(forged_draft)
            with self.assertRaises(GateAuthorityMismatchError):
                fixture.verifier.verify(forged_report)

    def test_verifier_accepts_a_canonical_gate_report_bytes(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            raw = canonical_json(fixture.report).encode("utf-8")

            fixture.verifier.verify_bytes(raw)

    def test_verifier_checks_task_report_file_hash(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            fixture.task_report_path.write_bytes(
                fixture.task_report_bytes + b"\n"
            )

            with self.assertRaises(GateEvidenceError):
                fixture.verifier.verify(fixture.report)

    def test_verifier_parses_task_report_v2_bytes(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            invalid_bytes = b"{not-json"
            fixture.task_report_path.write_bytes(invalid_bytes)
            invalid_draft = dict(fixture.draft)
            task_report_ref = dict(fixture.draft["task_reports"][0])
            task_report_ref["report_sha256"] = hashlib.sha256(
                invalid_bytes
            ).hexdigest()
            invalid_draft["task_reports"] = [task_report_ref]
            invalid_report = PhaseGateBuilder().build(invalid_draft)

            with self.assertRaises(GateEvidenceError):
                fixture.verifier.verify(invalid_report)

    def test_verifier_binds_task_report_reference_to_its_contents(self) -> None:
        cases = (
            ("ticket_id", "ticket-forged"),
            ("outcome", "FAIL"),
        )
        for field_name, forged_value in cases:
            with self.subTest(field_name=field_name):
                with self._trusted_gate_fixture() as fixture:
                    forged_draft = dict(fixture.draft)
                    task_report_ref = dict(fixture.draft["task_reports"][0])
                    task_report_ref[field_name] = forged_value
                    forged_draft["task_reports"] = [task_report_ref]
                    forged_report = PhaseGateBuilder().build(forged_draft)

                    with self.assertRaises(GateEvidenceError):
                        fixture.verifier.verify(forged_report)

    def test_verifier_cross_checks_task_report_against_authority(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            forged_draft = parse_task_report_v2_bytes(
                fixture.task_report_bytes
            )
            for computed_field in (
                "schema_version",
                "unexpected_changes",
                "outcome",
                "reason_codes",
                "report_payload_sha256",
            ):
                forged_draft.pop(computed_field)
            forged_draft["objective"] = "Forged gate task objective."
            forged_task_report = build_task_report_v2(forged_draft)
            forged_bytes = canonical_json(forged_task_report).encode("utf-8")
            fixture.task_report_path.write_bytes(forged_bytes)

            gate_draft = dict(fixture.draft)
            task_report_ref = dict(fixture.draft["task_reports"][0])
            task_report_ref["report_sha256"] = hashlib.sha256(
                forged_bytes
            ).hexdigest()
            gate_draft["task_reports"] = [task_report_ref]
            gate_report = PhaseGateBuilder().build(gate_draft)

            with self.assertRaises(GateAuthorityMismatchError):
                fixture.verifier.verify(gate_report)

    def test_verifier_binds_gate_identity_to_task_reports(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            forged_draft = dict(fixture.draft)
            forged_identity = dict(fixture.draft["identity_binding"])
            forged_identity["plan_hash"] = "9" * 64
            forged_draft["identity_binding"] = forged_identity
            forged_report = PhaseGateBuilder().build(forged_draft)

            with self.assertRaises(GateAuthorityMismatchError):
                fixture.verifier.verify(forged_report)

    def test_verifier_checks_every_gate_artifact_file_hash(self) -> None:
        for field_name in (
            "implementation_baseline",
            "code_freeze_manifest",
            "final_inventory",
            "reviewed_entry_policy",
            "scheduler_inventory",
        ):
            with self.subTest(field_name=field_name):
                with self._trusted_gate_fixture() as fixture:
                    artifact_path = fixture.artifact_paths[field_name]
                    artifact_path.write_bytes(
                        artifact_path.read_bytes() + b"\n"
                    )

                    with self.assertRaises(GateEvidenceError):
                        fixture.verifier.verify(fixture.report)

    def test_closer_atomically_records_a_passing_gate(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            closure = fixture.closer.close_bytes(
                canonical_json(fixture.report).encode("utf-8")
            )

            self.assertEqual(closure.phase, Phase.P0)
            self.assertEqual(closure.attempt_id, "p0r2-attempt-001")
            self.assertEqual(
                closure.gate_report_sha256,
                fixture.report["gate_report_sha256"],
            )
            self.assertEqual(closure.verdict, "PASS")
            self.assertEqual(
                fixture.reader.phase_gate_closure(
                    Phase.P0,
                    "p0r2-attempt-001",
                ),
                closure,
            )
            after_close = fixture.reader.phase_gate_snapshot(
                Phase.P0,
                "p0r2-attempt-001",
            )
            self.assertEqual(after_close.active_grant_ids, ())
            self.assertEqual(after_close.pending_outbox_count, 1)

    def test_closer_replays_the_same_gate_idempotently(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            first = fixture.closer.close(fixture.report)

            replay = fixture.closer.close(fixture.report)

            self.assertEqual(replay, first)
            after_replay = fixture.reader.phase_gate_snapshot(
                Phase.P0,
                "p0r2-attempt-001",
            )
            self.assertEqual(after_replay.pending_outbox_count, 1)

    def test_closer_rejects_a_different_gate_for_closed_attempt(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            fixture.closer.close(fixture.report)
            different_report = PhaseGateBuilder(
                clock=lambda: datetime(2026, 7, 26, 8, 16, tzinfo=timezone.utc)
            ).build(fixture.draft)

            with self.assertRaises(PhaseGateClosureConflictError):
                fixture.closer.close(different_report)

    def test_closer_replay_rechecks_evidence_before_returning_closure(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            fixture.closer.close(fixture.report)
            fixture.task_report_path.write_bytes(
                fixture.task_report_bytes + b"\n"
            )

            with self.assertRaises(GateEvidenceError):
                fixture.closer.close(fixture.report)

    def test_cli_verifies_a_passing_gate_read_only(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            report_path = fixture.root / "gate-report.json"
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                [
                    "gate",
                    "verify",
                    "--phase",
                    "P0",
                    "--attempt-id",
                    "p0r2-attempt-001",
                    "--report",
                    str(report_path),
                    "--read-only",
                ],
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 0)
            self.assertIn('"verdict":"PASS"', stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")

    def test_cli_preflight_reports_a_ready_authority_snapshot(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                [
                    "gate",
                    "preflight",
                    "--phase",
                    "P0",
                    "--attempt-id",
                    "p0r2-attempt-001",
                ],
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 0)
            self.assertIn('"status":"READY"', stdout.getvalue())
            self.assertIn('"pending_outbox_count":0', stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")

    def test_cli_builds_a_gate_candidate_from_hashed_inputs(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            output_path = fixture.root / "built-gate-report.json"
            args = [
                "gate",
                "build",
                "--phase",
                "P0",
                "--attempt-id",
                "p0r2-attempt-001",
                "--freeze-manifest",
                "artifacts/code_freeze_manifest.json",
                "--inventory",
                "artifacts/final_inventory.json",
                "--entry-policy",
                "artifacts/reviewed_entry_policy.json",
                "--scheduler-inventory",
                "artifacts/scheduler_inventory.json",
                "--task-report-id",
                "reports/gate.json",
                "--output",
                str(output_path),
            ]
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                args,
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(output_path.is_file())
            built = parse_gate_report_v1_bytes(output_path.read_bytes())
            self.assertEqual(built["phase"], "P0")
            self.assertEqual(built["attempt_id"], "p0r2-attempt-001")
            self.assertEqual(built["task_reports"][0]["ticket_id"], fixture.ticket_id)
            self.assertIn('"status":"BUILT"', stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")

    def test_cli_reports_identity_mismatch_with_exit_code_four(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            report_path = fixture.root / "gate-report.json"
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                [
                    "gate",
                    "verify",
                    "--phase",
                    "P1",
                    "--attempt-id",
                    "p0r2-attempt-001",
                    "--report",
                    str(report_path),
                    "--read-only",
                ],
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 4)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("GateAuthorityMismatchError", stderr.getvalue())

    def test_cli_returns_exit_code_two_for_a_verified_computed_fail(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            fail_draft = dict(fixture.draft)
            fail_draft["unresolved_risks"] = ["known-risk"]
            fail_report = PhaseGateBuilder().build(fail_draft)
            report_path = fixture.root / "gate-fail-report.json"
            report_path.write_text(
                canonical_json(fail_report),
                encoding="utf-8",
            )
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                [
                    "gate",
                    "verify",
                    "--phase",
                    "P0",
                    "--attempt-id",
                    "p0r2-attempt-001",
                    "--report",
                    str(report_path),
                    "--read-only",
                ],
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 2)
            self.assertIn('"verdict":"FAIL"', stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")

    def test_cli_returns_exit_code_three_for_corrupt_gate_evidence(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            report_path = fixture.root / "corrupt-gate-report.json"
            report_path.write_bytes(b"not-json")
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                [
                    "gate",
                    "verify",
                    "--phase",
                    "P0",
                    "--attempt-id",
                    "p0r2-attempt-001",
                    "--report",
                    str(report_path),
                    "--read-only",
                ],
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 3)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("GateValidationError", stderr.getvalue())

    def test_cli_returns_exit_code_five_for_unavailable_authority_store(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            report_path = fixture.root / "gate-report.json"
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )
            (fixture.root / "authority.sqlite3").unlink()
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                [
                    "gate",
                    "verify",
                    "--phase",
                    "P0",
                    "--attempt-id",
                    "p0r2-attempt-001",
                    "--report",
                    str(report_path),
                    "--read-only",
                ],
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 5)
            self.assertEqual(stdout.getvalue(), "")

    def test_cli_closes_a_gate_with_capability_from_stdin(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            report_path = fixture.root / "gate-report.json"
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                [
                    "gate",
                    "close",
                    "--phase",
                    "P0",
                    "--attempt-id",
                    "p0r2-attempt-001",
                    "--report",
                    str(report_path),
                    "--capability-stdin",
                ],
                stdout=stdout,
                stderr=stderr,
                stdin=StringIO(ROOT_SECRET),
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 0)
            self.assertIn('"status":"CLOSED"', stdout.getvalue())
            self.assertIn(fixture.report["gate_report_sha256"], stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")

    def test_cli_maps_invalid_close_capability_to_exit_code_four(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            report_path = fixture.root / "gate-report.json"
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                [
                    "gate",
                    "close",
                    "--phase",
                    "P0",
                    "--attempt-id",
                    "p0r2-attempt-001",
                    "--report",
                    str(report_path),
                    "--capability-stdin",
                ],
                stdout=stdout,
                stderr=stderr,
                stdin=StringIO("wrong-authority-capability-0123456789"),
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 4)
            self.assertEqual(stdout.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
