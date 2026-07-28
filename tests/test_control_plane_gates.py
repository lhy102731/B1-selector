from __future__ import annotations

import hashlib
import json
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

    def test_gate_rejects_unhashable_phase_as_validation_error(self) -> None:
        report = PhaseGateBuilder().build(self._passing_draft())
        report["phase"] = []

        with self.assertRaises(GateValidationError):
            validate_gate_report(report)

    def test_gate_rejects_unhashable_nested_enums_as_validation_errors(self) -> None:
        for field_name in (
            "task_reports",
            "scheduler_inventory",
            "test_receipts",
        ):
            with self.subTest(field_name=field_name):
                draft = self._passing_draft()
                if field_name == "task_reports":
                    task_reports = [dict(draft["task_reports"][0])]
                    task_reports[0]["outcome"] = []
                    draft[field_name] = task_reports
                elif field_name == "scheduler_inventory":
                    scheduler = dict(draft[field_name])
                    scheduler["status"] = {}
                    draft[field_name] = scheduler
                else:
                    receipts = [dict(draft["test_receipts"][0])]
                    receipts[0]["result"] = []
                    draft[field_name] = receipts

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

    def test_builder_rejects_oversized_serialized_gate_report(self) -> None:
        draft = self._passing_draft()
        draft["unresolved_risks"] = [
            f"risk-{index:06d}-" + ("x" * 120)
            for index in range(2_500)
        ]

        with self.assertRaisesRegex(
            GateBuildError,
            "gate report exceeds its byte limit",
        ):
            PhaseGateBuilder().build(draft)

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
            artifact_paths: dict[str, Path] = {}
            identity_dict = {
                "plan_hash": identity.plan_hash,
                "scope_hash": identity.scope_hash,
                "instruction_policy_hash": identity.instruction_policy_hash,
            }
            baseline_member: dict[str, object] = {
                "attempt_id": "p0r2-attempt-001",
                "git_head": "0" * 40,
                "branch": "codex/test-gate",
                "tracked_user_status_sha256": "0" * 64,
                "tracked_user_status_line_count": 0,
                "protected_tracked_changes": [],
                "file_state_count": 0,
                "file_states": {},
                "data_scan_policy": "Bounded test fixture; data is excluded.",
                "large_data_scanned": False,
                "production_or_research_task_started": False,
            }
            baseline_payload: dict[str, object] = {
                "schema_version": "control_plane.implementation_baseline.v2",
                "plan_version": "V3.4.2-P0R2",
                "phase": "P0",
                "baseline_payload_hash_algorithm": (
                    "sha256(canonical UTF-8 JSON of the baseline member; "
                    "sorted object keys; semantic array order preserved; "
                    "compact separators)"
                ),
                "baseline_payload_sha256": hashlib.sha256(
                    canonical_json(baseline_member).encode("utf-8")
                ).hexdigest(),
                "baseline": baseline_member,
            }
            baseline_ref = (
                "research_state/control_plane/p0r2/"
                "implementation_baseline.json"
            )
            baseline_path = root / baseline_ref
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            baseline_bytes = canonical_json(baseline_payload).encode("utf-8")
            baseline_path.write_bytes(baseline_bytes)
            baseline_sha256 = hashlib.sha256(baseline_bytes).hexdigest()
            artifact_paths["implementation_baseline"] = baseline_path
            task_spec = dict(task_spec)
            task_spec["baseline_ref"] = baseline_ref
            task_spec["baseline_sha256"] = baseline_sha256

            seam_specs = (
                (
                    "research_automation/autonomous_runner.py",
                    "callable:research_automation.autonomous_runner:"
                    "AutonomousRunnerV1.run",
                    "AutonomousRunnerV1.run",
                    [
                        "READ",
                        "WRITE_STAGING",
                        "RUN_RESEARCH",
                        "WRITE_KBASE",
                        "GIT_MUTATION",
                    ],
                ),
                (
                    "research_automation/discovery_execution_bridge.py",
                    "callable:research_automation.discovery_execution_bridge:"
                    "execute_plan",
                    "execute_plan",
                    ["WRITE_STAGING", "RUN_RESEARCH"],
                ),
                (
                    "research_automation/kbase_ag2_full_cycle.py",
                    "callable:research_automation.kbase_ag2_full_cycle:"
                    "run_kbase_ag2_full_cycle",
                    "run_kbase_ag2_full_cycle",
                    [
                        "READ",
                        "WRITE_STAGING",
                        "RUN_RESEARCH",
                        "GIT_MUTATION",
                    ],
                ),
            )
            freeze_files: list[dict[str, object]] = []
            seam_digests: dict[str, str] = {}
            gitattributes_path = root / ".gitattributes"
            gitattributes_bytes = b"*.py text eol=lf\n"
            gitattributes_path.write_bytes(gitattributes_bytes)
            gitattributes_digest = hashlib.sha256(
                gitattributes_bytes
            ).hexdigest()
            freeze_files.append(
                {
                    "path": ".gitattributes",
                    "sha256": gitattributes_digest,
                    "bytes": len(gitattributes_bytes),
                }
            )
            for path_text, _, _, _ in seam_specs:
                source_path = root.joinpath(*path_text.split("/"))
                source_path.parent.mkdir(parents=True, exist_ok=True)
                source_bytes = f"# fixture {path_text}\n".encode("utf-8")
                source_path.write_bytes(source_bytes)
                digest = hashlib.sha256(source_bytes).hexdigest()
                seam_digests[path_text] = digest
                freeze_files.append(
                    {
                        "path": path_text,
                        "sha256": digest,
                        "bytes": len(source_bytes),
                    }
                )
            action_path = root / "run_select.bat"
            action_bytes = b"# gate fixture run_select.bat\n"
            action_path.write_bytes(action_bytes)
            action_digest = hashlib.sha256(action_bytes).hexdigest()
            freeze_files.append(
                {
                    "path": "run_select.bat",
                    "sha256": action_digest,
                    "bytes": len(action_bytes),
                }
            )
            freeze_files.sort(key=lambda item: str(item["path"]))
            freeze_payload: dict[str, object] = {
                "schema_version": "control_plane.code_freeze_manifest.v1",
                "plan_version": "V3.4.2-P0R2",
                "phase": "P0",
                "attempt_id": "p0r2-attempt-001",
                "identity_binding": identity_dict,
                "files": freeze_files,
                "file_count": len(freeze_files),
            }
            freeze_payload["freeze_payload_sha256"] = hashlib.sha256(
                canonical_json(freeze_payload).encode("utf-8")
            ).hexdigest()
            freeze_ref = (
                "research_state/control_plane/p0r2/evidence/"
                "code_freeze_manifest.json"
            )
            freeze_path = root / freeze_ref
            freeze_path.parent.mkdir(parents=True, exist_ok=True)
            freeze_bytes = canonical_json(freeze_payload).encode("utf-8")
            freeze_path.write_bytes(freeze_bytes)
            artifact_paths["code_freeze_manifest"] = freeze_path

            scheduler_metadata = {
                "acl_summary": "owner=BUILTIN\\Administrators;sddl=O:BA",
                "action": "D:/workspace/run_select.bat",
                "principal": "Administrator|Interactive|Limited",
                "state": "Ready",
                "trigger": (
                    "MSFT_TaskDailyTrigger|start=2026-03-16T20:00:00|"
                    "days_interval=1|enabled=true"
                ),
            }
            inventory_entries: list[dict[str, object]] = [
                {
                    "entry_id": "file:.gitattributes",
                    "path": ".gitattributes",
                    "kind": "repository_policy",
                    "callable_name": "<byte-identity-policy>",
                    "actor_type": "human",
                    "content_sha256": gitattributes_digest,
                    "disposition": "ADMIN_ONLY",
                    "trust_state": "control_plane_policy",
                    "declared_side_effects": [],
                    "declared_phase": None,
                    "resource_roots": [],
                    "external_metadata": {},
                    "source": "filesystem_inventory",
                },
                {
                    "entry_id": "external:scheduler:/A\u80a1\u9009\u80a1",
                    "path": "/A\u80a1\u9009\u80a1",
                    "kind": "external_scheduler",
                    "callable_name": "D:/workspace/run_select.bat",
                    "actor_type": "scheduler",
                    "content_sha256": "d" * 64,
                    "disposition": "PRODUCTION_DAILY",
                    "trust_state": "production_daily",
                    "declared_side_effects": [],
                    "declared_phase": None,
                    "resource_roots": [],
                    "external_metadata": scheduler_metadata,
                    "source": "external_scheduler_inventory",
                },
                {
                    "entry_id": "file:run_select.bat",
                    "path": "run_select.bat",
                    "kind": "batch",
                    "callable_name": "<batch>",
                    "actor_type": "scheduler",
                    "content_sha256": action_digest,
                    "disposition": "PRODUCTION_DAILY",
                    "trust_state": "production_daily",
                    "declared_side_effects": [],
                    "declared_phase": None,
                    "resource_roots": [],
                    "external_metadata": {},
                    "source": "filesystem_inventory",
                },
            ]
            for path_text, entry_id, callable_name, effects in seam_specs:
                inventory_entries.append(
                    {
                        "entry_id": entry_id,
                        "path": path_text,
                        "kind": "python_callable",
                        "callable_name": callable_name,
                        "actor_type": "legacy_runner",
                        "content_sha256": seam_digests[path_text],
                        "disposition": "LEGACY_UNAUDITED",
                        "trust_state": "legacy_unaudited",
                        "declared_side_effects": effects,
                        "declared_phase": None,
                        "resource_roots": [],
                        "external_metadata": {},
                        "source": "required_import_seam",
                    }
                )
            inventory_entries.sort(
                key=lambda item: (
                    str(item["kind"]),
                    str(item["path"]),
                    str(item["entry_id"]),
                )
            )
            inventory_payload: dict[str, object] = {
                "schema_version": "control_plane.entry_inventory.v2",
                "plan_version": "V3.4.2-P0R2",
                "phase": "P0",
                "attempt_id": "p0r2-attempt-001",
                "identity_binding": identity_dict,
                "freeze_payload_sha256": freeze_payload[
                    "freeze_payload_sha256"
                ],
                "entries": inventory_entries,
                "entry_count": len(inventory_entries),
            }
            inventory_payload["inventory_payload_sha256"] = hashlib.sha256(
                canonical_json(inventory_payload).encode("utf-8")
            ).hexdigest()
            inventory_ref = (
                "research_state/control_plane/p0r2/evidence/"
                "final_inventory.json"
            )
            inventory_path = root / inventory_ref
            inventory_bytes = canonical_json(inventory_payload).encode("utf-8")
            inventory_path.write_bytes(inventory_bytes)
            artifact_paths["final_inventory"] = inventory_path

            policy_payload: dict[str, object] = {
                "schema_version": "control_plane.entry_policy.v1",
                "plan_version": "V3.4.2-P0R2",
                "phase": "P0",
                "attempt_id": "p0r2-attempt-001",
                "identity_binding": identity_dict,
                "review_state": "APPROVED",
                "reviewer_id": "independent-test-reviewer",
                "review_receipt_sha256": "e" * 64,
                "inventory_payload_sha256": inventory_payload[
                    "inventory_payload_sha256"
                ],
                "entries": inventory_entries,
                "entry_count": len(inventory_entries),
            }
            policy_payload["policy_payload_sha256"] = hashlib.sha256(
                canonical_json(policy_payload).encode("utf-8")
            ).hexdigest()
            policy_bytes = canonical_json(policy_payload).encode("utf-8")
            policy_sha256 = hashlib.sha256(policy_bytes).hexdigest()
            policy_ref = (
                "research_state/control_plane/policies/"
                f"{policy_sha256}.json"
            )
            policy_path = root / policy_ref
            policy_path.parent.mkdir(parents=True, exist_ok=True)
            policy_path.write_bytes(policy_bytes)
            artifact_paths["reviewed_entry_policy"] = policy_path

            scheduler_payload = {
                "schema_version": (
                    "control_plane.external_scheduler_inventory.v1"
                ),
                "phase": "P0",
                "observed_at": "2026-07-26T01:18:14+08:00",
                "collection_mode": "READ_ONLY",
                "task_path": "\\A\u80a1\u9009\u80a1",
                "task_state": "Ready",
                "operational_classification": "PRODUCTION_DAILY",
                "task_xml": {
                    "path": "C:/Windows/System32/Tasks/A\u80a1\u9009\u80a1",
                    "sha256": "d" * 64,
                },
                "action": {
                    "execute": "D:/workspace/run_select.bat",
                    "arguments": None,
                    "working_directory": None,
                    "content_sha256": action_digest,
                },
                "principal": {
                    "user_id": "Administrator",
                    "logon_type": "Interactive",
                    "run_level": "Limited",
                },
                "trigger": {
                    "type": "MSFT_TaskDailyTrigger",
                    "start_boundary": "2026-03-16T20:00:00",
                    "enabled": True,
                    "days_interval": 1,
                },
                "acl": {
                    "owner": "BUILTIN\\Administrators",
                    "sddl": "O:BA",
                },
                "altered_by_p0": False,
                "unresolved_risk": "Production scheduler evidence is explicit.",
            }
            scheduler_ref = (
                "research_state/control_plane/p0r2/evidence/"
                "scheduler_inventory.json"
            )
            scheduler_path = root / scheduler_ref
            scheduler_bytes = canonical_json(scheduler_payload).encode("utf-8")
            scheduler_path.write_bytes(scheduler_bytes)
            artifact_paths["scheduler_inventory"] = scheduler_path

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
                task_report_ref = (
                    "research_state/control_plane/p0r2/reports/gate.json"
                )
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
                artifact_refs = {
                    "implementation_baseline": baseline_ref,
                    "code_freeze_manifest": freeze_ref,
                    "final_inventory": inventory_ref,
                    "reviewed_entry_policy": policy_ref,
                    "scheduler_inventory": scheduler_ref,
                }
                for field_name, artifact_ref in artifact_refs.items():
                    artifact_bytes = artifact_paths[field_name].read_bytes()
                    draft[field_name] = {
                        "ref": artifact_ref,
                        "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
                    }
                scheduler_record = draft["scheduler_inventory"]
                self.assertIsInstance(scheduler_record, dict)
                scheduler_record["status"] = "VERIFIED"
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

    def _gate_build_cli_args(
        self,
        fixture: _TrustedGateFixture,
        output: str | Path,
    ) -> list[str]:
        def reference(field_name: str) -> str:
            artifact = fixture.draft[field_name]
            self.assertIsInstance(artifact, dict)
            return str(artifact["ref"])

        task_reports = fixture.draft["task_reports"]
        self.assertIsInstance(task_reports, list)
        task_report = task_reports[0]
        self.assertIsInstance(task_report, dict)
        return [
            "gate",
            "build",
            "--phase",
            "P0",
            "--attempt-id",
            "p0r2-attempt-001",
            "--baseline",
            reference("implementation_baseline"),
            "--freeze-manifest",
            reference("code_freeze_manifest"),
            "--inventory",
            reference("final_inventory"),
            "--entry-policy",
            reference("reviewed_entry_policy"),
            "--scheduler-inventory",
            reference("scheduler_inventory"),
            "--task-report-id",
            str(task_report["report_ref"]),
            "--output",
            str(output),
        ]

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

    def test_verifier_rejects_an_entry_added_after_final_inventory(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            added = fixture.root / "research_automation" / "late_entry.py"
            added.write_text("raise RuntimeError('late entry')\n", encoding="utf-8")

            with self.assertRaisesRegex(
                GateEvidenceError,
                "executable surface",
            ):
                fixture.verifier.verify(fixture.report)

    def test_verifier_rejects_semantically_invalid_gate_artifact(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            invalid_bytes = b'{"artifact_type":"final_inventory"}'
            inventory_path = fixture.artifact_paths["final_inventory"]
            inventory_path.write_bytes(invalid_bytes)
            draft = dict(fixture.draft)
            inventory_ref = dict(fixture.draft["final_inventory"])
            inventory_ref["sha256"] = hashlib.sha256(invalid_bytes).hexdigest()
            draft["final_inventory"] = inventory_ref
            report = PhaseGateBuilder().build(draft)

            with self.assertRaises(GateEvidenceError):
                fixture.verifier.verify(report)

    def test_verifier_derives_scheduler_status_from_artifact(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            scheduler_path = fixture.artifact_paths["scheduler_inventory"]
            scheduler_payload = json.loads(
                scheduler_path.read_text(encoding="utf-8")
            )
            scheduler_payload["collection_mode"] = "UNAVAILABLE"
            scheduler_bytes = canonical_json(scheduler_payload).encode("utf-8")
            scheduler_path.write_bytes(scheduler_bytes)
            draft = dict(fixture.draft)
            scheduler_ref = dict(fixture.draft["scheduler_inventory"])
            scheduler_ref["sha256"] = hashlib.sha256(
                scheduler_bytes
            ).hexdigest()
            scheduler_ref["status"] = "VERIFIED"
            draft["scheduler_inventory"] = scheduler_ref
            report = PhaseGateBuilder().build(draft)

            with self.assertRaises(GateEvidenceError):
                fixture.verifier.verify(report)

    def test_verifier_rejects_source_tree_policy_masquerade(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            policy_bytes = fixture.artifact_paths[
                "reviewed_entry_policy"
            ].read_bytes()
            source_policy = (
                fixture.root
                / "research_automation"
                / "control_plane"
                / "entry_policy.json"
            )
            source_policy.parent.mkdir(parents=True, exist_ok=True)
            source_policy.write_bytes(policy_bytes)
            draft = dict(fixture.draft)
            draft["reviewed_entry_policy"] = {
                "ref": "research_automation/control_plane/entry_policy.json",
                "sha256": hashlib.sha256(policy_bytes).hexdigest(),
            }
            report = PhaseGateBuilder().build(draft)

            with self.assertRaises(GateEvidenceError):
                fixture.verifier.verify(report)

    def test_task_report_must_bind_the_same_implementation_baseline(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            baseline_bytes = fixture.artifact_paths[
                "implementation_baseline"
            ].read_bytes() + b"\n"
            copied_ref = (
                "research_state/control_plane/p0r2/"
                "implementation_baseline_copy.json"
            )
            copied_path = fixture.root / copied_ref
            copied_path.write_bytes(baseline_bytes)
            draft = dict(fixture.draft)
            draft["implementation_baseline"] = {
                "ref": copied_ref,
                "sha256": hashlib.sha256(baseline_bytes).hexdigest(),
            }
            report = PhaseGateBuilder().build(draft)

            with self.assertRaises(GateAuthorityMismatchError):
                fixture.verifier.verify(report)

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
            args = self._gate_build_cli_args(fixture, output_path)
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
            fixture.verifier.verify(built)
            self.assertIn('"status":"BUILT"', stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")

    def test_cli_allows_large_gate_artifacts_but_rejects_oversized_artifacts(
        self,
    ) -> None:
        for size, expected_exit_code in (
            (256 * 1024 + 1, 0),
            (4 * 1024 * 1024 + 1, 3),
        ):
            with self.subTest(size=size):
                with self._trusted_gate_fixture() as fixture:
                    artifact_path = fixture.artifact_paths[
                        "scheduler_inventory"
                    ]
                    scheduler_payload = json.loads(
                        artifact_path.read_text(encoding="utf-8")
                    )
                    scheduler_payload["unresolved_risk"] = "x" * size
                    artifact_path.write_bytes(
                        canonical_json(scheduler_payload).encode("utf-8")
                    )
                    output_path = fixture.root / f"built-{size}.json"
                    stdout = StringIO()
                    stderr = StringIO()

                    exit_code = gate_cli_main(
                        self._gate_build_cli_args(fixture, output_path),
                        stdout=stdout,
                        stderr=stderr,
                        authority_reader=fixture.reader,
                        repository_root=fixture.root,
                    )

                    self.assertEqual(exit_code, expected_exit_code)
                    if expected_exit_code == 0:
                        self.assertTrue(output_path.is_file())
                    else:
                        self.assertFalse(output_path.exists())
                        self.assertIn("GateEvidenceError", stderr.getvalue())

    def test_cli_rejects_gate_output_collisions_and_source_paths(self) -> None:
        cases = (
            "reports/gate.json",
            "research_automation/control_plane/new-report.json",
            "reports/output.json:stream",
        )
        for output_text in cases:
            with self.subTest(output=output_text):
                with self._trusted_gate_fixture() as fixture:
                    stdout = StringIO()
                    stderr = StringIO()
                    exit_code = gate_cli_main(
                        self._gate_build_cli_args(fixture, output_text),
                        stdout=stdout,
                        stderr=stderr,
                        authority_reader=fixture.reader,
                        repository_root=fixture.root,
                    )

                    self.assertEqual(exit_code, 3)
                    self.assertEqual(stdout.getvalue(), "")
                    self.assertIn("GateEvidenceError", stderr.getvalue())

    def test_cli_does_not_overwrite_an_existing_gate_output(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            output_path = fixture.root / "existing-report.json"
            output_path.write_bytes(b"immutable")
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                self._gate_build_cli_args(fixture, output_path),
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 3)
            self.assertEqual(output_path.read_bytes(), b"immutable")
            self.assertIn("GateEvidenceError", stderr.getvalue())

    def test_cli_rejects_build_for_a_closed_gate_attempt(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            fixture.closer.close(fixture.report)
            output_path = fixture.root / "closed-attempt-report.json"
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                self._gate_build_cli_args(fixture, output_path),
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 4)
            self.assertFalse(output_path.exists())
            self.assertIn("GateAuthorityMismatchError", stderr.getvalue())

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

    def test_cli_maps_immutable_gate_conflict_to_exit_code_four(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            fixture.closer.close(fixture.report)
            different_report = PhaseGateBuilder(
                clock=lambda: datetime(
                    2026,
                    7,
                    26,
                    8,
                    16,
                    tzinfo=timezone.utc,
                )
            ).build(fixture.draft)
            report_path = fixture.root / "different-gate-report.json"
            report_path.write_text(
                canonical_json(different_report),
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

            self.assertEqual(exit_code, 4)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("PhaseGateClosureConflictError", stderr.getvalue())

    def test_cli_bounds_capability_stdin(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            class TrackingStdin(StringIO):
                requested_size: int | None = None

                def read(self, size: int = -1) -> str:
                    self.requested_size = size
                    return super().read(size)

            report_path = fixture.root / "gate-report.json"
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )
            stdout = StringIO()
            stderr = StringIO()
            capability_stdin = TrackingStdin("x" * 10_000)

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
                stdin=capability_stdin,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 4)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIsNotNone(capability_stdin.requested_size)
            self.assertGreaterEqual(capability_stdin.requested_size, 0)
            self.assertLessEqual(capability_stdin.requested_size, 4097)


if __name__ == "__main__":
    unittest.main()
