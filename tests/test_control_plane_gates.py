from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
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
from research_automation.control_plane import cli as cli_module
from research_automation.control_plane.cli import main as gate_cli_main
from research_automation.control_plane.contracts import (
    Actor,
    Phase,
    SideEffect,
    canonical_json,
)
from research_automation.control_plane.artifact_semantics import (
    reviewed_policy_receipt_sha256,
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
    PhaseGateClosureError,
    PhaseGateClosureConflictError,
)
from research_automation.control_plane.task_reports import (
    build_task_report_v2,
    parse_task_report_v2_bytes,
)
from research_automation.control_plane.inventory import (
    build_code_freeze_manifest,
    build_final_entry_inventory,
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
                    "ticket_id": "ticket-001",
                    "receipt_id": "gate-tests",
                    "command": "python -m unittest tests.test_control_plane_gates",
                    "exit_code": 0,
                    "result": "PASS",
                }
            ],
            "authority_snapshot": {
                "active_entry_policy_sha256": "d" * 64,
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

    def test_gate_requires_the_active_policy_to_match_the_reviewed_artifact(
        self,
    ) -> None:
        cases = (
            (None, "MISSING_ACTIVE_ENTRY_POLICY"),
            ("e" * 64, "ACTIVE_ENTRY_POLICY_MISMATCH"),
        )
        for active_digest, expected_reason in cases:
            with self.subTest(active_digest=active_digest):
                draft = self._passing_draft()
                authority_snapshot = dict(draft["authority_snapshot"])
                authority_snapshot["active_entry_policy_sha256"] = active_digest
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
    def _trusted_gate_fixture(
        self,
        *,
        activate_policy: bool = True,
        active_policy_payload_sha256: str | None = None,
        git_source_identity: bool = True,
        evidence_refs: tuple[dict[str, object], ...] = (),
        evidence_files: tuple[tuple[str, bytes], ...] = (),
        omit_evidence_commit: bool = False,
        task_requirements: dict[str, object] | None = None,
    ) -> Iterator[_TrustedGateFixture]:
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
                "required_test_receipt_ids": ["gate-tests"],
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
            baseline_sha256 = str(
                baseline_payload["baseline_payload_sha256"]
            )
            artifact_paths["implementation_baseline"] = baseline_path
            task_spec = dict(task_spec)
            task_spec["baseline_ref"] = baseline_ref
            task_spec["baseline_sha256"] = baseline_sha256
            if task_requirements is not None:
                task_spec["requirements"] = task_requirements
            if evidence_refs:
                task_spec["input_evidence_refs"] = list(evidence_refs)

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
            if git_source_identity:
                (root / "CHANGELOG.md").write_text(
                    "baseline\n",
                    encoding="utf-8",
                )
                eligibility_payload = {
                    "schema_version": (
                        "control_plane.legacy_quarantine_paths.v1"
                    ),
                    "paths": [],
                }
                source_policy_path = (
                    root
                    / "research_automation"
                    / "control_plane"
                    / "entry_policy.json"
                )
                source_policy_path.parent.mkdir(parents=True, exist_ok=True)
                source_policy_path.write_text(
                    canonical_json(
                        {
                            "entries": [],
                            "plan_hash": "1" * 64,
                            "policy_hash": "2" * 64,
                            "review_state": "APPROVED",
                            "schema_version": "control_plane.entry_policy.v1",
                            "scope_hash": "3" * 64,
                            "quarantine_eligible_paths": [],
                            "quarantine_eligible_paths_sha256": hashlib.sha256(
                                canonical_json(eligibility_payload).encode("utf-8")
                            ).hexdigest(),
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                subprocess.run(
                    ["git", "init", "--quiet"],
                    cwd=root,
                    check=True,
                    capture_output=True,
                )
                subprocess.run(
                    [
                        "git",
                        "add",
                        ".gitattributes",
                        "CHANGELOG.md",
                        "run_select.bat",
                        "research_automation",
                    ],
                    cwd=root,
                    check=True,
                    capture_output=True,
                )
                subprocess.run(
                    [
                        "git",
                        "-c",
                        "user.name=Control Plane Tests",
                        "-c",
                        "user.email=control-plane@example.invalid",
                        "commit",
                        "--quiet",
                        "-m",
                        "gate fixture",
                    ],
                    cwd=root,
                    check=True,
                    capture_output=True,
                )
                freeze_payload = build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P0R2",
                    phase="P0",
                    attempt_id="p0r2-attempt-001",
                    identity_binding=identity_dict,
                )
            else:
                freeze_payload = {
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
                "schema_version": (
                    "control_plane.entry_inventory.v3"
                    if git_source_identity
                    else "control_plane.entry_inventory.v2"
                ),
                "plan_version": "V3.4.2-P0R2",
                "phase": "P0",
                "attempt_id": "p0r2-attempt-001",
                "identity_binding": identity_dict,
                (
                    "source_identity_sha256"
                    if git_source_identity
                    else "freeze_payload_sha256"
                ): freeze_payload[
                    (
                        "source_identity_sha256"
                        if git_source_identity
                        else "freeze_payload_sha256"
                    )
                ],
                "entries": inventory_entries,
                "entry_count": len(inventory_entries),
            }
            inventory_payload["inventory_payload_sha256"] = hashlib.sha256(
                canonical_json(inventory_payload).encode("utf-8")
            ).hexdigest()
            if git_source_identity:
                inventory_payload = build_final_entry_inventory(
                    root,
                    plan_version="V3.4.2-P0R2",
                    phase="P0",
                    attempt_id="p0r2-attempt-001",
                    identity_binding=identity_dict,
                    freeze_manifest=freeze_payload,
                    scheduler_records=[
                        {
                            "task_path": r"\A股选股",
                            "command": "D:/workspace/run_select.bat",
                            "task_xml_sha256": "d" * 64,
                            "state": "Ready",
                            "principal": (
                                "Administrator|Interactive|Limited"
                            ),
                            "trigger": (
                                "MSFT_TaskDailyTrigger|"
                                "start=2026-03-16T20:00:00|"
                                "days_interval=1|enabled=true"
                            ),
                            "acl_summary": (
                                "owner=BUILTIN\\Administrators;sddl=O:BA"
                            ),
                        }
                    ],
                )
                inventory_entries = list(inventory_payload["entries"])
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
            policy_payload["review_receipt_sha256"] = (
                reviewed_policy_receipt_sha256(policy_payload)
            )
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
                gate_test_receipt = {
                    "receipt_id": "gate-tests",
                    "command": (
                        "python -m unittest tests.test_control_plane_gates"
                    ),
                    "exit_code": 0,
                    "result": "PASS",
                }
                gate_test_attestation = authority._attest_task_receipt(
                    lease,
                    receipt_kind="TEST",
                    issuer=Actor(
                        "trusted-gate-test-runner",
                        "automation",
                        "gate-tests-001",
                    ),
                    payload=gate_test_receipt,
                )
                authority._record_task_receipt(
                    lease,
                    attestation=gate_test_attestation,
                )
                for evidence in evidence_refs:
                    evidence_attestation = authority._attest_task_receipt(
                        lease,
                        receipt_kind="EVIDENCE",
                        issuer=Actor(
                            "activation-coordinator",
                            "automation",
                            "coordinator-evidence-001",
                        ),
                        payload=evidence,
                    )
                    authority._record_task_receipt(
                        lease,
                        attestation=evidence_attestation,
                    )
                if activate_policy:
                    authority._activate_reviewed_entry_policy(
                        lease,
                        reviewer=Actor(
                            "independent-test-reviewer",
                            "llm",
                            "review-gate-policy-001",
                        ),
                        policy_sha256=policy_sha256,
                        policy_payload_sha256=(
                            active_policy_payload_sha256
                            or str(policy_payload["policy_payload_sha256"])
                        ),
                        inventory_payload_sha256=str(
                            policy_payload["inventory_payload_sha256"]
                        ),
                        review_receipt_sha256=str(
                            policy_payload["review_receipt_sha256"]
                        ),
                        expected_active_sha256=None,
                    )
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
                        "test_receipts": [gate_test_receipt],
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
                draft["test_receipts"] = [
                    {
                        "ticket_id": ticket.ticket_id,
                        **gate_test_receipt,
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
                # P0-CR-008: gate evidence is read from committed blobs. Ensure a
                # git repo exists (idempotent for the git_source_identity=True path)
                # and commit the evidence files so the reader can resolve them.
                subprocess.run(
                    ["git", "init", "--quiet"],
                    cwd=root,
                    check=True,
                    capture_output=True,
                )
                if not omit_evidence_commit:
                    for evidence_ref, evidence_bytes in evidence_files:
                        evidence_path = root / evidence_ref
                        evidence_path.parent.mkdir(parents=True, exist_ok=True)
                        evidence_path.write_bytes(evidence_bytes)
                subprocess.run(
                    ["git", "add", "research_state"],
                    cwd=root,
                    check=True,
                    capture_output=True,
                )
                subprocess.run(
                    [
                        "git",
                        "-c",
                        "user.name=Control Plane Tests",
                        "-c",
                        "user.email=control-plane@example.invalid",
                        "commit",
                        "--quiet",
                        "-m",
                        "record gate evidence",
                    ],
                    cwd=root,
                    check=True,
                    capture_output=True,
                )
                if omit_evidence_commit:
                    # CR-009 negative case: evidence exists in the working
                    # tree but is NOT committed, so the verifier must fail
                    # closed instead of resolving it.
                    for evidence_ref, evidence_bytes in evidence_files:
                        evidence_path = root / evidence_ref
                        evidence_path.parent.mkdir(parents=True, exist_ok=True)
                        evidence_path.write_bytes(evidence_bytes)
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

    def _add_succeeded_task_report(
        self,
        fixture: _TrustedGateFixture,
        *,
        with_adverse_evidence: bool = False,
    ) -> Path:
        first_report = parse_task_report_v2_bytes(fixture.task_report_bytes)
        authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
        grant = authority._recover_claimed_grant(
            str(first_report["authorization_ref"])
        )
        task_spec = {
            "task_id": "P0R2-T3-GATE-VERIFY-SECOND",
            "objective": "Prove deterministic multi-report aggregation.",
            "dependencies": [],
            "idempotency_key": "p0r2-gate-verify-002",
            "task_spec_ref": (
                "research_state/control_plane/p0r2/task_specs/gate-second.json"
            ),
            "task_spec_sha256": "8" * 64,
            "requirements": {
                "required_test_receipt_ids": ["gate-tests"],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_automation/control_plane/gates.py"],
            "forbidden_files": ["data/"],
            "baseline_ref": first_report["baseline_ref"],
            "baseline_sha256": first_report["baseline_sha256"],
            "input_evidence_refs": [],
        }
        ticket = authority._issue_task_ticket(
            grant,
            task_spec,
            allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
        )
        lease = authority._begin_task(ticket)
        receipt = {
            "receipt_id": "gate-tests",
            "command": "python -m unittest tests.test_control_plane_gates",
            "exit_code": 0,
            "result": "PASS",
        }
        attestation = authority._attest_task_receipt(
            lease,
            receipt_kind="TEST",
            issuer=Actor(
                "trusted-gate-test-runner",
                "automation",
                "gate-tests-002",
            ),
            payload=receipt,
        )
        authority._record_task_receipt(lease, attestation=attestation)
        finished = authority._finish_task(
            lease,
            outcome="SUCCEEDED",
            evidence_ref="evidence/gate-verify-second.json",
        )
        journal = stores_module._OperationalJournal(root_secret=ROOT_SECRET)
        stores_module._mirror_authority_outbox(authority, journal, limit=100)
        report = build_task_report_v2(
            {
                "plan_version": first_report["plan_version"],
                "phase": first_report["phase"],
                "task_id": task_spec["task_id"],
                "attempt_id": first_report["attempt_id"],
                "authorization_ref": first_report["authorization_ref"],
                "ticket_id": ticket.ticket_id,
                "identity_binding": first_report["identity_binding"],
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
                "input_evidence_refs": [],
                "test_receipts": [receipt],
                "review_receipts": [],
                "review_findings": [],
                "changed_files": (
                    [
                        {
                            "path": "unexpected.py",
                            "change_type": "MODIFY",
                            "baseline_sha256": "1" * 64,
                            "current_sha256": "2" * 64,
                        }
                    ]
                    if with_adverse_evidence
                    else []
                ),
                "external_invocations": [],
                "side_effect_summary": {
                    "observed": (
                        ["NETWORK_EGRESS"] if with_adverse_evidence else []
                    ),
                    "unauthorized": (
                        ["NETWORK_EGRESS"] if with_adverse_evidence else []
                    ),
                },
                "ticket_state": "SUCCEEDED",
                "started_at": finished.started_at.isoformat(),
                "completed_at": finished.completed_at.isoformat(),
            }
        )
        path = (
            fixture.root
            / "research_state"
            / "control_plane"
            / "p0r2"
            / "reports"
            / "gate-second.json"
        )
        path.write_text(canonical_json(report), encoding="utf-8")
        subprocess.run(
            ["git", "add", "research_state/control_plane/p0r2/reports"],
            cwd=fixture.root,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "git",
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "record second task report",
            ],
            cwd=fixture.root,
            check=True,
            capture_output=True,
        )
        return path

    def _commit_gate_report(self, fixture: _TrustedGateFixture, filename: str) -> None:
        """Commit a gate report written into the fixture repo root."""
        subprocess.run(
            ["git", "add", filename],
            cwd=fixture.root,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "git",
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "record gate report",
            ],
            cwd=fixture.root,
            check=True,
            capture_output=True,
        )

    def test_verifier_requeries_the_authority_snapshot(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            self.assertEqual(
                fixture.snapshot,
                {
                    "active_entry_policy_sha256": fixture.draft[
                        "reviewed_entry_policy"
                    ]["sha256"],
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

    def test_verifier_rejects_an_active_policy_binding_mismatch(self) -> None:
        with self._trusted_gate_fixture(
            active_policy_payload_sha256="9" * 64,
        ) as fixture:
            self.assertEqual(fixture.report["verdict"], "PASS")

            with self.assertRaisesRegex(
                GateAuthorityMismatchError,
                "active entry policy",
            ):
                fixture.verifier.verify(fixture.report)

    def test_verifier_accepts_a_canonical_gate_report_bytes(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            raw = canonical_json(fixture.report).encode("utf-8")

            fixture.verifier.verify_bytes(raw)

    def test_verifier_accepts_git_source_identity_and_inventory_v3(self) -> None:
        with self._trusted_gate_fixture(git_source_identity=True) as fixture:
            fixture.verifier.verify(fixture.report)

    def test_public_verifier_accepts_gate_before_and_after_evidence_commit(
        self,
    ) -> None:
        with self._trusted_gate_fixture(git_source_identity=True) as fixture:
            raw = canonical_json(fixture.report).encode("utf-8")
            fixture.verifier.verify_bytes(raw)
            gate_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "gates"
                / "official_gate.json"
            )
            gate_path.parent.mkdir(parents=True, exist_ok=True)
            gate_path.write_bytes(raw)
            relative = gate_path.relative_to(fixture.root).as_posix()
            subprocess.run(
                ["git", "add", relative],
                cwd=fixture.root,
                check=True,
                capture_output=True,
            )
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "record official gate",
                ],
                cwd=fixture.root,
                check=True,
                capture_output=True,
            )

            fixture.verifier.verify_bytes(raw)

    def test_verifier_rejects_legacy_freeze_from_operational_gate(self) -> None:
        with self._trusted_gate_fixture(git_source_identity=False) as fixture:
            with self.assertRaisesRegex(
                GateEvidenceError,
                "require Git source identity",
            ):
                fixture.verifier.verify(fixture.report)

    def test_git_gate_records_but_does_not_bind_non_authoritative_report_drift(
        self,
    ) -> None:
        with self._trusted_gate_fixture(git_source_identity=True) as fixture:
            (fixture.root / "CHANGELOG.md").write_text(
                "operator notes\n",
                encoding="utf-8",
            )

            fixture.verifier.verify(fixture.report)

    def test_git_gate_rejects_tracked_source_drift(self) -> None:
        with self._trusted_gate_fixture(git_source_identity=True) as fixture:
            source = fixture.root / "research_automation" / "autonomous_runner.py"
            source.write_text("# changed after freeze\n", encoding="utf-8")

            with self.assertRaisesRegex(
                GateEvidenceError,
                "current executable surface",
            ):
                fixture.verifier.verify(fixture.report)

    def test_verifier_checks_task_report_file_hash(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            fixture.task_report_path.write_bytes(
                fixture.task_report_bytes + b"\n"
            )

            with self.assertRaises(GateEvidenceError):
                fixture.verifier.verify(fixture.report)

    def test_verifier_dereferences_committed_evidence_blobs(self) -> None:
        """CR-009 positive: a VERIFIED evidence ref resolves to a committed
        blob whose bytes hash to evidence_sha256, inside the gate's own
        phase/attempt evidence namespace."""
        evidence_ref = (
            "research_state/control_plane/p0/attempts/p0r2-attempt-001/"
            "evidence/activation-evidence-001.json"
        )
        evidence_bytes = canonical_json(
            {
                "schema_version": "control_plane.activation_evidence.v1",
                "evidence_id": "coordinator-evidence-001",
                "evidence_ref": evidence_ref,
                "status": "VERIFIED",
                "manifest_sha256": "d" * 64,
            }
        ).encode("utf-8")
        evidence_entry = {
            "evidence_id": "coordinator-evidence-001",
            "evidence_ref": evidence_ref,
            "evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
            "status": "VERIFIED",
        }
        with self._trusted_gate_fixture(
            evidence_refs=(evidence_entry,),
            evidence_files=((evidence_ref, evidence_bytes),),
        ) as fixture:
            fixture.verifier.verify(fixture.report)

    def test_verifier_rejects_evidence_ref_outside_the_evidence_namespace(
        self,
    ) -> None:
        """CR-009 negative: evidence refs must live under
        research_state/control_plane/."""
        evidence_ref = "artifacts/evidence/activation-001.json"
        evidence_bytes = canonical_json(
            {
                "schema_version": "control_plane.activation_evidence.v1",
                "evidence_id": "coordinator-evidence-002",
                "evidence_ref": evidence_ref,
                "status": "VERIFIED",
            }
        ).encode("utf-8")
        evidence_entry = {
            "evidence_id": "coordinator-evidence-002",
            "evidence_ref": evidence_ref,
            "evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
            "status": "VERIFIED",
        }
        with self._trusted_gate_fixture(
            evidence_refs=(evidence_entry,),
            evidence_files=((evidence_ref, evidence_bytes),),
        ) as fixture:
            with self.assertRaisesRegex(
                GateEvidenceError,
                "outside the control-plane evidence namespace",
            ):
                fixture.verifier.verify(fixture.report)

    def test_verifier_rejects_evidence_ref_binding_to_another_phase(self) -> None:
        """CR-009 negative: P7/P8/C0 refs that point into the p6 attempt
        directory must fail the phase/attempt binding."""
        evidence_ref = (
            "research_state/control_plane/p6/attempts/p6-attempt-003/"
            "evidence/activation-evidence-003.json"
        )
        evidence_bytes = canonical_json(
            {
                "schema_version": "control_plane.activation_evidence.v1",
                "evidence_id": "coordinator-evidence-003",
                "evidence_ref": evidence_ref,
                "status": "VERIFIED",
            }
        ).encode("utf-8")
        evidence_entry = {
            "evidence_id": "coordinator-evidence-003",
            "evidence_ref": evidence_ref,
            "evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
            "status": "VERIFIED",
        }
        with self._trusted_gate_fixture(
            evidence_refs=(evidence_entry,),
            evidence_files=((evidence_ref, evidence_bytes),),
        ) as fixture:
            with self.assertRaisesRegex(
                GateAuthorityMismatchError,
                "does not bind to the gate phase/attempt",
            ):
                fixture.verifier.verify(fixture.report)

    def test_verifier_rejects_path_string_evidence_hash(self) -> None:
        """CR-009 negative: a path-string SHA-256 (hash of the ref text
        instead of the committed blob bytes) must fail closed."""
        evidence_ref = (
            "research_state/control_plane/p0/attempts/p0r2-attempt-001/"
            "evidence/activation-evidence-004.json"
        )
        evidence_bytes = canonical_json(
            {
                "schema_version": "control_plane.activation_evidence.v1",
                "evidence_id": "coordinator-evidence-004",
                "evidence_ref": evidence_ref,
                "status": "VERIFIED",
            }
        ).encode("utf-8")
        evidence_entry = {
            "evidence_id": "coordinator-evidence-004",
            "evidence_ref": evidence_ref,
            "evidence_sha256": hashlib.sha256(
                evidence_ref.encode("utf-8")
            ).hexdigest(),
            "status": "VERIFIED",
        }
        with self._trusted_gate_fixture(
            evidence_refs=(evidence_entry,),
            evidence_files=((evidence_ref, evidence_bytes),),
        ) as fixture:
            with self.assertRaisesRegex(
                GateEvidenceError,
                "SHA-256 does not match the committed blob",
            ):
                fixture.verifier.verify(fixture.report)

    def test_verifier_rejects_dangling_evidence_ref(self) -> None:
        """CR-009 negative: an evidence ref with no committed blob fails."""
        evidence_ref = (
            "research_state/control_plane/p0/attempts/p0r2-attempt-001/"
            "evidence/activation-evidence-005.json"
        )
        evidence_entry = {
            "evidence_id": "coordinator-evidence-005",
            "evidence_ref": evidence_ref,
            "evidence_sha256": "0" * 64,
            "status": "VERIFIED",
        }
        with self._trusted_gate_fixture(
            evidence_refs=(evidence_entry,),
        ) as fixture:
            with self.assertRaisesRegex(
                GateEvidenceError,
                "not committed|not tracked|not a regular file",
            ):
                fixture.verifier.verify(fixture.report)

    def test_verifier_rejects_uncommitted_evidence(self) -> None:
        """CR-009 negative: evidence present only in the working tree (not
        committed) must fail the committed-blob reader."""
        evidence_ref = (
            "research_state/control_plane/p0/attempts/p0r2-attempt-001/"
            "evidence/activation-evidence-006.json"
        )
        evidence_bytes = canonical_json(
            {
                "schema_version": "control_plane.activation_evidence.v1",
                "evidence_id": "coordinator-evidence-006",
                "evidence_ref": evidence_ref,
                "status": "VERIFIED",
            }
        ).encode("utf-8")
        evidence_entry = {
            "evidence_id": "coordinator-evidence-006",
            "evidence_ref": evidence_ref,
            "evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
            "status": "VERIFIED",
        }
        with self._trusted_gate_fixture(
            evidence_refs=(evidence_entry,),
            evidence_files=((evidence_ref, evidence_bytes),),
            omit_evidence_commit=True,
        ) as fixture:
            with self.assertRaisesRegex(
                GateEvidenceError,
                "not committed|not tracked|working copy is dirty",
            ):
                fixture.verifier.verify(fixture.report)

    def test_verifier_rejects_empty_mandatory_requirements(self) -> None:
        """CR-009 negative: a TaskReport with no required test receipts, no
        required review receipts and no required evidence ids binds nothing
        and must fail the gate."""
        empty_requirements = {
            "required_test_receipt_ids": [],
            "required_review_receipt_ids": [],
            "required_evidence_ids": [],
        }
        with self._trusted_gate_fixture(
            task_requirements=empty_requirements,
        ) as fixture:
            with self.assertRaisesRegex(
                GateEvidenceError,
                "requirements are empty; the gate binds nothing",
            ):
                fixture.verifier.verify(fixture.report)
    def test_verifier_rejects_evidence_blob_ticket_mismatch(self) -> None:
        """CR-009 (Reviewer B-01): a committed evidence blob whose content
        binds a different ticket must fail the semantic check even when the
        blob hash matches."""
        evidence_ref = (
            "research_state/control_plane/p0/attempts/p0r2-attempt-001/"
            "evidence/activation-evidence-010.json"
        )
        evidence_bytes = canonical_json(
            {
                "schema_version": "control_plane.activation_evidence.v1",
                "evidence_id": "coordinator-evidence-010",
                "evidence_ref": evidence_ref,
                "status": "VERIFIED",
                "ticket_id": "forged-ticket",
            }
        ).encode("utf-8")
        evidence_entry = {
            "evidence_id": "coordinator-evidence-010",
            "evidence_ref": evidence_ref,
            "evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
            "status": "VERIFIED",
        }
        with self._trusted_gate_fixture(
            evidence_refs=(evidence_entry,),
            evidence_files=((evidence_ref, evidence_bytes),),
        ) as fixture:
            with self.assertRaisesRegex(
                GateEvidenceError,
                "ticket_id does not match the TaskReport ticket",
            ):
                fixture.verifier.verify(fixture.report)

    def test_verifier_rejects_evidence_ref_with_parent_segments(self) -> None:
        """CR-009 negative (Reviewer B-1): a crafted evidence ref using '..'
        segments must fail closed at TaskReport parse time before any
        namespace check could be tricked."""
        from research_automation.control_plane.task_reports import (
            TaskReportValidationError,
            parse_task_report_v2_bytes,
        )

        with self._trusted_gate_fixture() as fixture:
            parsed_report = json.loads(fixture.task_report_bytes.decode("utf-8"))
            parsed_report["input_evidence_refs"] = [
                {
                    "evidence_id": "coordinator-evidence-007",
                    "evidence_ref": (
                        "research_state/control_plane/P0/attempts/"
                        "p0-attempt-001/../../../p6/attempts/"
                        "p6-attempt-003/evidence/activation.json"
                    ),
                    "evidence_sha256": "0" * 64,
                    "status": "VERIFIED",
                }
            ]
            forged_bytes = json.dumps(
                parsed_report, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            with self.assertRaises(TaskReportValidationError):
                parse_task_report_v2_bytes(forged_bytes)

    def test_verifier_rejects_symlink_evidence_blob(self) -> None:
        """CR-009 negative (Reviewer A S-001 / B-4): a committed symlink
        inside the evidence namespace must not be dereferenced as a regular
        evidence blob."""
        evidence_ref = (
            "research_state/control_plane/p0/attempts/p0r2-attempt-001/"
            "evidence/activation-evidence-008.json"
        )
        target_ref = (
            "research_state/control_plane/p0/attempts/p0r2-attempt-001/"
            "evidence/activation-evidence-target.json"
        )
        target_bytes = canonical_json(
            {
                "schema_version": "control_plane.activation_evidence.v1",
                "evidence_id": "coordinator-evidence-target",
                "evidence_ref": target_ref,
                "status": "VERIFIED",
            }
        ).encode("utf-8")
        evidence_entry = {
            "evidence_id": "coordinator-evidence-008",
            "evidence_ref": evidence_ref,
            "evidence_sha256": hashlib.sha256(target_bytes).hexdigest(),
            "status": "VERIFIED",
        }
        with self._trusted_gate_fixture(
            evidence_refs=(evidence_entry,),
            evidence_files=((target_ref, target_bytes),),
        ) as fixture:
            # The symlink is created and committed after the fixture commit
            # so it cannot be swept into the add-only evidence commit.
            symlink_path = fixture.root / evidence_ref
            symlink_path.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(
                os.path.relpath(fixture.root / target_ref, symlink_path.parent),
                symlink_path,
            )
            subprocess.run(
                ["git", "add", evidence_ref],
                cwd=fixture.root,
                check=True,
                capture_output=True,
            )
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "add symlink evidence fixture",
                ],
                cwd=fixture.root,
                check=True,
                capture_output=True,
            )
            with self.assertRaisesRegex(
                GateEvidenceError,
                "not a regular file|non-regular mode|unsafe Git mode",
            ):
                fixture.verifier.verify(fixture.report)

    def test_verifier_hashes_committed_blob_not_working_copy(self) -> None:
        """CR-009 (Reviewer A M-002 / B-2): the dereference hashes the
        committed blob bytes (LF) via cat-file, not the checked-out working
        copy; a CRLF working copy must not change the verification."""
        evidence_ref = (
            "research_state/control_plane/p0/attempts/p0r2-attempt-001/"
            "evidence/activation-evidence-009.json"
        )
        evidence_bytes = canonical_json(
            {
                "schema_version": "control_plane.activation_evidence.v1",
                "evidence_id": "coordinator-evidence-009",
                "evidence_ref": evidence_ref,
                "status": "VERIFIED",
            }
        ).encode("utf-8")
        evidence_entry = {
            "evidence_id": "coordinator-evidence-009",
            "evidence_ref": evidence_ref,
            "evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
            "status": "VERIFIED",
        }
        with self._trusted_gate_fixture(
            evidence_refs=(evidence_entry,),
            evidence_files=((evidence_ref, evidence_bytes),),
        ) as fixture:
            fixture.verifier.verify(fixture.report)
            # Rewrite the working copy with CRLF line endings; the committed
            # blob is unchanged so the verifier must still pass.
            crlf_bytes = (
                canonical_json(
                    {
                        "schema_version": (
                            "control_plane.activation_evidence.v1"
                        ),
                        "evidence_id": "coordinator-evidence-009",
                        "evidence_ref": evidence_ref,
                        "status": "VERIFIED",
                    }
                )
                .replace("\n", "\r\n")
                .encode("utf-8")
            )
            (fixture.root / evidence_ref).write_bytes(crlf_bytes)
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
            forged_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "reports"
                / "gate-forged.json"
            )
            forged_path.parent.mkdir(parents=True, exist_ok=True)
            forged_path.write_bytes(forged_bytes)
            subprocess.run(
                ["git", "add", "research_state/control_plane/p0r2/reports"],
                cwd=fixture.root,
                check=True,
                capture_output=True,
            )
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "record forged task report",
                ],
                cwd=fixture.root,
                check=True,
                capture_output=True,
            )

            gate_draft = dict(fixture.draft)
            task_report_ref = dict(fixture.draft["task_reports"][0])
            task_report_ref["report_ref"] = (
                "research_state/control_plane/p0r2/reports/gate-forged.json"
            )
            task_report_ref["report_sha256"] = hashlib.sha256(
                forged_bytes
            ).hexdigest()
            gate_draft["task_reports"] = [task_report_ref]
            gate_report = PhaseGateBuilder().build(gate_draft)

            with self.assertRaises(GateAuthorityMismatchError):
                fixture.verifier.verify(gate_report)

    def test_verifier_rejects_a_gate_receipt_not_projected_from_task_reports(
        self,
    ) -> None:
        with self._trusted_gate_fixture() as fixture:
            forged_draft = dict(fixture.draft)
            forged_receipts = [dict(fixture.draft["test_receipts"][0])]
            forged_receipts[0]["command"] = "forged untrusted command"
            forged_draft["test_receipts"] = forged_receipts
            forged_report = PhaseGateBuilder().build(forged_draft)

            with self.assertRaisesRegex(
                GateEvidenceError,
                "projected TaskReport evidence",
            ):
                fixture.verifier.verify(forged_report)

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
            original = json.loads(
                fixture.artifact_paths["implementation_baseline"].read_bytes()
            )
            member = dict(original["baseline"])
            member["git_head"] = "f" * 40
            copied_payload = dict(original)
            copied_payload["baseline"] = member
            copied_payload["baseline_payload_sha256"] = hashlib.sha256(
                canonical_json(member).encode("utf-8")
            ).hexdigest()
            baseline_bytes = canonical_json(copied_payload).encode("utf-8")
            copied_ref = (
                "research_state/control_plane/p0r2/"
                "implementation_baseline_copy.json"
            )
            copied_path = fixture.root / copied_ref
            copied_path.write_bytes(baseline_bytes)
            subprocess.run(
                ["git", "add", "research_state/control_plane/p0r2"],
                cwd=fixture.root,
                check=True,
                capture_output=True,
            )
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "record copied implementation baseline",
                ],
                cwd=fixture.root,
                check=True,
                capture_output=True,
            )
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

    def test_closer_rechecks_active_policy_inside_the_close_transaction(
        self,
    ) -> None:
        with self._trusted_gate_fixture() as fixture:
            original_verify = fixture.verifier.verify
            old_digest = str(
                fixture.snapshot["active_entry_policy_sha256"]
            )
            rotated_bytes = fixture.artifact_paths[
                "reviewed_entry_policy"
            ].read_bytes() + b"\n"
            rotated_digest = hashlib.sha256(rotated_bytes).hexdigest()
            rotated_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "policies"
                / f"{rotated_digest}.json"
            )

            def verify_then_rotate(report: dict[str, object]) -> None:
                original_verify(report)
                rotated_path.write_bytes(rotated_bytes)
                connection = sqlite3.connect(fixture.root / "authority.sqlite3")
                try:
                    connection.execute("PRAGMA foreign_keys = ON")
                    connection.execute(
                        """
                        INSERT INTO reviewed_entry_policies_v1
                        SELECT ?, policy_payload_sha256,
                               inventory_payload_sha256,
                               review_receipt_sha256, reviewer_actor_id,
                               reviewer_actor_type, reviewer_invocation_id,
                               ticket_id, phase, attempt_id, plan_hash,
                               scope_hash, instruction_policy_hash, created_at
                        FROM reviewed_entry_policies_v1
                        WHERE policy_sha256 = ?
                        """,
                        (rotated_digest, old_digest),
                    )
                    connection.execute(
                        """
                        UPDATE active_entry_policy_v1
                        SET policy_sha256 = ?
                        WHERE singleton_id = 1 AND policy_sha256 = ?
                        """,
                        (rotated_digest, old_digest),
                    )
                    connection.commit()
                finally:
                    connection.close()

            with patch.object(
                PhaseGateVerifier,
                "verify",
                side_effect=verify_then_rotate,
            ):
                with self.assertRaisesRegex(
                    PhaseGateClosureError,
                    "authority changed before closure",
                ):
                    fixture.closer.close(fixture.report)

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
            report_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "gates"
                / "gate-report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )

            self._commit_gate_report(fixture, 'research_state/control_plane/p0r2/gates/gate-report.json')
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

    def test_cli_preflight_blocks_without_an_active_entry_policy(self) -> None:
        with self._trusted_gate_fixture(activate_policy=False) as fixture:
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

            self.assertEqual(exit_code, 2)
            self.assertIn('"status":"BLOCKED"', stdout.getvalue())
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

    def test_cli_build_rejects_source_drift_before_writing_pass(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            output_path = fixture.root / "drifted-gate-report.json"
            source = fixture.root / "research_automation" / "autonomous_runner.py"
            source.write_text("# drifted after freeze\n", encoding="utf-8")
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
            self.assertFalse(output_path.exists())
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("current executable surface", stderr.getvalue())

    def test_cli_build_rejects_legacy_freeze_evidence(self) -> None:
        with self._trusted_gate_fixture(git_source_identity=False) as fixture:
            output_path = fixture.root / "legacy-gate-report.json"
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
            self.assertFalse(output_path.exists())
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("require Git source identity", stderr.getvalue())

    def test_cli_aggregates_multiple_trusted_task_reports(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            second_path = self._add_succeeded_task_report(fixture)
            output_path = fixture.root / "multi-report-gate.json"
            args = self._gate_build_cli_args(fixture, output_path)
            option_index = args.index("--task-report-id")
            first_path = args[option_index + 1]
            args[option_index : option_index + 2] = [
                "--task-report-id",
                str(second_path),
                "--task-report-id",
                first_path,
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
            built = parse_gate_report_v1_bytes(output_path.read_bytes())
            self.assertEqual(built["verdict"], "PASS")
            self.assertEqual(len(built["task_reports"]), 2)
            self.assertEqual(len(built["test_receipts"]), 2)
            self.assertEqual(
                [item["ticket_id"] for item in built["task_reports"]],
                sorted(item["ticket_id"] for item in built["task_reports"]),
            )
            self.assertEqual(
                {item["receipt_id"] for item in built["test_receipts"]},
                {"gate-tests"},
            )
            fixture.verifier.verify(built)
            self.assertEqual(stderr.getvalue(), "")

    def test_cli_build_returns_two_when_a_succeeded_report_is_missing(
        self,
    ) -> None:
        with self._trusted_gate_fixture() as fixture:
            self._add_succeeded_task_report(fixture)
            output_path = fixture.root / "missing-report-gate.json"
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                self._gate_build_cli_args(fixture, output_path),
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 2)
            built = parse_gate_report_v1_bytes(output_path.read_bytes())
            self.assertEqual(built["verdict"], "FAIL")
            self.assertTrue(
                any(
                    reason.startswith("MISSING_TASK_REPORT:")
                    for reason in built["reason_codes"]
                )
            )
            fixture.verifier.verify(built)
            self.assertEqual(stderr.getvalue(), "")

    def test_cli_projects_task_side_effects_and_file_deltas(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            second_path = self._add_succeeded_task_report(
                fixture,
                with_adverse_evidence=True,
            )
            output_path = fixture.root / "adverse-evidence-gate.json"
            args = self._gate_build_cli_args(fixture, output_path)
            args.extend(["--task-report-id", str(second_path)])
            stdout = StringIO()
            stderr = StringIO()

            exit_code = gate_cli_main(
                args,
                stdout=stdout,
                stderr=stderr,
                authority_reader=fixture.reader,
                repository_root=fixture.root,
            )

            self.assertEqual(exit_code, 2)
            built = parse_gate_report_v1_bytes(output_path.read_bytes())
            self.assertEqual(
                built["side_effect_summary"],
                {
                    "observed": ["NETWORK_EGRESS"],
                    "unauthorized": ["NETWORK_EGRESS"],
                },
            )
            self.assertEqual(
                built["file_delta_summary"],
                {
                    "changed_files": ["unexpected.py"],
                    "unexpected_changes": ["unexpected.py"],
                },
            )
            fixture.verifier.verify(built)
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
                    original_scheduler = json.loads(
                        fixture.artifact_paths[
                            "scheduler_inventory"
                        ].read_text(encoding="utf-8")
                    )
                    scheduler_payload = dict(original_scheduler)
                    scheduler_payload["unresolved_risk"] = "x" * size
                    sized_bytes = canonical_json(scheduler_payload).encode("utf-8")
                    sized_ref = (
                        "research_state/control_plane/p0r2/"
                        f"scheduler_sized_{size}.json"
                    )
                    sized_path = fixture.root / sized_ref
                    sized_path.write_bytes(sized_bytes)
                    subprocess.run(
                        ["git", "add", "research_state/control_plane/p0r2"],
                        cwd=fixture.root,
                        check=True,
                        capture_output=True,
                    )
                    subprocess.run(
                        [
                            "git",
                            "-c",
                            "user.name=Control Plane Tests",
                            "-c",
                            "user.email=control-plane@example.invalid",
                            "commit",
                            "--quiet",
                            "-m",
                            "record sized scheduler inventory",
                        ],
                        cwd=fixture.root,
                        check=True,
                        capture_output=True,
                    )
                    fixture.draft["scheduler_inventory"] = {
                        "ref": sized_ref,
                        "sha256": hashlib.sha256(sized_bytes).hexdigest(),
                    }
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

    def test_cli_loses_a_concurrent_create_without_overwriting_it(self) -> None:
        with self._trusted_gate_fixture() as fixture:
            output_path = fixture.root / "concurrent-report.json"
            stdout = StringIO()
            stderr = StringIO()

            def concurrent_create(_source: object, destination: object) -> None:
                Path(destination).write_bytes(b"concurrent-winner")
                raise FileExistsError

            with patch.object(
                cli_module.os,
                "link",
                side_effect=concurrent_create,
            ):
                exit_code = gate_cli_main(
                    self._gate_build_cli_args(fixture, output_path),
                    stdout=stdout,
                    stderr=stderr,
                    authority_reader=fixture.reader,
                    repository_root=fixture.root,
                )

            self.assertEqual(exit_code, 3)
            self.assertEqual(output_path.read_bytes(), b"concurrent-winner")
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
            report_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "gates"
                / "gate-report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )

            self._commit_gate_report(fixture, 'research_state/control_plane/p0r2/gates/gate-report.json')
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
        with self._trusted_gate_fixture(activate_policy=False) as fixture:
            fail_report = fixture.report
            report_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "gates"
                / "gate-fail-report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                canonical_json(fail_report),
                encoding="utf-8",
            )

            self._commit_gate_report(fixture, 'research_state/control_plane/p0r2/gates/gate-fail-report.json')
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
            report_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "gates"
                / "corrupt-gate-report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_bytes(b"not-json")

            self._commit_gate_report(fixture, 'research_state/control_plane/p0r2/gates/corrupt-gate-report.json')
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
            report_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "gates"
                / "gate-report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )

            self._commit_gate_report(fixture, 'research_state/control_plane/p0r2/gates/gate-report.json')
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
            report_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "gates"
                / "gate-report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )

            self._commit_gate_report(fixture, 'research_state/control_plane/p0r2/gates/gate-report.json')
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
            report_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "gates"
                / "gate-report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )

            self._commit_gate_report(fixture, 'research_state/control_plane/p0r2/gates/gate-report.json')
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
            report_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "gates"
                / "different-gate-report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                canonical_json(different_report),
                encoding="utf-8",
            )

            self._commit_gate_report(fixture, 'research_state/control_plane/p0r2/gates/different-gate-report.json')
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

            report_path = (
                fixture.root
                / "research_state"
                / "control_plane"
                / "p0r2"
                / "gates"
                / "gate-report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                canonical_json(fixture.report),
                encoding="utf-8",
            )

            self._commit_gate_report(fixture, 'research_state/control_plane/p0r2/gates/gate-report.json')
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
