import hashlib
import json
import shutil
import sqlite3
import unittest
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch


def canonical_bytes(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


class EvidenceLearningVerticalSliceTests(unittest.TestCase):
    def _write_json(self, root, ref, payload):
        raw = canonical_bytes(payload)
        path = root / ref
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return {
            "evidence_ref": ref,
            "evidence_sha256": hashlib.sha256(raw).hexdigest(),
            "status": "VERIFIED",
        }

    def _authority_fixture(self, root, *, claim=None):
        from research_automation.control_plane.contracts import SideEffect
        from research_automation.control_plane.evidence_learning import EvidenceAdapter
        from research_automation.control_plane import evidence_learning as module
        from research_automation.control_plane.task_reports import build_task_report_v2

        claim = {"kind": "NEGATIVE"} if claim is None else claim
        protocol = {"label": "signal-day", "embargo_days": 5}
        artifact = {
            "schema_version": "runner.artifact.v1",
            "runner": "fixture-runner",
            "runner_version": "1.0.0",
            "status": "COMPLETED",
            "claim": claim,
            "protocol_conformance": "CONFORMING",
            "executed_protocol": protocol,
            "artifact_refs": [
                {"ref": "fixtures/result.json", "sha256": "e" * 64}
            ],
            "access_event_ids": ["event:fixture-001"],
            "taint_refs": [],
        }
        source_ref = "research_automation/control_plane/evidence_learning.py"
        source_raw = Path(module.__file__).read_bytes()
        source_path = root / source_ref
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(source_raw)
        adapter = {
            "schema_version": "control_plane.runner_adapter.v1",
            "adapter_id": "EvidenceAdapter.v1",
            "source_ref": source_ref,
            "source_sha256": hashlib.sha256(source_raw).hexdigest(),
            "runners": {"fixture-runner": "1.0.0"},
        }
        base = "research_state/control_plane/p4/fixtures"
        refs = {
            "approved-claim": self._write_json(root, f"{base}/claim.json", claim),
            "approved-protocol": self._write_json(
                root, f"{base}/protocol.json", protocol
            ),
            "runner-adapter": self._write_json(
                root, f"{base}/adapter.json", adapter
            ),
            "runner-artifact": self._write_json(
                root, f"{base}/artifact.json", artifact
            ),
        }
        evidence = EvidenceAdapter(
            known_runners=adapter["runners"],
            approved_protocol=protocol,
            approved_claim=claim,
        ).evaluate(artifact)
        decision = {
            "schema_version": "control_plane.evidence_decision.v1",
            "bindings": {
                name: refs[name]["evidence_sha256"]
                for name in sorted(refs)
            },
            "claim": claim,
            "evidence": {
                "verdict": evidence.verdict,
                "protocol_conformance": evidence.protocol_conformance,
                "audit_grade": evidence.audit_grade,
                "scientific_outcome": evidence.scientific_outcome,
                "promotion_eligible": evidence.promotion_eligible,
                "evidence_refs": list(evidence.evidence_refs),
                "access_event_ids": list(evidence.access_event_ids),
                "taint_refs": list(evidence.taint_refs),
                "invalidation_codes": list(evidence.invalidation_codes),
            },
        }
        refs["learning-decision"] = self._write_json(
            root, f"{base}/decision.json", decision
        )
        baseline_ref = "research_state/control_plane/p3/baseline.json"
        baseline_raw = canonical_bytes(
            {
                "phase": "P3",
                "repository_root": str(root.resolve()),
                "status": "PASS",
            }
        )
        baseline_path = root / baseline_ref
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        baseline_path.write_bytes(baseline_raw)
        input_refs = []
        for evidence_id in sorted(refs):
            input_refs.append({"evidence_id": evidence_id, **refs[evidence_id]})
        report = build_task_report_v2({
            "plan_version": "V3.4.2-P0R2",
            "phase": "P4",
            "task_id": "P4-LEARNING-COMMIT",
            "attempt_id": "p4-fixture",
            "authorization_ref": "auth-learning-001",
            "ticket_id": "ticket-learning-001",
            "identity_binding": {
                "plan_hash": "a" * 64,
                "scope_hash": "b" * 64,
                "instruction_policy_hash": "c" * 64,
            },
            "objective": "Project one independently reviewed Learning decision.",
            "dependencies": [],
            "idempotency_key": "p4-learning-commit-001",
            "task_spec_ref": "research_state/control_plane/p4/task_specs/learning.json",
            "task_spec_sha256": "d" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": sorted(refs),
            },
            "ticket_state": "SUCCEEDED",
            "allowed_files": [
                "research_state/control_plane/learning_commit.sqlite3",
                "research_state/control_plane/learning_packets/",
            ],
            "forbidden_files": ["data/", "knowledge/"],
            "baseline_ref": baseline_ref,
            "baseline_sha256": hashlib.sha256(baseline_raw).hexdigest(),
            "input_evidence_refs": input_refs,
            "test_receipts": [],
            "review_receipts": [],
            "review_findings": [],
            "changed_files": [],
            "external_invocations": [],
            "side_effect_summary": {"observed": [], "unauthorized": []},
            "started_at": "2026-07-30T08:00:00Z",
            "completed_at": "2026-07-30T08:01:00Z",
        })
        binding = SimpleNamespace(
            ticket_id="ticket-learning-001",
            report_payload_sha256=report["report_payload_sha256"],
            actor_id="independent-evidence-reviewer",
            allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
            ticket_state="SUCCEEDED",
            terminal_evidence_ref=refs["learning-decision"]["evidence_ref"],
        )
        return report, binding, artifact, evidence, refs

    def test_clean_complete_run_without_claim_is_no_material_finding(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        result = EvidenceAdapter(known_runners=("test-runner",)).evaluate(
            {
                "schema_version": "runner.artifact.v1",
                "runner": "test-runner",
                "status": "COMPLETED",
                "claim": None,
                "protocol_conformance": "CONFORMING",
                "artifact_refs": [],
                "access_event_ids": [],
                "taint_refs": [],
            }
        )
        self.assertEqual(result.verdict, "NO_MATERIAL_FINDING")
        self.assertFalse(result.promotion_eligible)

    def test_runner_boolean_cannot_set_promotion_outcome(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        result = EvidenceAdapter(known_runners=("test-runner",)).evaluate(
            {
                "schema_version": "runner.artifact.v1",
                "runner": "test-runner",
                "status": "COMPLETED",
                "claim": {"kind": "POSITIVE"},
                "promotion_gate_passed": True,
                "protocol_conformance": "CONFORMING",
                "artifact_refs": ({"ref": "fixture.json", "sha256": "a" * 64},),
                "access_event_ids": (),
                "taint_refs": (),
            }
        )
        self.assertEqual(result.verdict, "RESEARCH_ONLY")

    def test_tainted_metrics_are_evidence_invalid(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        result = EvidenceAdapter(known_runners=("test-runner",)).evaluate(
            {
                "schema_version": "runner.artifact.v1",
                "runner": "test-runner",
                "status": "COMPLETED",
                "claim": {"kind": "POSITIVE"},
                "protocol_conformance": "CONFORMING",
                "artifact_refs": (),
                "access_event_ids": ("event-001",),
                "taint_refs": ("TEST_DERIVED",),
            }
        )
        self.assertIn("TAINTED_EVIDENCE", result.invalidation_codes)

    def test_unknown_runner_and_protocol_mismatch_are_invalid(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        adapter = EvidenceAdapter(
            known_runners={"test-runner": "runner-v1"},
            approved_protocol={"label": "return_5d"},
        )
        result = adapter.evaluate(
            {
                "schema_version": "runner.artifact.v1",
                "runner": "test-runner",
                "runner_version": "runner-v1",
                "status": "COMPLETED",
                "claim": None,
                "protocol_conformance": "CONFORMING",
                "executed_protocol": {"label": "return_10d"},
                "artifact_refs": (),
                "access_event_ids": (),
                "taint_refs": (),
            }
        )
        self.assertIn("EXECUTED_PROTOCOL_MISMATCH", result.invalidation_codes)

    def test_artifact_collection_type_errors_are_fail_closed_verdicts(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        base = {
            "schema_version": "runner.artifact.v1",
            "runner": "test-runner",
            "status": "COMPLETED",
            "claim": {"kind": "NEGATIVE"},
            "protocol_conformance": "CONFORMING",
            "artifact_refs": [],
            "access_event_ids": ["event-001"],
            "taint_refs": [],
        }
        for field_name, invalid_value in (
            ("artifact_refs", None),
            ("access_event_ids", "event-001"),
            ("taint_refs", None),
        ):
            with self.subTest(field_name=field_name):
                artifact = dict(base)
                artifact[field_name] = invalid_value
                result = EvidenceAdapter(
                    known_runners=("test-runner",),
                    approved_claim={"kind": "NEGATIVE"},
                ).evaluate(artifact)
                self.assertEqual(result.verdict, "EVIDENCE_INVALID")
                self.assertIn("INVALID_ARTIFACT_COLLECTION", result.invalidation_codes)

    def test_unknown_runner_schema_is_invalid(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        result = EvidenceAdapter(known_runners=("test-runner",)).evaluate(
            {
                "schema_version": "runner.artifact.v0",
                "runner": "test-runner",
                "status": "COMPLETED",
                "claim": None,
                "protocol_conformance": "CONFORMING",
                "artifact_refs": [],
                "access_event_ids": [],
                "taint_refs": [],
            }
        )
        self.assertIn("UNKNOWN_RUNNER_SCHEMA", result.invalidation_codes)

    def test_unknown_runner_is_invalid(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        artifact = {
            "schema_version": "runner.artifact.v1",
            "runner": "unknown-runner",
            "status": "COMPLETED",
            "claim": None,
            "protocol_conformance": "CONFORMING",
            "artifact_refs": [],
            "access_event_ids": [],
            "taint_refs": [],
        }
        self.assertIn(
            "UNKNOWN_RUNNER",
            EvidenceAdapter(known_runners=("test-runner",)).evaluate(
                artifact
            ).invalidation_codes,
        )

    def test_runner_version_mismatch_is_invalid(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        result = EvidenceAdapter(
            known_runners={"test-runner": "runner-v1"}
        ).evaluate(
            {
                "schema_version": "runner.artifact.v1",
                "runner": "test-runner",
                "runner_version": "runner-v2",
                "status": "COMPLETED",
                "claim": None,
                "protocol_conformance": "CONFORMING",
                "artifact_refs": [],
                "access_event_ids": [],
                "taint_refs": [],
            }
        )
        self.assertIn("RUNNER_VERSION_MISMATCH", result.invalidation_codes)

    def test_matching_runner_protocol_and_claim_is_valid(self):
        with TemporaryDirectory() as tmp:
            _, _, _, evidence, _ = self._authority_fixture(Path(tmp))
        self.assertEqual(evidence.verdict, "VALID")
        self.assertTrue(evidence.promotion_eligible)

    def test_non_compact_artifact_reference_is_invalid(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        claim = {"kind": "NEGATIVE"}
        result = EvidenceAdapter(
            known_runners=("test-runner",),
            approved_claim=claim,
        ).evaluate(
            {
                "schema_version": "runner.artifact.v1",
                "runner": "test-runner",
                "status": "COMPLETED",
                "claim": claim,
                "protocol_conformance": "CONFORMING",
                "artifact_refs": [
                    {"ref": "result.json", "sha256": "a" * 64, "raw_log": "secret"}
                ],
                "access_event_ids": ["event-001"],
                "taint_refs": [],
            }
        )
        self.assertIn("INVALID_ARTIFACT_REFERENCE", result.invalidation_codes)

    def test_malformed_runner_and_reference_elements_are_fail_closed(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        base = {
            "schema_version": "runner.artifact.v1",
            "runner": "test-runner",
            "status": "COMPLETED",
            "claim": {"kind": "NEGATIVE"},
            "protocol_conformance": "CONFORMING",
            "artifact_refs": [{"ref": "result.json", "sha256": "a" * 64}],
            "access_event_ids": ["event-001"],
            "taint_refs": [],
        }
        for mutation in (
            {"runner": []},
            {"artifact_refs": [{"ref": "result.json", "sha256": 1}]},
        ):
            with self.subTest(mutation=mutation):
                artifact = {**base, **mutation}
                result = EvidenceAdapter(
                    known_runners=("test-runner",),
                    approved_claim={"kind": "NEGATIVE"},
                ).evaluate(artifact)
                self.assertEqual(result.verdict, "EVIDENCE_INVALID")

    def test_tainted_authority_artifact_cannot_commit(self):
        from research_automation.control_plane.evidence_learning import LearningCommitService

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, refs = self._authority_fixture(root)
            artifact_path = root / refs["runner-artifact"]["evidence_ref"]
            artifact = json.loads(artifact_path.read_bytes())
            artifact["taint_refs"] = ["TEST_DERIVED"]
            raw = canonical_bytes(artifact)
            artifact_path.write_bytes(raw)
            for reference in report["input_evidence_refs"]:
                if reference["evidence_id"] == "runner-artifact":
                    reference["evidence_sha256"] = hashlib.sha256(raw).hexdigest()
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaisesRegex(ValueError, "not commit eligible"):
                    LearningCommitService(repository_root=root).commit(report)

    def test_learning_commit_requires_exact_task_id(self):
        from research_automation.control_plane.evidence_learning import (
            LearningCommitAuthorizationError,
            LearningCommitService,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            report["task_id"] = "P4-UNRELATED"
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaises(LearningCommitAuthorizationError):
                    LearningCommitService(repository_root=root).commit(report)

    def test_learning_commit_requires_exact_output_sinks(self):
        from research_automation.control_plane.evidence_learning import (
            LearningCommitAuthorizationError,
            LearningCommitService,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            report["allowed_files"].append("data/")
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaises(LearningCommitAuthorizationError):
                    LearningCommitService(repository_root=root).commit(report)

    def test_learning_commit_requires_exact_side_effect_set(self):
        from research_automation.control_plane.contracts import SideEffect
        from research_automation.control_plane.evidence_learning import (
            LearningCommitAuthorizationError,
            LearningCommitService,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            binding.allowed_side_effects = (
                SideEffect.WRITE_CONTROL_PLANE,
                SideEffect.READ,
            )
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaises(LearningCommitAuthorizationError):
                    LearningCommitService(repository_root=root).commit(report)

    def test_authority_report_cannot_be_replayed_into_another_repository(self):
        from research_automation.control_plane.evidence_learning import (
            LearningCommitAuthorizationError,
            LearningCommitService,
        )

        with TemporaryDirectory() as first_tmp, TemporaryDirectory() as second_tmp:
            first = Path(first_tmp)
            second = Path(second_tmp)
            report, binding, _, _, _ = self._authority_fixture(first)
            shutil.copytree(first, second, dirs_exist_ok=True)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaisesRegex(
                    LearningCommitAuthorizationError,
                    "repository root",
                ):
                    LearningCommitService(repository_root=second).commit(report)

    def test_terminal_authority_report_is_required_before_any_write(self):
        from research_automation.control_plane.evidence_learning import (
            LearningCommitAuthorizationError,
            LearningCommitService,
        )
        from research_automation.control_plane.stores import TaskReportAuthorityError

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                side_effect=TaskReportAuthorityError("forged"),
            ):
                with self.assertRaises(LearningCommitAuthorizationError):
                    LearningCommitService(repository_root=root).commit({})
            self.assertFalse((root / "research_state/control_plane/learning_packets").exists())

    def test_terminal_evidence_must_name_the_exact_learning_decision(self):
        from research_automation.control_plane.evidence_learning import (
            LearningCommitAuthorizationError,
            LearningCommitService,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            binding.terminal_evidence_ref = (
                "research_state/control_plane/p4/fixtures/other.json"
            )
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaisesRegex(
                    LearningCommitAuthorizationError,
                    "terminal evidence",
                ):
                    LearningCommitService(repository_root=root).commit(report)
            self.assertFalse(
                (root / "research_state/control_plane/learning_packets").exists()
            )

    def test_valid_authority_projection_is_content_addressed_and_idempotent(self):
        from research_automation.control_plane.evidence_learning import LearningCommitService

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            service = LearningCommitService(repository_root=root)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                first = service.commit(report)
                second = service.commit(report)
                ledger = service.rebuild_ledger()
            self.assertEqual(first, second)
            self.assertEqual(ledger["packet_hashes"], [first])

    def test_existing_original_journal_schema_projects_without_migration(self):
        from research_automation.control_plane.evidence_learning import LearningCommitService

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            journal = root / "research_state/control_plane/learning_commit.sqlite3"
            journal.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(journal)
            connection.execute(
                "CREATE TABLE learning_commit_events ("
                "sequence INTEGER PRIMARY KEY, packet_hash TEXT NOT NULL UNIQUE, "
                "actor_id TEXT NOT NULL, previous_event_sha256 TEXT NOT NULL, "
                "event_sha256 TEXT NOT NULL UNIQUE)"
            )
            connection.execute(
                "CREATE TABLE learning_commit_head ("
                "singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1), "
                "head_sequence INTEGER NOT NULL, head_event_sha256 TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO learning_commit_head VALUES (1, 0, ?)",
                ("0" * 64,),
            )
            connection.commit()
            connection.close()
            service = LearningCommitService(repository_root=root)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                packet_hash = service.commit(report)
                ledger = service.rebuild_ledger()
            self.assertEqual(ledger["packet_hashes"], [packet_hash])
            self.assertEqual(ledger["orphan_packet_hashes"], [])

    def test_populated_v1_journal_remains_legacy_unaudited_before_v2_append(self):
        from research_automation.control_plane.evidence_learning import LearningCommitService

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            directory = root / "research_state/control_plane/learning_packets"
            directory.mkdir(parents=True)
            legacy_packet = {
                "schema_version": "control_plane.learning_packet.v1",
                "claim": {"kind": "NEGATIVE"},
                "evidence_refs": [
                    {"ref": "legacy/result.json", "sha256": "a" * 64}
                ],
                "access_event_refs": ["legacy-event-001"],
                "taint_refs": [],
                "audit_grade": "PASS",
                "invalidation_codes": [],
            }
            legacy_raw = canonical_bytes(legacy_packet)
            legacy_hash = hashlib.sha256(legacy_raw).hexdigest()
            (directory / f"{legacy_hash}.json").write_bytes(legacy_raw)
            event_payload = {
                "schema_version": "control_plane.learning_commit_event.v1",
                "sequence": 1,
                "packet_hash": legacy_hash,
                "actor_id": "legacy-runner",
                "previous_event_sha256": "0" * 64,
            }
            legacy_event_hash = hashlib.sha256(
                b"control_plane.learning_commit_event.v1\0"
                + canonical_bytes(event_payload)
            ).hexdigest()
            journal = root / "research_state/control_plane/learning_commit.sqlite3"
            connection = sqlite3.connect(journal)
            connection.execute(
                "CREATE TABLE learning_commit_events ("
                "sequence INTEGER PRIMARY KEY, packet_hash TEXT NOT NULL UNIQUE, "
                "actor_id TEXT NOT NULL, previous_event_sha256 TEXT NOT NULL, "
                "event_sha256 TEXT NOT NULL UNIQUE)"
            )
            connection.execute(
                "CREATE TABLE learning_commit_head ("
                "singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1), "
                "head_sequence INTEGER NOT NULL, head_event_sha256 TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO learning_commit_events VALUES (1, ?, ?, ?, ?)",
                (legacy_hash, "legacy-runner", "0" * 64, legacy_event_hash),
            )
            connection.execute(
                "INSERT INTO learning_commit_head VALUES (1, 1, ?)",
                (legacy_event_hash,),
            )
            connection.commit()
            connection.close()
            service = LearningCommitService(repository_root=root)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                trusted_hash = service.commit(report)
                ledger = service.rebuild_ledger()
            self.assertEqual(ledger["packet_hashes"], [trusted_hash])
            self.assertEqual(
                ledger["legacy_unaudited_packet_hashes"],
                [legacy_hash],
            )

    def test_real_authority_backed_task_report_projects_without_mocking_reader(self):
        from research_automation.control_plane.contracts import (
            Actor,
            Phase,
            SideEffect,
        )
        from research_automation.control_plane.evidence_learning import LearningCommitService
        from research_automation.control_plane.task_reports import build_task_report_v2
        from research_automation.control_plane import stores as stores_module

        root_secret = "test-only-authority-root-capability-0123456789abcdef"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture_report, _, _, _, _ = self._authority_fixture(root)
            now = datetime(2026, 7, 30, 8, 0, tzinfo=timezone.utc)
            actor = Actor("learning-controller", "automation", "integration-run")
            issuer = Actor("evidence-reviewer", "automation", "review-run")
            identity = stores_module.AuthorityIdentity(
                "a" * 64,
                "b" * 64,
                "c" * 64,
            )
            task_spec = {
                "task_id": "P4-LEARNING-COMMIT",
                "objective": fixture_report["objective"],
                "dependencies": [],
                "idempotency_key": "p4-learning-integration-001",
                "task_spec_ref": "research_state/control_plane/p4/task_specs/learning.json",
                "task_spec_sha256": "d" * 64,
                "requirements": fixture_report["requirements"],
                "allowed_files": fixture_report["allowed_files"],
                "forbidden_files": fixture_report["forbidden_files"],
                "baseline_ref": fixture_report["baseline_ref"],
                "baseline_sha256": fixture_report["baseline_sha256"],
                "input_evidence_refs": fixture_report["input_evidence_refs"],
            }
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
                _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
            ):
                stores_module._trusted_bootstrap(root_secret=root_secret)
                authority = stores_module._AuthorityStore(
                    root_secret=root_secret,
                    clock=lambda: now,
                )
                policy_spec = {
                    "task_id": "P0-POLICY-ACTIVATE",
                    "objective": "Activate fixture entry policy.",
                    "dependencies": [],
                    "idempotency_key": "p0-policy-fixture-001",
                    "task_spec_ref": "research_state/control_plane/p0/policy.json",
                    "task_spec_sha256": "1" * 64,
                    "requirements": {
                        "required_test_receipt_ids": [],
                        "required_review_receipt_ids": [],
                        "required_evidence_ids": [],
                    },
                    "allowed_files": ["research_state/control_plane/policies/"],
                    "forbidden_files": ["data/"],
                    "baseline_ref": "research_state/control_plane/p0/baseline.json",
                    "baseline_sha256": "2" * 64,
                    "input_evidence_refs": [],
                }
                policy_envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0-policy-fixture",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                policy_grant = authority.claim_authorization(
                    policy_envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0-policy-fixture",
                    actor=actor,
                    identity=identity,
                )
                policy_ticket = authority._issue_task_ticket(
                    policy_grant,
                    policy_spec,
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                policy_lease = authority._begin_task(policy_ticket)
                authority._activate_reviewed_entry_policy(
                    policy_lease,
                    reviewer=Actor("policy-reviewer", "llm", "policy-review"),
                    policy_sha256="3" * 64,
                    policy_payload_sha256="4" * 64,
                    inventory_payload_sha256="5" * 64,
                    review_receipt_sha256="6" * 64,
                    expected_active_sha256=None,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P4,
                    attempt_id="p4-integration",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P4,
                    expected_attempt_id="p4-integration",
                    actor=actor,
                    identity=identity,
                )
                ticket = authority._issue_task_ticket(
                    grant,
                    task_spec,
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                lease = authority._begin_task(ticket)
                for receipt in task_spec["input_evidence_refs"]:
                    attestation = authority._attest_task_receipt(
                        lease,
                        receipt_kind="EVIDENCE",
                        issuer=issuer,
                        payload=receipt,
                    )
                    authority._record_task_receipt(
                        lease,
                        attestation=attestation,
                    )
                terminal = authority._finish_task(
                    lease,
                    outcome="SUCCEEDED",
                    evidence_ref=next(
                        item["evidence_ref"]
                        for item in task_spec["input_evidence_refs"]
                        if item["evidence_id"] == "learning-decision"
                    ),
                )
                report = build_task_report_v2(
                    {
                        "plan_version": "V3.4.2-P0R2",
                        "phase": "P4",
                        "task_id": task_spec["task_id"],
                        "attempt_id": "p4-integration",
                        "authorization_ref": ticket.authorization_ref,
                        "ticket_id": ticket.ticket_id,
                        "identity_binding": {
                            "plan_hash": identity.plan_hash,
                            "scope_hash": identity.scope_hash,
                            "instruction_policy_hash": identity.instruction_policy_hash,
                        },
                        **task_spec,
                        "test_receipts": [],
                        "review_receipts": [],
                        "review_findings": [],
                        "changed_files": [],
                        "external_invocations": [],
                        "side_effect_summary": {
                            "observed": ["WRITE_CONTROL_PLANE"],
                            "unauthorized": [],
                        },
                        "ticket_state": terminal.state,
                        "started_at": terminal.started_at.isoformat().replace(
                            "+00:00", "Z"
                        ),
                        "completed_at": terminal.completed_at.isoformat().replace(
                            "+00:00", "Z"
                        ),
                    }
                )
                service = LearningCommitService(repository_root=root)
                packet_hash = service.commit(report)
                ledger = service.rebuild_ledger()
            self.assertEqual(ledger["packet_hashes"], [packet_hash])

    def test_callers_cannot_self_approve_by_omitting_protocol_binding(self):
        from research_automation.control_plane.evidence_learning import (
            LearningCommitAuthorizationError,
            LearningCommitService,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            report["input_evidence_refs"] = [
                ref for ref in report["input_evidence_refs"]
                if ref["evidence_id"] != "approved-protocol"
            ]
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaises(LearningCommitAuthorizationError):
                    LearningCommitService(repository_root=root).commit(report)

    def test_decision_must_match_authority_bound_recomputation(self):
        from research_automation.control_plane.evidence_learning import LearningCommitService

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, refs = self._authority_fixture(root)
            decision_path = root / refs["learning-decision"]["evidence_ref"]
            decision = json.loads(decision_path.read_bytes())
            decision["claim"]["kind"] = "POSITIVE"
            raw = canonical_bytes(decision)
            decision_path.write_bytes(raw)
            for reference in report["input_evidence_refs"]:
                if reference["evidence_id"] == "learning-decision":
                    reference["evidence_sha256"] = hashlib.sha256(raw).hexdigest()
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaisesRegex(ValueError, "differs"):
                    LearningCommitService(repository_root=root).commit(report)

    def test_raw_log_claim_is_rejected(self):
        from research_automation.control_plane.evidence_learning import LearningCommitService

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(
                root,
                claim={"kind": "NEGATIVE", "raw_log": "secret"},
            )
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaises(ValueError):
                    LearningCommitService(repository_root=root).commit(report)

    def test_concurrent_duplicate_projection_has_one_ordered_event(self):
        from research_automation.control_plane.evidence_learning import LearningCommitService

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            service = LearningCommitService(repository_root=root)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with ThreadPoolExecutor(max_workers=8) as pool:
                    hashes = list(pool.map(lambda _: service.commit(report), range(8)))
                ledger = service.rebuild_ledger()
            self.assertEqual(len(set(hashes)), 1)
            self.assertEqual(ledger["event_count"], 1)

    def test_packet_publication_tolerates_transient_windows_read_denial(self):
        from research_automation.control_plane.evidence_learning import (
            LearningCommitService,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            service = LearningCommitService(repository_root=root)
            original_read_bytes = Path.read_bytes
            denials = 0

            def deny_new_packet_twice(path: Path) -> bytes:
                nonlocal denials
                if path.parent.name == "learning_packets" and denials < 2:
                    denials += 1
                    raise PermissionError("simulated transient sharing denial")
                return original_read_bytes(path)

            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ), patch.object(Path, "read_bytes", deny_new_packet_twice):
                packet_hash = service.commit(report)

            self.assertEqual(denials, 2)
            self.assertTrue(
                (
                    root
                    / "research_state/control_plane/learning_packets"
                    / f"{packet_hash}.json"
                ).is_file()
            )

    def test_authority_verification_and_projection_share_one_report_snapshot(self):
        from research_automation.control_plane.evidence_learning import (
            LearningCommitService,
        )

        class MutatingReport(dict):
            armed = False
            mutated = False

            def get(self, key, default=None):
                if key == "phase" and self.armed and not self.mutated:
                    self["input_evidence_refs"] = deepcopy(
                        self.alternate_input_evidence_refs
                    )
                    self.mutated = True
                return super().get(key, default)

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            original_refs = deepcopy(report["input_evidence_refs"])
            alternate_refs = deepcopy(original_refs)
            for reference in alternate_refs:
                if reference["evidence_id"] == "learning-decision":
                    continue
                original = root / reference["evidence_ref"]
                alternate = original.with_name(f"alternate-{original.name}")
                alternate.write_bytes(original.read_bytes())
                reference["evidence_ref"] = alternate.relative_to(root).as_posix()
            mutable = MutatingReport(report)
            mutable.alternate_input_evidence_refs = alternate_refs

            def verify_and_arm(candidate):
                self.assertEqual(
                    candidate["input_evidence_refs"],
                    original_refs,
                )
                mutable.armed = True
                return binding

            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                side_effect=verify_and_arm,
            ):
                packet_hash = LearningCommitService(
                    repository_root=root
                ).commit(mutable)

            packet = json.loads(
                (
                    root
                    / "research_state/control_plane/learning_packets"
                    / f"{packet_hash}.json"
                ).read_bytes()
            )
            self.assertEqual(
                packet["authority_task_report"]["input_evidence_refs"],
                original_refs,
            )

    def test_rebuild_reports_packet_without_commit_event_as_orphan(self):
        from research_automation.control_plane.evidence_learning import LearningCommitService

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            service = LearningCommitService(repository_root=root)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                packet_hash = service.commit(report)
            (root / "research_state/control_plane/learning_commit.sqlite3").unlink()
            self.assertEqual(service.rebuild_ledger()["orphan_packet_hashes"], [packet_hash])

    def test_recomputed_journal_tamper_fails_authority_anchor(self):
        from research_automation.control_plane.evidence_learning import (
            LearningCommitService,
            _authority_order_key,
            _event_sha256,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report, binding, _, _, _ = self._authority_fixture(root)
            service = LearningCommitService(repository_root=root)
            authority_patch = patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            )
            with authority_patch:
                service.commit(report)
            journal = root / "research_state/control_plane/learning_commit.sqlite3"
            connection = sqlite3.connect(journal)
            try:
                row = connection.execute(
                    "SELECT sequence, packet_hash, previous_event_sha256 "
                    "FROM learning_commit_events"
                ).fetchone()
                payload = {
                    "schema_version": "control_plane.learning_commit_event.v2",
                    "sequence": row[0],
                    "packet_hash": row[1],
                    "ticket_id": binding.ticket_id,
                    "report_payload_sha256": binding.report_payload_sha256,
                    "authority_order_key": _authority_order_key(
                        report["completed_at"],
                        binding.ticket_id,
                    ),
                    "actor_id": "forged",
                    "previous_event_sha256": row[2],
                }
                forged_hash = _event_sha256(payload)
                connection.execute(
                    "UPDATE learning_commit_events SET actor_id = ?, event_sha256 = ?",
                    ("forged", forged_hash),
                )
                connection.execute(
                    "UPDATE learning_commit_head SET head_event_sha256 = ?",
                    (forged_hash,),
                )
                connection.commit()
            finally:
                connection.close()
            with authority_patch:
                with self.assertRaisesRegex(ValueError, "anchor mismatch"):
                    service.rebuild_ledger()

    def test_reordered_rehashed_events_fail_authority_order(self):
        from research_automation.control_plane.evidence_learning import (
            LearningCommitService,
            _authority_order_key,
            _event_sha256,
        )
        from research_automation.control_plane.task_reports import (
            task_report_v2_payload_sha256,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            first_report, first_binding, _, _, _ = self._authority_fixture(root)
            second_report = deepcopy(first_report)
            second_report["ticket_id"] = "ticket-learning-002"
            second_report["idempotency_key"] = "p4-learning-commit-002"
            second_report["completed_at"] = "2026-07-30T08:02:00Z"
            second_report["report_payload_sha256"] = task_report_v2_payload_sha256(
                second_report
            )
            second_binding = SimpleNamespace(
                ticket_id=second_report["ticket_id"],
                report_payload_sha256=second_report["report_payload_sha256"],
                actor_id=first_binding.actor_id,
                allowed_side_effects=first_binding.allowed_side_effects,
                ticket_state="SUCCEEDED",
                terminal_evidence_ref=first_binding.terminal_evidence_ref,
            )
            bindings = {
                first_report["ticket_id"]: first_binding,
                second_report["ticket_id"]: second_binding,
            }
            service = LearningCommitService(repository_root=root)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                side_effect=lambda report: bindings[report["ticket_id"]],
            ):
                first_hash = service.commit(first_report)
                second_hash = service.commit(second_report)
            reports = {
                first_hash: first_report,
                second_hash: second_report,
            }
            journal = root / "research_state/control_plane/learning_commit.sqlite3"
            connection = sqlite3.connect(journal)
            try:
                connection.execute(
                    "UPDATE learning_commit_events SET sequence = 0 WHERE sequence = 1"
                )
                connection.execute(
                    "UPDATE learning_commit_events SET sequence = 1 WHERE sequence = 2"
                )
                connection.execute(
                    "UPDATE learning_commit_events SET sequence = 2 WHERE sequence = 0"
                )
                rows = connection.execute(
                    "SELECT sequence, packet_hash, actor_id "
                    "FROM learning_commit_events ORDER BY sequence"
                ).fetchall()
                previous = "0" * 64
                for row in rows:
                    report = reports[row[1]]
                    authority = bindings[report["ticket_id"]]
                    payload = {
                        "schema_version": "control_plane.learning_commit_event.v2",
                        "sequence": row[0],
                        "packet_hash": row[1],
                        "ticket_id": authority.ticket_id,
                        "report_payload_sha256": authority.report_payload_sha256,
                        "authority_order_key": _authority_order_key(
                            report["completed_at"],
                            authority.ticket_id,
                        ),
                        "actor_id": row[2],
                        "previous_event_sha256": previous,
                    }
                    current = _event_sha256(payload)
                    connection.execute(
                        "UPDATE learning_commit_events SET previous_event_sha256 = ?, "
                        "event_sha256 = ? WHERE sequence = ?",
                        (previous, current, row[0]),
                    )
                    previous = current
                connection.execute(
                    "UPDATE learning_commit_head SET head_event_sha256 = ?",
                    (previous,),
                )
                connection.commit()
            finally:
                connection.close()
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                side_effect=lambda report: bindings[report["ticket_id"]],
            ):
                with self.assertRaisesRegex(ValueError, "Authority order"):
                    service.rebuild_ledger()

    def test_equivalent_timestamp_offsets_use_utc_authority_order(self):
        from research_automation.control_plane.evidence_learning import LearningCommitService
        from research_automation.control_plane.task_reports import (
            task_report_v2_payload_sha256,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            first, first_binding, _, _, _ = self._authority_fixture(root)
            first["completed_at"] = "2026-07-30T16:01:00+08:00"
            first["report_payload_sha256"] = task_report_v2_payload_sha256(first)
            first_binding.report_payload_sha256 = first["report_payload_sha256"]
            second = deepcopy(first)
            second["ticket_id"] = "ticket-learning-002"
            second["idempotency_key"] = "p4-learning-commit-002"
            second["completed_at"] = "2026-07-30T08:02:00Z"
            second["report_payload_sha256"] = task_report_v2_payload_sha256(second)
            second_binding = SimpleNamespace(
                ticket_id=second["ticket_id"],
                report_payload_sha256=second["report_payload_sha256"],
                actor_id=first_binding.actor_id,
                allowed_side_effects=first_binding.allowed_side_effects,
                ticket_state="SUCCEEDED",
                terminal_evidence_ref=first_binding.terminal_evidence_ref,
            )
            bindings = {
                first["ticket_id"]: first_binding,
                second["ticket_id"]: second_binding,
            }
            service = LearningCommitService(repository_root=root)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                side_effect=lambda report: bindings[report["ticket_id"]],
            ):
                service.commit(first)
                service.commit(second)
                ledger = service.rebuild_ledger()
            self.assertEqual(ledger["event_count"], 2)


if __name__ == "__main__":
    unittest.main()
