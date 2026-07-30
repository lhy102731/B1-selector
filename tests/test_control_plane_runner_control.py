import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


class P4RunControllerVerticalSliceTests(unittest.TestCase):
    def test_runner_reported_pass_cannot_bypass_semantic_evidence_check(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceAdapter,
            LearningCommitService,
        )
        from research_automation.control_plane.runner_control import P4RunController

        approved_protocol = {"label": "signal-day", "embargo_days": 5}
        approved_claim = {
            "kind": "NEGATIVE",
            "summary": "Fixture-only negative result.",
        }
        artifact = {
            "schema_version": "runner.artifact.v1",
            "runner": "fixture-runner",
            "runner_version": "1.0.0",
            "status": "COMPLETED",
            "runner_reported_pass": True,
            "claim": approved_claim,
            "protocol_conformance": "CONFORMING",
            "executed_protocol": {"label": "future-data", "embargo_days": 0},
            "artifact_refs": [
                {"ref": "fixtures/result.json", "sha256": "a" * 64}
            ],
            "access_event_ids": ["event:fixture-001"],
            "taint_refs": [],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = P4RunController(
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=approved_protocol,
                    approved_claim=approved_claim,
                ),
                learning_commit_service=LearningCommitService(repository_root=root),
            ).finalize(artifact=artifact, claim=approved_claim, actor=object())
            self.assertEqual(result.evidence.verdict, "EVIDENCE_INVALID")
            self.assertIsNone(result.packet_hash)
            self.assertFalse((root / "research_state").exists())

    def test_no_material_finding_is_recorded_as_outcome_without_empty_packet(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceAdapter,
            LearningCommitService,
        )
        from research_automation.control_plane.runner_control import P4RunController

        artifact = {
            "schema_version": "runner.artifact.v1",
            "runner": "fixture-runner",
            "runner_version": "1.0.0",
            "status": "COMPLETED",
            "claim": None,
            "protocol_conformance": "CONFORMING",
            "executed_protocol": {"label": "signal-day", "embargo_days": 5},
            "artifact_refs": [],
            "access_event_ids": ["event:fixture-002"],
            "taint_refs": [],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = P4RunController(
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "signal-day", "embargo_days": 5},
                ),
                learning_commit_service=LearningCommitService(repository_root=root),
            ).finalize(artifact=artifact, claim={}, actor=object())
            self.assertEqual(result.evidence.verdict, "NO_MATERIAL_FINDING")
            self.assertIsNone(result.packet_hash)
            self.assertFalse((root / "research_state").exists())

    def test_valid_evidence_cannot_commit_without_live_authority_lease(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceAdapter,
            LearningCommitService,
        )
        from research_automation.control_plane.runner_control import (
            P4RunController,
            RunAuthorizationError,
        )

        claim = {"kind": "NEGATIVE", "summary": "Fixture-only negative result."}
        artifact = {
            "schema_version": "runner.artifact.v1",
            "runner": "fixture-runner",
            "runner_version": "1.0.0",
            "status": "COMPLETED",
            "claim": claim,
            "protocol_conformance": "CONFORMING",
            "executed_protocol": {"label": "signal-day", "embargo_days": 5},
            "artifact_refs": [
                {"ref": "fixtures/result.json", "sha256": "b" * 64}
            ],
            "access_event_ids": ["event:fixture-003"],
            "taint_refs": [],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = P4RunController(
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "signal-day", "embargo_days": 5},
                    approved_claim=claim,
                ),
                learning_commit_service=LearningCommitService(repository_root=root),
            )
            with self.assertRaises(RunAuthorizationError):
                controller.finalize(artifact=artifact, claim=claim, actor=object())
            self.assertFalse((root / "research_state").exists())

    def test_valid_evidence_with_live_p4_lease_commits_once(self):
        from research_automation.control_plane.contracts import (
            Actor,
            Phase,
            SideEffect,
        )
        from research_automation.control_plane.evidence_learning import (
            EvidenceAdapter,
            LearningCommitService,
        )
        from research_automation.control_plane.runner_control import P4RunController
        from research_automation.control_plane import stores as stores_module

        actor = Actor("runner-controller", "automation", "fixture-invocation")
        identity = stores_module.AuthorityIdentity("a" * 64, "b" * 64, "c" * 64)
        lease = stores_module.TaskExecutionLease(
            lease_id="lease_fixture_runner_control",
            ticket_id="ticket_fixture_runner_control",
            grant_id="grant_fixture_runner_control",
            authorization_ref="auth_fixture_runner_control",
            phase=Phase.P4,
            attempt_id="p4-fixture",
            task_id="P4-RUNNER-CONTROL",
            entry_policy_sha256="d" * 64,
            allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
            actor=actor,
            identity=identity,
            _bearer_secret=stores_module._BearerSecret("fixture-secret"),
        )
        binding = SimpleNamespace(
            phase=Phase.P4,
            allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
            actor=actor,
        )
        claim = {"kind": "NEGATIVE", "summary": "Fixture-only negative result."}
        artifact = {
            "schema_version": "runner.artifact.v1",
            "runner": "fixture-runner",
            "runner_version": "1.0.0",
            "status": "COMPLETED",
            "claim": claim,
            "protocol_conformance": "CONFORMING",
            "executed_protocol": {"label": "signal-day", "embargo_days": 5},
            "artifact_refs": [
                {"ref": "fixtures/result.json", "sha256": "e" * 64}
            ],
            "access_event_ids": ["event:fixture-004"],
            "taint_refs": [],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = LearningCommitService(repository_root=root)
            controller = P4RunController(
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "signal-day", "embargo_days": 5},
                    approved_claim=claim,
                ),
                learning_commit_service=service,
            )
            with patch.object(
                stores_module.AuthorityReader,
                "execution_lease_binding",
                return_value=binding,
            ):
                first = controller.finalize(
                    artifact=artifact,
                    claim=claim,
                    actor=actor,
                    authority_lease=lease,
                )
                second = controller.finalize(
                    artifact=artifact,
                    claim=claim,
                    actor=actor,
                    authority_lease=lease,
                )
            self.assertEqual(first.packet_hash, second.packet_hash)
            self.assertEqual(service.rebuild_ledger()["event_count"], 1)


if __name__ == "__main__":
    unittest.main()
