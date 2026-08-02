import tempfile
import unittest
from pathlib import Path
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
            ).finalize(artifact=artifact)
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
            ).finalize(artifact=artifact)
            self.assertEqual(result.evidence.verdict, "NO_MATERIAL_FINDING")
            self.assertIsNone(result.packet_hash)
            self.assertFalse((root / "research_state").exists())

    def test_valid_evidence_cannot_commit_without_terminal_authority_report(self):
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
                controller.finalize(artifact=artifact)
            self.assertFalse((root / "research_state").exists())

    def test_valid_evidence_projects_through_terminal_authority_report(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceAdapter,
            LearningCommitService,
        )
        from research_automation.control_plane.runner_control import P4RunController
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
            authority_report = {"ticket_id": "fixture-terminal-report"}
            with patch.object(
                LearningCommitService,
                "commit",
                return_value="f" * 64,
            ):
                first = controller.finalize(
                    artifact=artifact,
                    authority_task_report=authority_report,
                )
                second = controller.finalize(
                    artifact=artifact,
                    authority_task_report=authority_report,
                )
            self.assertEqual(first.packet_hash, second.packet_hash)
            self.assertEqual(first.packet_hash, "f" * 64)


if __name__ == "__main__":
    unittest.main()
