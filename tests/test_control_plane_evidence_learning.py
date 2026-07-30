import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

from research_automation.control_plane.contracts import Actor


class EvidenceLearningVerticalSliceTests(unittest.TestCase):
    def test_clean_complete_run_without_claim_is_no_material_finding(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        result = EvidenceAdapter().evaluate(
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

        result = EvidenceAdapter().evaluate(
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
        self.assertFalse(result.promotion_eligible)

    def test_invalid_evidence_cannot_commit(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceResult,
            LearningCommitService,
        )

        invalid = EvidenceResult(
            "EVIDENCE_INVALID", "UNKNOWN", "INVALID", "UNKNOWN", False,
            (), (), (), ("MISSING_STATUS",),
        )
        with TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                LearningCommitService(repository_root=Path(tmp)).commit(
                    invalid, {"kind": "POSITIVE"},
                    Actor("test", "automation", "test-invocation"),
                )

    def test_valid_packet_is_content_addressed_and_idempotent(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceResult,
            LearningCommitService,
        )

        valid = EvidenceResult(
            "VALID", "CONFORMING", "PASS", "POSITIVE", True,
            ({"ref": "evidence.json", "sha256": "a" * 64},),
            (), (), (),
        )
        with TemporaryDirectory() as tmp:
            service = LearningCommitService(repository_root=Path(tmp))
            actor = Actor("test", "automation", "test-invocation")
            first = service.commit(valid, {"kind": "POSITIVE"}, actor)
            second = service.commit(valid, {"kind": "POSITIVE"}, actor)
            self.assertEqual(first, second)
            self.assertTrue((Path(tmp) / "research_state/control_plane/learning_packets" / f"{first}.json").is_file())


if __name__ == "__main__":
    unittest.main()
