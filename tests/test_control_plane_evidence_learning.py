import unittest
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
from pathlib import Path
import sqlite3

from research_automation.control_plane.contracts import Actor


class EvidenceLearningVerticalSliceTests(unittest.TestCase):
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
        self.assertFalse(result.promotion_eligible)

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
        self.assertEqual(result.verdict, "EVIDENCE_INVALID")
        self.assertIn("TAINTED_EVIDENCE", result.invalidation_codes)

    def test_unknown_runner_schema_is_invalid(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        result = EvidenceAdapter(known_runners=("test-runner",)).evaluate(
            {
                "schema_version": "runner.artifact.v1",
                "runner": "unknown-runner",
                "status": "COMPLETED",
                "claim": None,
                "protocol_conformance": "CONFORMING",
                "artifact_refs": (),
                "access_event_ids": (),
                "taint_refs": (),
            }
        )
        self.assertEqual(result.verdict, "EVIDENCE_INVALID")
        self.assertIn("UNKNOWN_RUNNER", result.invalidation_codes)

    def test_executed_protocol_mismatch_is_invalid(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        adapter = EvidenceAdapter(
            known_runners={"test-runner": "runner-v1"},
            approved_protocol={"label": "return_5d", "horizon_days": 5},
        )
        result = adapter.evaluate(
            {
                "schema_version": "runner.artifact.v1",
                "runner": "test-runner",
                "runner_version": "runner-v1",
                "status": "COMPLETED",
                "claim": None,
                "protocol_conformance": "CONFORMING",
                "executed_protocol": {"label": "return_10d", "horizon_days": 10},
                "artifact_refs": (),
                "access_event_ids": (),
                "taint_refs": (),
            }
        )
        self.assertEqual(result.verdict, "EVIDENCE_INVALID")
        self.assertIn("EXECUTED_PROTOCOL_MISMATCH", result.invalidation_codes)

    def test_matching_trusted_runner_protocol_and_approved_claim_is_valid(self):
        from research_automation.control_plane.evidence_learning import EvidenceAdapter

        protocol = {
            "label": "return_5d",
            "horizon_days": 5,
            "purge_days": 5,
            "embargo_days": 5,
            "generation_id": "generation-001",
            "code_sha256": "b" * 64,
        }
        claim = {"kind": "NEGATIVE"}
        adapter = EvidenceAdapter(
            known_runners={"test-runner": "runner-v1"},
            approved_protocol=protocol,
            approved_claim=claim,
        )
        result = adapter.evaluate(
            {
                "schema_version": "runner.artifact.v1",
                "runner": "test-runner",
                "runner_version": "runner-v1",
                "status": "COMPLETED",
                "claim": claim,
                "protocol_conformance": "CONFORMING",
                "executed_protocol": protocol,
                "artifact_refs": ({"ref": "evidence.json", "sha256": "a" * 64},),
                "access_event_ids": ("event-001",),
                "taint_refs": (),
            }
        )
        self.assertEqual(result.verdict, "VALID")
        self.assertTrue(result.promotion_eligible)

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

    def test_tainted_valid_evidence_cannot_commit(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceResult,
            LearningCommitService,
        )

        tainted = EvidenceResult(
            "VALID", "CONFORMING", "PASS", "POSITIVE", True,
            (), (), ("TEST_DERIVED",), (),
        )
        with TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                LearningCommitService(repository_root=Path(tmp)).commit(
                    tainted, {"kind": "POSITIVE"},
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
            ledger = service.rebuild_ledger()
            self.assertEqual(ledger["packet_count"], 1)
            self.assertEqual(ledger["packet_hashes"], [first])

    def test_learning_packet_rejects_raw_log_fields(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceResult,
            LearningCommitService,
        )

        valid = EvidenceResult(
            "VALID", "CONFORMING", "PASS", "NEGATIVE", True,
            (), (), (), (),
        )
        with TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                LearningCommitService(repository_root=Path(tmp)).commit(
                    valid,
                    {"kind": "NEGATIVE", "raw_log": "do not persist me"},
                    Actor("test", "automation", "test-invocation"),
                )

    def test_learning_packet_rejects_non_compact_evidence_refs(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceResult,
            LearningCommitService,
        )

        valid = EvidenceResult(
            "VALID", "CONFORMING", "PASS", "NEGATIVE", True,
            ({"ref": "evidence.json", "sha256": "a" * 64, "raw_log": "secret"},),
            (), (), (),
        )
        with TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                LearningCommitService(repository_root=Path(tmp)).commit(
                    valid, {"kind": "NEGATIVE"},
                    Actor("test", "automation", "test-invocation"),
                )

    def test_concurrent_duplicate_commit_has_one_ordered_event(self):
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
            with ThreadPoolExecutor(max_workers=8) as pool:
                hashes = list(pool.map(
                    lambda _: service.commit(valid, {"kind": "POSITIVE"}, actor),
                    range(8),
                ))
            self.assertEqual(len(set(hashes)), 1)
            ledger = service.rebuild_ledger()
            self.assertEqual(ledger["event_count"], 1)
            self.assertEqual(ledger["sequences"], [1])

    def test_rebuild_reports_packet_without_commit_event_as_orphan(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceResult,
            LearningCommitService,
        )

        valid = EvidenceResult(
            "VALID", "CONFORMING", "PASS", "PARTIAL", True,
            (), (), (), (),
        )
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = LearningCommitService(repository_root=root)
            packet_hash = service.commit(
                valid, {"kind": "PARTIAL"},
                Actor("test", "automation", "test-invocation"),
            )
            (root / "research_state/control_plane/learning_commit.sqlite3").unlink()
            ledger = service.rebuild_ledger()
            self.assertEqual(ledger["packet_count"], 0)
            self.assertEqual(ledger["orphan_packet_hashes"], [packet_hash])

    def test_rebuild_rejects_logically_tampered_journal_event(self):
        from research_automation.control_plane.evidence_learning import (
            EvidenceResult,
            LearningCommitService,
        )

        valid = EvidenceResult(
            "VALID", "CONFORMING", "PASS", "POSITIVE", True,
            (), (), (), (),
        )
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = LearningCommitService(repository_root=root)
            service.commit(
                valid, {"kind": "POSITIVE"},
                Actor("test", "automation", "test-invocation"),
            )
            journal = root / "research_state/control_plane/learning_commit.sqlite3"
            connection = sqlite3.connect(journal)
            connection.execute(
                "UPDATE learning_commit_events SET actor_id = 'forged' WHERE sequence = 1"
            )
            connection.commit()
            connection.close()
            with self.assertRaises(ValueError):
                service.rebuild_ledger()


if __name__ == "__main__":
    unittest.main()
