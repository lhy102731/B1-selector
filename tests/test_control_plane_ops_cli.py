from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import run_research
from research_automation.control_plane import operations


class ReadOnlyOpsCliTests(unittest.TestCase):
    """P7R2-T4: read-only run_research.py status|audit|doctor|export commands."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.journal = self.root / "journal.sqlite3"
        connection = sqlite3.connect(self.journal)
        try:
            connection.execute(
                """
                CREATE TABLE events (
                    sequence INTEGER PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            for index in range(1, 4):
                connection.execute(
                    "INSERT INTO events(sequence, event_type, aggregate_id, payload_json, created_at) VALUES (?, 'TEST_EVENT', ?, '{}', '2026-08-10T00:00:00+00:00')",
                    (index, f"agg-{index}"),
                )
            connection.commit()
        finally:
            connection.close()

    def test_status_is_readonly_and_never_builds_runner(self) -> None:
        with patch.object(run_research, "_orchestrator_class", side_effect=AssertionError("Runner constructed")):
            status = run_research.main(["status"])
        self.assertEqual(0, status)

    def test_status_renders_required_surfaces(self) -> None:
        with patch("research_automation.control_plane.operations.journal_path", return_value=self.journal):
            with patch.object(run_research, "_orchestrator_class", side_effect=AssertionError("Runner constructed")):
                with patch("sys.stdout") as fake_stdout:
                    run_research.main(["status"])
            output = ""
            for call in fake_stdout.write.call_args_list:
                output += call[0][0]
        for surface in ("campaign", "budget", "lease", "roster", "generation", "evidence", "access", "usage", "publication", "failure"):
            self.assertIn(surface, output.lower())

    def test_audit_export_is_deterministic_manifest_without_secrets_or_holdout(self) -> None:
        with patch("research_automation.control_plane.operations.journal_path", return_value=self.journal):
            with patch.object(run_research, "_orchestrator_class", side_effect=AssertionError("Runner constructed")):
                first = operations.read_only_audit_manifest(self.root)
                second = operations.read_only_audit_manifest(self.root)
        self.assertEqual(first, second)
        text = json.dumps(first, sort_keys=True)
        self.assertNotIn("secret", text.lower())
        self.assertNotIn("holdout", text.lower())
        self.assertIn("events", text)

    def test_doctor_reports_blocked_and_failure_causes(self) -> None:
        with patch("research_automation.control_plane.operations.journal_path", return_value=self.journal):
            with patch.object(run_research, "_orchestrator_class", side_effect=AssertionError("Runner constructed")):
                report = operations.read_only_doctor_report(self.root)
        self.assertIn("failure_causes", report)
        self.assertIn("blocked", report)
        self.assertIsInstance(report["failure_causes"], list)

    def test_export_returns_references_without_writing_repository(self) -> None:
        before = set(Path(run_research.__file__).parent.rglob("*.json"))
        with patch("research_automation.control_plane.operations.journal_path", return_value=self.journal):
            with patch.object(run_research, "_orchestrator_class", side_effect=AssertionError("Runner constructed")):
                bundle = operations.read_only_export_bundle(self.root)
        after = set(Path(run_research.__file__).parent.rglob("*.json"))
        self.assertEqual(before, after)
        self.assertIn("journal", bundle)
        self.assertIn("journal_sha256", bundle)

    def test_readonly_surface_rejects_runner_and_holdout_paths(self) -> None:
        with self.assertRaises(operations.ProtectedStoreError):
            operations.read_only_status(Path("research_state/control_plane/operational/operational.sqlite3"))
        with self.assertRaises(operations.ProtectedStoreError):
            operations.read_only_audit_manifest(Path("research_state/control_plane/authority/authority.sqlite3"))

    def test_cli_entry_guards_still_pass(self) -> None:
        status = run_research.main(["status"])
        self.assertEqual(0, status)


class AuditBundleTests(unittest.TestCase):
    """P7R2-T5: deterministic audit bundle with exclusion of secrets, raw
    labels, Final Holdout, and unrelated large files."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "normal.txt").write_text("hello audit", encoding="utf-8")
        (self.root / "secret.env").write_text("API_KEY=super-secret-value", encoding="utf-8")
        (self.root / "raw_labels.csv").write_text("raw_label,value", encoding="utf-8")
        (self.root / "final_holdout.parquet").write_text("holdout-bytes", encoding="utf-8")
        (self.root / "big.bin").write_bytes(b"x" * (2_000_000))
        (self.root / "nested").mkdir()
        (self.root / "nested" / "reference.json").write_text('{"ref": "ok"}', encoding="utf-8")
        journal = self.root / "journal.sqlite3"
        connection = sqlite3.connect(journal)
        try:
            connection.execute("CREATE TABLE events (sequence INTEGER PRIMARY KEY, event_type TEXT NOT NULL)")
            connection.execute("INSERT INTO events(sequence, event_type) VALUES (1, 'E')")
            connection.commit()
        finally:
            connection.close()

    def test_bundle_is_deterministic(self) -> None:
        first = operations.build_audit_bundle(self.root)
        second = operations.build_audit_bundle(self.root)
        self.assertEqual(first, second)
        self.assertEqual(
            json.dumps(first, sort_keys=True),
            json.dumps(second, sort_keys=True),
        )

    def test_bundle_excludes_secret_raw_holdout_and_large(self) -> None:
        bundle = operations.build_audit_bundle(self.root)
        refs = [entry["path"] for entry in bundle["entries"]]
        self.assertIn("normal.txt", refs)
        self.assertIn("nested/reference.json", refs)
        self.assertIn("journal.sqlite3", refs)
        for excluded in ("secret.env", "raw_labels.csv", "final_holdout.parquet", "big.bin"):
            self.assertNotIn(excluded, refs)
        text = json.dumps(bundle, sort_keys=True).lower()
        self.assertNotIn("super-secret-value", text)
        self.assertNotIn("holdout-bytes", text)

    def test_bundle_has_hashes_and_references(self) -> None:
        bundle = operations.build_audit_bundle(self.root)
        self.assertEqual("control_plane.p7r2_audit_bundle.v1", bundle["schema_version"])
        entry = next(item for item in bundle["entries"] if item["path"] == "normal.txt")
        self.assertEqual(64, len(entry["sha256"]))
        self.assertEqual(entry["size_bytes"], (self.root / "normal.txt").stat().st_size)
        self.assertEqual("secrets", next(item for item in bundle["exclusions"] if item["path"] == "secret.env")["reason"])

    def test_bundle_rejects_protected_roots(self) -> None:
        for protected in ("research_state/control_plane/authority", "research_state/control_plane/operational"):
            with self.assertRaises(operations.ProtectedStoreError):
                operations.build_audit_bundle(Path(protected))



class _FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class BackfillAdapterTests(unittest.TestCase):
    """P7R2-T6: rate-limited, sharded, low-priority, pausable historical
    backfill adapter against synthetic fixtures only."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.processed: list[int] = []

    def test_build_plan_shards_total_and_low_priority(self) -> None:
        adapter = operations.BackfillAdapter(self.root)
        plan = adapter.build_plan(total_items=25, shard_count=4)
        self.assertEqual(25, plan.total_items)
        self.assertEqual("low", plan.priority)
        self.assertEqual(4, len(plan.shards))
        self.assertEqual((0, 7), (plan.shards[0].start, plan.shards[0].end))
        self.assertEqual(25, sum(shard.item_count for shard in plan.shards))
        with self.assertRaises(ValueError):
            adapter.build_plan(total_items=10, shard_count=2, priority="high")

    def test_run_processes_all_shards_with_injected_worker(self) -> None:
        adapter = operations.BackfillAdapter(
            self.root,
            limiter=operations.TokenBucketLimiter(capacity=1000, refill_per_second=1.0),
        )
        plan = adapter.build_plan(total_items=10, shard_count=2)
        result = adapter.run(lambda item_index: self.processed.append(item_index))
        self.assertEqual(list(range(10)), self.processed)
        self.assertEqual("COMPLETED", result.status)
        self.assertFalse(result.paused)
        self.assertFalse(result.throttled)
        self.assertEqual(2, len(result.completed_shard_ids))
        self.assertEqual(10, result.processed_items)
        self.assertFalse(adapter.status()["real_backfill"])

    def test_token_bucket_limiter_enforces_capacity(self) -> None:
        clock = _FakeClock()
        limiter = operations.TokenBucketLimiter(
            capacity=3,
            refill_per_second=1.0,
            clock=clock,
        )
        self.assertTrue(limiter.try_acquire())
        self.assertTrue(limiter.try_acquire())
        self.assertTrue(limiter.try_acquire())
        self.assertFalse(limiter.try_acquire())
        clock.advance(2.0)
        self.assertTrue(limiter.try_acquire())
        self.assertTrue(limiter.try_acquire())
        self.assertFalse(limiter.try_acquire())

    def test_run_is_throttled_and_resumes_from_checkpoint(self) -> None:
        clock = _FakeClock()
        limiter = operations.TokenBucketLimiter(
            capacity=2,
            refill_per_second=1.0,
            clock=clock,
        )
        adapter = operations.BackfillAdapter(self.root, limiter=limiter)
        plan = adapter.build_plan(total_items=5, shard_count=1)
        first = adapter.run(lambda item_index: self.processed.append(item_index))
        self.assertEqual([0, 1], self.processed)
        self.assertTrue(first.throttled)
        self.assertFalse(first.paused)
        self.assertEqual("THROTTLED", first.status)
        self.assertEqual([], list(first.completed_shard_ids))
        clock.advance(10.0)
        second = adapter.run(lambda item_index: self.processed.append(item_index))
        self.assertEqual([0, 1, 2, 3], self.processed)
        self.assertTrue(second.throttled)
        clock.advance(10.0)
        third = adapter.run(lambda item_index: self.processed.append(item_index))
        self.assertEqual([0, 1, 2, 3, 4], self.processed)
        self.assertEqual("COMPLETED", third.status)
        self.assertEqual(1, len(third.completed_shard_ids))

    def test_pause_between_shards_and_resume_continues(self) -> None:
        adapter = operations.BackfillAdapter(self.root)
        plan = adapter.build_plan(total_items=10, shard_count=2)

        def worker(item_index: int) -> None:
            self.processed.append(item_index)
            if len(self.processed) == 5:
                adapter.pause()

        first = adapter.run(worker)
        self.assertTrue(first.paused)
        self.assertFalse(first.throttled)
        self.assertEqual("PAUSED", first.status)
        self.assertEqual([0, 1, 2, 3, 4], self.processed)
        self.assertEqual(1, len(first.completed_shard_ids))
        adapter.resume()
        second = adapter.run(worker)
        self.assertEqual(list(range(10)), self.processed)
        self.assertEqual("COMPLETED", second.status)

    def test_adapter_rejects_protected_and_repository_roots(self) -> None:
        for protected in ("research_state/control_plane/authority", "research_state/control_plane/operational"):
            with self.assertRaises(operations.ProtectedStoreError):
                operations.BackfillAdapter(Path(protected))
        repository_root = Path(__file__).resolve().parents[1]
        with self.assertRaises(operations.ProtectedStoreError):
            operations.BackfillAdapter(repository_root)

    def test_adapter_requires_existing_synthetic_fixture(self) -> None:
        with self.assertRaises(FileNotFoundError):
            operations.BackfillAdapter(self.root / "missing-fixture")


class ConflictExplanationTests(unittest.TestCase):
    """P7R2-T8: human-readable conflict/blocked explanations with direct
    evidence references on a synthetic fixture surface."""

    def test_conflict_explanation_is_human_readable_with_evidence_refs(self) -> None:
        result = operations.explain_conflict(
            {
                "conflict_id": "conflict-001",
                "kind": "SCOPE_OVERLAP",
                "summary": "two claims overlap on the same universe and time window",
                "evidence_refs": [
                    "research_state/control_plane/p7/attempts/p7-attempt-001/evidence/t1_completion_receipt.json",
                    "docs/superpowers/plans/2026-07-26-v342-07-operations.md",
                ],
            }
        )
        self.assertIsInstance(result["explanation"], str)
        self.assertIn("two claims overlap", result["explanation"])
        self.assertIn("SCOPE_OVERLAP", result["explanation"])
        self.assertEqual(
            [
                "docs/superpowers/plans/2026-07-26-v342-07-operations.md",
                "research_state/control_plane/p7/attempts/p7-attempt-001/evidence/t1_completion_receipt.json",
            ],
            result["evidence_references"],
        )

    def test_blocked_explanation_includes_direct_evidence(self) -> None:
        result = operations.explain_blocked(
            {
                "blocked_id": "blocked-001",
                "reason": "campaign boundary rejected for legacy surface",
                "evidence_refs": [
                    "research_state/control_plane/p7/attempts/p7-attempt-001/evidence/t1_completion_receipt.json"
                ],
            }
        )
        self.assertIn("campaign boundary rejected", result["explanation"])
        self.assertEqual(
            ["research_state/control_plane/p7/attempts/p7-attempt-001/evidence/t1_completion_receipt.json"],
            result["evidence_references"],
        )

    def test_explanations_reject_malformed_input(self) -> None:
        with self.assertRaises(ValueError):
            operations.explain_conflict({"conflict_id": "conflict-001"})
        with self.assertRaises(ValueError):
            operations.explain_blocked({"blocked_id": "blocked-001"})
        with self.assertRaises(TypeError):
            operations.explain_conflict(None)
        with self.assertRaises(TypeError):
            operations.explain_blocked(None)

    def test_explanations_are_deterministic_and_synthetic(self) -> None:
        conflict = {
            "conflict_id": "conflict-001",
            "kind": "SCOPE_OVERLAP",
            "summary": "two claims overlap",
            "evidence_refs": ["evidence/a.json"],
        }
        blocked = {
            "blocked_id": "blocked-001",
            "reason": "campaign boundary",
            "evidence_refs": ["evidence/b.json"],
        }
        first_conflict = operations.explain_conflict(conflict)
        second_conflict = operations.explain_conflict(conflict)
        first_blocked = operations.explain_blocked(blocked)
        second_blocked = operations.explain_blocked(blocked)
        self.assertEqual(first_conflict, second_conflict)
        self.assertEqual(first_blocked, second_blocked)
        self.assertFalse(first_conflict["real_data"])
        self.assertFalse(first_blocked["real_data"])
        self.assertEqual("control_plane.p7r2_conflict_explanation.v1", first_conflict["schema_version"])
        self.assertEqual("control_plane.p7r2_blocked_explanation.v1", first_blocked["schema_version"])


if __name__ == "__main__":
    unittest.main()
