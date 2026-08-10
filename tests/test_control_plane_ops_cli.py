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



if __name__ == "__main__":
    unittest.main()
