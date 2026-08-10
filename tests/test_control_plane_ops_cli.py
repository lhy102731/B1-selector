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


if __name__ == "__main__":
    unittest.main()
