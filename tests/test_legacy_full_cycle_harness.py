"""P6R2 T4: legacy full-cycle harness quarantine tests.

The legacy verify_full_research_cycle.py harness must be a no-effect
migration boundary: no subprocess, no run output directories, no real
research/backtest/campaign side effects, and a deterministic fail-closed
exit code.
"""
from __future__ import annotations

import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import research_automation.verify_full_research_cycle as harness


class LegacyHarnessQuarantineTests(unittest.TestCase):
    def test_module_does_not_import_subprocess(self):
        self.assertNotIn("subprocess", vars(harness))

    def test_module_does_not_import_legacy_runner(self):
        self.assertNotIn("AutonomousRunnerV1", vars(harness))
        self.assertNotIn("run_research_cycle", vars(harness))

    def test_main_fails_closed_without_subprocess(self):
        with mock.patch(
            "subprocess.run",
            side_effect=AssertionError("legacy harness must not spawn subprocesses"),
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                exit_code = harness.main()
        self.assertEqual(exit_code, harness.QUARANTINE_EXIT_CODE)
        self.assertEqual(exit_code, 3)
        output = out.getvalue()
        self.assertIn("QUARANTINED", output)
        self.assertIn("no-effect migration boundary", output)

    def test_no_output_runs_directory_created(self):
        output_root = REPO_ROOT / "research_automation" / "_output"
        before = (
            {p.resolve() for p in output_root.rglob("*")} if output_root.exists() else set()
        )
        with mock.patch(
            "subprocess.run",
            side_effect=AssertionError("legacy harness must not spawn subprocesses"),
        ):
            with redirect_stdout(io.StringIO()):
                exit_code = harness.main()
        after = (
            {p.resolve() for p in output_root.rglob("*")} if output_root.exists() else set()
        )
        self.assertEqual(exit_code, harness.QUARANTINE_EXIT_CODE)
        self.assertEqual(before, after)

    def test_production_boundary_unchanged(self):
        before = harness.snapshot_production_boundary()
        with redirect_stdout(io.StringIO()):
            exit_code = harness.main()
        after = harness.snapshot_production_boundary()
        self.assertEqual(exit_code, harness.QUARANTINE_EXIT_CODE)
        self.assertEqual(before, after)
        self.assertGreaterEqual(len(before), 4)


if __name__ == "__main__":
    unittest.main()
