"""Tests for the bounded C0 chaos worker (C0R2 T2/T3)."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

from research_automation.control_plane.rollout_chaos_worker import (
    NetworkGuard,
    RolloutChaosNetworkDenied,
    RolloutChaosWorkerOutputRejected,
    validate_worker_output,
)


class NetworkGuardTests(unittest.TestCase):
    def tearDown(self) -> None:
        NetworkGuard._installed = False
        NetworkGuard.attempts = 0

    def test_network_probe_is_denied(self) -> None:
        NetworkGuard.install()
        # A probe must not raise (denial is silent fail-closed), but
        # attempts must be recorded.
        NetworkGuard.deny_probe()
        self.assertGreaterEqual(NetworkGuard.attempts, 1)

    def test_proxy_credentials_cleared(self) -> None:
        import os

        os.environ["HTTP_PROXY"] = "http://proxy"
        os.environ["OPENAI_API_KEY"] = "sk-test"
        NetworkGuard.install()
        self.assertNotIn("HTTP_PROXY", os.environ)
        self.assertNotIn("OPENAI_API_KEY", os.environ)


class WorkerOutputTests(unittest.TestCase):
    def test_valid_output_passes(self) -> None:
        payload = {
            "schema_version": "control_plane.rollout_chaos_worker_result.v1",
            "step": "prepare",
            "outcome": "SUCCEEDED",
            "completed_cycles": 0,
            "state_digest": None,
            "scenario_digest": None,
            "worker_identity": {"pid": 1},
            "pause_events": [],
            "network_attempts": 0,
            "evidence": [],
        }
        result = validate_worker_output(payload)
        self.assertEqual(result["step"], "prepare")

    def test_unknown_field_rejected(self) -> None:
        payload = {
            "schema_version": "control_plane.rollout_chaos_worker_result.v1",
            "step": "prepare",
            "outcome": "SUCCEEDED",
            "completed_cycles": 0,
            "state_digest": None,
            "scenario_digest": None,
            "worker_identity": {"pid": 1},
            "pause_events": [],
            "network_attempts": 0,
            "evidence": [],
            "raw_labels": ["secret"],
        }
        with self.assertRaises(RolloutChaosWorkerOutputRejected):
            validate_worker_output(payload)

    def test_invalid_outcome_rejected(self) -> None:
        payload = {
            "schema_version": "control_plane.rollout_chaos_worker_result.v1",
            "step": "prepare",
            "outcome": "UNKNOWN",
            "completed_cycles": 0,
            "state_digest": None,
            "scenario_digest": None,
            "worker_identity": {"pid": 1},
            "pause_events": [],
            "network_attempts": 0,
            "evidence": [],
        }
        with self.assertRaises(RolloutChaosWorkerOutputRejected):
            validate_worker_output(payload)


class WorkerProcessTests(unittest.TestCase):
    def test_worker_process_emits_bounded_json(self) -> None:
        child = subprocess.run(
            [
                sys.executable,
                "-m",
                "research_automation.control_plane.rollout_chaos_worker",
                "prepare",
                "fixture-ref",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=Path(__file__).resolve().parents[1],
        )
        self.assertEqual(child.returncode, 0, msg=child.stderr)
        import json as _json

        payload = _json.loads(child.stdout)
        self.assertEqual(payload["outcome"], "SUCCEEDED")
        self.assertEqual(payload["step"], "prepare")

    def test_worker_rejects_unknown_step(self) -> None:
        child = subprocess.run(
            [
                sys.executable,
                "-m",
                "research_automation.control_plane.rollout_chaos_worker",
                "bogus-step",
                "fixture-ref",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=Path(__file__).resolve().parents[1],
        )
        self.assertEqual(child.returncode, 1)
        self.assertIn(b"UNKNOWN_STEP", child.stderr.encode())


if __name__ == "__main__":
    unittest.main()
