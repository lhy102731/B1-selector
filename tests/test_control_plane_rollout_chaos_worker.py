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
        # CR-010 final verification: restore the intercepted stdlib surface
        # so the guard never leaks into later tests in the same process
        # (denied socket/Popen would break git/ffprobe subprocesses).
        NetworkGuard.uninstall()
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

    def test_uninstall_restores_scrubbed_environment(self) -> None:
        """CR-010 F-12: uninstall() must restore the scrubbed environment
        variables EXACTLY -- an offline run can never permanently change
        the calling process environment."""
        import os

        os.environ["HTTP_PROXY"] = "http://proxy"
        os.environ["HTTPS_PROXY"] = "https://proxy"
        os.environ["OPENAI_API_KEY"] = "sk-test"
        NetworkGuard.install()
        NetworkGuard.uninstall()
        self.assertEqual(os.environ["HTTP_PROXY"], "http://proxy")
        self.assertEqual(os.environ["HTTPS_PROXY"], "https://proxy")
        self.assertEqual(os.environ["OPENAI_API_KEY"], "sk-test")

    def test_uninstall_restores_absent_variables_as_absent(self) -> None:
        """CR-010 F-12: a variable that was NOT present before install must
        stay absent after uninstall."""
        import os

        os.environ.pop("ALL_PROXY", None)
        NetworkGuard.install()
        NetworkGuard.uninstall()
        self.assertNotIn("ALL_PROXY", os.environ)

    def test_unowned_python_child_denied(self) -> None:
        """CR-010 F-05: while the guard is installed, an UNOWNED python
        subprocess (plain ``subprocess.Popen``, even with the python
        executable) is DENIED -- the allowlist is bound to the
        controller-owned launcher, never to the executable basename."""
        NetworkGuard.install()
        with self.assertRaises(RolloutChaosNetworkDenied):
            subprocess.Popen([sys.executable, "-c", "pass"])
        with self.assertRaises(RolloutChaosNetworkDenied):
            subprocess.Popen([sys.executable, "-m", "json.tool"])
        with self.assertRaises(RolloutChaosNetworkDenied):
            subprocess.Popen([sys.executable])

    def test_sanctioned_launcher_rejects_raw_argv(self) -> None:
        """CR-010 A4 8.1: the fixed purpose-built launchers reject
        ``python -c``, unknown modules and extra argv -- the only
        accepted child is the sanctioned module invocation."""
        NetworkGuard.install()
        for bad_argv in (
            [sys.executable, "-c", "import sys; sys.exit(0)"],
            [sys.executable, "-m", "json.tool"],
            [sys.executable],
        ):
            with self.subTest(argv=repr(bad_argv)):
                with self.assertRaises(RolloutChaosNetworkDenied):
                    NetworkGuard.spawn_step_worker(bad_argv)
                with self.assertRaises(RolloutChaosNetworkDenied):
                    NetworkGuard.spawn_campaign_executor(bad_argv)

    def test_sanctioned_step_launcher_allowed(self) -> None:
        """CR-010 A4: the fixed step launcher (python -m
        rollout_chaos_worker) is the ONLY allowed child path; it runs the
        child through the original Popen and records the attempt."""
        import tempfile as _tempfile

        NetworkGuard.install()
        attempts_before = NetworkGuard.attempts
        with _tempfile.TemporaryDirectory() as tmp:
            child = NetworkGuard.spawn_step_worker(
                [
                    sys.executable,
                    "-m",
                    "research_automation.control_plane.rollout_chaos_worker",
                    "prepare",
                    "fixture-ref",
                    tmp,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                cwd=Path(__file__).resolve().parents[1],
            )
            stdout_text, stderr_text = child.communicate(timeout=120)
            # the real worker ran (missing step input -> rc 1) and the
            # sanctioned spawn was recorded as an interception
            self.assertEqual(child.returncode, 1, stderr_text)
        self.assertGreaterEqual(NetworkGuard.attempts, attempts_before + 1)


def _base_worker_payload(step: str = "prepare") -> dict[str, object]:
    """A structurally complete worker output for one controller step."""
    return {
        "schema_version": "control_plane.rollout_chaos_worker_result.v1",
        "step": step,
        "outcome": "SUCCEEDED",
        "completed_cycles": 0,
        "state_digest": "a" * 64,
        "scenario_digest": "b" * 64,
        "worker_identity": {
            "pid": 1,
            "host_id": "win32",
            "fixture_ref": "fixture-1",
            "started_at_ns": 1,
        },
        "root_identity": "C:/fixture-root",
        "pause_events": [],
        "network_attempts": 1,
        "evidence": [],
        "completed_step": step,
    }


class WorkerOutputTests(unittest.TestCase):
    def test_valid_output_passes(self) -> None:
        result = validate_worker_output(_base_worker_payload())
        self.assertEqual(result["step"], "prepare")

    def test_missing_field_rejected(self) -> None:
        # CR-010 F-03: the worker result contract is EXACT -- every base
        # field, plus completed_step for controller steps and decision for
        # the two decision steps, is required.  An absent field can never
        # be accepted.
        for field in (
            "schema_version",
            "step",
            "outcome",
            "completed_cycles",
            "state_digest",
            "scenario_digest",
            "worker_identity",
            "root_identity",
            "pause_events",
            "network_attempts",
            "evidence",
        ):
            payload = _base_worker_payload()
            del payload[field]
            with self.subTest(field=field):
                with self.assertRaises(RolloutChaosWorkerOutputRejected):
                    validate_worker_output(payload)

    def test_completed_step_required_for_controller_steps(self) -> None:
        payload = _base_worker_payload()
        del payload["completed_step"]
        with self.assertRaises(RolloutChaosWorkerOutputRejected):
            validate_worker_output(payload)

    def test_decision_required_for_decision_steps(self) -> None:
        for step in ("next_cycle_decision", "replay_decision"):
            payload = _base_worker_payload(step=step)
            payload["decision"] = "CONTINUE"
            del payload["decision"]
            with self.subTest(step=step):
                with self.assertRaises(RolloutChaosWorkerOutputRejected):
                    validate_worker_output(payload)

    def test_unknown_field_rejected(self) -> None:
        payload = _base_worker_payload()
        payload["raw_labels"] = ["secret"]
        with self.assertRaises(RolloutChaosWorkerOutputRejected):
            validate_worker_output(payload)

    def test_invalid_outcome_rejected(self) -> None:
        payload = _base_worker_payload()
        payload["outcome"] = "UNKNOWN"
        with self.assertRaises(RolloutChaosWorkerOutputRejected):
            validate_worker_output(payload)

    def test_integer_fields_reject_bool_and_float(self) -> None:
        # type(value) is int -- bool and float must never pass as ints.
        for field, bad in (
            ("completed_cycles", True),
            ("network_attempts", 1.5),
            ("completed_cycles", 1.0),
        ):
            payload = _base_worker_payload()
            payload[field] = bad
            with self.subTest(field=field, value=bad):
                with self.assertRaises(RolloutChaosWorkerOutputRejected):
                    validate_worker_output(payload)

    def test_identity_integer_fields_reject_bool_and_float(self) -> None:
        for field, bad in (("pid", True), ("started_at_ns", 1.0)):
            payload = _base_worker_payload()
            payload["worker_identity"][field] = bad
            with self.subTest(field=field, value=bad):
                with self.assertRaises(RolloutChaosWorkerOutputRejected):
                    validate_worker_output(payload)

    def test_digest_fields_must_be_64_hex(self) -> None:
        for field in ("state_digest", "scenario_digest"):
            payload = _base_worker_payload()
            payload[field] = "not-a-digest"
            with self.subTest(field=field):
                with self.assertRaises(RolloutChaosWorkerOutputRejected):
                    validate_worker_output(payload)

    def test_worker_identity_unknown_field_rejected(self) -> None:
        payload = _base_worker_payload()
        payload["worker_identity"]["root_path"] = "C:/secret"
        with self.assertRaises(RolloutChaosWorkerOutputRejected):
            validate_worker_output(payload)


class WorkerProcessTests(unittest.TestCase):
    def test_worker_process_emits_bounded_json(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            child = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "research_automation.control_plane.rollout_chaos_worker",
                    "prepare",
                    "fixture-ref",
                    tmp,
                ],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=Path(__file__).resolve().parents[1],
            )
            # CR-010 F-07: a controller step WITHOUT the supervisor step
            # input fails closed (the worker never fabricates a SUCCEEDED).
            self.assertEqual(child.returncode, 1, msg=child.stderr)
            self.assertIn("MISSING_STEP_INPUT", child.stderr)

    def test_worker_verify_process_emits_bounded_json(self) -> None:
        import sqlite3
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            op = sqlite3.connect(str(root / "operational.sqlite3"))
            op.execute(
                "CREATE TABLE campaign_events (sequence INTEGER PRIMARY KEY "
                "AUTOINCREMENT, campaign_id TEXT NOT NULL, cycle_id TEXT NOT NULL, "
                "event_type TEXT NOT NULL, payload_json TEXT NOT NULL, "
                "payload_sha256 TEXT NOT NULL, event_sha256 TEXT NOT NULL, "
                "created_at TEXT NOT NULL)"
            )
            op.execute(
                "INSERT INTO campaign_events (campaign_id, cycle_id, event_type, "
                "payload_json, payload_sha256, event_sha256, created_at) "
                "VALUES ('c', 'c1', 't', '{}', 'x'*64, 'y'*64, "
                "'2026-01-01T00:00:00Z')"
            )
            op.commit()
            op.close()
            auth = sqlite3.connect(str(root / "authority.sqlite3"))
            auth.execute(
                "CREATE TABLE authorizations_v2 (authorization_ref TEXT PRIMARY KEY, "
                "phase TEXT NOT NULL, attempt_id TEXT NOT NULL, actor_id TEXT NOT NULL, "
                "actor_type TEXT NOT NULL, invocation_id TEXT NOT NULL, "
                "plan_hash TEXT NOT NULL, scope_hash TEXT NOT NULL, "
                "instruction_policy_hash TEXT NOT NULL, state TEXT NOT NULL, "
                "created_at TEXT NOT NULL)"
            )
            auth.execute(
                "INSERT INTO authorizations_v2 VALUES ('a1', 'P0', 'att', 'act', "
                "'automation', 'inv', 'p'*64, 's'*64, 'i'*64, 'ACTIVE', "
                "'2026-01-01T00:00:00Z')"
            )
            auth.commit()
            auth.close()
            child = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "research_automation.control_plane.rollout_chaos_worker",
                    "verify",
                    "fixture-ref",
                    tmp,
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
            self.assertEqual(payload["step"], "verify")
            # CR010-R05b: the worker output binds a REAL state digest, the
            # real PID identity and the guard interception attempts
            self.assertTrue(payload["state_digest"])
            self.assertGreaterEqual(
                int(payload["worker_identity"]["pid"]), 1
            )
            self.assertGreaterEqual(
                int(payload["network_attempts"]), 1
            )

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
