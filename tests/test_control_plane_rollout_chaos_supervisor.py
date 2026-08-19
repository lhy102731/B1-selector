"""CR-010 B-05: supervisor strict step-output validation negative tests.

The supervisor must reject forged/partial worker step outputs (wrong
step/root/outcome/digest/fields, reused process identity, empty or
non-JSON output) -- a worker can never pass by emitting a fake SUCCEEDED.
"""

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from research_automation.control_plane import rollout_chaos
from research_automation.control_plane.rollout_chaos_worker import (
    HOST_ID,
    _durable_state_digest,
    _evidence_refs,
)

FIXTURE_REF = rollout_chaos._FIXTURE_REF
SCENARIO_DIGEST = "b" * 64
PROBE_PID = 4242
PROBE_STARTED_AT_NS = 1700000000000000000


def _valid_payload(root: Path, step: str = "prepare") -> dict[str, object]:
    return {
        "schema_version": "control_plane.rollout_chaos_worker_result.v1",
        "step": step,
        "outcome": "SUCCEEDED",
        "completed_cycles": 0,
        "state_digest": _durable_state_digest(root),
        "scenario_digest": SCENARIO_DIGEST,
        "worker_identity": {
            "pid": PROBE_PID,
            "host_id": HOST_ID,
            "fixture_ref": FIXTURE_REF,
            "started_at_ns": PROBE_STARTED_AT_NS,
        },
        "root_identity": str(root.resolve()),
        "pause_events": [],
        "network_attempts": 1,
        "evidence": _evidence_refs(root),
        "completed_step": step,
    }


class _FakePopen:
    """A minimal subprocess.Popen stand-in for the patched launcher."""

    def __init__(
        self,
        *,
        pid: int,
        stdout: str,
        stderr: str,
        returncode: int,
    ) -> None:
        self.pid = pid
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr

    def communicate(self, timeout=None):  # noqa: ANN001
        return self._stdout, self._stderr


class SupervisorStepValidationTests(unittest.TestCase):
    def _validate(
        self,
        payload,
        root,
        step="prepare",
        seen=None,
        observed=None,
        returncode=0,
    ):
        return rollout_chaos._validate_worker_step_output(
            payload=payload,
            root=root,
            requested_step=step,
            expect_decision=None,
            seen_identities=set() if seen is None else seen,
            expected_fixture_ref=FIXTURE_REF,
            expected_host_id=HOST_ID,
            expected_scenario_digest=SCENARIO_DIGEST,
            parent_network_attempts=1,
            observed_identity=(
                observed
                if observed is not None
                else rollout_chaos.ObservedWorkerIdentity(
                    pid=PROBE_PID,
                    started_at_ns=PROBE_STARTED_AT_NS,
                )
            ),
            returncode=returncode,
        )

    def _run_worker_step_patched(
        self,
        root,
        *,
        stdout,
        returncode,
        step="prepare",
        pid=PROBE_PID,
        started_at_ns=PROBE_STARTED_AT_NS,
        **kwargs,
    ):
        from unittest.mock import patch

        fake = _FakePopen(
            pid=pid,
            stdout=stdout,
            stderr="",
            returncode=returncode,
        )
        with patch(
            "research_automation.control_plane.rollout_chaos.subprocess.Popen",
            return_value=fake,
        ), patch(
            "research_automation.control_plane.rollout_chaos."
            "_observe_process_started_at_ns",
            return_value=started_at_ns,
        ):
            return rollout_chaos._run_worker_step(
                root,
                step,
                {"step": step},
                "probe",
                fixture_ref=FIXTURE_REF,
                expected_scenario_digest=SCENARIO_DIGEST,
                **kwargs,
            )

    def test_wrong_step_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["step"] = "start"
            payload["completed_step"] = "start"
            with self.assertRaisesRegex(RuntimeError, "step mismatch"):
                self._validate(payload, root)

    def test_wrong_root_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["root_identity"] = "C:/other-root"
            with self.assertRaisesRegex(RuntimeError, "root identity"):
                self._validate(payload, root)

    def test_wrong_outcome_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["outcome"] = "FAILED"
            with self.assertRaisesRegex(RuntimeError, "outcome"):
                self._validate(payload, root)

    def test_missing_completed_step_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            del payload["completed_step"]
            with self.assertRaisesRegex(RuntimeError, "completed_step"):
                self._validate(payload, root)

    def test_missing_field_rejected(self) -> None:
        # CR-010 F-03: the SUPERVISOR contract is the same exact schema --
        # every base field and the step-specific extras must be present.
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
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                payload = _valid_payload(root)
                del payload[field]
                with self.subTest(field=field):
                    with self.assertRaises(RuntimeError):
                        self._validate(payload, root)

    def test_decision_missing_for_decision_step_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root, step="next_cycle_decision")
            payload["completed_step"] = "next_cycle_decision"
            payload["decision"] = "CONTINUE"
            del payload["decision"]
            with self.assertRaises(RuntimeError):
                self._validate(payload, root, step="next_cycle_decision")

    def test_wrong_scenario_digest_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["scenario_digest"] = "0" * 64
            with self.assertRaisesRegex(RuntimeError, "scenario digest"):
                self._validate(payload, root)

    def test_wrong_fixture_ref_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["worker_identity"]["fixture_ref"] = "other-fixture"
            with self.assertRaisesRegex(RuntimeError, "fixture_ref"):
                self._validate(payload, root)

    def test_wrong_host_id_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["worker_identity"]["host_id"] = "attacker-host"
            with self.assertRaisesRegex(RuntimeError, "host_id"):
                self._validate(payload, root)

    def test_network_attempts_zero_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["network_attempts"] = 0
            with self.assertRaisesRegex(RuntimeError, "network"):
                self._validate(payload, root)

    def test_completed_cycles_mismatch_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["completed_cycles"] = 7
            with self.assertRaisesRegex(RuntimeError, "completed_cycles"):
                self._validate(payload, root)

    def test_unknown_field_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["raw_labels"] = ["secret"]
            with self.assertRaises(RuntimeError):
                self._validate(payload, root)

    def test_wrong_state_digest_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["state_digest"] = "0" * 64
            with self.assertRaisesRegex(RuntimeError, "state digest"):
                self._validate(payload, root)

    def test_empty_payload_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(RuntimeError, "empty"):
                self._validate({}, root)

    def test_reused_process_identity_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            seen = set()
            self._validate(_valid_payload(root), root, seen=seen)
            with self.assertRaisesRegex(RuntimeError, "reused"):
                self._validate(_valid_payload(root), root, seen=seen)

    def test_crash_with_empty_stdout_fails_closed(self) -> None:
        """CR-010 B-05: an rc=9 worker that printed NOTHING must fail
        closed -- never an IndexError, never a silent continue."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(RuntimeError, "empty"):
                self._run_worker_step_patched(
                    root,
                    stdout="",
                    returncode=9,
                )

    def test_crash_with_non_json_stdout_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(RuntimeError, "not strict JSON"):
                self._run_worker_step_patched(
                    root,
                    stdout="NOT JSON\n",
                    returncode=9,
                )

    def test_crash_with_forged_minimal_json_fails_closed(self) -> None:
        """CR-010 B-05: a crash payload with only a decision (the B-05
        probe) must be rejected -- missing step/root/digest fail closed."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(RuntimeError):
                self._run_worker_step_patched(
                    root,
                    stdout='{"decision":"CONTINUE"}\n',
                    returncode=9,
                )

    def test_worker_output_with_prefix_or_extra_json_line_rejected(
        self,
    ) -> None:
        """CR-010 F-03: stdout must contain EXACTLY one non-empty JSON
        object line -- leading/trailing log text and multiple JSON lines
        are rejected; evidence is never discarded via splitlines()[-1]."""
        import json as _json

        valid = _json.dumps(_valid_payload(Path("C:/nonexistent")))
        cases = {
            "prefix-log-line": f"some log text\n{valid}",
            "trailing-log-line": f"{valid}\nmore log text",
            "two-json-lines": f"{valid}\n{valid}",
        }
        for label, stdout in cases.items():
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    with self.assertRaisesRegex(
                        RuntimeError, "exactly one JSON line"
                    ):
                        self._run_worker_step_patched(
                            root,
                            stdout=stdout,
                            returncode=0,
                        )

    def test_forged_unrelated_identity_pair_rejected(self) -> None:
        """CR-010 B-05: a payload carrying a positive but UNRELATED
        PID/start pair must fail closed against the parent observation."""
        import json as _json

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            stdout = _json.dumps(payload)
            with self.assertRaisesRegex(
                RuntimeError, "does not match the parent-observed"
            ):
                self._run_worker_step_patched(
                    root,
                    stdout=stdout,
                    returncode=0,
                    pid=9999,
                    started_at_ns=123456789,
                )

    def test_pid_reuse_with_different_start_time_rejected(self) -> None:
        """CR-010 B-05: the same PID with a DIFFERENT observed start time
        must fail closed -- PID reuse can never pass."""
        import json as _json

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            stdout = _json.dumps(payload)
            with self.assertRaisesRegex(
                RuntimeError, "does not match the parent-observed"
            ):
                self._run_worker_step_patched(
                    root,
                    stdout=stdout,
                    returncode=0,
                    pid=PROBE_PID,
                    started_at_ns=PROBE_STARTED_AT_NS + 1,
                )

    def test_right_pid_with_forged_start_time_rejected(self) -> None:
        """CR-010 B-05: the right PID with a FORGED start time must fail
        closed -- the (pid, start) PAIR is the identity."""
        import json as _json

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _valid_payload(root)
            payload["worker_identity"]["started_at_ns"] = 999999
            stdout = _json.dumps(payload)
            with self.assertRaisesRegex(
                RuntimeError, "does not match the parent-observed"
            ):
                self._run_worker_step_patched(
                    root,
                    stdout=stdout,
                    returncode=0,
                    pid=PROBE_PID,
                    started_at_ns=PROBE_STARTED_AT_NS,
                )

    def test_real_short_lived_child_is_observed(self) -> None:
        """CR-010 B-05: the parent-observation helper captures a REAL
        short-lived child's start time immediately after spawn -- never 0,
        never a retry, never a fallback."""
        import subprocess as _sp
        import sys as _sys

        child = _sp.Popen(
            [_sys.executable, "-c", "pass"],
            stdout=_sp.PIPE,
            stderr=_sp.PIPE,
        )
        started_at_ns = rollout_chaos._observe_process_started_at_ns(
            child.pid
        )
        stdout, stderr = child.communicate(timeout=30)
        self.assertGreater(started_at_ns, 0)
        self.assertLessEqual(started_at_ns, time.time_ns())


if __name__ == "__main__":
    unittest.main()
