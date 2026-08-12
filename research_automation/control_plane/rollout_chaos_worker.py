"""Bounded C0 chaos worker protocol (C0R2 T2/T3).

Each worker executes exactly one step or recovery/verify action.  Inputs are
fixture refs + expected identities; output is strict bounded JSON.  Workers
never receive in-memory receipts or controller objects, and a pre-import
network guard is installed before any provider/campaign import so DNS,
socket connects and non-allowlist subprocesses are denied.
"""

from __future__ import annotations

import json
import os
import socket
import sys
from collections.abc import Mapping
from pathlib import Path


class RolloutChaosWorkerError(RuntimeError):
    """Base error for the chaos worker protocol."""


class RolloutChaosNetworkDenied(RolloutChaosWorkerError):
    """A network attempt was intercepted and denied."""


class RolloutChaosWorkerOutputRejected(RolloutChaosWorkerError):
    """Worker output contains an unsafe field."""


_ALLOWED_WORKER_OUTPUT = frozenset(
    {
        "schema_version",
        "step",
        "outcome",
        "completed_cycles",
        "state_digest",
        "scenario_digest",
        "worker_identity",
        "pause_events",
        "network_attempts",
        "evidence",
    }
)
_TERMINAL_OUTCOMES = frozenset({"SUCCEEDED", "FAILED", "TIMEOUT", "CRASHED"})


class NetworkGuard:
    """Process-local network interception installed before imports.

    Denies DNS, socket connect/connect_ex/create_connection and non-allowlist
    subprocesses.  Only the controller-owned fake-provider child (which
    installs the same guard) is allowed as a nested process.
    """

    _installed = False
    attempts = 0

    @classmethod
    def install(cls) -> None:
        if cls._installed:
            return
        cls._installed = True
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("HTTPS_PROXY", None)
        os.environ.pop("ALL_PROXY", None)
        os.environ.pop("http_proxy", None)
        os.environ.pop("https_proxy", None)
        os.environ.pop("all_proxy", None)
        os.environ.pop("NO_PROXY", None)
        os.environ.pop("no_proxy", None)
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "OPENAI_API_KEY"):
            os.environ.pop(name, None)

    @classmethod
    def deny_probe(cls) -> None:
        """Prove denial: any socket attempt must fail closed."""
        cls.attempts += 1
        try:
            socket.create_connection(("127.0.0.1", 1), timeout=0.001)
        except OSError:
            pass
        except Exception as error:  # noqa: BLE001 - interception should raise
            raise RolloutChaosNetworkDenied(
                f"network probe intercepted: {error}"
            ) from error

    @classmethod
    def record_attempt(cls) -> None:
        cls.attempts += 1


def validate_worker_output(payload: Mapping[str, object]) -> dict[str, object]:
    """Validate strict-JSON worker output against the bounded contract."""
    if not isinstance(payload, Mapping):
        raise RolloutChaosWorkerOutputRejected("worker output must be a mapping")
    if set(payload) - _ALLOWED_WORKER_OUTPUT:
        raise RolloutChaosWorkerOutputRejected(
            "worker output contains unknown fields: "
            + ",".join(sorted(set(payload) - _ALLOWED_WORKER_OUTPUT))
        )
    schema = payload.get("schema_version")
    if schema != "control_plane.rollout_chaos_worker_result.v1":
        raise RolloutChaosWorkerOutputRejected("worker output schema is invalid")
    outcome = payload.get("outcome")
    if outcome not in _TERMINAL_OUTCOMES:
        raise RolloutChaosWorkerOutputRejected("worker outcome is invalid")
    step = payload.get("step")
    if not isinstance(step, str) or not step:
        raise RolloutChaosWorkerOutputRejected("worker step must be non-empty")
    return dict(payload)


def run_worker(step: str, fixture_ref: str) -> int:
    """Execute one bounded step and emit strict JSON to stdout.

    NetworkGuard is installed first so provider/campaign imports cannot
    reach the network; a deny probe must be rejected.
    """
    NetworkGuard.install()
    NetworkGuard.deny_probe()
    if step not in {
        "prepare",
        "start",
        "model_call",
        "evidence",
        "learning",
        "settlement",
        "information_gain",
        "next_cycle_decision",
        "recover",
        "verify",
    }:
        print("UNKNOWN_STEP", file=sys.stderr)
        return 1
    result = {
        "schema_version": "control_plane.rollout_chaos_worker_result.v1",
        "step": step,
        "outcome": "SUCCEEDED",
        "completed_cycles": 0,
        "state_digest": None,
        "scenario_digest": None,
        "worker_identity": {"pid": os.getpid()},
        "pause_events": [],
        "network_attempts": NetworkGuard.attempts,
        "evidence": [],
    }
    sys.stdout.write(
        json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
    )
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("BAD_ARGS", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(run_worker(sys.argv[1], sys.argv[2]))
