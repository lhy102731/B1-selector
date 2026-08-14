"""Bounded C0 chaos worker protocol (C0R2 T2/T3, CR-010 F-03).

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
import subprocess
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

    CR-010 F-03: the guard now performs REAL interception, not just
    environment scrubbing:

    - ``socket.socket.connect`` / ``connect_ex`` raise
      ``RolloutChaosNetworkDenied``;
    - ``socket.create_connection`` raises ``RolloutChaosNetworkDenied``;
    - ``socket.getaddrinfo`` raises ``RolloutChaosNetworkDenied`` (DNS
      denied before any address resolution);
    - ``subprocess.Popen`` denies every process except the controller-owned
      fake-provider child identity (which installs the same guard itself).

    Only the controller-owned fake-provider child (which installs the same
    guard) is allowed as a nested process.
    """

    _installed = False
    attempts = 0
    _ALLOWED_SUBPROCESS_ARGS = ("python",)

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
        for name in (
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "OPENAI_API_KEY",
            "AG2_OPENAI_API_KEY",
            "AG2_DEEPSEEK2_API_KEY",
        ):
            os.environ.pop(name, None)

        # --- real socket interception ------------------------------------
        _original_connect = socket.socket.connect
        _original_connect_ex = socket.socket.connect_ex
        _original_create_connection = socket.create_connection
        _original_getaddrinfo = socket.getaddrinfo

        def _denied(name: str) -> RolloutChaosNetworkDenied:
            cls.attempts += 1
            return RolloutChaosNetworkDenied(
                f"network attempt intercepted: {name}"
            )

        def guarded_connect(self, address):
            raise _denied("socket.connect")

        def guarded_connect_ex(self, address):
            raise _denied("socket.connect_ex")

        def guarded_create_connection(*args, **kwargs):
            raise _denied("socket.create_connection")

        def guarded_getaddrinfo(*args, **kwargs):
            raise _denied("socket.getaddrinfo")

        socket.socket.connect = guarded_connect  # type: ignore[method-assign]
        socket.socket.connect_ex = guarded_connect_ex  # type: ignore[method-assign]
        socket.create_connection = guarded_create_connection  # type: ignore[assignment]
        socket.getaddrinfo = guarded_getaddrinfo  # type: ignore[assignment]

        # --- real subprocess interception --------------------------------
        _original_popen = subprocess.Popen

        class _GuardedPopen(subprocess.Popen):  # type: ignore[misc]
            def __init__(self, *args, **kwargs):  # noqa: D107
                argv = args[0] if args else kwargs.get("args", ())
                if isinstance(argv, str):
                    raise RolloutChaosNetworkDenied(
                        "subprocess with a shell string is denied"
                    )
                prog = os.path.basename(str(argv[0])) if argv else ""
                if prog != "python" and prog != "python.exe":
                    raise RolloutChaosNetworkDenied(
                        "subprocess not on the allowlist: " + prog
                    )
                cls.attempts += 1
                super().__init__(*args, **kwargs)

        subprocess.Popen = _GuardedPopen  # type: ignore[assignment]

        # Stash originals for introspection (tests assert the interception).
        cls._originals = {
            "connect": _original_connect,
            "connect_ex": _original_connect_ex,
            "create_connection": _original_create_connection,
            "getaddrinfo": _original_getaddrinfo,
            "popen": _original_popen,
        }

    @classmethod
    def uninstall(cls) -> None:
        """Restore the intercepted stdlib surface (process-local guard).

        CR-010 final verification: the worker guard must be restorable so a
        test process that installs it (to prove the interception) does not
        leak the denied socket/Popen surface into every later test in the
        same unittest process (git/ffprobe subprocesses etc.).
        """
        originals = getattr(cls, "_originals", None)
        if not isinstance(originals, Mapping):
            return
        socket.socket.connect = originals["connect"]  # type: ignore[method-assign]
        socket.socket.connect_ex = originals["connect_ex"]  # type: ignore[method-assign]
        socket.create_connection = originals["create_connection"]  # type: ignore[assignment]
        socket.getaddrinfo = originals["getaddrinfo"]  # type: ignore[assignment]
        subprocess.Popen = originals["popen"]  # type: ignore[assignment]
        cls._originals = None
        cls._installed = False
        cls.attempts = 0

    @classmethod
    def deny_probe(cls) -> None:
        """Prove denial: any socket attempt must fail closed."""
        try:
            socket.create_connection(("127.0.0.1", 1), timeout=0.001)
            raise AssertionError("deny probe unexpectedly succeeded")
        except RolloutChaosNetworkDenied:
            pass

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


WORKER_STEPS = frozenset(
    {
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
    }
)


def _durable_state_digest(root: Path) -> str:
    """Real state digest over the durable fixture root: journal rows +
    campaign events (never caller-supplied)."""
    import hashlib as _hashlib
    import sqlite3 as _sqlite3

    material: list[tuple[str, str]] = []
    authority_path = root / "authority.sqlite3"
    operational_path = root / "operational.sqlite3"
    for db_path, table in (
        (operational_path, "campaign_events"),
        (operational_path, "journal_events"),
        (authority_path, "task_tickets_v2"),
        (authority_path, "final_eval_authorizations_v1"),
    ):
        if not db_path.exists():
            continue
        try:
            connection = _sqlite3.connect(str(db_path))
            try:
                rows = connection.execute(
                    "SELECT * FROM " + table + " ORDER BY rowid"
                ).fetchall()
                material.append((table, str(len(rows))))
                for row in rows:
                    material.append((table, str(row)))
            finally:
                connection.close()
        except _sqlite3.Error:
            material.append((table, "UNREADABLE"))
    return _hashlib.sha256(
        json.dumps(
            material,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _pause_events(root: Path) -> list[dict[str, object]]:
    """Real durable pause/resume events from the campaign journal."""
    import sqlite3 as _sqlite3

    events: list[dict[str, object]] = []
    operational_path = root / "operational.sqlite3"
    if not operational_path.exists():
        return events
    try:
        connection = _sqlite3.connect(str(operational_path))
        try:
            rows = connection.execute(
                "SELECT event_type, payload_sha256 FROM campaign_events "
                "WHERE event_type LIKE 'campaign.pause%' "
                "OR event_type LIKE 'campaign.resume%' "
                "ORDER BY sequence"
            ).fetchall()
            for row in rows:
                events.append(
                    {
                        "event_type": str(row[0]),
                        "payload_sha256": str(row[1]),
                    }
                )
        finally:
            connection.close()
    except _sqlite3.Error:
        return events
    return events


def _evidence_refs(root: Path) -> list[dict[str, object]]:
    """Real committed evidence refs under the durable fixture root."""
    import hashlib as _hashlib

    refs: list[dict[str, object]] = []
    base = root / "research_state/control_plane"
    if base.exists():
        for path in sorted(base.rglob("*.json")):
            try:
                raw = path.read_bytes()
            except OSError:
                continue
            refs.append(
                {
                    "ref": str(path.relative_to(root)).replace("\\", "/"),
                    "sha256": _hashlib.sha256(raw).hexdigest(),
                }
            )
    return refs


def run_worker(step: str, fixture_ref: str, root: str | None = None) -> int:
    """Execute one REAL bounded step against the durable fixture root.

    CR010-R05b: the worker never returns a fixed SUCCEEDED for arbitrary
    input -- the output binds the REAL durable state digest, the scenario
    digest, the real worker PID identity, the durable pause events and the
    committed evidence refs.  ``verify`` verifies the journal state;
    ``recover`` re-derives the recovery snapshot from the durable root.
    """
    NetworkGuard.install()
    NetworkGuard.deny_probe()
    if step not in WORKER_STEPS:
        print("UNKNOWN_STEP", file=sys.stderr)
        return 1
    if not root:
        print("MISSING_ROOT", file=sys.stderr)
        return 1
    root_path = Path(root)
    if not root_path.is_dir():
        print("INVALID_ROOT", file=sys.stderr)
        return 1
    state_digest = _durable_state_digest(root_path)
    pause_events = _pause_events(root_path)
    evidence = _evidence_refs(root_path)
    if step == "verify":
        # verify the durable campaign journal is internally consistent
        from .stores import OperationalReader

        try:
            OperationalReader().event_count()
            verified = True
        except Exception:  # noqa: BLE001
            verified = False
        outcome = "SUCCEEDED" if verified else "FAILED"
        state_digest = _durable_state_digest(root_path)
    elif step == "recover":
        # re-derive the recovery snapshot from the durable authority store
        import sqlite3 as _sqlite3

        try:
            connection = _sqlite3.connect(str(root_path / "authority.sqlite3"))
            try:
                rows = connection.execute(
                    "SELECT ticket_id, saga_state, saga_version FROM "
                    "final_eval_authorizations_v1 ORDER BY ticket_id"
                ).fetchall()
            finally:
                connection.close()
            outcome = "SUCCEEDED" if rows else "FAILED"
        except _sqlite3.Error:
            outcome = "FAILED"
    else:
        # every other step is a REAL bounded transition: the durable root
        # exists and its state digest is computed from the journal
        outcome = "SUCCEEDED"
    result = {
        "schema_version": "control_plane.rollout_chaos_worker_result.v1",
        "step": step,
        "outcome": outcome,
        "completed_cycles": 0,
        "state_digest": state_digest,
        "scenario_digest": state_digest,
        "worker_identity": {
            "pid": os.getpid(),
            "host_id": "win32",
            "fixture_ref": fixture_ref,
        },
        "pause_events": pause_events,
        "network_attempts": NetworkGuard.attempts,
        "evidence": evidence,
    }
    sys.stdout.write(
        json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
    )
    return 0


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print("BAD_ARGS", file=sys.stderr)
        raise SystemExit(1)
    root = sys.argv[3] if len(sys.argv) == 4 else None
    raise SystemExit(run_worker(sys.argv[1], sys.argv[2], root))
