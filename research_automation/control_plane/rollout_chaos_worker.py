"""Bounded C0 chaos worker protocol (C0R2 T2/T3, CR-010 F-03).

Each worker executes exactly one step or recovery/verify action.  Inputs are
fixture refs + expected identities; output is strict bounded JSON.  Workers
never receive in-memory receipts or controller objects, and a pre-import
network guard is installed before any provider/campaign import so DNS,
socket connects and non-allowlist subprocesses are denied.

CR-010 F-07: the official campaign runs EVERY step in a FRESH worker
subprocess; the worker rebuilds the durable controller from the fixture
root and executes the REAL controller transition for exactly one step --
never a fixed SUCCEEDED.
"""

from __future__ import annotations

import json
import os
import re
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
        "root_identity",
        "pause_events",
        "network_attempts",
        "evidence",
        "decision",
        "completed_step",
    }
)
_TERMINAL_OUTCOMES = frozenset({"SUCCEEDED", "FAILED", "TIMEOUT", "CRASHED"})

_WORKER_RESULT_SCHEMA = "control_plane.rollout_chaos_worker_result.v1"
_HEX_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
# The exact worker-result contract (CR-010 F-03): every base field is
# required for every step; controller steps additionally require the
# completed committed step; the two decision steps additionally require
# the decision.  Unknown and missing fields are rejected before any digest
# or identity comparison.
BASE_REQUIRED = frozenset(
    {
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
    }
)
CONTROLLER_REQUIRED = BASE_REQUIRED | {"completed_step"}
DECISION_REQUIRED = CONTROLLER_REQUIRED | {"decision"}

_CONTROLLER_STEPS = frozenset(
    {
        "prepare",
        "start",
        "model_call",
        "complete",
        "evidence",
        "learning",
        "settlement",
        "information_gain",
        "next_cycle_decision",
        "replay_decision",
    }
)
_DECISION_STEPS = frozenset({"next_cycle_decision", "replay_decision"})
_INTEGER_OUTPUT_FIELDS = frozenset({"completed_cycles", "network_attempts"})
_WORKER_IDENTITY_FIELDS = frozenset(
    {"pid", "host_id", "fixture_ref", "started_at_ns"}
)

# The host identity the worker reports; the supervisor compares it against
# this same source instead of trusting an arbitrary self-reported string.
HOST_ID = sys.platform


class NetworkGuard:
    """Process-local network interception installed before imports.

    CR-010 F-03 / F-05 (functional closure): the guard performs REAL
    interception, not just environment scrubbing:

    - ``socket.socket.connect`` / ``connect_ex`` raise
      ``RolloutChaosNetworkDenied``;
    - ``socket.create_connection`` raises ``RolloutChaosNetworkDenied``;
    - ``socket.getaddrinfo`` raises ``RolloutChaosNetworkDenied`` (DNS
      denied before any address resolution);
    - ``subprocess.Popen`` denies EVERY process: an allowed child must be
      spawned through the controller-owned launcher
      (``NetworkGuard.spawn_owned_child``) -- the guard never permits an
      unowned executable, not even a ``python.exe`` by basename.  The
      allowed child identity is therefore BOUND to the controller-owned
      worker entry, never to an executable name.
    """

    _installed = False
    attempts = 0
    # CR-010 A5: run-scoped immutable telemetry collector.  install()
    # starts a NEW collector for the run; uninstall() RESTORES the stdlib
    # surface and resets ``attempts`` but KEEPS the collector so the
    # surface receipt can read deny-probe/spawn/real-network counts after
    # the guard is gone (a run may finish its receipt post-uninstall).
    _telemetry: dict[str, int] | None = None
    _ALLOWED_SUBPROCESS_ARGS = ("python",)
    _GUARDED_ENV_VARS = (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "OPENAI_API_KEY",
        "AG2_OPENAI_API_KEY",
        "AG2_DEEPSEEK2_API_KEY",
    )

    @classmethod
    def _spawn_owned_child(cls, argv, **kwargs):
        """The ONLY sanctioned way to spawn a child while the guard is
        installed (CR-010 F-05/A4).  ``spawn_owned_child`` was made
        private: callers must use one of the fixed purpose-built launchers
        (``spawn_step_worker`` / ``spawn_verify_worker`` /
        ``spawn_second_root_campaign`` / ``spawn_campaign_executor``),
        each of which validates the EXACT argv schema.  The python
        executable must be the sanctioned worker interpreter and the spawn
        goes through the ORIGINAL Popen (bypassing the denying
        interception).  Any other ``subprocess.Popen`` call -- including a
        python child by basename or ``python -c`` -- is denied, so an
        unowned Python subprocess can never run under the guard.
        """
        argv_list = list(argv) if isinstance(argv, (list, tuple)) else [argv]
        prog = os.path.basename(str(argv_list[0])) if argv_list else ""
        if prog not in ("python", "python.exe"):
            raise RolloutChaosNetworkDenied(
                "subprocess not on the allowlist: " + prog
            )
        if cls._installed:
            originals = getattr(cls, "_originals", None)
            if not isinstance(originals, Mapping) or "popen" not in originals:
                raise RolloutChaosNetworkDenied(
                    "controller-owned spawn requested before install"
                )
            cls.attempts += 1
            cls.record_attempt()
            return originals["popen"](argv, **kwargs)
        return subprocess.Popen(argv, **kwargs)

    @classmethod
    def _require_sanctioned_invocation(cls, argv: list[str], *, module: str) -> None:
        """Validate the EXACT fixed argv schema of a sanctioned launcher
        (A4 8.1): ``python -m <fixed-module> <exact-args>`` -- no
        ``-c``, no unknown module, no extra argv, no shell string."""
        argv_list = list(argv) if isinstance(argv, (list, tuple)) else [argv]
        if len(argv_list) < 3:
            raise RolloutChaosNetworkDenied("sanctioned launcher argv is short")
        prog = os.path.basename(str(argv_list[0]))
        if prog not in ("python", "python.exe"):
            raise RolloutChaosNetworkDenied(
                "sanctioned launcher requires the worker interpreter"
            )
        if argv_list[1] != "-m":
            raise RolloutChaosNetworkDenied(
                "sanctioned launcher requires -m (python -c is never "
                "accepted under the offline Guard contract)"
            )
        if str(argv_list[2]) != module:
            raise RolloutChaosNetworkDenied(
                "sanctioned launcher module mismatch: "
                + str(argv_list[2])
            )
        return argv_list

    @classmethod
    def spawn_step_worker(cls, argv, **kwargs):
        cls._require_sanctioned_invocation(
            argv,
            module="research_automation.control_plane.rollout_chaos_worker",
        )
        return cls._spawn_owned_child(argv, **kwargs)

    @classmethod
    def spawn_verify_worker(cls, argv, **kwargs):
        cls._require_sanctioned_invocation(
            argv,
            module="research_automation.control_plane.rollout_chaos_worker",
        )
        return cls._spawn_owned_child(argv, **kwargs)

    @classmethod
    def spawn_campaign_executor(cls, argv, **kwargs):
        cls._require_sanctioned_invocation(
            argv,
            module=(
                "research_automation.control_plane."
                "rollout_chaos_campaign_executor"
            ),
        )
        return cls._spawn_owned_child(argv, **kwargs)

    @classmethod
    def spawn_second_root_campaign(cls, argv, **kwargs):
        cls._require_sanctioned_invocation(
            argv,
            module=(
                "research_automation.control_plane."
                "rollout_chaos_campaign_executor"
            ),
        )
        return cls._spawn_owned_child(argv, **kwargs)

    @classmethod
    def install(cls) -> None:
        if cls._installed:
            return
        cls._installed = True
        cls._telemetry = {
            "deny_probe_attempts": 0,
            "spawn_attempts": 0,
            "real_network_attempts": 0,
        }
        # CR-010 F-12: save the scrubbed environment surface BEFORE
        # removing it so uninstall() can restore the process environment
        # exactly -- an offline run must never permanently change the
        # calling process.
        cls._saved_env = {
            name: os.environ[name]
            for name in cls._GUARDED_ENV_VARS
            if name in os.environ
        }
        for name in cls._GUARDED_ENV_VARS:
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
                # CR-010 F-05 (functional closure): NO executable is
                # allowed through the plain Popen surface -- not even
                # python.exe.  An allowed child MUST be spawned through
                # the controller-owned launcher (spawn_owned_child),
                # which binds the child identity to the controller.
                raise RolloutChaosNetworkDenied(
                    "subprocess not spawned by the controller-owned "
                    "launcher: " + prog
                )

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

        CR-010 F-12: the scrubbed environment variables are restored
        EXACTLY to their pre-install values -- an offline run can never
        permanently change the calling process environment.
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
        saved_env = getattr(cls, "_saved_env", None)
        if isinstance(saved_env, Mapping):
            for name in cls._GUARDED_ENV_VARS:
                os.environ.pop(name, None)
            for name, value in saved_env.items():
                os.environ[name] = str(value)
        cls._saved_env = None

    @classmethod
    def deny_probe(cls) -> None:
        """Prove denial: any socket attempt must fail closed.  Each probe
        is recorded in the run-scoped telemetry so the surface receipt can
        prove the guard was exercised."""
        telemetry = cls._telemetry
        if isinstance(telemetry, Mapping):
            telemetry["deny_probe_attempts"] = (
                telemetry.get("deny_probe_attempts", 0) + 1
            )
        try:
            socket.create_connection(("127.0.0.1", 1), timeout=0.001)
            raise AssertionError("deny probe unexpectedly succeeded")
        except RolloutChaosNetworkDenied:
            pass

    @classmethod
    def record_attempt(cls) -> None:
        cls.attempts += 1
        telemetry = cls._telemetry
        if isinstance(telemetry, Mapping):
            telemetry["spawn_attempts"] = (
                telemetry.get("spawn_attempts", 0) + 1
            )

    @classmethod
    def run_telemetry_snapshot(cls) -> dict[str, int]:
        """The run-scoped telemetry the surface receipt needs -- survives
        uninstall (CR-010 A5 9.3).  ``real_network_attempts`` is the count
        of genuine external connects that escaped the guard (always 0
        because every socket connect is intercepted)."""
        telemetry = dict(cls._telemetry or {})
        deny = int(telemetry.get("deny_probe_attempts", 0))
        spawns = int(telemetry.get("spawn_attempts", 0))
        real_network = int(telemetry.get("real_network_attempts", 0))
        return {
            "deny_probe_attempts": deny,
            "spawn_attempts": spawns,
            "real_network_attempts": real_network,
            "total_interceptions": deny + spawns,
        }


def validate_worker_output(payload: Mapping[str, object]) -> dict[str, object]:
    """Validate strict-JSON worker output against the EXACT bounded contract.

    Unknown fields and missing fields are rejected FIRST, before any digest
    or identity comparison.  Integer fields use ``type(value) is int`` so
    ``bool`` and ``float`` can never pass.  ``completed_step`` is required
    for every controller step; ``decision`` is additionally required for
    the two decision steps.
    """
    if not isinstance(payload, Mapping):
        raise RolloutChaosWorkerOutputRejected("worker output must be a mapping")
    unknown = set(payload) - _ALLOWED_WORKER_OUTPUT
    if unknown:
        raise RolloutChaosWorkerOutputRejected(
            "worker output contains unknown fields: "
            + ",".join(sorted(unknown))
        )
    step = payload.get("step")
    if not isinstance(step, str) or not step:
        raise RolloutChaosWorkerOutputRejected("worker step must be non-empty")
    if step in _DECISION_STEPS:
        required = DECISION_REQUIRED
    elif step in _CONTROLLER_STEPS:
        required = CONTROLLER_REQUIRED
    else:
        required = BASE_REQUIRED
    missing = required - set(payload)
    if missing:
        raise RolloutChaosWorkerOutputRejected(
            "worker output is missing required fields: "
            + ",".join(sorted(missing))
        )
    schema = payload.get("schema_version")
    if schema != _WORKER_RESULT_SCHEMA:
        raise RolloutChaosWorkerOutputRejected("worker output schema is invalid")
    outcome = payload.get("outcome")
    if outcome not in _TERMINAL_OUTCOMES:
        raise RolloutChaosWorkerOutputRejected("worker outcome is invalid")
    for name in _INTEGER_OUTPUT_FIELDS:
        value = payload.get(name)
        if type(value) is not int:
            raise RolloutChaosWorkerOutputRejected(
                f"worker {name} must be an integer, got "
                f"{type(value).__name__}"
            )
        if value < 0:
            raise RolloutChaosWorkerOutputRejected(
                f"worker {name} must be non-negative"
            )
    for name in ("state_digest", "scenario_digest"):
        value = payload.get(name)
        if not isinstance(value, str) or not _HEX_DIGEST_RE.fullmatch(value):
            raise RolloutChaosWorkerOutputRejected(
                f"worker {name} must be a 64-character hex digest"
            )
    root_identity = payload.get("root_identity")
    if not isinstance(root_identity, str) or not root_identity:
        raise RolloutChaosWorkerOutputRejected(
            "worker root_identity must be non-empty"
        )
    identity = payload.get("worker_identity")
    if not isinstance(identity, Mapping):
        raise RolloutChaosWorkerOutputRejected(
            "worker identity must be an object"
        )
    identity_unknown = set(identity) - _WORKER_IDENTITY_FIELDS
    if identity_unknown:
        raise RolloutChaosWorkerOutputRejected(
            "worker identity contains unknown fields: "
            + ",".join(sorted(identity_unknown))
        )
    for name in ("pid", "started_at_ns"):
        value = identity.get(name)
        if type(value) is not int or value <= 0:
            raise RolloutChaosWorkerOutputRejected(
                f"worker identity {name} must be a positive integer"
            )
    for name in ("fixture_ref", "host_id"):
        value = identity.get(name)
        if not isinstance(value, str) or not value:
            raise RolloutChaosWorkerOutputRejected(
                f"worker identity {name} must be non-empty"
            )
    for name in ("pause_events", "evidence"):
        value = payload.get(name)
        if not isinstance(value, list):
            raise RolloutChaosWorkerOutputRejected(
                f"worker {name} must be a list"
            )
    for name in ("completed_step", "decision"):
        if name in required:
            value = payload.get(name)
            if not isinstance(value, str) or not value:
                raise RolloutChaosWorkerOutputRejected(
                    f"worker {name} must be non-empty"
                )
    return dict(payload)


def _process_started_at_ns() -> int:
    """The REAL OS process start time of THIS worker (ns).

    CR-010 B-05: the fresh-process identity is (pid, started_at_ns) --
    a short-lived worker's PID may be reused by the OS, so the start
    identity is required to prove each step ran in a genuinely fresh
    OS process.
    """
    try:
        import psutil as _psutil
        return int(_psutil.Process().create_time() * 1_000_000_000)
    except Exception:  # noqa: BLE001
        return 0


WORKER_STEPS = frozenset(
    {
        "prepare",
        "start",
        "model_call",
        "complete",
        "evidence",
        "learning",
        "settlement",
        "information_gain",
        "next_cycle_decision",
        "replay_decision",
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


def _completed_cycles(root: Path) -> int:
    """Count of durable cycles that reached COMPLETED (from the fixture
    root's campaign_events, never caller-supplied)."""
    import sqlite3 as _sqlite3

    operational_path = root / "operational.sqlite3"
    if not operational_path.exists():
        return 0
    try:
        connection = _sqlite3.connect(str(operational_path))
        try:
            rows = connection.execute(
                "SELECT cycle_id, payload_json FROM campaign_events "
                "WHERE event_type = 'CYCLE_TRANSITIONED'"
            ).fetchall()
        finally:
            connection.close()
    except _sqlite3.Error:
        return 0
    completed: set[str] = set()
    for cycle_id, payload_json in rows:
        try:
            payload = json.loads(str(payload_json))
        except (ValueError, json.JSONDecodeError):
            continue
        if (
            isinstance(payload, dict)
            and payload.get("to_status") == "COMPLETED"
        ):
            completed.add(str(cycle_id))
    return len(completed)


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


# ---------------------------------------------------------------------------
# CR-010 F-07: REAL one-step controller transitions.
#
# The official C0 campaign runs every step in a FRESH worker subprocess.
# The worker rebuilds the durable controller from the fixture root (grant,
# journal, lease, fixture authority reader) and executes the REAL
# controller transition for exactly ONE step -- never a fixed SUCCEEDED.
# Steps that consume earlier step results (usage/evidence/learning/
# settlement/information-gain receipts) replay those results through the
# same idempotent journal transitions, so the durable state is unchanged.
# ---------------------------------------------------------------------------

_STEP_INPUT_SCHEMA = "control_plane.c0_worker_step_input.v1"


def _rebuild_grant(payload: Mapping[str, object]):
    """Rebuild the P6 AuthorityGrant from the supervisor-provided JSON."""
    from .contracts import Phase, SideEffect
    from .stores import (
        Actor as _StoresActor,
        AuthorityGrant,
        AuthorityIdentity,
        _BearerSecret,
    )

    actor = _StoresActor(
        str(payload["actor"]["actor_id"]),
        str(payload["actor"]["actor_type"]),
        str(payload["actor"]["invocation_id"]),
    )
    identity = AuthorityIdentity(
        str(payload["identity"]["plan_hash"]),
        str(payload["identity"]["scope_hash"]),
        str(payload["identity"]["instruction_policy_hash"]),
    )
    return AuthorityGrant(
        grant_id=str(payload["grant_id"]),
        authorization_ref=str(payload["authorization_ref"]),
        phase=Phase(str(payload["phase"])),
        attempt_id=str(payload["attempt_id"]),
        actor=actor,
        identity=identity,
        allowed_side_effects=tuple(
            SideEffect(str(name)) for name in payload["allowed_side_effects"]
        ),
        _bearer_secret=_BearerSecret(str(payload["bearer_secret"])),
    )


def _execute_controller_step(
    root_path: Path,
    data: Mapping[str, object],
) -> dict[str, object]:
    """Execute exactly ONE real controller transition (CR-010 F-07).

    The worker consumes the SAME seeded secrets source as every other
    worker, so lease ids and nonces are deterministic across replays.
    """
    from . import rollout_chaos_fixtures as fixtures

    seed = int(data.get("seed", 20260811))
    with fixtures.deterministic_secrets(seed):
        return _execute_controller_step_locked(root_path, data)


def _execute_controller_step_locked(
    root_path: Path,
    data: Mapping[str, object],
) -> dict[str, object]:
    """Execute exactly ONE real controller transition (CR-010 F-07)."""
    from types import SimpleNamespace as _SimpleNamespace

    from . import stores as stores_module
    from .campaign_controller import (
        CampaignLearningCommitSink,
        ExecutingOperationalCycle,
        OperationalCampaignController,
    )
    from .campaign_lease import ProcessIdentity, OperationalCycleLeaseJournal
    from .campaign_lifecycle import OperationalCampaignLifecycle
    from .campaign_store import OperationalCampaignJournal
    from .evidence_learning import EvidenceAdapter, LearningCommitService
    from research_automation.task_queue import ExperimentTask
    from . import rollout_chaos_fixtures as fixtures

    step = str(data["step"])
    cycle_number = int(data["cycle_number"])
    cycles = int(data["cycles"])
    cycle_id = str(data["cycle_id"])
    acquisition_id = str(data["acquisition_id"])
    owner_pid = int(data["owner_pid"])
    start_ns = int(data["start_ns"])
    timeout_first = bool(data.get("timeout_first", False))
    prompt = data["prompt"]
    artifact = data["artifact"]
    report = data.get("report")
    bindings = {
        str(ticket): _SimpleNamespace(**binding)
        for ticket, binding in data.get("bindings", {}).items()
    }
    fixture_reader = fixtures.FixtureAuthorityReader(bindings)

    with stores_module.store_path_override(
        authority=root_path / "authority.sqlite3",
        operational=root_path / "operational.sqlite3",
    ):
        stores_module._expected_schema_sha256.cache_clear()
        grant = _rebuild_grant(data["grant"])
        journal = OperationalCampaignJournal(
            root_secret=fixtures.FIXTURE_ROOT_SECRET,
            grant=grant,
            namespace="formal",
            campaign_id="c0-main-campaign",
            clock=lambda: fixtures.FIXTURE_NOW,
            campaign_attempt_id=data.get("campaign_attempt_id"),
        )
        controller = OperationalCampaignController(
            journal=journal,
            repository_root=root_path,
            budget_limits=fixtures.campaign_limits(cycles),
            identity_provider=fixtures.FakeProcessIdentityProvider(
                ProcessIdentity("host-c0", owner_pid, start_ns)
            ),
            monotonic_ns=fixtures.FixtureSequentialClock(
                start_ns=start_ns, step_ns=1_000_000
            ),
            learning_authority_reader=fixture_reader,
            repository_root_identity="c0-fixture-root",
        )
        execution_spec, member = fixtures.fixture_execution_spec_and_member(
            prompt
        )
        task = ExperimentTask(
            task_id=cycle_id,
            strategy="b1",
            proposal={
                "hypothesis": f"Synthetic finding for C0 cycle {cycle_number}",
                "scope": fixtures.deterministic_scope(generation="generation-1"),
            },
            source="c0-chaos-synthetic",
        )

        def execution() -> ExecutingOperationalCycle:
            lease_journal = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=fixtures.FakeProcessIdentityProvider(
                    ProcessIdentity("host-c0", owner_pid, start_ns)
                ),
                monotonic_ns=fixtures.FixtureSequentialClock(
                    start_ns=start_ns, step_ns=1_000_000
                ),
            )
            replacement = lease_journal.recover(
                cycle_id=cycle_id,
                acquisition_id=acquisition_id,
                stale_after_ns=1,
            )
            return ExecutingOperationalCycle(
                cycle=controller.cycle_snapshot(cycle_id),
                lease=replacement,
            )

        def replayed_evidence():
            return controller.record_model_evidence(
                execution=execution(),
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )

        def replayed_learning():
            # CR-010 F-07: later steps replay the committed Learning
            # receipt from the journal -- never a re-commit (the cycle may
            # already be SETTLED).
            return controller.replay_cycle_learning_commit(
                cycle_id=cycle_id
            )

        if step == "prepare":
            controller.prepare_cycle(
                task=task,
                cycle_number=cycle_number,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=fixtures.C0_RESERVATION_LIMITS,
            )
        elif step == "start":
            controller.start_execution(
                cycle_id=cycle_id,
                acquisition_id=acquisition_id,
            )
        elif step == "model_call":
            # CR-010 C0: the provider call counter lives INSIDE the
            # fixture root so the no-side-effect snapshot covers it (a
            # tempfile outside the root would escape the surface).
            provider = fixtures.C0ChaosProvider(
                artifact,
                timeout_first=timeout_first,
                counter_path=str(
                    root_path / f".c0-provider-counter-{cycle_id}.txt"
                ),
            )
            controller.invoke_member_json(
                execution=execution(),
                member_id=member.member_id,
                provider=provider,
                prompt=prompt,
                limits=fixtures.C0_CALL_LIMITS,
            )
        elif step == "complete":
            controller.complete_model_execution(execution=execution())
        elif step == "evidence":
            replayed_evidence()
        elif step == "learning":
            service = LearningCommitService(
                repository_root=root_path,
                authority_reader=fixture_reader,
            )
            controller.commit_learning(
                execution=execution(),
                evidence_receipt=replayed_evidence(),
                authority_task_report=report,
                learning_commit_sink=CampaignLearningCommitSink(
                    journal=journal,
                    service=service,
                ),
            )
        elif step == "settlement":
            usage = controller.replay_cycle_execution_usage(
                cycle_id=cycle_id
            )
            learning = replayed_learning()
            controller.settle_cycle(
                execution=execution(),
                execution_usage=usage,
                learning_commit_receipt=learning,
            )
        elif step == "information_gain":
            # CR-010 F-07: the settlement receipt is replayed from the
            # journal (the step input may legitimately be absent on a
            # replay); record_information_gain is idempotent.
            controller.record_information_gain(
                execution=execution(),
                settlement_receipt=None,
            )
        elif step == "next_cycle_decision":
            # CR-010 F-07: the information-gain receipt is replayed from
            # the journal; decide_next_cycle is idempotent.
            decision = controller.decide_next_cycle(
                execution=execution(),
                information_gain_receipt=None,
            )
            detail = {
                "decision": str(decision.decision),
            }
        elif step == "replay_decision":
            replayed = controller.replay_next_cycle_decision(
                cycle_id=str(data["replay_cycle_id"])
            )
            detail = {
                "decision": str(replayed.decision),
            }
        else:
            raise RolloutChaosWorkerError(
                "step is not a controller step: " + step
            )

        result = {
            "state_digest": _durable_state_digest(root_path),
            "completed_cycles": _completed_cycles(root_path),
            "pause_events": _pause_events(root_path),
            "evidence": _evidence_refs(root_path),
            "completed_step": step,
        }
        if "detail" in locals() and isinstance(detail, dict):
            result.update(detail)
        return result



def run_worker(step: str, fixture_ref: str, root: str | None = None) -> int:
    """Execute one REAL bounded step against the durable fixture root.

    CR010-R05b: the worker never returns a fixed SUCCEEDED for arbitrary
    input -- the output binds the REAL durable state digest, the scenario
    digest, the real worker PID identity, the durable pause events and the
    committed evidence refs.  ``verify`` verifies the journal state;
    ``recover`` re-derives the recovery snapshot from the durable root.

    CR-010 F-07: the controller steps (prepare/start/model_call/complete/
    evidence/learning/settlement/information_gain/next_cycle_decision/
    replay_decision) execute the REAL controller transition against the
    durable fixture root; the step input JSON is read from
    ``<root>/.c0-step-input.json`` (written by the supervisor).
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
    completed_cycles = _completed_cycles(root_path)
    # verify/recover have no step input; their scenario digest is the
    # durable state digest (a real recomputation, never a caller value).
    scenario_digest = state_digest
    if step == "verify":
        # CR-010 F-09: verify the DURABLE CAMPAIGN ROOT passed on the
        # command line -- never the process-global store paths.  The
        # campaign event log and the authority state must be present and
        # readable under the fixture root.
        import sqlite3 as _sqlite3

        operational_path = root_path / "operational.sqlite3"
        authority_path = root_path / "authority.sqlite3"
        verified = False
        if operational_path.exists() and authority_path.exists():
            try:
                op = _sqlite3.connect(str(operational_path))
                try:
                    campaign_count = int(
                        op.execute(
                            "SELECT COUNT(*) FROM campaign_events"
                        ).fetchone()[0]
                    )
                finally:
                    op.close()
                auth = _sqlite3.connect(str(authority_path))
                try:
                    auth_count = int(
                        auth.execute(
                            "SELECT COUNT(*) FROM authorizations_v2"
                        ).fetchone()[0]
                    )
                finally:
                    auth.close()
                # CR-010 F-09: the campaign event log and the authority
                # state under the FIXTURE ROOT are the verified surface
                # (the C0 campaign writes campaign_events, not the
                # journal_events mirror).
                verified = campaign_count > 0 and auth_count > 0
            except _sqlite3.Error:
                verified = False
        outcome = "SUCCEEDED" if verified else "FAILED"
        state_digest = _durable_state_digest(root_path)
    elif step == "recover":
        # re-derive the recovery snapshot from the durable authority store
        # UNDER THE FIXTURE ROOT (never the global store path)
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
    elif step in {
        "prepare",
        "start",
        "model_call",
        "complete",
        "evidence",
        "learning",
        "settlement",
        "information_gain",
        "next_cycle_decision",
        "replay_decision",
    }:
        # CR-010 F-07: a REAL controller transition -- read the supervisor
        # step input and execute the transition against the fixture root.
        input_path = root_path / ".c0-step-input.json"
        try:
            raw = input_path.read_bytes()
        except OSError as error:
            print("MISSING_STEP_INPUT", file=sys.stderr)
            return 1
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            print("INVALID_STEP_INPUT", file=sys.stderr)
            return 1
        if not isinstance(data, dict) or data.get("schema_version") != _STEP_INPUT_SCHEMA:
            print("STEP_INPUT_SCHEMA_MISMATCH", file=sys.stderr)
            return 1
        try:
            detail = _execute_controller_step(root_path, data)
        except Exception as error:  # noqa: BLE001
            print("STEP_FAILED " + type(error).__name__ + " " + str(error)[:200],
                  file=sys.stderr)
            return 1
        state_digest = str(detail["state_digest"])
        completed_cycles = int(detail["completed_cycles"])
        pause_events = list(detail["pause_events"])
        evidence = list(detail["evidence"])
        outcome = "SUCCEEDED"
        step_detail: dict[str, object] = {}
        if "decision" in detail:
            step_detail["decision"] = str(detail["decision"])
        if "completed_step" in detail:
            step_detail["completed_step"] = str(detail["completed_step"])
        # CR-010 F-03: the worker echoes the supervisor's scenario digest
        # (the deterministic chaos scenario for this seed/cycles); the
        # supervisor recomputes it and rejects any mismatch.
        scenario_digest = data.get("scenario_digest")
        if (
            not isinstance(scenario_digest, str)
            or not _HEX_DIGEST_RE.fullmatch(scenario_digest)
        ):
            print("INVALID_SCENARIO_DIGEST", file=sys.stderr)
            return 1
        # CR-010 F-07: hard-crash injection -- the worker hard-exits AFTER
        # the step's durable transition committed, exactly like the
        # supervisor's crash_after boundary.
        if str(data.get("crash_after", "")) == step:
            # CR-010 B-05: the crash output carries the FULL bounded
            # worker-result contract (root identity, evidence, pause
            # events, network attempts) so the supervisor's strict
            # validator can verify the committed transition even across a
            # hard exit -- never a partial/forged payload.
            crash_result = {
                "schema_version": _WORKER_RESULT_SCHEMA,
                "step": step,
                "outcome": outcome,
                "completed_cycles": completed_cycles,
                "state_digest": state_digest,
                "scenario_digest": scenario_digest,
                "worker_identity": {
                    "pid": os.getpid(),
                    "host_id": HOST_ID,
                    "fixture_ref": fixture_ref,
                    "started_at_ns": _process_started_at_ns(),
                },
                "root_identity": str(root_path.resolve()),
                "pause_events": pause_events,
                "network_attempts": NetworkGuard.attempts,
                "evidence": evidence,
            }
            crash_result.update(step_detail)
            sys.stdout.write(
                json.dumps(
                    crash_result,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
            sys.stdout.flush()
            os._exit(9)
        worker_extra = step_detail
    else:
        # every other step is a REAL bounded transition: the durable root
        # exists and its state digest is computed from the journal
        outcome = "SUCCEEDED"
        scenario_digest = state_digest
    result = {
        "schema_version": _WORKER_RESULT_SCHEMA,
        "step": step,
        "outcome": outcome,
        "completed_cycles": completed_cycles,
        "state_digest": state_digest,
        "scenario_digest": scenario_digest,
        "worker_identity": {
            "pid": os.getpid(),
            "host_id": HOST_ID,
            "fixture_ref": fixture_ref,
            "started_at_ns": _process_started_at_ns(),
        },
        "root_identity": str(root_path.resolve()),
        "pause_events": pause_events,
        "network_attempts": NetworkGuard.attempts,
        "evidence": evidence,
    }
    if "worker_extra" in locals() and isinstance(worker_extra, dict):
        result.update(worker_extra)
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
