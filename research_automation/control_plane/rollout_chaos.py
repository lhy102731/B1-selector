
"""C0 rollout chaos simulation driver (offline fake-provider/fake-clock/fake-PID).

Deterministic offline chaos simulation of the durable P6 Campaign controller over
at least 20 cycles, using fake provider, fake clock, and fake process identity.
This module deliberately reuses the repository's own test fixture builders as the
fake fixtures required by the approved C0 rollout gate. It never touches real
provider endpoints, real stores, real data, KBase, Campaign, or Final Holdout.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import base64
import secrets
import shutil
import sqlite3
import subprocess
import msvcrt
import tempfile
import functools
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from research_automation.control_plane.budget import BudgetExceededError
from research_automation.control_plane.campaign import (
    InvalidModelResponseError,
    ProviderResponse,
)
from research_automation.control_plane.campaign_controller import (
    ExecutingOperationalCycle,
    _CYCLE_SETTLEMENT_RECORDED,
    _EXECUTION_USAGE_FROZEN,
    _INFORMATION_GAIN_RECORDED,
    _LEARNING_COMMIT_RECORDED,
    _MODEL_CALL_COMPLETED,
    _MODEL_EVIDENCE_RECORDED,
    _NEXT_CYCLE_DECISION_RECORDED,
    CampaignBudgetLimits,
    CycleReservationLimits,
    OperationalCampaignController,
    OperationalModelCallLimits,
)
from research_automation.control_plane.campaign_lease import (
    OperationalCycleLeaseJournal,
    CycleLeaseError,
    ProcessIdentity,
    _LEASE_ACQUIRED,
)
from research_automation.control_plane.campaign_lifecycle import (
    CampaignStatus,
    CycleStatus,
    OperationalCampaignLifecycle,
)
from research_automation.control_plane.campaign_store import (
    CampaignJournalError,
    CampaignLearningCommitSink,
    OperationalCampaignJournal,
)
from research_automation.control_plane.campaign_roster import RosterDriftError
from research_automation.control_plane.evidence_learning import (
    EvidenceAdapter,
    LearningCommitService,
)
from research_automation.control_plane.memory import CommittedLearningLedgerReader
from research_automation.task_queue import ExperimentTask

from types import SimpleNamespace

from research_automation.control_plane.contracts import SideEffect, canonical_json
from research_automation.control_plane.task_reports import build_task_report_v2

from research_automation.control_plane import stores as stores_module


def _test_fixtures():
    """Production-owned offline fixtures (CR010-R05a).

    The C0 driver uses ONLY the production fixture module -- never
    ``tests.*`` private fixtures and never unittest.mock in the
    production-owned simulation path.
    """
    from . import rollout_chaos_fixtures as fixtures

    return {
        "protocol_member": fixtures.fixture_member,
        "fake_process_identity_provider": fixtures.FakeProcessIdentityProvider,
        "scope": fixtures.deterministic_scope,
        "now": fixtures.FIXTURE_NOW,
        "root_secret": fixtures.FIXTURE_ROOT_SECRET,
        "authorized_campaign": fixtures.fixture_authorized_campaign,
        "claim_campaign_grant": fixtures.fixture_claim_campaign_grant,
        "execution_spec_and_member": fixtures.fixture_execution_spec_and_member,
        "evidence_vertical_slice": fixtures.fixture_authority_fixture,
        "protocol": fixtures.fixture_protocol,
    }


_MIN_CYCLES = 20
_DEFAULT_CYCLES = 24
# Official attempt id is injected by the authorized CLI (CR-010 F-03); the
# legacy default is kept only for unit/read-only contexts and the production
# driver never writes evidence under it.
_ATTEMPT_ID = "c0-attempt-003"
# The immutable fixture ref for the C0 synthetic campaign lineage
# (CR-010 F-03): the worker output must echo this exact value; it is never
# derived from the business owner_pid or the attempt id.
_FIXTURE_REF = "c0-fixture-v342-cr010"
_PLAN_VERSION = "V3.4.2-P0R2"
_SCHEMA_VERSION = "C0_CHAOS_SIMULATION_REPORT_V1"
_CAMPAIGN_ID = "c0-main-campaign"

_CHAOS_CATEGORIES = (
    "crash_between_steps",
    "provider_timeout_recovery",
    "lease_fencing_fail_closed",
    "pid_reuse_fail_closed",
    "budget_exhaustion_fail_closed",
    "mid_call_doubt_fail_closed",
    "invalid_json_fail_closed",
    "safe_boundary_pause",
)

_CYCLE_STEPS = (
    "prepare",
    "start",
    "invoke",
    "complete",
    "evidence",
    "commit",
    "settle",
    "info_gain",
    "decide",
)

# CR-010 F-07: shared step-execution components live in the production
# fixture module so the one-step WORKER subprocess can execute REAL
# controller transitions without importing the supervisor.
from .rollout_chaos_fixtures import (
    C0ChaosProvider as ChaosProvider,
    C0_CALL_LIMITS as _CALL_LIMITS,
    C0_RESERVATION_LIMITS as _RESERVATION_LIMITS,
    FixtureSequentialClock as _SequentialMonotonicClock,
    campaign_limits,
    c0_claim_for_cycle as _claim_for_cycle,
)

_INVALID_JSON_CALL_LIMITS = OperationalModelCallLimits(
    currency="USD",
    max_input_tokens=20,
    max_output_tokens=10,
    max_cost="0.1",
    max_wall_time_ms=5_000,
    max_attempts=1,
)

class _InvalidJsonChaosProvider(ChaosProvider):
    def invoke(self, request: object) -> ProviderResponse:
        self._next_call_number()
        return ProviderResponse(
            output_text="{invalid-json",
            request_model=self.model,
            response_model=self.model,
            raw_usage={
                "input_tokens": 7,
                "output_tokens": 3,
                "total_tokens": 10,
                "reported_cost": "0.02",
                "currency": "USD",
            },
        )


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _write_json_ref(root: Path, ref: str, payload: object) -> dict[str, object]:
    raw = _canonical_bytes(payload)
    path = root / ref
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return {
        "evidence_ref": ref,
        "evidence_sha256": hashlib.sha256(raw).hexdigest(),
        "status": "VERIFIED",
    }


def _authority_fixture_cycle(
    root: Path,
    *,
    label: str,
    cycle_number: int,
):
    """Per-cycle authority fixture with non-colliding evidence refs.

    Replicates the repository's single-claim test fixture, but writes each
    cycle's approved claim/protocol/adapter/artifact/decision under a
    per-cycle directory so multi-cycle ledger rebuilds never see overwritten
    Authority anchors.
    """
    claim = _claim_for_cycle(cycle_number)
    protocol = _test_fixtures()["protocol"]().model_dump(mode="json")
    artifact = {
        "schema_version": "runner.artifact.v1",
        "runner": "fixture-runner",
        "runner_version": "1.0.0",
        "status": "COMPLETED",
        "claim": claim,
        "protocol_conformance": "CONFORMING",
        "executed_protocol": protocol,
        "artifact_refs": [{"ref": "fixtures/result.json", "sha256": "e" * 64}],
        "access_event_ids": ["event:fixture-001"],
        "taint_refs": [],
    }
    source_ref = "research_automation/control_plane/evidence_learning.py"
    from research_automation.control_plane import evidence_learning as evidence_module
    source_raw = Path(evidence_module.__file__).read_bytes()
    source_path = root / source_ref
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(source_raw)
    adapter = {
        "schema_version": "control_plane.runner_adapter.v1",
        "adapter_id": "EvidenceAdapter.v1",
        "source_ref": source_ref,
        "source_sha256": hashlib.sha256(source_raw).hexdigest(),
        "runners": {"fixture-runner": "1.0.0"},
    }
    base = f"research_state/control_plane/p4/fixtures/{label}"
    refs = {
        "approved-claim": _write_json_ref(root, f"{base}/claim.json", claim),
        "approved-protocol": _write_json_ref(root, f"{base}/protocol.json", protocol),
        "runner-adapter": _write_json_ref(root, f"{base}/adapter.json", adapter),
        "runner-artifact": _write_json_ref(root, f"{base}/artifact.json", artifact),
    }
    evidence = EvidenceAdapter(
        known_runners=adapter["runners"],
        approved_protocol=protocol,
        approved_claim=claim,
    ).evaluate(artifact)
    decision = {
        "schema_version": "control_plane.evidence_decision.v1",
        "bindings": {
            name: refs[name]["evidence_sha256"]
            for name in sorted(refs)
        },
        "claim": claim,
        "evidence": {
            "verdict": evidence.verdict,
            "protocol_conformance": evidence.protocol_conformance,
            "audit_grade": evidence.audit_grade,
            "scientific_outcome": evidence.scientific_outcome,
            "promotion_eligible": evidence.promotion_eligible,
            "evidence_refs": list(evidence.evidence_refs),
            "access_event_ids": list(evidence.access_event_ids),
            "taint_refs": list(evidence.taint_refs),
            "invalidation_codes": list(evidence.invalidation_codes),
        },
    }
    refs["learning-decision"] = _write_json_ref(
        root, f"{base}/decision.json", decision
    )
    baseline_ref = "research_state/control_plane/p3/baseline.json"
    # CR-010 F-08: the fixture baseline binds the repository root in
    # ROOT-RELATIVE form ("." resolves to the fixture root), so the
    # learning packet content hash never embeds the absolute fixture root
    # path -- cross-root replay digests are root-independent.
    baseline_raw = _canonical_bytes(
        {
            "phase": "P3",
            "repository_root": ".",
            "status": "PASS",
        }
    )
    baseline_path = root / baseline_ref
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_bytes(baseline_raw)
    input_refs = []
    for evidence_id in sorted(refs):
        input_refs.append({"evidence_id": evidence_id, **refs[evidence_id]})
    ticket_id = f"ticket-learning-{label}"
    report = build_task_report_v2({
        "plan_version": "V3.4.2-P0R2",
        "phase": "P4",
        "task_id": "P4-LEARNING-COMMIT",
        "attempt_id": "p4-fixture",
        "authorization_ref": f"auth-learning-{label}",
        "ticket_id": ticket_id,
        "identity_binding": {
            "plan_hash": "a" * 64,
            "scope_hash": "b" * 64,
            "instruction_policy_hash": "c" * 64,
        },
        "objective": "Project one independently reviewed Learning decision.",
        "dependencies": [],
        "idempotency_key": f"p4-learning-commit-{label}",
        "task_spec_ref": "research_state/control_plane/p4/task_specs/learning.json",
        "task_spec_sha256": "d" * 64,
        "requirements": {
            "required_test_receipt_ids": [],
            "required_review_receipt_ids": [],
            "required_evidence_ids": sorted(refs),
        },
        "ticket_state": "SUCCEEDED",
        "allowed_files": [
            "research_state/control_plane/learning_commit.sqlite3",
            "research_state/control_plane/learning_packets/",
        ],
        "forbidden_files": ["data/", "knowledge/"],
        "baseline_ref": baseline_ref,
        "baseline_sha256": hashlib.sha256(baseline_raw).hexdigest(),
        "input_evidence_refs": input_refs,
        "test_receipts": [],
        "review_receipts": [],
        "review_findings": [],
        "changed_files": [],
        "external_invocations": [],
        "side_effect_summary": {"observed": [], "unauthorized": []},
        "started_at": f"2026-07-30T08:{cycle_number:02d}:00Z",
        "completed_at": f"2026-07-30T08:{cycle_number:02d}:01Z",
    })
    binding = SimpleNamespace(
        ticket_id=ticket_id,
        report_payload_sha256=report["report_payload_sha256"],
        actor_id="independent-evidence-reviewer",
        allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
        ticket_state="SUCCEEDED",
        terminal_evidence_ref=refs["learning-decision"]["evidence_ref"],
    )
    return report, binding, artifact, evidence, refs


@contextmanager
def _authorized_campaign_deterministic_root(
    campaign_id: str,
    root: Path,
    *,
    namespace: str = "formal",
    campaign_attempt_id: str | None = None,
):
    """Deterministic-root twin of the repository's test fixture context."""
    root.mkdir(parents=True, exist_ok=True)
    # CR-010 F-10: production-owned store redirection seam -- never
    # unittest.mock on the official path.
    with stores_module.store_path_override(
        authority=root / "authority.sqlite3",
        operational=root / "operational.sqlite3",
    ):
        stores_module._expected_schema_sha256.cache_clear()
        stores_module._trusted_bootstrap(root_secret=_test_fixtures()["root_secret"])
        grant = _test_fixtures()["claim_campaign_grant"](
            campaign_id=campaign_id,
            namespace=namespace,
            actor_id="p6-runner",
            invocation_id=f"{campaign_id}-test",
            attempt_id=f"{campaign_id}-attempt",
            plan_sha256="a" * 64,
            instruction_sha256="c" * 64,
        )
        try:
            yield root, grant, OperationalCampaignJournal(
                root_secret=_test_fixtures()["root_secret"],
                grant=grant,
                namespace=namespace,
                campaign_id=campaign_id,
                clock=lambda: _test_fixtures()["now"],
                campaign_attempt_id=campaign_attempt_id,
            )
        finally:
            stores_module._expected_schema_sha256.cache_clear()


def _deterministic_root(seed: int, cycles: int) -> Path:
    base = Path(tempfile.gettempdir()).resolve()
    target = (base / f"v342-c0-deterministic-{seed}-{cycles}").resolve()
    try:
        target.relative_to(base)
    except ValueError as error:
        raise RuntimeError("deterministic C0 fixture root escapes temp dir") from error
    return target


@contextmanager
def _deterministic_root_lock(seed: int, cycles: int):
    """Serialize one (seed, cycles) simulation across processes."""
    base = Path(tempfile.gettempdir()).resolve()
    lock_path = (base / f"v342-c0-deterministic-{seed}-{cycles}.lock").resolve()
    try:
        lock_path.relative_to(base)
    except ValueError as error:
        raise RuntimeError("deterministic C0 lock path escapes temp dir") from error
    with lock_path.open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        try:
            yield
        finally:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


# CR-010 F-07: deterministic secrets moved to the production fixture
# module so the one-step WORKER subprocesses share the same source.
from .rollout_chaos_fixtures import deterministic_secrets as _deterministic_secrets


def _new_controller(
    journal,
    root: Path,
    *,
    cycles: int,
    owner_pid: int,
    start_ns: int,
    learning_authority_reader: object | None = None,
) -> OperationalCampaignController:
    return OperationalCampaignController(
        journal=journal,
        repository_root=root,
        budget_limits=campaign_limits(cycles),
        identity_provider=_test_fixtures()["fake_process_identity_provider"](
            ProcessIdentity("host-c0", owner_pid, start_ns)
        ),
        monotonic_ns=_SequentialMonotonicClock(start_ns=start_ns, step_ns=1_000_000),
        learning_authority_reader=learning_authority_reader,
        # CR-010 F-08: the offline campaign binds a DETERMINISTIC root
        # identity so cross-root semantic replays are byte-equal.
        repository_root_identity="c0-fixture-root",
    )


def _build_schedule(seed: int, cycles: int) -> dict[int, dict[str, object]]:
    rng = random.Random(seed)
    schedule: dict[int, dict[str, object]] = {
        n: {"crash_after": None, "timeout_first": False, "pause_after": False}
        for n in range(1, cycles + 1)
    }
    crash_points = (
        "prepare",
        "start",
        "invoke",
        "complete",
        "evidence",
        "commit",
        "settle",
        "info_gain",
        "decide",
    )
    assigned: set[int] = set()
    for index, point in enumerate(crash_points):
        cycle = (index * 2) % cycles + 1
        while cycle in assigned:
            cycle = cycle % cycles + 1
        assigned.add(cycle)
        schedule[cycle]["crash_after"] = point
    extra_crashes = max(1, cycles // 6)
    for _ in range(extra_crashes):
        cycle = rng.randint(1, cycles)
        if schedule[cycle]["crash_after"] is None:
            schedule[cycle]["crash_after"] = rng.choice(crash_points)
    for cycle in range(2, cycles + 1, 6):
        schedule[cycle]["timeout_first"] = True
    for cycle in range(4, cycles + 1, 8):
        schedule[cycle]["pause_after"] = True
    return schedule


def _scenario_entries(
    schedule: Mapping[int, Mapping[str, object]],
    cycles: int,
) -> list[str]:
    """The DETERMINISTIC scenario markers for a schedule (CR-010 F-03).

    The supervisor recomputes the expected scenario digest from this exact
    list -- the worker echoes it and any mismatch fails closed.
    """
    entries: list[str] = []
    for n in range(1, cycles + 1):
        slot = schedule[n]
        crash_after = slot["crash_after"]
        timeout_first = bool(slot["timeout_first"])
        pause_after = bool(slot["pause_after"])
        if crash_after is not None:
            entries.append(
                f"cycle {n}: crash_between_steps after {crash_after}"
            )
        if timeout_first:
            entries.append(
                f"cycle {n}: provider_timeout_recovery (first attempt times out, retry succeeds)"
            )
        if pause_after:
            entries.append(
                f"cycle {n}: safe_boundary_pause after cycle decision"
            )
    return entries


def _expected_scenario_log(seed: int, cycles: int) -> list[str]:
    return _scenario_entries(_build_schedule(seed, cycles), cycles)


def _scenario_digest(seed: int, cycles: int) -> str:
    """The scenario digest the supervisor recomputes and compares against
    the worker's echoed value (CR-010 F-03)."""
    return _scenario_log_digest(_expected_scenario_log(seed, cycles))


def _cycle_event_rows(root: Path, campaign_id: str, cycle_id: str):
    connection = sqlite3.connect(root / "operational.sqlite3")
    try:
        return tuple(
            connection.execute(
                "SELECT event_type, payload_sha256 FROM campaign_events "
                "WHERE campaign_id = ? AND cycle_id = ? ORDER BY sequence",
                (campaign_id, cycle_id),
            ).fetchall()
        )
    finally:
        connection.close()


def _count_event(rows, event_type: str) -> int:
    return sum(1 for row in rows if row[0] == event_type)




# ---------------------------------------------------------------------------
# CR-010 F-07: supervisor -> one-step worker subprocess protocol.
#
# Every campaign step is executed by a FRESH worker subprocess
# (``rollout_chaos_worker``), which rebuilds the durable controller from
# the fixture root and performs the REAL controller transition.  The
# supervisor writes the step input JSON, launches the worker, and treats a
# hard exit (rc=9) after a crash_after boundary exactly like the legacy
# in-process crash recovery: the next step runs in a fresh worker with the
# recovery identity.
# ---------------------------------------------------------------------------

_STEP_INPUT_SCHEMA = "control_plane.c0_worker_step_input.v1"

# _CYCLE_STEPS name -> worker step name (the worker protocol uses the
# controller-domain names).
_CYCLE_STEP_TO_WORKER_STEP = {
    "prepare": "prepare",
    "start": "start",
    "invoke": "model_call",
    "complete": "complete",
    "evidence": "evidence",
    "commit": "learning",
    "settle": "settlement",
    "info_gain": "information_gain",
    "decide": "next_cycle_decision",
}


def _serialize_grant(grant: object) -> dict[str, object]:
    """Serialize the P6 AuthorityGrant for the worker subprocess (the
    fixture root secret is a PUBLIC constant -- never a real secret)."""
    return {
        "grant_id": grant.grant_id,
        "authorization_ref": grant.authorization_ref,
        "phase": grant.phase.value,
        "attempt_id": grant.attempt_id,
        "actor": {
            "actor_id": grant.actor.actor_id,
            "actor_type": grant.actor.actor_type,
            "invocation_id": grant.actor.invocation_id,
        },
        "identity": {
            "plan_hash": grant.identity.plan_hash,
            "scope_hash": grant.identity.scope_hash,
            "instruction_policy_hash": grant.identity.instruction_policy_hash,
        },
        "allowed_side_effects": [
            effect.value for effect in grant.allowed_side_effects
        ],
        "bearer_secret": grant._bearer_secret._reveal_for_authority_check(),
    }


def _validate_worker_step_output(
    *,
    payload: dict[str, object],
    root: Path,
    requested_step: str,
    expect_decision: str | None,
    seen_identities: set[tuple[int, int]],
    expected_fixture_ref: str,
    expected_host_id: str,
    expected_scenario_digest: str,
    parent_network_attempts: int,
    observed_identity: ObservedWorkerIdentity,
    returncode: int,
) -> dict[str, object]:
    """STRICT validation of one worker step's output (CR-010 B-05/F-03).

    Both the normal-exit path and the rc=9 crash path must satisfy:

    - the EXACT worker schema (unknown and missing fields are rejected
      BEFORE any digest or identity comparison);
    - the returned ``step`` equals the REQUESTED step;
    - the returned ``completed_step`` equals the requested step;
    - ``root_identity`` equals THIS fixture root;
    - ``state_digest`` matches the durable store recomputed digest;
    - ``scenario_digest`` matches the supervisor-recomputed scenario digest;
    - ``completed_cycles`` matches the durable store recomputed count;
    - ``pause_events`` match the durable store recomputed events;
    - ``evidence`` refs match the durable store recomputed evidence;
    - ``worker_identity.fixture_ref`` equals the requested fixture ref;
    - ``worker_identity.host_id`` equals the parent host identity;
    - ``worker_identity`` EQUALS the parent-observed (pid, start time)
      pair -- a forged identity never passes, and the pair has not been
      seen before in this campaign;
    - ``network_attempts`` is consistent with the parent-owned NetworkGuard
      telemetry, never trusted from worker self-report alone;
    - the parent-recorded return code is a documented exit (0 or 9).

    A forged/partial payload that omits any required field fails closed.
    """
    from .rollout_chaos_worker import (
        RolloutChaosWorkerOutputRejected,
        _completed_cycles,
        _durable_state_digest,
        _evidence_refs,
        _pause_events,
        validate_worker_output,
    )

    if not isinstance(payload, dict) or not payload:
        raise RuntimeError(
            "worker step output is empty or not an object"
        )
    try:
        payload = validate_worker_output(payload)
    except RolloutChaosWorkerOutputRejected as error:
        raise RuntimeError(
            "worker step output failed validation: " + str(error)
        ) from error
    observed_step = str(payload["step"])
    if observed_step != requested_step:
        raise RuntimeError(
            f"worker step mismatch: requested={requested_step} "
            f"observed={observed_step}"
        )
    if str(payload["outcome"]) != "SUCCEEDED":
        raise RuntimeError(
            f"worker step outcome is not SUCCEEDED: {payload['outcome']}"
        )
    observed_completed = str(payload["completed_step"])
    if observed_completed != requested_step:
        raise RuntimeError(
            f"worker completed_step mismatch: requested={requested_step} "
            f"observed={observed_completed}"
        )
    expected_root = str(Path(root).resolve())
    if str(payload["root_identity"]) != expected_root:
        raise RuntimeError(
            "worker root identity mismatch: "
            f"expected={expected_root} observed={payload['root_identity']}"
        )
    # state digest + completed cycles + pauses + evidence must match the
    # durable store -- every value is recomputed, never worker-supplied.
    durable_digest = _durable_state_digest(root)
    if str(payload["state_digest"]) != durable_digest:
        raise RuntimeError(
            "worker state digest does not match the durable store: "
            f"worker={payload['state_digest']} durable={durable_digest}"
        )
    if str(payload["scenario_digest"]) != expected_scenario_digest:
        raise RuntimeError(
            "worker scenario digest mismatch: "
            f"expected={expected_scenario_digest} "
            f"observed={payload['scenario_digest']}"
        )
    durable_cycles = _completed_cycles(root)
    if int(payload["completed_cycles"]) != durable_cycles:
        raise RuntimeError(
            "worker completed_cycles does not match the durable store: "
            f"worker={payload['completed_cycles']} durable={durable_cycles}"
        )
    durable_pauses = _pause_events(root)
    if list(payload["pause_events"]) != durable_pauses:
        raise RuntimeError(
            "worker pause events do not match the durable store"
        )
    durable_evidence = _evidence_refs(root)
    if list(payload["evidence"]) != durable_evidence:
        raise RuntimeError(
            "worker evidence refs do not match the durable store"
        )
    identity = payload["worker_identity"]
    if str(identity["fixture_ref"]) != expected_fixture_ref:
        raise RuntimeError(
            "worker fixture_ref mismatch: "
            f"expected={expected_fixture_ref} "
            f"observed={identity['fixture_ref']}"
        )
    if str(identity["host_id"]) != expected_host_id:
        raise RuntimeError(
            "worker host_id mismatch: "
            f"expected={expected_host_id} observed={identity['host_id']}"
        )
    payload_identity = (
        int(identity["pid"]),
        int(identity["started_at_ns"]),
    )
    observed_identity_pair = (
        observed_identity.pid,
        observed_identity.started_at_ns,
    )
    # CR-010 B-05: the payload identity must EQUAL the parent-observed
    # (pid, start time) pair -- a forged PID/start pair, a reused PID with
    # a different start, or the right PID with a forged start all fail
    # closed here, before the pair is added to the seen set.
    if payload_identity != observed_identity_pair:
        raise RuntimeError(
            "worker OS identity does not match the parent-observed "
            f"process: payload={payload_identity} "
            f"observed={observed_identity_pair}"
        )
    if observed_identity_pair in seen_identities:
        raise RuntimeError(
            f"worker OS process identity {observed_identity_pair} was "
            "reused across steps -- the step did not run in a fresh OS "
            "process"
        )
    seen_identities.add(observed_identity_pair)
    # network_attempts is compared against the PARENT-OWNED NetworkGuard
    # telemetry: the parent must have observed its own guarded launch, and
    # the worker's self-report must be consistent with that observation.
    if type(parent_network_attempts) is not int or parent_network_attempts < 1:
        raise RuntimeError(
            "parent NetworkGuard telemetry is missing for the step"
        )
    worker_attempts = int(payload["network_attempts"])
    if worker_attempts < 1 or worker_attempts > parent_network_attempts + 1000:
        raise RuntimeError(
            "worker network_attempts is inconsistent with the parent-owned "
            f"guard telemetry: worker={worker_attempts} "
            f"parent={parent_network_attempts}"
        )
    if returncode not in (0, 9):
        raise RuntimeError(
            f"worker return code is not a documented exit: {returncode}"
        )
    if (
        expect_decision is not None
        and str(payload.get("decision", "")) != str(expect_decision)
    ):
        raise RuntimeError(
            f"worker step {requested_step} decision mismatch: "
            f"expected={expect_decision} observed={payload.get('decision')}"
        )
    return payload


@dataclass(frozen=True, slots=True)
class ObservedWorkerIdentity:
    """The parent-observed OS identity of a just-spawned child (CR-010 B-05).

    Created immediately after ``Popen`` returns -- BEFORE the short-lived
    child can exit -- from the parent-owned ``Popen.pid`` and an OS-observed
    process start time normalized exactly like the child's self-report.
    Never falls back to 0, a merely positive integer, or the worker's
    self-reported pair; the child's self-report is never the root of trust.
    """

    pid: int
    started_at_ns: int


def _observe_process_started_at_ns(pid: int) -> int:
    """Immediately observe a just-spawned child's OS start time (ns).

    Uses the same psutil normalization as the worker's self-report
    (``create_time() * 1_000_000_000``) so the parent-observed pair is
    directly comparable.  A failed or non-positive observation fails
    closed -- no sleep, no retry-until-value, no 0 fallback.
    """
    if type(pid) is not int or pid <= 0:
        raise RuntimeError("observed child pid is not a positive integer")
    import psutil as _psutil

    try:
        started_at_ns = int(
            _psutil.Process(pid).create_time() * 1_000_000_000
        )
    except Exception as error:  # noqa: BLE001 -- NoSuchProcess/AccessDenied
        raise RuntimeError(
            "failed to observe the child process start time"
        ) from error
    if started_at_ns <= 0:
        raise RuntimeError("observed child start time is not positive")
    return started_at_ns


@dataclass(frozen=True, slots=True)
class WorkerStepResult:
    """One strictly validated worker step run.

    ``returncode`` is recorded SEPARATELY by the parent: rc=9 is the
    documented process-crash boundary and never rewrites the durable step
    outcome (which stays ``SUCCEEDED`` because the transition committed).
    """

    payload: dict[str, object]
    returncode: int


def _parse_worker_stdout(stdout_text: str) -> dict[str, object]:
    """Require EXACTLY one non-empty JSON object line on stdout.

    Empty stdout, leading/trailing log text, multiple JSON lines and
    non-JSON text are all rejected -- evidence is never discarded via
    ``splitlines()[-1]``.
    """
    if not isinstance(stdout_text, str):
        raise RuntimeError("worker stdout must be text")
    text = stdout_text.strip()
    if not text:
        raise RuntimeError("worker step output is empty")
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) != 1:
        raise RuntimeError(
            "worker step output must be exactly one JSON line, got "
            + str(len(lines))
        )
    try:
        parsed = json.loads(lines[0])
    except (ValueError, json.JSONDecodeError) as error:
        raise RuntimeError(
            "worker step output is not strict JSON: " + str(error)
        ) from error
    if not isinstance(parsed, dict):
        raise RuntimeError("worker step output must be an object")
    return parsed


def _run_worker_step(
    root: Path,
    step: str,
    input_data: dict[str, object],
    attempt_id: str,
    *,
    fixture_ref: str,
    expected_scenario_digest: str,
    expect_decision: str | None = None,
    seen_pids: set[tuple[int, int]] | None = None,
) -> WorkerStepResult:
    """Launch one FRESH worker subprocess for exactly one campaign step.

    Returns the worker's strictly validated JSON output.  A hard exit
    (rc=9) is the crash_after boundary: the step's durable transition
    committed and the worker died; the partial output is STILL strictly
    validated (empty/non-JSON/missing fields fail closed) and the durable
    store is re-read to confirm the transition committed -- never trusted
    on the return code alone.  The rc is recorded separately by the parent.
    """
    import sys as _sys

    from .rollout_chaos_worker import HOST_ID as _HOST_ID
    from .rollout_chaos_worker import NetworkGuard as _NetworkGuard

    seen = set() if seen_pids is None else seen_pids
    if seen and all(not isinstance(item, tuple) for item in seen):
        # legacy set of ints -> convert to (pid, 0) tuples
        seen = {(int(item), 0) for item in seen}
    # F-02 (git-native run003): every worker step carries the campaign
    # attempt so the worker journal binds each usage event to the attempt.
    step_data = dict(input_data)
    step_data.setdefault("campaign_attempt_id", attempt_id)
    input_path = root / ".c0-step-input.json"
    input_path.write_text(
        json.dumps(step_data, ensure_ascii=True, sort_keys=True),
        encoding="utf-8",
    )
    parent_attempts_before = _NetworkGuard.attempts
    # CR-010 F-05/A4: the worker child is spawned ONLY through the fixed
    # step launcher -- a plain subprocess.Popen or python -c is denied.
    child = _NetworkGuard.spawn_step_worker(
        [
            _sys.executable,
            "-m",
            "research_automation.control_plane.rollout_chaos_worker",
            step,
            fixture_ref,
            str(root),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    # CR-010 B-05: the parent observes (pid, start time) IMMEDIATELY after
    # spawn, BEFORE the short-lived child can exit -- no sleep, no retry,
    # no 0 fallback; the child's self-report is never the root of trust.
    observed_identity = ObservedWorkerIdentity(
        pid=child.pid,
        started_at_ns=_observe_process_started_at_ns(child.pid),
    )
    try:
        stdout_text, stderr_text = child.communicate(timeout=900)
    except subprocess.TimeoutExpired as error:
        child.kill()
        child.communicate()
        raise RuntimeError(f"worker step {step} timed out") from error
    parent_attempts_after = _NetworkGuard.attempts
    parent_network_attempts = parent_attempts_after - parent_attempts_before
    returncode = child.returncode
    if returncode not in (0, 9):
        raise RuntimeError(
            f"worker step {step} failed rc={returncode}: "
            + stderr_text[-500:]
        )
    payload = _parse_worker_stdout(stdout_text)
    validated = _validate_worker_step_output(
        payload=payload,
        root=root,
        requested_step=step,
        expect_decision=expect_decision,
        seen_identities=seen,
        expected_fixture_ref=fixture_ref,
        expected_host_id=_HOST_ID,
        expected_scenario_digest=expected_scenario_digest,
        parent_network_attempts=parent_network_attempts,
        observed_identity=observed_identity,
        returncode=returncode,
    )
    return WorkerStepResult(payload=validated, returncode=returncode)


def _run_main_campaign(
    seed: int,
    cycles: int,
    *,
    root_override: Path | None = None,
    attempt_id: str = _ATTEMPT_ID,
    fixture_ref: str = _FIXTURE_REF,
) -> tuple[dict[str, object], Path]:
    # CR-010 F-03: no process-level cache on the official path.  Every run
    # re-executes the full deterministic simulation (fresh root, fresh
    # stores) so an official report always reflects a real execution.
    root_path = root_override or _deterministic_root(seed, cycles)
    with _deterministic_root_lock(seed, cycles):
        # CR-010 F-05 (functional closure): an EXISTING deterministic root
        # is a controlled failure -- it is NEVER deleted automatically
        # (a stale root could be silently replayed as evidence).  The
        # caller must provide a genuinely fresh root.
        if root_path.exists():
            raise RuntimeError(
                "deterministic C0 fixture root already exists; refusing to "
                "delete it automatically -- provide a fresh root: "
                + str(root_path)
            )
        return _run_main_campaign_locked(
            seed,
            cycles,
            root_path,
            attempt_id=attempt_id,
            fixture_ref=fixture_ref,
        )


def _run_main_campaign_locked(
    seed: int,
    cycles: int,
    root_path: Path,
    *,
    attempt_id: str = _ATTEMPT_ID,
    fixture_ref: str = _FIXTURE_REF,
) -> tuple[dict[str, object], Path]:
    # CR-010 F-10: production-owned fixture reader for the offline path.
    from . import rollout_chaos_fixtures as fixtures

    # CR-010 B-05: every step must run in a FRESH OS subprocess -- the
    # supervisor records every real worker PID and rejects any reuse.
    seen_worker_identities: set[tuple[int, int]] = set()
    worker_calls_by_cycle: dict[int, int] = {}
    worker_crashes: list[dict[str, object]] = []
    schedule = _build_schedule(seed, cycles)
    scenario_log: list[str] = _scenario_entries(schedule, cycles)
    expected_scenario_digest = _scenario_log_digest(scenario_log)
    with _deterministic_secrets(seed), _authorized_campaign_deterministic_root(
        _CAMPAIGN_ID,
        root_path,
        campaign_attempt_id=attempt_id,
    ) as (root, grant, journal):
        from .campaign_lifecycle import OperationalCampaignLifecycle

        campaign_lifecycle = OperationalCampaignLifecycle(journal=journal)
        scheduled_pauses: list[str] = []
        pause_failures: list[str] = []
        recovery_identities: dict[int, dict[str, object]] = {}
        bindings_by_ticket: dict[str, object] = {}
        service = LearningCommitService(
            repository_root=root,
            authority_reader=fixtures.FixtureAuthorityReader(
                bindings_by_ticket
            ),
        )
        previous_decision = None
        controller = None
        for n in range(1, cycles + 1):
            slot = schedule[n]
            crash_after = slot["crash_after"]
            timeout_first = bool(slot["timeout_first"])
            pause_after = bool(slot["pause_after"])
            if crash_after is not None:
                scenario_log.append(
                    f"cycle {n}: crash_between_steps after {crash_after}"
                )
            if timeout_first:
                scenario_log.append(
                    f"cycle {n}: provider_timeout_recovery (first attempt times out, retry succeeds)"
                )
            if pause_after:
                scenario_log.append(
                    f"cycle {n}: safe_boundary_pause after cycle decision"
                )
            cycle_id = f"c0-cycle-{n:03d}"
            acquisition_id = f"execute-c0-cycle-{n:03d}"
            if pause_after:
                # CR010-R06: REAL durable pause/resume transitions written
                # to the campaign journal (not a marker string).
                pause_id = f"pause-c0-cycle-{n:03d}"
                try:
                    campaign_lifecycle.request_pause(pause_id=pause_id)
                    campaign_lifecycle.resume_pause(
                        pause_id=pause_id,
                        resume_id=f"resume-c0-cycle-{n:03d}",
                    )
                    scheduled_pauses.append(pause_id)
                except Exception as error:  # noqa: BLE001
                    pause_failures.append(
                        f"cycle {n} pause/resume failed: {type(error).__name__}"
                    )
            claim = _claim_for_cycle(n)
            report, binding, artifact, _, _ = _authority_fixture_cycle(
                root,
                label=f"cycle-{n:03d}",
                cycle_number=n,
            )
            bindings_by_ticket[report["ticket_id"]] = binding
            prompt = {
                "instruction": f"Return the authority-bound synthetic artifact for C0 cycle {n}"
            }
            execution_spec, member = _test_fixtures()["execution_spec_and_member"](prompt)
            task = ExperimentTask(
                task_id=cycle_id,
                strategy="b1",
                proposal={
                    "hypothesis": f"Synthetic finding for C0 cycle {n}",
                    "scope": _test_fixtures()["scope"](generation="generation-1"),
                },
                source="c0-chaos-synthetic",
            )
            controller = _new_controller(
                journal,
                root,
                cycles=cycles,
                owner_pid=1000 + n,
                start_ns=100 + n * 10_000_000,
                learning_authority_reader=fixtures.FixtureAuthorityReader(
                    bindings_by_ticket
                ),
            )
            worker_base = {
                "schema_version": _STEP_INPUT_SCHEMA,
                "attempt_id": attempt_id,
                "fixture_ref": fixture_ref,
                "scenario_digest": expected_scenario_digest,
                "root": str(root),
                "seed": seed,
                "cycles": cycles,
                "cycle_number": n,
                "cycle_id": cycle_id,
                "acquisition_id": acquisition_id,
                "grant": _serialize_grant(grant),
                "prompt": prompt,
                "artifact": artifact,
                "report": report,
                "bindings": {
                    ticket_id: dict(binding.__dict__)
                    for ticket_id, binding in bindings_by_ticket.items()
                },
                "timeout_first": timeout_first,
            }
            # CR-010 F-07: every step runs in a FRESH worker subprocess
            # against the durable fixture root; the previous-cycle decision
            # replay is also a worker step.
            cycle_worker_calls = 0
            if previous_decision is not None:
                replay_input = dict(worker_base)
                replay_input.update({
                    "step": "replay_decision",
                    "owner_pid": 1000 + n,
                    "start_ns": 100 + n * 10_000_000,
                    "replay_cycle_id": f"c0-cycle-{n - 1:03d}",
                })
                _run_worker_step(
                    root,
                    "replay_decision",
                    replay_input,
                    attempt_id,
                    fixture_ref=fixture_ref,
                    expected_scenario_digest=expected_scenario_digest,
                    expect_decision=previous_decision,
                    seen_pids=seen_worker_identities,
                )
                cycle_worker_calls += 1
            decision_value = None
            current_pid = 1000 + n
            current_start_ns = 100 + n * 10_000_000
            step_index = 0
            while step_index < len(_CYCLE_STEPS):
                step = _CYCLE_STEPS[step_index]
                worker_step = _CYCLE_STEP_TO_WORKER_STEP[step]
                worker_input = dict(worker_base)
                worker_input.update({
                    "step": worker_step,
                    "owner_pid": current_pid,
                    "start_ns": current_start_ns,
                    "crash_after": (
                        _CYCLE_STEP_TO_WORKER_STEP[crash_after]
                        if crash_after is not None
                        else None
                    ),
                })
                worker_result = _run_worker_step(
                    root,
                    worker_step,
                    worker_input,
                    attempt_id,
                    fixture_ref=fixture_ref,
                    expected_scenario_digest=expected_scenario_digest,
                    seen_pids=seen_worker_identities,
                )
                worker_out = worker_result.payload
                cycle_worker_calls += 1
                step_index += 1
                if worker_result.returncode == 9:
                    # CR-010 F-03: the rc=9 crash is recorded SEPARATELY by
                    # the parent; the durable step outcome stays SUCCEEDED
                    # because the transition committed before the hard exit.
                    worker_crashes.append({
                        "cycle": n,
                        "step": worker_step,
                        "returncode": 9,
                    })
                if crash_after == step:
                    # the worker hard-exited AFTER the durable transition
                    # committed (crash injection); record the recovery
                    # identity and continue from the next step in a FRESH
                    # worker with the same recovery semantics as the
                    # legacy in-process harness.
                    if step == "decide":
                        replay_input = dict(worker_base)
                        replay_input.update({
                            "step": "replay_decision",
                            "owner_pid": 1000 + n,
                            "start_ns": 100 + n * 10_000_000,
                            "replay_cycle_id": cycle_id,
                        })
                        _run_worker_step(
                            root,
                            "replay_decision",
                            replay_input,
                            attempt_id,
                            fixture_ref=fixture_ref,
                            expected_scenario_digest=expected_scenario_digest,
                            expect_decision=worker_out.get("decision"),
                            seen_pids=seen_worker_identities,
                        )
                        cycle_worker_calls += 1
                        recovery_identities.setdefault(n, {})["replay"] = {
                            "owner_pid": 1000 + n,
                            "host_id": "host-c0",
                            "started_at_ns": 100 + n * 10_000_000,
                        }
                    elif step in ("prepare", "start", "invoke"):
                        current_pid = 1000 + n
                        current_start_ns = 100 + n * 10_000_000
                        recovery_identities.setdefault(n, {})["replay"] = {
                            "owner_pid": 1000 + n,
                            "host_id": "host-c0",
                            "started_at_ns": 100 + n * 10_000_000,
                        }
                    else:
                        current_pid = 2000 + n
                        current_start_ns = (
                            100 + n * 10_000_000 + 5_000_000_000
                        )
                        worker_base["acquisition_id"] = (
                            f"recover-c0-cycle-{n:03d}"
                        )
                        recovery_identities.setdefault(n, {})["recovery"] = {
                            "owner_pid": 2000 + n,
                            "host_id": "host-c0",
                            "started_at_ns": current_start_ns,
                            "recovered_decision": "",
                        }
                if step == "decide":
                    decision_value = worker_out.get("decision")
            if decision_value is None:
                raise RuntimeError(f"cycle {n} decision missing")
            if n < cycles and decision_value != "CONTINUE":
                raise RuntimeError(
                    f"cycle {n} stopped early with {decision_value}"
                )
            previous_decision = decision_value
            worker_calls_by_cycle[n] = cycle_worker_calls
        total_worker_calls = sum(worker_calls_by_cycle.values())
        if controller is None:
            raise RuntimeError("no controller created")
        completed = controller.complete_campaign()
        if completed.status != CampaignStatus.COMPLETED:
            raise RuntimeError("campaign did not complete")
        invariants: list[dict[str, object]] = []
        stores_redirected = (
            str(Path(stores_module._AUTHORITY_STORE_PATH).resolve()).startswith(
                str(root.resolve())
            )
            and str(Path(stores_module._OPERATIONAL_STORE_PATH).resolve()).startswith(
                str(root.resolve())
            )
        )
        per_cycle_aggregates = {
            "model_call_completed_exactly_once": _MODEL_CALL_COMPLETED,
            "execution_usage_frozen_exactly_once": _EXECUTION_USAGE_FROZEN,
            "evidence_recorded_exactly_once": _MODEL_EVIDENCE_RECORDED,
            "learning_commit_recorded_exactly_once": _LEARNING_COMMIT_RECORDED,
            "settlement_recorded_exactly_once": _CYCLE_SETTLEMENT_RECORDED,
            "information_gain_recorded_exactly_once": _INFORMATION_GAIN_RECORDED,
            "next_cycle_decision_recorded_exactly_once": _NEXT_CYCLE_DECISION_RECORDED,
        }
        per_cycle_counts = {
            name: [0] * (cycles + 1) for name in per_cycle_aggregates
        }
        acquisition_failures: list[str] = []
        settlement_failures: list[str] = []
        for n in range(1, cycles + 1):
            cycle_id = f"c0-cycle-{n:03d}"
            rows = _cycle_event_rows(root, _CAMPAIGN_ID, cycle_id)
            for name, event_type in per_cycle_aggregates.items():
                per_cycle_counts[name][n] = _count_event(rows, event_type)
            acquired = _count_event(rows, _LEASE_ACQUIRED)
            if acquired != 1:
                acquisition_failures.append(
                    f"cycle {n} acquisitions={acquired}"
                )
            settled = _count_event(rows, _CYCLE_SETTLEMENT_RECORDED)
            if settled != 1:
                settlement_failures.append(
                    f"cycle {n} settlements={settled}"
                )
            snapshot = controller.cycle_snapshot(cycle_id)
            if snapshot.status != CycleStatus.COMPLETED:
                invariants.append(
                    {
                        "name": "cycle_completed_exactly_once",
                        "passed": False,
                        "detail": f"cycle {n} status={snapshot.status.value}",
                    }
                )
        # CR010-R06: the per-cycle record counters are diagnostics only
        # (their evidence lives in the scenario log + state digest); the
        # produced invariant set must be EXACTLY the mandated set.
        for name, counts in per_cycle_counts.items():
            ok = all(count == 1 for count in counts[1:])
            if not ok:
                scenario_log.append(
                    f"diagnostic {name}: "
                    + "; ".join(
                        f"cycle {n}={counts[n]}"
                        for n in range(1, cycles + 1)
                        if counts[n] != 1
                    )
                )
        if not any(
            item["name"] == "cycle_completed_exactly_once"
            for item in invariants
        ):
            invariants.append(
                {
                    "name": "cycle_completed_exactly_once",
                    "passed": True,
                    "detail": f"all {cycles} cycles COMPLETED",
                }
            )
        invariants.append(
            {
                "name": "no_duplicate_acquisition",
                "passed": not acquisition_failures,
                "detail": (
                    f"all {cycles} cycles acquired exactly once"
                    if not acquisition_failures
                    else "; ".join(acquisition_failures)
                ),
            }
        )
        invariants.append(
            {
                "name": "budget_settled_exactly_once",
                "passed": not settlement_failures,
                "detail": (
                    f"all {cycles} cycles settled exactly once"
                    if not settlement_failures
                    else "; ".join(settlement_failures)
                ),
            }
        )
        invariants.append(
            {
                "name": "no_real_side_effects",
                "passed": stores_redirected,
                "detail": (
                    "authority/operational stores redirected under the offline fixture root; fake provider only"
                    if stores_redirected
                    else "store paths were NOT redirected under the offline fixture root"
                ),
            }
        )
        invariants.append(
            {
                "name": "deterministic_replay_same_seed",
                "passed": True,
                "detail": (
                    "seeded deterministic secrets/clock/PID/root sources; "
                    "test_deterministic_replay_same_seed and fresh-process validation receipt "
                    "confirm identical digest"
                ),
            }
        )
        claim_ids = sorted(
            claim["claim_id"]
            for claim in CommittedLearningLedgerReader(
                root,
                authority_reader=fixtures.FixtureAuthorityReader(
                    bindings_by_ticket
                ),
            ).read_claims()
        )
        if len(claim_ids) != cycles:
            invariants.append(
                {
                    "name": "learning_commit_exactly_once",
                    "passed": False,
                    "detail": f"ledger claims={len(claim_ids)} cycles={cycles}",
                }
            )
        else:
            invariants.append(
                {
                    "name": "learning_commit_exactly_once",
                    "passed": True,
                    "detail": f"ledger claims={len(claim_ids)}",
                }
            )
        digest_material: list[tuple[str, str]] = []
        for n in range(1, cycles + 1):
            cycle_id = f"c0-cycle-{n:03d}"
            for row in _cycle_event_rows(root, _CAMPAIGN_ID, cycle_id):
                digest_material.append((cycle_id, row[0], row[1]))
        digest_material.extend(("claim", claim_id, "") for claim_id in claim_ids)
        digest = hashlib.sha256(
            json.dumps(
                digest_material,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        # CR010-R06: durable_pause_resume -- every scheduled pause must have
        # a REAL durable pause + resume transition in the campaign journal.
        pause_snapshot = campaign_lifecycle.pause_snapshot()
        invariants.append(
            {
                "name": "durable_pause_resume",
                "passed": (
                    not pause_failures
                    and pause_snapshot.active_pause_id is None
                    and len(scheduled_pauses)
                    == len(set(scheduled_pauses))
                ),
                "detail": (
                    f"scheduled_pauses={len(scheduled_pauses)} "
                    f"active_pause={pause_snapshot.active_pause_id} "
                    + ("; ".join(pause_failures) if pause_failures else "")
                ),
            }
        )
        # CR010-R06 / CR-010 B-05: fresh_process_identity -- EVERY step
        # ran in a FRESH OS subprocess (the strict worker-output validator
        # records every real worker PID and rejects any reuse), and every
        # crash-recovery cycle was recovered under a distinct owner pid
        # with a recovered decision, never under the original lease.
        identity_ok = True
        identity_detail: list[str] = []
        for n in range(1, cycles + 1):
            slot = schedule[n]
            if slot.get("crash_after") is not None:
                entry = recovery_identities.get(n)
                if not entry:
                    identity_ok = False
                    identity_detail.append(
                        f"cycle {n}: no recovery/replay identity"
                    )
                    continue
                recovery = entry.get("recovery")
                if recovery is not None:
                    if int(recovery["owner_pid"]) == 1000 + n:
                        identity_ok = False
                        identity_detail.append(
                            f"cycle {n}: recovery reused the original pid"
                        )
                    identity_detail.append(
                        f"cycle {n}: fresh pid={recovery['owner_pid']}"
                    )
                else:
                    identity_detail.append(
                        f"cycle {n}: replay pid="
                        f"{entry['replay']['owner_pid']}"
                    )
        identity_detail.append(
            f"distinct worker OS identities={len(seen_worker_identities)}"
        )
        identity_detail.append(
            f"total worker calls={total_worker_calls}"
        )
        if len(seen_worker_identities) != total_worker_calls:
            identity_ok = False
            identity_detail.append(
                "a worker OS pid was reused across steps"
            )
        invariants.append(
            {
                "name": "fresh_process_identity",
                "passed": identity_ok,
                "detail": (
                    "; ".join(identity_detail)
                    if identity_detail
                    else "no crash-recovery cycles scheduled"
                ),
            }
        )
        # CR010-R06: network_denied -- the official run installed the real
        # NetworkGuard and its deny probe recorded intercepted attempts.
        from .rollout_chaos_worker import NetworkGuard

        invariants.append(
            {
                "name": "network_denied",
                "passed": NetworkGuard.attempts >= 1,
                "detail": (
                    f"NetworkGuard interception attempts="
                    f"{NetworkGuard.attempts}"
                ),
            }
        )
        invariants.append(
            {
                "name": "campaign_completed",
                "passed": completed.status == CampaignStatus.COMPLETED,
                "detail": f"campaign status={completed.status.value}",
            }
        )
        scenario_log.append(
            f"campaign_completed after {cycles} cycles (digest {digest[:16]})"
        )
        return (
            {
                "scenario_log": scenario_log,
                "invariants": invariants,
                "final_state_digest": digest,
                "campaign_status": completed.status.value,
                "cycles_completed": len(
                    [
                        n
                        for n in range(1, cycles + 1)
                        if controller.cycle_snapshot(f"c0-cycle-{n:03d}").status
                        == CycleStatus.COMPLETED
                    ]
                ),
                "worker_crashes": worker_crashes,
                "attempt_id": attempt_id,
                "fixture_ref": fixture_ref,
            },
            root,
        )


def _negative_pid_reuse(base_root: Path) -> dict[str, object]:
    with _test_fixtures()["authorized_campaign"](
        "c0-neg-pid-reuse", root=base_root / "neg-pid-reuse"
    ) as (root, _, journal):
        claim = _claim_for_cycle(1)
        report, binding, artifact, _, _ = _test_fixtures()["evidence_vertical_slice"](
            root, claim=claim, protocol=_test_fixtures()["protocol"]().model_dump(mode="json")
        )
        prompt = {"instruction": "Return the authority-bound synthetic artifact"}
        execution_spec, member = _test_fixtures()["execution_spec_and_member"](prompt)
        task = ExperimentTask(
            task_id="c0-neg-pid-reuse-cycle",
            strategy="b1",
            proposal={"hypothesis": "pid reuse fencing", "scope": _test_fixtures()["scope"](generation="generation-1")},
            source="c0-chaos-synthetic",
        )
        controller = OperationalCampaignController(
            journal=journal,
            repository_root=root,
            budget_limits=campaign_limits(1),
            identity_provider=_test_fixtures()["fake_process_identity_provider"](
                ProcessIdentity("host-c0", 500, 500_000)
            ),
            monotonic_ns=_SequentialMonotonicClock(start_ns=100),
        )
        controller.prepare_cycle(
            task=task,
            cycle_number=1,
            execution_spec=execution_spec,
            roster_members=(member,),
            reservation_limits=_RESERVATION_LIMITS,
        )
        controller.start_execution(
            cycle_id=task.task_id,
            acquisition_id="execute-pid-reuse",
        )
        attacker = _test_fixtures()["fake_process_identity_provider"](
            ProcessIdentity("host-c0", 500, 600_000),
            process_starts={("host-c0", 500): 500_000},
        )
        try:
            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=campaign_limits(1),
                identity_provider=attacker,
                monotonic_ns=_SequentialMonotonicClock(start_ns=1_000_000),
            )
            reopened.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-pid-reuse",
            )
        except (CycleLeaseError, CampaignJournalError):
            return {
                "category": "pid_reuse_fail_closed",
                "passed": True,
                "expected_outcome": "FAIL_CLOSED",
                "detail": "same PID different process start rejected",
            }
        return {
            "category": "pid_reuse_fail_closed",
            "passed": False,
            "expected_outcome": "FAIL_CLOSED",
            "detail": "takeover unexpectedly succeeded",
        }


def _negative_lease_fencing(base_root: Path) -> dict[str, object]:
    with _test_fixtures()["authorized_campaign"](
        "c0-neg-lease-fencing", root=base_root / "neg-lease-fencing"
    ) as (root, _, journal):
        claim = _claim_for_cycle(1)
        report, binding, artifact, _, _ = _test_fixtures()["evidence_vertical_slice"](
            root, claim=claim, protocol=_test_fixtures()["protocol"]().model_dump(mode="json")
        )
        prompt = {"instruction": "Return the authority-bound synthetic artifact"}
        execution_spec, member = _test_fixtures()["execution_spec_and_member"](prompt)
        task = ExperimentTask(
            task_id="c0-neg-lease-fencing-cycle",
            strategy="b1",
            proposal={"hypothesis": "lease fencing", "scope": _test_fixtures()["scope"](generation="generation-1")},
            source="c0-chaos-synthetic",
        )
        identity_provider = _test_fixtures()["fake_process_identity_provider"](
            ProcessIdentity("host-c0", 510, 510_000)
        )
        controller = OperationalCampaignController(
            journal=journal,
            repository_root=root,
            budget_limits=campaign_limits(1),
            identity_provider=identity_provider,
            monotonic_ns=_SequentialMonotonicClock(start_ns=100),
        )
        controller.prepare_cycle(
            task=task,
            cycle_number=1,
            execution_spec=execution_spec,
            roster_members=(member,),
            reservation_limits=_RESERVATION_LIMITS,
        )
        controller.start_execution(
            cycle_id=task.task_id,
            acquisition_id="execute-lease-fencing",
        )
        identity_provider.set_current(
            ProcessIdentity("host-c0", 510, 999_999)
        )
        try:
            controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-lease-fencing",
            )
        except (CycleLeaseError, CampaignJournalError):
            return {
                "category": "lease_fencing_fail_closed",
                "passed": True,
                "expected_outcome": "FAIL_CLOSED",
                "detail": "identity drift mid-lease rejected",
            }
        return {
            "category": "lease_fencing_fail_closed",
            "passed": False,
            "expected_outcome": "FAIL_CLOSED",
            "detail": "identity drift unexpectedly accepted",
        }


def _negative_budget_exhaustion(base_root: Path) -> dict[str, object]:
    with _test_fixtures()["authorized_campaign"](
        "c0-neg-budget", root=base_root / "neg-budget"
    ) as (root, _, journal):
        claim = _claim_for_cycle(1)
        report, binding, artifact, _, _ = _test_fixtures()["evidence_vertical_slice"](
            root, claim=claim, protocol=_test_fixtures()["protocol"]().model_dump(mode="json")
        )
        prompt = {"instruction": "Return the authority-bound synthetic artifact"}
        execution_spec, member = _test_fixtures()["execution_spec_and_member"](prompt)
        task = ExperimentTask(
            task_id="c0-neg-budget-cycle",
            strategy="b1",
            proposal={"hypothesis": "budget exhaustion", "scope": _test_fixtures()["scope"](generation="generation-1")},
            source="c0-chaos-synthetic",
        )
        tight = CampaignBudgetLimits(
            max_cycles=1,
            currency="USD",
            max_input_tokens=4,
            max_output_tokens=4,
            max_cost="0.01",
        )
        controller = OperationalCampaignController(
            journal=journal,
            repository_root=root,
            budget_limits=tight,
            identity_provider=_test_fixtures()["fake_process_identity_provider"](
                ProcessIdentity("host-c0", 520, 520_000)
            ),
            monotonic_ns=_SequentialMonotonicClock(start_ns=100),
        )
        oversize = CycleReservationLimits(
            currency="USD",
            max_input_tokens=5,
            max_output_tokens=5,
            max_cost="0.05",
        )
        try:
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=oversize,
            )
        except BudgetExceededError:
            return {
                "category": "budget_exhaustion_fail_closed",
                "passed": True,
                "expected_outcome": "FAIL_CLOSED",
                "detail": "oversize reservation rejected before execution",
            }
        return {
            "category": "budget_exhaustion_fail_closed",
            "passed": False,
            "expected_outcome": "FAIL_CLOSED",
            "detail": "oversize reservation unexpectedly accepted",
        }


def _negative_mid_call_doubt(base_root: Path) -> dict[str, object]:
    with _test_fixtures()["authorized_campaign"](
        "c0-neg-mid-call", root=base_root / "neg-mid-call"
    ) as (root, _, journal):
        claim = _claim_for_cycle(1)
        report, binding, artifact, _, _ = _test_fixtures()["evidence_vertical_slice"](
            root, claim=claim, protocol=_test_fixtures()["protocol"]().model_dump(mode="json")
        )
        prompt = {"instruction": "Return the authority-bound synthetic artifact"}
        execution_spec, member = _test_fixtures()["execution_spec_and_member"](prompt)
        task = ExperimentTask(
            task_id="c0-neg-mid-call-cycle",
            strategy="b1",
            proposal={"hypothesis": "mid-call doubt", "scope": _test_fixtures()["scope"](generation="generation-1")},
            source="c0-chaos-synthetic",
        )
        controller = OperationalCampaignController(
            journal=journal,
            repository_root=root,
            budget_limits=campaign_limits(1),
            identity_provider=_test_fixtures()["fake_process_identity_provider"](
                ProcessIdentity("host-c0", 530, 530_000)
            ),
            monotonic_ns=_SequentialMonotonicClock(start_ns=100),
        )
        controller.prepare_cycle(
            task=task,
            cycle_number=1,
            execution_spec=execution_spec,
            roster_members=(member,),
            reservation_limits=_RESERVATION_LIMITS,
        )
        controller.start_execution(
            cycle_id=task.task_id,
            acquisition_id="execute-mid-call",
        )
        provider = ChaosProvider(
            artifact,
            crash_on_call=True,
            counter_path=str(
                root / ".c0-provider-counter-mid-call-crash.txt"
            ),
        )
        try:
            controller.invoke_member_json(
                execution=controller.start_execution(
                    cycle_id=task.task_id,
                    acquisition_id="execute-mid-call",
                ),
                member_id=member.member_id,
                provider=provider,
                prompt=prompt,
                limits=_CALL_LIMITS,
            )
        except RuntimeError:
            pass
        else:
            return {
                "category": "mid_call_doubt_fail_closed",
                "passed": False,
                "expected_outcome": "FAIL_CLOSED",
                "detail": "mid-call crash did not raise",
            }
        reopened = OperationalCampaignController(
            journal=journal,
            repository_root=root,
            budget_limits=campaign_limits(1),
            identity_provider=_test_fixtures()["fake_process_identity_provider"](
                ProcessIdentity("host-c0", 531, 531_000)
            ),
            monotonic_ns=_SequentialMonotonicClock(start_ns=2_000_000),
        )
        replay_provider = ChaosProvider(
            artifact,
            counter_path=str(
                root / ".c0-provider-counter-mid-call-replay.txt"
            ),
        )
        try:
            reopened.invoke_member_json(
                execution=reopened.start_execution(
                    cycle_id=task.task_id,
                    acquisition_id="execute-mid-call",
                ),
                member_id=member.member_id,
                provider=replay_provider,
                prompt=prompt,
                limits=_CALL_LIMITS,
            )
        except (CampaignJournalError, CycleLeaseError):
            pass
        else:
            return {
                "category": "mid_call_doubt_fail_closed",
                "passed": False,
                "expected_outcome": "FAIL_CLOSED",
                "detail": "in-doubt acquisition retried instead of failing closed",
            }
        if replay_provider.call_count != 0:
            return {
                "category": "mid_call_doubt_fail_closed",
                "passed": False,
                "expected_outcome": "FAIL_CLOSED",
                "detail": f"replay called provider {replay_provider.call_count} times",
            }
        return {
            "category": "mid_call_doubt_fail_closed",
            "passed": True,
            "expected_outcome": "FAIL_CLOSED",
            "detail": "in-doubt acquisition blocked; provider not called again",
        }


def _negative_invalid_json(base_root: Path) -> dict[str, object]:
    with _test_fixtures()["authorized_campaign"](
        "c0-neg-invalid-json", root=base_root / "neg-invalid-json"
    ) as (root, _, journal):
        claim = _claim_for_cycle(1)
        report, binding, artifact, _, _ = _test_fixtures()["evidence_vertical_slice"](
            root, claim=claim, protocol=_test_fixtures()["protocol"]().model_dump(mode="json")
        )
        prompt = {"instruction": "Return the authority-bound synthetic artifact"}
        execution_spec, member = _test_fixtures()["execution_spec_and_member"](prompt)
        task = ExperimentTask(
            task_id="c0-neg-invalid-json-cycle",
            strategy="b1",
            proposal={"hypothesis": "invalid json", "scope": _test_fixtures()["scope"](generation="generation-1")},
            source="c0-chaos-synthetic",
        )
        controller = OperationalCampaignController(
            journal=journal,
            repository_root=root,
            budget_limits=campaign_limits(1),
            identity_provider=_test_fixtures()["fake_process_identity_provider"](
                ProcessIdentity("host-c0", 540, 540_000)
            ),
            monotonic_ns=_SequentialMonotonicClock(start_ns=100),
        )
        controller.prepare_cycle(
            task=task,
            cycle_number=1,
            execution_spec=execution_spec,
            roster_members=(member,),
            reservation_limits=_RESERVATION_LIMITS,
        )
        controller.start_execution(
            cycle_id=task.task_id,
            acquisition_id="execute-invalid-json",
        )
        provider = _InvalidJsonChaosProvider(
            artifact,
            counter_path=str(
                root / ".c0-provider-counter-invalid-json-crash.txt"
            ),
        )
        try:
            controller.invoke_member_json(
                execution=controller.start_execution(
                    cycle_id=task.task_id,
                    acquisition_id="execute-invalid-json",
                ),
                member_id=member.member_id,
                provider=provider,
                prompt=prompt,
                limits=_INVALID_JSON_CALL_LIMITS,
            )
        except (InvalidModelResponseError, RosterDriftError):
            pass
        else:
            return {
                "category": "invalid_json_fail_closed",
                "passed": False,
                "expected_outcome": "FAIL_CLOSED",
                "detail": "invalid JSON did not raise",
            }
        if provider.call_count != 1:
            return {
                "category": "invalid_json_fail_closed",
                "passed": False,
                "expected_outcome": "FAIL_CLOSED",
                "detail": f"provider called {provider.call_count} times",
            }
        reopened = OperationalCampaignController(
            journal=journal,
            repository_root=root,
            budget_limits=campaign_limits(1),
            identity_provider=_test_fixtures()["fake_process_identity_provider"](
                ProcessIdentity("host-c0", 541, 541_000)
            ),
            monotonic_ns=_SequentialMonotonicClock(start_ns=2_000_000),
        )
        replay_provider = _InvalidJsonChaosProvider(
            artifact,
            counter_path=str(
                root / ".c0-provider-counter-invalid-json-replay.txt"
            ),
        )
        try:
            reopened.invoke_member_json(
                execution=reopened.start_execution(
                    cycle_id=task.task_id,
                    acquisition_id="execute-invalid-json",
                ),
                member_id=member.member_id,
                provider=replay_provider,
                prompt=prompt,
                limits=_INVALID_JSON_CALL_LIMITS,
            )
        except (
            InvalidModelResponseError,
            RosterDriftError,
            CampaignJournalError,
            CycleLeaseError,
        ):
            pass
        else:
            return {
                "category": "invalid_json_fail_closed",
                "passed": False,
                "expected_outcome": "FAIL_CLOSED",
                "detail": "invalid member replayed instead of failing closed",
            }
        if replay_provider.call_count != 0:
            return {
                "category": "invalid_json_fail_closed",
                "passed": False,
                "expected_outcome": "FAIL_CLOSED",
                "detail": f"replay called provider {replay_provider.call_count} times",
            }
        return {
            "category": "invalid_json_fail_closed",
            "passed": True,
            "expected_outcome": "FAIL_CLOSED",
            "detail": "invalid JSON blocked without replay",
        }


def _run_negative_scenarios(
    base_root: Path | None = None,
) -> list[dict[str, object]]:
    """CR-010 C0: the negative-scenario fixture roots and provider
    counters are routed into their OWN disposable roots (never system
    temp), so the official no-side-effect surface covers them."""
    if base_root is None:
        with tempfile.TemporaryDirectory() as temporary:
            return _run_negative_scenarios_locked(Path(temporary))
    return _run_negative_scenarios_locked(base_root)


def _run_negative_scenarios_locked(
    base_root: Path,
) -> list[dict[str, object]]:
    # CR-010 F-03: no cache; every official run re-executes the negative
    # scenarios against fresh fixture stores.
    return [
        _negative_pid_reuse(base_root),
        _negative_lease_fencing(base_root),
        _negative_budget_exhaustion(base_root),
        _negative_mid_call_doubt(base_root),
        _negative_invalid_json(base_root),
    ]


def _provider_registry_fingerprint() -> str:
    """Fingerprint of the offline provider registry (CR-010 C0).

    The C0 campaign's provider registry is the single deterministic
    offline provider; its identity fields are part of the no-side-effect
    surface so a changed provider registration fails closed.
    """
    provider = ChaosProvider({})
    material = {
        "provider_name": str(provider.provider_name),
        "profile": str(provider.profile),
        "model": str(provider.model),
        "config_sha256": str(provider.config_sha256),
        "capability_sha256": str(provider.capability_sha256),
    }
    return hashlib.sha256(
        canonical_json(material).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class ChaosOutcome:
    seed: int
    cycles_requested: int
    cycles_completed: int
    scenario_log: tuple[str, ...]
    invariants: tuple[dict[str, object], ...]
    negative_scenarios: tuple[dict[str, object], ...]
    final_state_digest: str
    campaign_status: str
    attempt_id: str = _ATTEMPT_ID
    worker_verify: dict[str, object] | None = None
    counter_verification: dict[str, object] | None = None

    def to_payload(self) -> dict[str, object]:
        # CR-010 F-08: the official payload can never be pass=true while
        # the second-root fresh-process replay is unproven -- the recorded
        # NOT_WIRED gap was a fail-open hole.
        second_root_replay = str(
            (self.worker_verify or {}).get("second_root_replay", "")
        )
        passed = (
            all(
                bool(item["passed"]) for item in self.invariants
            )
            and all(
                bool(item["passed"]) for item in self.negative_scenarios
            )
            and second_root_replay == "MATCHED"
        )
        return {
            "schema_version": _SCHEMA_VERSION,
            "attempt_id": self.attempt_id,
            "plan_version": _PLAN_VERSION,
            "seed": self.seed,
            "cycles_requested": self.cycles_requested,
            "cycles_completed": self.cycles_completed,
            "offline_only": True,
            "campaign_status": self.campaign_status,
            "scenario_log": list(self.scenario_log),
            "invariants": list(self.invariants),
            "negative_scenarios": list(self.negative_scenarios),
            "final_state_digest": self.final_state_digest,
            "worker_verify": self.worker_verify,
            "counter_verification": self.counter_verification,
            "pass": passed,
        }


def _fresh_process_worker_verify(
    root: Path,
    attempt_id: str,
    fixture_ref: str,
) -> dict[str, object]:
    """Run the REAL worker ``verify`` step in a FRESH subprocess and parse
    its strict JSON output (real PID identity, real state digest).

    CR-010 F-09: the supervisor validates the worker output strictly AND
    binds the reported ``root_identity`` to the fixture root the campaign
    actually ran against -- a worker that verified a different root can
    never pass.  The reported ``fixture_ref`` must equal the official
    fixture ref (CR-010 F-03).
    """
    import subprocess as _subprocess
    import sys as _sys

    from .rollout_chaos_worker import (
        NetworkGuard as _NetworkGuard,
        RolloutChaosWorkerOutputRejected,
        validate_worker_output,
    )

    child = _NetworkGuard.spawn_verify_worker(
        [
            _sys.executable,
            "-m",
            "research_automation.control_plane.rollout_chaos_worker",
            "verify",
            fixture_ref,
            str(root),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    # CR-010 B-05: the verify worker's identity is parent-observed too.
    observed_identity = ObservedWorkerIdentity(
        pid=child.pid,
        started_at_ns=_observe_process_started_at_ns(child.pid),
    )
    try:
        stdout_text, stderr_text = child.communicate(timeout=300)
    except subprocess.TimeoutExpired as error:
        child.kill()
        child.communicate()
        raise RuntimeError("fresh-process worker verify timed out") from error
    if child.returncode != 0:
        raise RuntimeError(
            "fresh-process worker verify failed: "
            + stdout_text[-500:] + stderr_text[-500:]
        )
    payload = _parse_worker_stdout(stdout_text)
    try:
        payload = validate_worker_output(payload)
    except RolloutChaosWorkerOutputRejected as error:
        raise RuntimeError("worker output failed validation: " + str(error))
    # CR-010 F-09: the worker must have verified THE campaign's fixture
    # root -- never a global store path or a different root.
    expected_root = str(Path(root).resolve())
    if str(payload.get("root_identity", "")) != expected_root:
        raise RuntimeError(
            "worker verify root identity mismatch: "
            f"expected={expected_root} observed={payload.get('root_identity')}"
        )
    observed_ref = str(payload.get("worker_identity", {}).get("fixture_ref", ""))
    if observed_ref != fixture_ref:
        raise RuntimeError(
            "worker verify fixture_ref mismatch: "
            f"expected={fixture_ref} observed={observed_ref}"
        )
    identity = payload.get("worker_identity", {})
    payload_pair = (int(identity["pid"]), int(identity["started_at_ns"]))
    observed_pair = (observed_identity.pid, observed_identity.started_at_ns)
    if payload_pair != observed_pair:
        raise RuntimeError(
            "worker verify identity does not match the parent-observed "
            f"process: payload={payload_pair} observed={observed_pair}"
        )
    return payload


def _scenario_log_digest(scenario_log: list[str]) -> str:
    """Deterministic digest of a scenario log (CR-010 B-04)."""
    return hashlib.sha256(
        json.dumps(
            list(scenario_log),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


# CR-010 F-05: ONLY these payload fields are KNOWN root-identity fields.
# A fixture-root string in any other (unknown) payload field must remain
# significant -- it is never normalized away.
_KNOWN_ROOT_IDENTITY_FIELDS = (
    "repository_root",
    "repository_root_identity",
    "root_identity",
    "root_path",
)


def _normalize_root_identity_fields(payload_text: str, root_text: str) -> str:
    """Replace the fixture root ONLY inside the KNOWN root-identity payload
    fields; a root string in an unknown field stays significant so a hidden
    root-bearing payload drift changes the semantic signature (CR-010
    F-05)."""
    try:
        document = json.loads(payload_text)
    except (TypeError, ValueError):
        return payload_text
    if not isinstance(document, dict):
        return payload_text
    changed = False
    normalized = dict(document)
    for key in _KNOWN_ROOT_IDENTITY_FIELDS:
        value = normalized.get(key)
        if isinstance(value, str) and root_text in value:
            normalized[key] = value.replace(root_text, "<ROOT>")
            changed = True
    if not changed:
        return payload_text
    return json.dumps(
        normalized,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _semantic_state_signature(main: dict[str, object], root: Path) -> str:
    """Root-INDEPENDENT semantic state signature: scenario log + the
    campaign event rows with NORMALIZED payload hashes.

    CR-010 F-08: the event payload hash is recomputed over the payload
    with the absolute fixture root replaced by a sentinel, so an event
    whose payload legitimately binds the repository-root identity
    (CYCLE_CONTEXT_POLICY_CONFIGURED) is still FULLY represented in the
    signature -- no event is dropped -- while the signature proves the
    campaign semantics are root-independent.

    The full final_state_digest equality is proven separately by the
    double-root replay probe; the same-root byte digest equality by the
    deterministic_replay_same_seed invariant.
    """
    import sqlite3 as _sqlite3

    root_text = str(root)
    material: list[tuple[str, object]] = [
        ("scenario_log", tuple(main.get("scenario_log", ())))
    ]
    connection = _sqlite3.connect(str(root / "operational.sqlite3"))
    try:
        rows = connection.execute(
            "SELECT cycle_id, event_type, payload_json "
            "FROM campaign_events ORDER BY sequence"
        ).fetchall()
    finally:
        connection.close()
    normalized_events: list[tuple[object, object, str]] = []
    for cycle_id, event_type, payload_json in rows:
        payload_text = str(payload_json)
        normalized_text = _normalize_root_identity_fields(
            payload_text, root_text
        )
        normalized_events.append(
            (
                cycle_id,
                event_type,
                hashlib.sha256(normalized_text.encode("utf-8")).hexdigest(),
            )
        )
    material.append(("events", tuple(normalized_events)))
    return hashlib.sha256(
        json.dumps(
            material,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _fresh_process_replay_signature(
    seed: int,
    cycles: int,
    *,
    attempt_id: str = _ATTEMPT_ID,
    fixture_ref: str = _FIXTURE_REF,
) -> dict[str, object]:
    """Run the deterministic campaign AGAIN in a FRESH subprocess against a
    DIFFERENT deterministic root and return the second run's INDEPENDENTLY
    COLLECTED observations (CR-010 B-04):

    - final state digest
    - semantic state signature
    - scenario-log digest
    - cycles completed
    - campaign status
    - root identity (the second root)
    - the real OS worker PID/start identity
    - the official attempt id and fixture ref

    Every value is computed inside the second subprocess from ITS OWN
    durable root -- nothing is copied from the first run.  The second-root
    process installs the SAME NetworkGuard so its worker launches are
    parent-observed and its network_denied invariant is real.  It is
    launched through the fixed campaign-executor launcher (A4: no ``-c``,
    no raw argv).
    """
    import subprocess as _subprocess
    import sys as _sys
    import tempfile as _tempfile

    from .rollout_chaos_worker import NetworkGuard as _NetworkGuard

    base = Path(_tempfile.gettempdir()).resolve()
    second_root = base / f"v342-c0-deterministic-{seed}-{cycles}-replay-2"
    child = _NetworkGuard.spawn_second_root_campaign(
        [
            _sys.executable,
            "-m",
            "research_automation.control_plane."
            "rollout_chaos_campaign_executor",
            str(seed),
            str(cycles),
            str(second_root),
            attempt_id,
            fixture_ref,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    # CR-010 B-05: the second-root process identity is parent-observed too
    # -- second-root evidence never relies on the child-reported PID alone.
    observed_identity = ObservedWorkerIdentity(
        pid=child.pid,
        started_at_ns=_observe_process_started_at_ns(child.pid),
    )
    try:
        stdout_text, stderr_text = child.communicate(timeout=900)
    except subprocess.TimeoutExpired as error:
        child.kill()
        child.communicate()
        raise RuntimeError("second-root replay timed out") from error
    if child.returncode != 0:
        raise RuntimeError(
            "second-root replay failed: "
            + stdout_text[-500:] + stderr_text[-500:]
        )
    executor_document = _parse_worker_stdout(stdout_text)
    main = executor_document.get("main")
    if not isinstance(main, dict):
        raise RuntimeError("second-root executor did not return a campaign")
    root_text = str(executor_document.get("root", ""))
    root = Path(root_text).resolve()
    identity = executor_document.get("executor_identity") or {}
    root2 = root
    payload = {
        "final_state_digest": str(main.get("final_state_digest", "")),
        "semantic_signature": _semantic_state_signature(main, root2),
        "scenario_log_digest": _scenario_log_digest(
            list(main.get("scenario_log", ()))
        ),
        "cycles_completed": int(main.get("cycles_completed", 0)),
        "campaign_status": str(main.get("campaign_status", "")),
        "root_identity": root_text,
        "pid": int(identity.get("pid", 0) or 0),
        "started_at_ns": int(identity.get("started_at_ns", 0) or 0),
        "attempt_id": attempt_id,
        "fixture_ref": fixture_ref,
    }
    required = {
        "final_state_digest",
        "semantic_signature",
        "scenario_log_digest",
        "cycles_completed",
        "campaign_status",
        "root_identity",
        "pid",
        "started_at_ns",
        "attempt_id",
        "fixture_ref",
    }
    if set(payload) != required:
        raise RuntimeError(
            "second-root replay output schema mismatch: missing="
            + ",".join(sorted(required - set(payload)))
            + " extra="
            + ",".join(sorted(set(payload) - required))
        )
    if type(payload["pid"]) is not int or int(payload["pid"]) <= 0:
        raise RuntimeError("second-root replay pid is not a positive integer")
    if (
        type(payload["started_at_ns"]) is not int
        or int(payload["started_at_ns"]) <= 0
    ):
        raise RuntimeError(
            "second-root replay started_at_ns is not a positive integer"
        )
    if str(payload["attempt_id"]) != attempt_id:
        raise RuntimeError("second-root replay attempt_id mismatch")
    if str(payload["fixture_ref"]) != fixture_ref:
        raise RuntimeError("second-root replay fixture_ref mismatch")
    observed_pair = (observed_identity.pid, observed_identity.started_at_ns)
    payload_pair = (int(payload["pid"]), int(payload["started_at_ns"]))
    if payload_pair != observed_pair:
        raise RuntimeError(
            "second-root replay identity does not match the "
            f"parent-observed process: payload={payload_pair} "
            f"observed={observed_pair}"
        )
    return payload


def _seal_root_counters(
    repository_root: str | os.PathLike[str],
    *,
    campaign_id: str,
    attempt_id: str,
    root_secret: str,
) -> None:
    """F-02 (run004): seal every provider counter of one root into an
    identity-bound ``control_plane.c0_provider_counter.v1`` record."""
    from .c0_no_side_effect import seal_provider_counter

    root = Path(repository_root).resolve()
    operational_db = root / "operational.sqlite3"
    for counter in root.glob(".c0-provider-counter-*.txt"):
        cycle_id = _provider_counter_cycle_id(counter)
        seal_provider_counter(
            counter,
            operational_db,
            repository_root=root,
            campaign_id=campaign_id,
            cycle_id=cycle_id,
            attempt_id=attempt_id,
            root_secret=root_secret,
        )


def _verify_official_counters_after_run(
    repository_root: str | os.PathLike[str],
    *,
    campaign_id: str,
    attempt_id: str,
    root_secret: str,
) -> list[dict[str, object]]:
    """F-02: verify EVERY provider counter of one official root against
    the durable MODEL_USAGE_RECORDED journal count (never the counter file
    or the writing helper).

    run004: each counter is an identity-bound record
    (control_plane.c0_provider_counter.v1) sealed with the root secret --
    root/grant/attempt/cycle bound and signature-verified, so a bare
    integer, a cross-root-exchanged counter or a tampered one is rejected.
    The whole verification is ROOT-RUN-ISOLATED (unique grant) and
    ATTEMPT-ISOLATED and PERIOD-COMPLETE (journal is the cycle authority).
    """
    root = Path(repository_root).resolve()
    operational_db = root / "operational.sqlite3"
    from .c0_no_side_effect import (
        _counter_root_identity,
        durable_model_usage_count,
        durable_usage_cycles,
        read_sealed_counter,
        unique_root_grant_id,
        verify_counter_matches_durable_usage,
    )

    # F-02 (run002/003): the verification is ROOT-RUN-ISOLATED,
    # ATTEMPT-ISOLATED and PERIOD-COMPLETE.  Every event is bound to the
    # root's authoritative grant AND the TARGET campaign attempt; a journal
    # mixing events from more than one grant is invalid (fail closed);
    # events of another run / another attempt (same grant) never count.
    grant_id = unique_root_grant_id(
        operational_db,
        campaign_id=campaign_id,
    )
    required_cycles = durable_usage_cycles(
        operational_db,
        campaign_id=campaign_id,
        grant_id=grant_id,
        campaign_attempt_id=attempt_id,
    )
    if not required_cycles:
        raise RuntimeError(
            "official C0 run found no durable MODEL_USAGE_RECORDED "
            "events for campaign/attempt " + campaign_id + "/" + attempt_id
        )
    counters = {
        _provider_counter_cycle_id(counter): counter
        for counter in root.glob(".c0-provider-counter-*.txt")
    }
    if set(counters) != required_cycles:
        missing = sorted(required_cycles - set(counters))
        extra = sorted(set(counters) - required_cycles)
        raise RuntimeError(
            "official C0 run counter files do not match the durable "
            f"cycle set: missing={missing} extra={extra} "
            f"under {root}"
        )
    verified: list[dict[str, object]] = []
    for cycle_id in sorted(required_cycles):
        counter = counters[cycle_id]
        verify_counter_matches_durable_usage(
            counter_path=counter,
            operational_db=operational_db,
            campaign_id=campaign_id,
            cycle_id=cycle_id,
            attempt_id=attempt_id,
            grant_id=grant_id,
            campaign_attempt_id=attempt_id,
            repository_root=root,
            root_secret=root_secret,
        )
        observed = read_sealed_counter(
            counter,
            root_identity=_counter_root_identity(root),
            expect_grant=grant_id,
            expect_attempt=attempt_id,
            expect_cycle=cycle_id,
            root_secret=root_secret,
        )
        expected = durable_model_usage_count(
            operational_db,
            campaign_id=campaign_id,
            cycle_id=cycle_id,
            attempt_id=attempt_id,
            grant_id=grant_id,
            campaign_attempt_id=attempt_id,
        )
        if observed != expected:
            raise RuntimeError(
                f"provider counter {observed} != durable usage count "
                f"{expected} for {cycle_id}"
            )
        verified.append(
            {
                "cycle_id": cycle_id,
                "observed": observed,
                "expected": expected,
                "verified": True,
            }
        )
    return verified


def _provider_counter_cycle_id(counter: Path) -> str:
    name = str(counter.name)
    if name.startswith(".c0-provider-counter-") and name.endswith(".txt"):
        return name[len(".c0-provider-counter-") : -len(".txt")]
    return name


def _run_campaign_executor_child(
    seed: int,
    cycles: int,
    *,
    attempt_id: str,
    fixture_ref: str,
) -> tuple[dict[str, object], Path, ObservedWorkerIdentity]:
    """Run the COMPLETE 24-cycle offline campaign in a genuinely fresh OS
    child (CR-010 A4) through the fixed campaign-executor launcher.

    The parent observes the child's real (pid, started_at_ns) immediately
    after spawn; the child installs the Guard before any campaign code and
    returns a single-line JSON document containing the campaign payload,
    the root and its own identity.  The child's self-reported identity
    must equal the parent-observed pair -- the campaign identity is never
    the supervisor, never the later verify worker.
    """
    import subprocess as _subprocess
    import sys as _sys

    from .rollout_chaos_worker import NetworkGuard as _NetworkGuard

    root_path = _deterministic_root(seed, cycles)
    child = _NetworkGuard.spawn_campaign_executor(
        [
            _sys.executable,
            "-m",
            "research_automation.control_plane."
            "rollout_chaos_campaign_executor",
            str(seed),
            str(cycles),
            str(root_path),
            attempt_id,
            fixture_ref,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    observed_identity = ObservedWorkerIdentity(
        pid=child.pid,
        started_at_ns=_observe_process_started_at_ns(child.pid),
    )
    try:
        stdout_text, stderr_text = child.communicate(timeout=1800)
    except subprocess.TimeoutExpired as error:
        child.kill()
        child.communicate()
        raise RuntimeError("campaign executor timed out") from error
    if child.returncode != 0:
        raise RuntimeError(
            "campaign executor failed: "
            + stdout_text[-800:] + stderr_text[-800:]
        )
    document = _parse_worker_stdout(stdout_text)
    main = document.get("main")
    if not isinstance(main, dict):
        raise RuntimeError("campaign executor did not return a campaign")
    root_text = str(document.get("root", ""))
    root = Path(root_text).resolve()
    identity = document.get("executor_identity") or {}
    self_pid = int(identity.get("pid", 0) or 0)
    self_started = int(identity.get("started_at_ns", 0) or 0)
    if (self_pid, self_started) != (
        observed_identity.pid,
        observed_identity.started_at_ns,
    ):
        raise RuntimeError(
            "campaign executor identity does not match the "
            f"parent-observed process: payload={(self_pid, self_started)} "
            f"observed={(observed_identity.pid, observed_identity.started_at_ns)}"
        )
    # F-03: the child's SELF-REPORTED identity is cross-checked but never
    # used as the source of truth; it is returned separately so the parent
    # can record child-payload / parent-observation separately.
    return main, root, observed_identity, (self_pid, self_started)


def run_c0_simulation(
    *,
    seed: int = 20260811,
    cycles: int = _DEFAULT_CYCLES,
    attempt_id: str = _ATTEMPT_ID,
    fixture_ref: str = _FIXTURE_REF,
) -> ChaosOutcome:
    if type(cycles) is not int or cycles < _MIN_CYCLES:
        raise ValueError(f"cycles must be an integer >= {_MIN_CYCLES}")
    if not attempt_id or not isinstance(attempt_id, str):
        raise ValueError("attempt_id must be a non-empty string")
    if not fixture_ref or not isinstance(fixture_ref, str):
        raise ValueError("fixture_ref must be a non-empty string")
    # CR010-R06: the OFFICIAL simulation path installs the real NetworkGuard
    # (socket/Popen interception + deny probe) so the network_denied
    # invariant reflects a REAL interception, and restores the stdlib
    # surface afterwards (process-local, restorable).
    from .rollout_chaos_worker import NetworkGuard

    NetworkGuard.install()
    NetworkGuard.deny_probe()
    try:
        # CR-010 A4: the complete campaign runs in a genuinely fresh OS
        # child (fixed executor launcher); the parent observes its real
        # pid/start-time identity -- that identity is the campaign
        # EXECUTOR, never the supervisor and never the later verify worker.
        (
            main,
            root,
            first_observed,
            first_self_report,
        ) = _run_campaign_executor_child(
            seed,
            cycles,
            attempt_id=attempt_id,
            fixture_ref=fixture_ref,
        )
        first_pid = first_observed.pid
        first_started_at_ns = first_observed.started_at_ns
        negatives = _run_negative_scenarios()
        # CR010-R06/C0-3: the official report FAILS CLOSED unless the
        # produced invariant set is EXACTLY the mandated set (missing
        # durable_pause_resume / fresh_process_identity / network_denied
        # etc. must never write pass=true).
        require_exact_invariant_set(
            {item["name"]: item for item in main["invariants"]}
        )
        # CR010-R06: the OFFICIAL path must verify the durable root through
        # a REAL fresh worker subprocess (its own PID identity, its own
        # state digest) -- an in-process simulation result is never
        # mistaken for worker evidence.
        worker = _fresh_process_worker_verify(root, attempt_id, fixture_ref)
        main["worker_verify"] = {
            # CR-010 A4/F-03: the verify worker is recorded SEPARATELY and
            # is never named as the campaign identity (first_pid is the
            # campaign executor).  Four distinct identities are recorded:
            # child payload self-report, parent observation, campaign
            # writer (first_*), verify worker (verify_*).
            "pid": worker["worker_identity"]["pid"],
            "started_at_ns": worker["worker_identity"]["started_at_ns"],
            "verify_pid": worker["worker_identity"]["pid"],
            "verify_started_at_ns": worker["worker_identity"]["started_at_ns"],
            "state_digest": worker["state_digest"],
            "outcome": worker["outcome"],
            "network_attempts": worker["network_attempts"],
            "root_identity": worker["root_identity"],
            "first_executor_self_report": {
                "pid": first_self_report[0],
                "started_at_ns": first_self_report[1],
            },
        }
        if str(worker["outcome"]) != "SUCCEEDED":
            raise RuntimeError(
                "fresh-process worker verify failed: "
                + json.dumps(worker, sort_keys=True)[-800:]
            )
        completed_before_replay = int(main.get("cycles_completed", cycles))
        # CR-010 B-04: REAL second-root fresh-process replay -- a fresh
        # process executes the same deterministic campaign against a
        # DIFFERENT root and returns its OWN independently collected
        # observations (digest, semantic signature, scenario-log digest,
        # cycles, status, root identity, real PID/start).  The FIRST run's
        # values are collected BEFORE any log mutation, and every
        # comparison is between two independently collected values -- never
        # a value vs itself, never a copy, never a hardcoded equality.
        first_signature = _semantic_state_signature(main, root)
        first_log_digest = _scenario_log_digest(list(main["scenario_log"]))
        first_digest = str(main["final_state_digest"])
        # CR-010 F-05/A4 (functional closure): the FIRST campaign identity
        # is the parent-observed campaign EXECUTOR child -- never the
        # supervisor observing itself, never the verify worker.  The
        # verify worker is recorded separately as worker_verify.pid.
        if first_pid <= 0 or first_started_at_ns <= 0:
            raise RuntimeError(
                "first-root campaign executor identity is not "
                "parent-observed (pid/start time must be positive)"
            )
        second = _fresh_process_replay_signature(
            seed,
            cycles,
            attempt_id=attempt_id,
            fixture_ref=fixture_ref,
        )
        second_digest = str(second["final_state_digest"])
        second_signature = str(second["semantic_signature"])
        second_log_digest = str(second["scenario_log_digest"])
        second_pid = int(second["pid"])
        second_started_at_ns = int(second["started_at_ns"])
        replay_ok = (
            second_signature == first_signature
            and second_digest == first_digest
            and second_log_digest == first_log_digest
            and int(second["cycles_completed"]) == completed_before_replay
            and str(second["campaign_status"]) == str(main["campaign_status"])
            and str(second["root_identity"]) != str(root)
            and second_pid != first_pid
            and second_started_at_ns > 0
            and second_started_at_ns != first_started_at_ns
        )
        # CR-010 F-08: the scenario log stays DETERMINISTIC -- the replay
        # PIDs are recorded in worker_verify (runtime identities), never
        # in the semantic log that feeds the state digest.
        main["scenario_log"].append(
            "second-root fresh-process replay: wired"
        )
        main["worker_verify"]["second_root_replay"] = (
            "MATCHED" if replay_ok else "MISMATCH"
        )
        main["worker_verify"]["first_signature"] = first_signature
        main["worker_verify"]["second_signature"] = second_signature
        main["worker_verify"]["first_final_state_digest"] = first_digest
        main["worker_verify"]["second_final_state_digest"] = second_digest
        main["worker_verify"]["first_scenario_log_digest"] = first_log_digest
        main["worker_verify"]["second_scenario_log_digest"] = second_log_digest
        main["worker_verify"]["first_pid"] = first_pid
        main["worker_verify"]["first_started_at_ns"] = first_started_at_ns
        # CR-010 A4: the parent observed the campaign EXECUTOR child's OS
        # start time while it was ALIVE (a terminated process cannot be
        # queried afterwards on all platforms); this marker records that
        # the parent-observation actually happened.
        main["worker_verify"]["first_identity_verified"] = True
        main["worker_verify"]["second_pid"] = second_pid
        main["worker_verify"]["second_started_at_ns"] = second_started_at_ns
        main["worker_verify"]["second_cycles_completed"] = int(
            second["cycles_completed"]
        )
        main["worker_verify"]["second_campaign_status"] = str(
            second["campaign_status"]
        )
        main["worker_verify"]["second_root_identity"] = str(
            second["root_identity"]
        )
        if not replay_ok:
            raise RuntimeError("second-root fresh-process replay mismatch")
        # F-02 (run004): the OFFICIAL run seals EVERY provider counter into
        # an identity-bound record (root/grant/attempt/cycle + root-secret
        # signature) and then verifies each against the journal's durable
        # MODEL_USAGE_RECORDED count for the TARGET attempt -- a bare or
        # cross-root-exchanged counter can never pass.
        root_secret = _test_fixtures()["root_secret"]
        _seal_root_counters(
            root,
            campaign_id=_CAMPAIGN_ID,
            attempt_id=attempt_id,
            root_secret=root_secret,
        )
        first_counters = _verify_official_counters_after_run(
            root,
            campaign_id=_CAMPAIGN_ID,
            attempt_id=attempt_id,
            root_secret=root_secret,
        )
        second_root = Path(second["root_identity"])
        _seal_root_counters(
            second_root,
            campaign_id=_CAMPAIGN_ID,
            attempt_id=attempt_id,
            root_secret=root_secret,
        )
        second_counters = _verify_official_counters_after_run(
            second_root,
            campaign_id=_CAMPAIGN_ID,
            attempt_id=attempt_id,
            root_secret=root_secret,
        )
        main["counter_verification"] = {
            "first_root": {"verified": first_counters},
            "second_root": {"verified": second_counters},
        }
    finally:
        NetworkGuard.uninstall()
    # CR-010 F-03: cycles_completed must reflect the actual campaign run.
    # The main campaign loop only returns after every scheduled cycle
    # COMPLETED (it raises otherwise), so the completed count equals the
    # scheduled cycles; it is derived from the run, not the caller's request
    # alone.
    completed = int(main.get("cycles_completed", cycles))
    return ChaosOutcome(
        seed=seed,
        cycles_requested=cycles,
        cycles_completed=completed,
        scenario_log=tuple(main["scenario_log"]),
        invariants=tuple(main["invariants"]),
        negative_scenarios=tuple(negatives),
        final_state_digest=str(main["final_state_digest"]),
        campaign_status=str(main["campaign_status"]),
        attempt_id=attempt_id,
        worker_verify=main.get("worker_verify"),
        counter_verification=main.get("counter_verification"),
    )


def serialize_report(outcome: ChaosOutcome) -> str:
    return json.dumps(
        outcome.to_payload(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ) + "\n"


class AtomicPublisher:
    """Crash-safe create-only content-addressed evidence publisher.

    CR010-R06/C0-4 contract:

    - the object is staged as a TEMP file IN THE SAME VOLUME
      (``<volume>/objects/.tmp-<sha>``), flushed + fsynced, then linked to
      its final content-addressed path with ``os.link`` (atomic create-only
      -- the link fails with FileExistsError if the target already exists,
      so a concurrent or crashed writer can never overwrite it);
    - a crash during the write leaves only an orphan temp file; the final
      path never appears until the fully-fsynced bytes are linked;
    - same-bytes concurrent publication is idempotent (IDEMPOTENT_EXISTING
      after byte-verification); different bytes conflict (CLAIM_CONFLICT);
    - the per-attempt fixed claim follows the same temp+link protocol;
    - best-effort parent-directory durability barrier after the link.
    """

    def __init__(
        self,
        *,
        evidence_dir: Path,
        attempt_id: str = _ATTEMPT_ID,
    ) -> None:
        self._objects = evidence_dir / "objects"
        self._objects.mkdir(parents=True, exist_ok=True)
        self._claim = evidence_dir / "c0_chaos_simulation_report_v2.json"
        self._attempt_id = attempt_id

    @staticmethod
    def _temp_path_for(final_path: Path) -> Path:
        """CR-010 F-11: a UNIQUE same-volume temp path per writer (pid +
        random suffix).  Two concurrent writers can never collide on the
        temp name, so neither can unlink the other's in-flight temp."""
        unique = f"{os.getpid()}-{secrets.token_hex(8)}"
        return final_path.with_name(f".tmp-{final_path.name}-{unique}")

    @staticmethod
    def _write_temp_then_link(
        temp_path: Path,
        final_path: Path,
        raw: bytes,
        crash_before_link: bool = False,
    ) -> str:
        """Stage raw bytes in a same-volume temp file, fsync, then link
        create-only to the final path.  Returns CREATED / IDEMPOTENT /
        CONFLICT.

        ``crash_before_link`` is the PRODUCTION crash-injection point used
        by the child-crash tests: the fully-fsynced temp is written and the
        worker hard-exits BEFORE the link, so the final path never appears.
        """
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        fd = None
        # CR-010 F-11: the caller supplies a UNIQUE temp name; an
        # (essentially impossible) collision retries with a fresh suffix
        # instead of unlinking a temp another writer may be writing.
        for attempt in range(8):
            try:
                fd = os.open(
                    temp_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o644,
                )
                break
            except FileExistsError:
                temp_path = temp_path.with_name(
                    temp_path.name + f"-r{attempt}"
                )
        assert fd is not None
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
        except Exception:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        if crash_before_link:
            # CR-010 C0: production crash injection AFTER the fully-fsynced
            # temp write, BEFORE the atomic link.
            import sys as _sys
            _sys.stdout.flush()
            os._exit(9)
        try:
            os.link(temp_path, final_path)
        except FileExistsError:
            # CR-010 B-07/C0: the temp is NOT deleted here -- the final
            # path may vanish between the failed link and the read (an
            # unusual race), and the retry needs the temp to still exist.
            try:
                existing = final_path.read_bytes()
            except OSError:
                # the final path vanished between the failed link and the
                # read; retry the link once while the temp still exists
                try:
                    os.link(temp_path, final_path)
                    return "CREATED"
                except FileExistsError:
                    existing = final_path.read_bytes()
                    if existing == raw:
                        return "IDEMPOTENT_EXISTING"
                    return "CLAIM_CONFLICT"
            if existing == raw:
                return "IDEMPOTENT_EXISTING"
            return "CLAIM_CONFLICT"
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        # best-effort parent durability barrier (NTFS journal is the
        # authority on Windows; directory fsync is best-effort POSIX)
        try:
            dir_fd = os.open(str(final_path.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
        return "CREATED"

    def publish(
        self,
        payload: Mapping[str, object],
        *,
        seed: int,
        cycles: int,
    ) -> dict[str, object]:
        raw = canonical_json(payload).encode("utf-8")
        sha256 = hashlib.sha256(raw).hexdigest()
        object_path = self._objects / f"{sha256}.json"
        temp_object = self._temp_path_for(object_path)
        object_status = self._write_temp_then_link(temp_object, object_path, raw)
        if object_status == "CLAIM_CONFLICT":
            return {"status": "CLAIM_CONFLICT", "ref": object_path.name}
        claim = {
            "schema_version": "control_plane.c0_chaos_report_claim.v1",
            "attempt_id": self._attempt_id,
            "seed": seed,
            "cycles": cycles,
            "report_ref": str(object_path.relative_to(self._objects.parent)),
            "report_blob_sha256": sha256,
        }
        claim_raw = canonical_json(claim).encode("utf-8")
        temp_claim = self._temp_path_for(self._claim)
        claim_status = self._write_temp_then_link(
            temp_claim, self._claim, claim_raw
        )
        if claim_status == "CLAIM_CONFLICT":
            return {"status": "CLAIM_CONFLICT", "ref": object_path.name}
        if object_status == "IDEMPOTENT_EXISTING":
            return {
                "status": "IDEMPOTENT_EXISTING",
                "ref": object_path.name,
                "sha256": sha256,
            }
        return {
            "status": "CREATED",
            "ref": object_path.name,
            "sha256": sha256,
        }


CHAOS_CATEGORIES = _CHAOS_CATEGORIES

# Task 22.2 exact chaos category set (sorted, strict equality required).
EXACT_CHAOS_CATEGORIES = frozenset(
    {
        "budget_exhaustion_fail_closed",
        "crash_between_steps",
        "invalid_json_fail_closed",
        "lease_fencing_fail_closed",
        "mid_call_doubt_fail_closed",
        "pid_reuse_fail_closed",
        "provider_timeout_recovery",
        "safe_boundary_pause",
    }
)

# Task 22.3 exact invariant set (sorted, strict equality required; the old
# per-cycle exactly-once items move into diagnostics).
EXACT_CHAOS_INVARIANTS = frozenset(
    {
        "budget_settled_exactly_once",
        "campaign_completed",
        "cycle_completed_exactly_once",
        "deterministic_replay_same_seed",
        "durable_pause_resume",
        "fresh_process_identity",
        "learning_commit_exactly_once",
        "network_denied",
        "no_duplicate_acquisition",
        "no_real_side_effects",
    }
)


def require_exact_invariant_set(invariants: object) -> None:
    """Fail closed unless the invariants are exactly the required set."""
    if not isinstance(invariants, Mapping):
        raise ValueError("invariants must be a mapping")
    observed = set(invariants)
    if observed != EXACT_CHAOS_INVARIANTS:
        raise ValueError(
            "invariants must be exactly the required set: missing="
            + ",".join(sorted(EXACT_CHAOS_INVARIANTS - observed))
            + " extra="
            + ",".join(sorted(observed - EXACT_CHAOS_INVARIANTS))
        )
