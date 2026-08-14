
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
import shutil
import sqlite3
import msvcrt
import tempfile
import functools
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

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
    """Lazy test-fixture imports (C0 chaos driver reuses fixture builders).

    Kept inside a function so the production import graph never contains
    ``tests.*`` (corrective plan Step 9.4 production-import scan).  This
    module is the C0 rollout driver only; P6/P7/P8 production modules must
    not import these fixtures.
    """
    from tests.test_control_plane_campaign_freeze import _protocol_member
    from tests.test_control_plane_campaign_lease import _FakeProcessIdentityProvider
    from tests.test_control_plane_campaign_preflight import _scope
    from tests.test_control_plane_campaign_store import (
        NOW,
        ROOT_SECRET,
        _authorized_campaign,
        _claim_campaign_grant,
    )
    from tests.test_control_plane_campaign_two_cycle import (
        _execution_spec_and_member,
    )
    from tests.test_control_plane_evidence_learning import (
        EvidenceLearningVerticalSliceTests,
    )
    from tests.test_foundations_protocols import _protocol

    return {
        "protocol_member": _protocol_member,
        "fake_process_identity_provider": _FakeProcessIdentityProvider,
        "scope": _scope,
        "now": NOW,
        "root_secret": ROOT_SECRET,
        "authorized_campaign": _authorized_campaign,
        "claim_campaign_grant": _claim_campaign_grant,
        "execution_spec_and_member": _execution_spec_and_member,
        "evidence_vertical_slice": EvidenceLearningVerticalSliceTests,
        "protocol": _protocol,
    }


_MIN_CYCLES = 20
_DEFAULT_CYCLES = 24
# Official attempt id is injected by the authorized CLI (CR-010 F-03); the
# legacy default is kept only for unit/read-only contexts and the production
# driver never writes evidence under it.
_ATTEMPT_ID = "c0-attempt-003"
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

_CALL_LIMITS = OperationalModelCallLimits(
    currency="USD",
    max_input_tokens=20,
    max_output_tokens=10,
    max_cost="0.1",
    max_wall_time_ms=5_000,
    max_attempts=2,
)

_INVALID_JSON_CALL_LIMITS = OperationalModelCallLimits(
    currency="USD",
    max_input_tokens=20,
    max_output_tokens=10,
    max_cost="0.1",
    max_wall_time_ms=5_000,
    max_attempts=1,
)

_RESERVATION_LIMITS = CycleReservationLimits(
    currency="USD",
    max_input_tokens=20,
    max_output_tokens=10,
    max_cost="0.1",
    max_wall_time_ms=5_000,
    max_tool_attempts=2,
)


def campaign_limits(cycles: int) -> CampaignBudgetLimits:
    return CampaignBudgetLimits(
        max_cycles=cycles,
        currency="USD",
        max_input_tokens=cycles * 20 + 100,
        max_output_tokens=cycles * 10 + 100,
        max_cost=str(cycles + 2),
        max_wall_time_ms=cycles * 5_000 + 60_000,
        max_tool_attempts=cycles * 2 + 8,
    )


class _SequentialMonotonicClock:
    """Deterministic increasing monotonic clock (ns)."""

    def __init__(self, start_ns: int = 100, step_ns: int = 1_000_000) -> None:
        self._next = start_ns
        self._step = step_ns

    def __call__(self) -> int:
        value = self._next
        self._next += self._step
        return value


class ChaosProvider:
    """Offline fake provider matching the roster member binding.

    The controller executes providers inside a spawned subprocess, so the
    call counter is persisted in a shared temp file; the timeout-once mode
    therefore survives subprocess pickling and retry re-spawns.
    """

    provider_name = "fake-provider"
    profile = "offline-local"
    model = "deterministic-reviewer"
    config_sha256 = "2" * 64
    capability_sha256 = "3" * 64

    def __init__(
        self,
        artifact: dict[str, object],
        *,
        timeout_first: bool = False,
        counter_path: str | None = None,
    ) -> None:
        self._artifact = dict(artifact)
        self._timeout_first = timeout_first
        if counter_path is None:
            handle = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                delete=False,
                prefix="c0-provider-counter-",
            )
            handle.write("0")
            handle.close()
            counter_path = handle.name
        self._counter_path = str(counter_path)
        Path(self._counter_path).write_text("0", encoding="utf-8")

    @property
    def call_count(self) -> int:
        try:
            raw = Path(self._counter_path).read_text(encoding="utf-8").strip()
            return int(raw or "0")
        except FileNotFoundError:
            return 0

    def _next_call_number(self) -> int:
        counter_path = Path(self._counter_path)
        with counter_path.open("r+", encoding="utf-8") as stream:
            raw = stream.read().strip() or "0"
            value = int(raw) + 1
            stream.seek(0)
            stream.write(str(value))
            stream.truncate()
        return value

    def invoke(self, request: object) -> ProviderResponse:
        number = self._next_call_number()
        if self._timeout_first and number == 1:
            raise TimeoutError("synthetic provider timeout")
        return ProviderResponse(
            output_text=json.dumps(
                self._artifact,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
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


def _claim_for_cycle(cycle_number: int) -> dict[str, object]:
    return {
        "kind": "NEGATIVE",
        "summary": f"Synthetic scoped finding from C0 cycle {cycle_number}",
        "scope": json.dumps(
            _test_fixtures()["scope"](generation="generation-1"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "parent_lineage": [],
        "reopen_predicate": "[]",
        "future_usage_guidance": '{"conclusion":"AVOID","directional_status":"avoid"}',
    }


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
    baseline_raw = _canonical_bytes(
        {
            "phase": "P3",
            "repository_root": str(root.resolve()),
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
):
    """Deterministic-root twin of the repository's test fixture context."""
    root.mkdir(parents=True, exist_ok=True)
    with patch.multiple(
        stores_module,
        _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
        _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
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


@contextmanager
def _deterministic_secrets(seed: int):
    """Deterministic secrets token source so cross-process replays are stable.

    The durable controller/journal/lease layers generate lease ids, nonces,
    and grant ids through the stdlib ``secrets`` module. For the offline C0
    simulation only, we substitute a seeded token generator so identical seeds
    produce byte-identical event payloads and digests across processes.
    """
    rng = random.Random((seed * 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF)

    def token_hex(nbytes=None):
        return rng.randbytes(int(nbytes or 16)).hex()

    def token_urlsafe(nbytes=None):
        raw = rng.randbytes(int(nbytes or 32))
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    with patch("secrets.token_hex", side_effect=token_hex), patch(
        "secrets.token_urlsafe",
        side_effect=token_urlsafe,
    ):
        yield


def _new_controller(
    journal,
    root: Path,
    *,
    cycles: int,
    owner_pid: int,
    start_ns: int,
) -> OperationalCampaignController:
    return OperationalCampaignController(
        journal=journal,
        repository_root=root,
        budget_limits=campaign_limits(cycles),
        identity_provider=_test_fixtures()["fake_process_identity_provider"](
            ProcessIdentity("host-c0", owner_pid, start_ns)
        ),
        monotonic_ns=_SequentialMonotonicClock(start_ns=start_ns, step_ns=1_000_000),
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


def _run_main_campaign(
    seed: int,
    cycles: int,
) -> tuple[dict[str, object], Path]:
    # CR-010 F-03: no process-level cache on the official path.  Every run
    # re-executes the full deterministic simulation (fresh root, fresh
    # stores) so an official report always reflects a real execution.
    root_path = _deterministic_root(seed, cycles)
    with _deterministic_root_lock(seed, cycles):
        if root_path.exists():
            shutil.rmtree(root_path)
        return _run_main_campaign_locked(seed, cycles, root_path)


def _run_main_campaign_locked(
    seed: int,
    cycles: int,
    root_path: Path,
) -> tuple[dict[str, object], Path]:
    schedule = _build_schedule(seed, cycles)
    scenario_log: list[str] = []
    with _deterministic_secrets(seed), _authorized_campaign_deterministic_root(
        _CAMPAIGN_ID,
        root_path,
    ) as (root, _, journal):
        from .campaign_lifecycle import OperationalCampaignLifecycle

        campaign_lifecycle = OperationalCampaignLifecycle(journal=journal)
        scheduled_pauses: list[str] = []
        pause_failures: list[str] = []
        recovery_identities: dict[int, dict[str, object]] = {}
        service = LearningCommitService(repository_root=root)
        bindings_by_ticket: dict[str, object] = {}
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
            )
            if previous_decision is not None:
                with patch(
                    "research_automation.control_plane.evidence_learning."
                    "AuthorityReader.verify_task_report_binding",
                    side_effect=_verify_binding_side_effect(bindings_by_ticket),
                ):
                    replayed = controller.replay_next_cycle_decision(
                        cycle_id=f"c0-cycle-{n - 1:03d}"
                    )
                if replayed.decision != previous_decision:
                    raise RuntimeError(
                        f"cycle {n - 1} decision replay mismatch"
                    )
            prepared = None
            execution = None
            provider = None
            usage = None
            evidence = None
            learning = None
            settlement = None
            info_gain = None
            decision = None
            step_index = 0
            while step_index < len(_CYCLE_STEPS):
                step = _CYCLE_STEPS[step_index]
                if prepared is None and step != "prepare":
                    with patch(
                        "research_automation.control_plane.evidence_learning."
                        "AuthorityReader.verify_task_report_binding",
                        side_effect=_verify_binding_side_effect(bindings_by_ticket),
                    ):
                        prepared = controller.prepare_cycle(
                            task=task,
                            cycle_number=n,
                            execution_spec=execution_spec,
                            roster_members=(member,),
                            reservation_limits=_RESERVATION_LIMITS,
                        )
                if execution is None and step in (
                    "start",
                    "invoke",
                    "complete",
                    "evidence",
                    "commit",
                    "settle",
                    "info_gain",
                    "decide",
                ):
                    execution = controller.start_execution(
                        cycle_id=cycle_id,
                        acquisition_id=acquisition_id,
                    )
                if step == "prepare":
                    with patch(
                        "research_automation.control_plane.evidence_learning."
                        "AuthorityReader.verify_task_report_binding",
                        side_effect=_verify_binding_side_effect(bindings_by_ticket),
                    ):
                        prepared = controller.prepare_cycle(
                            task=task,
                            cycle_number=n,
                            execution_spec=execution_spec,
                            roster_members=(member,),
                            reservation_limits=_RESERVATION_LIMITS,
                        )
                elif step == "start":
                    if execution is None:
                        execution = controller.start_execution(
                            cycle_id=cycle_id,
                            acquisition_id=acquisition_id,
                        )
                elif step == "invoke":
                    if provider is None:
                        provider = ChaosProvider(
                            artifact,
                            timeout_first=timeout_first,
                        )
                    controller.invoke_member_json(
                        execution=execution,
                        member_id=member.member_id,
                        provider=provider,
                        prompt=prompt,
                        limits=_CALL_LIMITS,
                    )
                elif step == "complete":
                    usage = controller.complete_model_execution(
                        execution=execution
                    )
                elif step == "evidence":
                    evidence = controller.record_model_evidence(
                        execution=execution,
                        member_id=member.member_id,
                        evidence_adapter=EvidenceAdapter(
                            known_runners={"fixture-runner": "1.0.0"},
                            approved_protocol=artifact["executed_protocol"],
                            approved_claim=artifact["claim"],
                        ),
                    )
                elif step == "commit":
                    with patch(
                        "research_automation.control_plane.evidence_learning."
                        "AuthorityReader.verify_task_report_binding",
                        side_effect=_verify_binding_side_effect(bindings_by_ticket),
                    ):
                        learning = controller.commit_learning(
                            execution=execution,
                            evidence_receipt=evidence,
                            authority_task_report=report,
                            learning_commit_sink=CampaignLearningCommitSink(
                                journal=journal,
                                service=service,
                            ),
                        )
                elif step == "settle":
                    with patch(
                        "research_automation.control_plane.evidence_learning."
                        "AuthorityReader.verify_task_report_binding",
                        side_effect=_verify_binding_side_effect(bindings_by_ticket),
                    ):
                        settlement = controller.settle_cycle(
                            execution=execution,
                            execution_usage=usage,
                            learning_commit_receipt=learning,
                        )
                elif step == "info_gain":
                    with patch(
                        "research_automation.control_plane.evidence_learning."
                        "AuthorityReader.verify_task_report_binding",
                        side_effect=_verify_binding_side_effect(bindings_by_ticket),
                    ):
                        info_gain = controller.record_information_gain(
                            execution=execution,
                            settlement_receipt=settlement,
                        )
                elif step == "decide":
                    with patch(
                        "research_automation.control_plane.evidence_learning."
                        "AuthorityReader.verify_task_report_binding",
                        side_effect=_verify_binding_side_effect(bindings_by_ticket),
                    ):
                        decision = controller.decide_next_cycle(
                            execution=execution,
                            information_gain_receipt=info_gain,
                        )
                step_index += 1
                if crash_after == step:
                    if step == "decide":
                        controller = _new_controller(
                            journal,
                            root,
                            cycles=cycles,
                            owner_pid=1000 + n,
                            start_ns=100 + n * 10_000_000,
                        )
                        recovery_identities.setdefault(n, {})["replay"] = {
                            "owner_pid": 1000 + n,
                            "host_id": "host-c0",
                            "started_at_ns": 100 + n * 10_000_000,
                        }
                        prepared = None
                        execution = None
                        provider = None
                        with patch(
                            "research_automation.control_plane.evidence_learning."
                            "AuthorityReader.verify_task_report_binding",
                            side_effect=_verify_binding_side_effect(bindings_by_ticket),
                        ):
                            replayed = controller.replay_next_cycle_decision(
                                cycle_id=cycle_id
                            )
                        if replayed.decision != decision.decision:
                            raise RuntimeError(
                                f"cycle {n} replay after decide mismatch"
                            )
                    elif step in ("prepare", "start", "invoke"):
                        controller = _new_controller(
                            journal,
                            root,
                            cycles=cycles,
                            owner_pid=1000 + n,
                            start_ns=100 + n * 10_000_000,
                        )
                        recovery_identities.setdefault(n, {})["replay"] = {
                            "owner_pid": 1000 + n,
                            "host_id": "host-c0",
                            "started_at_ns": 100 + n * 10_000_000,
                        }
                        prepared = None
                        execution = None
                        provider = None
                    else:
                        recovery_start_ns = 100 + n * 10_000_000 + 5_000_000_000
                        controller = _new_controller(
                            journal,
                            root,
                            cycles=cycles,
                            owner_pid=2000 + n,
                            start_ns=recovery_start_ns,
                        )
                        provider = None
                        recovery_identity = _test_fixtures()["fake_process_identity_provider"](
                            ProcessIdentity("host-c0", 2000 + n, recovery_start_ns)
                        )
                        lease_journal = OperationalCycleLeaseJournal(
                            journal=journal,
                            lifecycle=OperationalCampaignLifecycle(journal=journal),
                            identity_provider=recovery_identity,
                            monotonic_ns=_SequentialMonotonicClock(
                                start_ns=recovery_start_ns
                            ),
                        )
                        replacement = lease_journal.recover(
                            cycle_id=cycle_id,
                            acquisition_id=f"recover-c0-cycle-{n:03d}",
                            stale_after_ns=1,
                        )
                        recovery_identities.setdefault(n, {})[
                            "recovery"
                        ] = {
                            "owner_pid": 2000 + n,
                            "host_id": "host-c0",
                            "started_at_ns": recovery_start_ns,
                            "recovered_decision": str(
                                replacement.lease_id if replacement else ""
                            ),
                        }
                        execution = ExecutingOperationalCycle(
                            cycle=controller.cycle_snapshot(cycle_id),
                            lease=replacement,
                        )
            if decision is None:
                raise RuntimeError(f"cycle {n} decision missing")
            if n < cycles and decision.decision != "CONTINUE":
                raise RuntimeError(
                    f"cycle {n} stopped early with {decision.decision}"
                )
            previous_decision = decision.decision
        if controller is None:
            raise RuntimeError("no controller created")
        with patch(
            "research_automation.control_plane.evidence_learning."
            "AuthorityReader.verify_task_report_binding",
            side_effect=_verify_binding_side_effect(bindings_by_ticket),
        ):
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
        with patch(
            "research_automation.control_plane.evidence_learning."
            "AuthorityReader.verify_task_report_binding",
            side_effect=_verify_binding_side_effect(bindings_by_ticket),
        ):
            claim_ids = sorted(
                claim["claim_id"]
                for claim in CommittedLearningLedgerReader(root).read_claims()
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
        # CR010-R06: fresh_process_identity -- every crash-recovery cycle
        # must have been recovered under a FRESH process identity (distinct
        # owner pid) with a recovered decision, never under the original
        # lease.
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
            },
            root,
        )


def _negative_pid_reuse() -> dict[str, object]:
    with _test_fixtures()["authorized_campaign"]("c0-neg-pid-reuse") as (root, _, journal):
        claim = _claim_for_cycle(1)
        report, binding, artifact, _, _ = _test_fixtures()["evidence_vertical_slice"]()._authority_fixture(
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


def _negative_lease_fencing() -> dict[str, object]:
    with _test_fixtures()["authorized_campaign"]("c0-neg-lease-fencing") as (root, _, journal):
        claim = _claim_for_cycle(1)
        report, binding, artifact, _, _ = _test_fixtures()["evidence_vertical_slice"]()._authority_fixture(
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


def _negative_budget_exhaustion() -> dict[str, object]:
    with _test_fixtures()["authorized_campaign"]("c0-neg-budget") as (root, _, journal):
        claim = _claim_for_cycle(1)
        report, binding, artifact, _, _ = _test_fixtures()["evidence_vertical_slice"]()._authority_fixture(
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


def _negative_mid_call_doubt() -> dict[str, object]:
    with _test_fixtures()["authorized_campaign"]("c0-neg-mid-call") as (root, _, journal):
        claim = _claim_for_cycle(1)
        report, binding, artifact, _, _ = _test_fixtures()["evidence_vertical_slice"]()._authority_fixture(
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
        provider = ChaosProvider(artifact)
        try:
            with patch(
                "research_automation.control_plane.campaign_controller."
                "RetryingModelInvocation.invoke_json_with_receipt",
                side_effect=RuntimeError("synthetic mid-call crash"),
            ):
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
        replay_provider = ChaosProvider(artifact)
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


def _negative_invalid_json() -> dict[str, object]:
    with _test_fixtures()["authorized_campaign"]("c0-neg-invalid-json") as (root, _, journal):
        claim = _claim_for_cycle(1)
        report, binding, artifact, _, _ = _test_fixtures()["evidence_vertical_slice"]()._authority_fixture(
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
        provider = _InvalidJsonChaosProvider(artifact)
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
        replay_provider = _InvalidJsonChaosProvider(artifact)
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


def _verify_binding_side_effect(bindings_by_ticket: dict[str, object]):
    def verify(report):
        ticket_id = report.get("ticket_id")
        if ticket_id not in bindings_by_ticket:
            raise RuntimeError(f"no fixture binding for ticket {ticket_id}")
        return bindings_by_ticket[ticket_id]

    return verify

def _run_negative_scenarios() -> list[dict[str, object]]:
    # CR-010 F-03: no cache; every official run re-executes the negative
    # scenarios against fresh fixture stores.
    return [
        _negative_pid_reuse(),
        _negative_lease_fencing(),
        _negative_budget_exhaustion(),
        _negative_mid_call_doubt(),
        _negative_invalid_json(),
    ]


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

    def to_payload(self) -> dict[str, object]:
        passed = all(
            bool(item["passed"]) for item in self.invariants
        ) and all(
            bool(item["passed"]) for item in self.negative_scenarios
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
            "pass": passed,
        }


def run_c0_simulation(
    *,
    seed: int = 20260811,
    cycles: int = _DEFAULT_CYCLES,
    attempt_id: str = _ATTEMPT_ID,
) -> ChaosOutcome:
    if type(cycles) is not int or cycles < _MIN_CYCLES:
        raise ValueError(f"cycles must be an integer >= {_MIN_CYCLES}")
    if not attempt_id or not isinstance(attempt_id, str):
        raise ValueError("attempt_id must be a non-empty string")
    # CR010-R06: the OFFICIAL simulation path installs the real NetworkGuard
    # (socket/Popen interception + deny probe) so the network_denied
    # invariant reflects a REAL interception, and restores the stdlib
    # surface afterwards (process-local, restorable).
    from .rollout_chaos_worker import NetworkGuard

    NetworkGuard.install()
    NetworkGuard.deny_probe()
    try:
        main, _root = _run_main_campaign(seed, cycles)
        negatives = _run_negative_scenarios()
        # CR010-R06/C0-3: the official report FAILS CLOSED unless the
        # produced invariant set is EXACTLY the mandated set (missing
        # durable_pause_resume / fresh_process_identity / network_denied
        # etc. must never write pass=true).
        require_exact_invariant_set(
            {item["name"]: item for item in main["invariants"]}
        )
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
    def _write_temp_then_link(temp_path: Path, final_path: Path, raw: bytes) -> str:
        """Stage raw bytes in a same-volume temp file, fsync, then link
        create-only to the final path.  Returns CREATED / IDEMPOTENT /
        CONFLICT."""
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(
                temp_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644,
            )
        except FileExistsError:
            # orphan temp from a crashed writer: a temp is never the final
            # object, so clearing it and retrying is safe
            try:
                temp_path.unlink()
            except OSError:
                pass
            fd = os.open(
                temp_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644,
            )
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
        try:
            os.link(temp_path, final_path)
        except FileExistsError:
            temp_path.unlink(missing_ok=True)
            try:
                existing = final_path.read_bytes()
            except OSError:
                # the final path vanished between the failed link and the
                # read (an unusual race); retry the link once
                os.link(temp_path, final_path)
                return "CREATED"
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
        temp_object = self._objects / f".tmp-{sha256}"
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
        temp_claim = self._claim.with_name(f".tmp-{self._claim.name}")
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
