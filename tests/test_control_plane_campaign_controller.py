from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import replace
from decimal import Context, Inexact, Rounded, localcontext
from threading import Barrier, Event, get_ident
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sys
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from research_automation.control_plane import campaign_controller as campaign_controller_module
from research_automation.control_plane import campaign_context as campaign_context_module
from research_automation.control_plane import campaign_freeze as campaign_freeze_module
from research_automation.control_plane.campaign_metering import (
    ResourceObservation,
)
from research_automation.control_plane.campaign_controller import (
    CampaignBudgetLimits,
    CycleReservationLimits,
    ExecutingOperationalCycle,
    OperationalEvidenceReceipt,
    OperationalLearningCommitReceipt,
    OperationalModelCallLimits,
    OperationalCampaignController,
    _controller_sha256,
    operational_prompt_sha256,
)
from research_automation.control_plane.campaign import (
    InvalidModelResponseError,
    InvocationOutcome,
    ModelInvocationProviderError,
    ModelInvocationTimeoutError,
    ProviderResponse,
    UsageEnvelope,
    UsageStatus,
    _is_usage_journal_error,
)
from research_automation.control_plane.evidence_learning import (
    EvidenceAdapter,
    LearningCommitAuthorizationError,
    LearningCommitService,
)
from research_automation.control_plane.campaign_context import (
    CycleContextConflictError,
    CycleContextIntegrityError,
    OperationalCycleContextJournal,
)
from research_automation.control_plane.memory import CommittedLearningLedgerReader
from research_automation.control_plane.campaign_freeze import (
    CycleFreezeConflictError,
    CycleFreezeError,
    CycleFreezeIntegrityError,
    OperationalCycleFreezeJournal,
)
from research_automation.control_plane.budget import (
    BudgetConflictError,
    BudgetExceededError,
)
from research_automation.control_plane.campaign_lease import (
    CycleLeaseIntegrityError,
    OperationalCycleLeaseJournal,
    ProcessIdentity,
)
from research_automation.control_plane.campaign_lifecycle import (
    CampaignLifecycleError,
    CampaignStateConflictError,
    CampaignStatus,
    CycleStatus,
    OperationalCampaignLifecycle,
)
from research_automation.control_plane.campaign_store import (
    CampaignLearningCommitSink,
    CampaignJournalError,
    OperationalCampaignJournal,
    OperationalUsageJournal,
    _event_integrity_sha256,
    _event_domain_payload,
)
from research_automation.control_plane.campaign_roster import (
    OperationalRosterJournal,
    RosterDriftError,
)
from research_automation.control_plane.sqlite_uow import (
    SqliteStoreBusyError,
    _SqliteUnitOfWork,
)
from research_automation.control_plane.task_reports import build_task_report_v2
from research_automation.foundations.protocols import (
    MaterialProtocolChangeError,
    ProtocolDefinition,
    compile_execution_spec,
)
from research_automation.task_queue import ExperimentTask
from tests.test_control_plane_campaign_freeze import (
    _protocol_member,
    _swap_and_resign_event_sequences,
)
from tests.test_control_plane_campaign_lease import _FakeProcessIdentityProvider
from tests.test_control_plane_campaign_preflight import _claim, _scope
from tests.test_control_plane_campaign_store import (
    NOW,
    ROOT_SECRET,
    _authorized_campaign,
)
from tests.test_control_plane_evidence_learning import (
    EvidenceLearningVerticalSliceTests,
)
from tests.test_foundations_protocols import _approval, _protocol


_PROVIDER_CALL_COUNTER_PATHS: set[Path] = set()


def _synthetic_protocol() -> dict[str, object]:
    return _protocol().model_dump(mode="json")


def _synthetic_claim_scope_text(*, generation: str = "generation-1") -> str:
    return json.dumps(
        _scope(generation=generation),
        sort_keys=True,
        separators=(",", ":"),
    )


def _synthetic_protocol_with_notes(notes: str) -> dict[str, object]:
    payload = _synthetic_protocol()
    payload["metadata"]["notes"] = notes
    return ProtocolDefinition.model_validate_json(
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        strict=True,
    ).model_dump(mode="json")


def _new_provider_call_counter_path() -> str:
    descriptor, raw_path = tempfile.mkstemp(
        prefix="control-plane-provider-call-",
        suffix=".sqlite3",
    )
    os.close(descriptor)
    path = Path(raw_path)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE provider_call_counter ("
            "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
            "value INTEGER NOT NULL CHECK (value >= 0))"
        )
        connection.execute(
            "INSERT INTO provider_call_counter (singleton, value) VALUES (1, 0)"
        )
        connection.commit()
    finally:
        connection.close()
    _PROVIDER_CALL_COUNTER_PATHS.add(path)
    return str(path)


def tearDownModule() -> None:
    for path in tuple(_PROVIDER_CALL_COUNTER_PATHS):
        path.unlink(missing_ok=True)
    _PROVIDER_CALL_COUNTER_PATHS.clear()


class _BoundFakeProvider:
    provider_name = "fake-provider"
    profile = "offline-local"
    model = "deterministic-reviewer"
    config_sha256 = "2" * 64
    capability_sha256 = "3" * 64

    def __init__(self, *, timeouts_before_success: int = 0) -> None:
        self._call_count_path = _new_provider_call_counter_path()
        self.last_request: object | None = None
        self._timeouts_before_success = timeouts_before_success
        self._last_call_count = 0

    @property
    def call_count(self) -> int:
        connection = sqlite3.connect(self._call_count_path, timeout=30)
        try:
            row = connection.execute(
                "SELECT value FROM provider_call_counter WHERE singleton = 1"
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise AssertionError("provider call counter is missing")
        return int(row[0])

    def _increment_call_count(self) -> int:
        connection = sqlite3.connect(
            self._call_count_path,
            timeout=30,
            isolation_level=None,
        )
        try:
            connection.execute("PRAGMA busy_timeout = 30000")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE provider_call_counter SET value = value + 1 "
                "WHERE singleton = 1"
            )
            row = connection.execute(
                "SELECT value FROM provider_call_counter WHERE singleton = 1"
            ).fetchone()
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        if row is None:
            raise AssertionError("provider call counter is missing")
        self._last_call_count = int(row[0])
        return self._last_call_count

    def invoke(self, request: object) -> ProviderResponse:
        call_count = self._increment_call_count()
        self.last_request = request
        if call_count <= self._timeouts_before_success:
            raise TimeoutError("synthetic provider timeout")
        return ProviderResponse(
            output_text='{"status":"ok","source":"synthetic"}',
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


def _increment_bound_fake_provider_counter(provider: _BoundFakeProvider) -> int:
    return provider._increment_call_count()


class _ProcessMarkerBoundFakeProvider(_BoundFakeProvider):
    def __init__(self, marker_path: str) -> None:
        super().__init__()
        self._marker_path = marker_path

    def invoke(self, request: object) -> ProviderResponse:
        Path(self._marker_path).write_text(str(os.getpid()), encoding="ascii")
        return super().invoke(request)


class _UnpicklableBoundFakeProvider(_BoundFakeProvider):
    def __init__(self) -> None:
        super().__init__()
        self.reduce_calls = 0

    def __reduce_ex__(self, protocol: int) -> object:
        del protocol
        self.reduce_calls += 1
        raise TypeError("synthetic provider serialization failure")


class _RestoredSerializationProbeProvider:
    provider_name = "fake-provider"
    profile = "offline-local"
    model = "deterministic-reviewer"
    config_sha256 = "2" * 64
    capability_sha256 = "3" * 64

    def invoke(self, request: object) -> ProviderResponse:
        del request
        return ProviderResponse(
            output_text='{"status":"ok","source":"synthetic"}',
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


class _BlockingSerializationBoundFakeProvider(
    _RestoredSerializationProbeProvider
):
    def __init__(self, started: Event, release: Event) -> None:
        self._started = started
        self._release = release
        self.reduce_calls = 0

    def __reduce_ex__(self, protocol: int) -> object:
        del protocol
        self.reduce_calls += 1
        self._started.set()
        if not self._release.wait(timeout=5):
            raise TimeoutError("synthetic provider serialization was not released")
        return (_RestoredSerializationProbeProvider, ())


class _BlockingFailingSerializationBoundFakeProvider(
    _RestoredSerializationProbeProvider
):
    def __init__(self, started: Event, release: Event) -> None:
        self._started = started
        self._release = release
        self.reduce_calls = 0

    def __reduce_ex__(self, protocol: int) -> object:
        del protocol
        self.reduce_calls += 1
        self._started.set()
        if not self._release.wait(timeout=5):
            raise TimeoutError("synthetic provider serialization was not released")
        raise TypeError("synthetic provider serialization failure")


class _HangingProcessMarkerBoundFakeProvider(_BoundFakeProvider):
    def __init__(self, marker_path: str) -> None:
        super().__init__()
        self._marker_path = marker_path

    def invoke(self, request: object) -> ProviderResponse:
        del request
        Path(self._marker_path).write_text(str(os.getpid()), encoding="ascii")
        while True:
            time.sleep(0.05)


class _InvalidJsonBoundFakeProvider(_BoundFakeProvider):
    def __init__(self, output_text: str) -> None:
        super().__init__()
        self._output_text = output_text

    def invoke(self, request: object) -> ProviderResponse:
        return replace(super().invoke(request), output_text=self._output_text)


class _RequestModelDriftBoundFakeProvider(_BoundFakeProvider):
    def invoke(self, request: object) -> ProviderResponse:
        return replace(
            super().invoke(request),
            request_model="provider-attributed-drift-model",
        )


class _LeaseSwapBoundFakeProvider(_BoundFakeProvider):
    def __init__(self, barrier: Barrier) -> None:
        super().__init__()
        self._barrier = barrier

    @property
    def provider_name(self) -> str:
        self._barrier.wait(timeout=5)
        self._barrier.wait(timeout=5)
        return "fake-provider"


class _MissingCurrencyBoundFakeProvider(_BoundFakeProvider):
    def invoke(self, request: object) -> ProviderResponse:
        response = super().invoke(request)
        return replace(
            response,
            raw_usage={
                "input_tokens": 7,
                "output_tokens": 3,
                "total_tokens": 10,
                "reported_cost": "0.02",
            },
        )


class _EvidenceArtifactBoundFakeProvider(_BoundFakeProvider):
    def invoke(self, request: object) -> ProviderResponse:
        self._increment_call_count()
        self.last_request = request
        return ProviderResponse(
            output_text=(
                '{"access_event_ids":[],"artifact_refs":[],'
                '"claim":null,"executed_protocol":'
                '{"label":"synthetic-only"},'
                '"protocol_conformance":"CONFORMING",'
                '"runner":"fixture-runner",'
                '"runner_version":"1.0.0",'
                '"schema_version":"runner.artifact.v1",'
                '"status":"COMPLETED","taint_refs":[]}'
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


class _ObservedCostEvidenceArtifactBoundFakeProvider(
    _EvidenceArtifactBoundFakeProvider
):
    def __init__(self, *, reported_cost: str, currency: str | None) -> None:
        super().__init__()
        self._reported_cost = reported_cost
        self._currency = currency

    def invoke(self, request: object) -> ProviderResponse:
        response = super().invoke(request)
        raw_usage = {
            **response.raw_usage,
            "reported_cost": self._reported_cost,
        }
        if self._currency is None:
            raw_usage.pop("currency", None)
        else:
            raw_usage["currency"] = self._currency
        return replace(response, raw_usage=raw_usage)


class _MixedCurrencyRetryEvidenceArtifactBoundFakeProvider(
    _EvidenceArtifactBoundFakeProvider
):
    def __init__(self, *, success_reported_cost: str = "0.02") -> None:
        super().__init__()
        self._success_reported_cost = success_reported_cost

    def invoke(self, request: object) -> ProviderResponse:
        response = super().invoke(request)
        if self._last_call_count == 1:
            return replace(
                response,
                output_text="{",
                raw_usage={
                    "input_tokens": 7,
                    "output_tokens": 3,
                    "total_tokens": 10,
                    "reported_cost": "100",
                    "currency": "JPY",
                },
            )
        return replace(
            response,
            raw_usage={
                **response.raw_usage,
                "reported_cost": self._success_reported_cost,
                "currency": "USD",
            },
        )


class _CanonicalCurrencyRetryEvidenceArtifactBoundFakeProvider(
    _EvidenceArtifactBoundFakeProvider
):
    def __init__(
        self,
        *,
        first_reported_cost: str,
        success_reported_cost: str,
    ) -> None:
        super().__init__()
        self._first_reported_cost = first_reported_cost
        self._success_reported_cost = success_reported_cost

    def invoke(self, request: object) -> ProviderResponse:
        response = super().invoke(request)
        return replace(
            response,
            output_text=(
                "{" if self._last_call_count == 1 else response.output_text
            ),
            raw_usage={
                **response.raw_usage,
                "reported_cost": (
                    self._first_reported_cost
                    if self._last_call_count == 1
                    else self._success_reported_cost
                ),
                "currency": "USD",
            },
        )


class _InvalidEvidenceArtifactBoundFakeProvider(
    _EvidenceArtifactBoundFakeProvider
):
    def invoke(self, request: object) -> ProviderResponse:
        return replace(
            super().invoke(request),
            output_text=(
                '{"access_event_ids":[],"artifact_refs":[],'
                '"claim":null,"protocol_conformance":"CONFORMING",'
                '"runner":"fixture-runner",'
                '"schema_version":"runner.artifact.unknown",'
                '"status":"COMPLETED","taint_refs":[]}'
            ),
        )


class _TaintedEvidenceArtifactBoundFakeProvider(
    _EvidenceArtifactBoundFakeProvider
):
    def invoke(self, request: object) -> ProviderResponse:
        return replace(
            super().invoke(request),
            output_text=(
                '{"access_event_ids":["event:synthetic-tainted"],'
                '"artifact_refs":[],"claim":null,'
                '"executed_protocol":{"label":"synthetic-only"},'
                '"protocol_conformance":"CONFORMING",'
                '"runner":"fixture-runner",'
                '"runner_version":"1.0.0",'
                '"schema_version":"runner.artifact.v1",'
                '"status":"COMPLETED",'
                '"taint_refs":["taint:synthetic-test"]}'
            ),
        )


class _NonObjectEvidenceArtifactBoundFakeProvider(
    _EvidenceArtifactBoundFakeProvider
):
    def invoke(self, request: object) -> ProviderResponse:
        return replace(super().invoke(request), output_text="[]")


class _EligibleEvidenceArtifactBoundFakeProvider(_BoundFakeProvider):
    def __init__(self, *, executed_protocol=None) -> None:
        super().__init__()
        self._executed_protocol = (
            _synthetic_protocol()
            if executed_protocol is None
            else executed_protocol
        )

    def invoke(self, request: object) -> ProviderResponse:
        self._increment_call_count()
        self.last_request = request
        return ProviderResponse(
            output_text=(
                '{"access_event_ids":["event:synthetic-eligible"],'
                '"artifact_refs":[{"ref":"fixtures/result.json",'
                '"sha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}],'
                '"claim":{"kind":"NEGATIVE",'
                '"summary":"Synthetic eligible finding.",'
                '"scope":'
                + json.dumps(_synthetic_claim_scope_text())
                + '},'
                '"executed_protocol":'
                + json.dumps(
                    self._executed_protocol,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + ','
                '"protocol_conformance":"CONFORMING",'
                '"runner":"fixture-runner",'
                '"runner_version":"1.0.0",'
                '"schema_version":"runner.artifact.v1",'
                '"status":"COMPLETED","taint_refs":[]}'
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


class _MissingExecutedProtocolEligibleFakeProvider(
    _EligibleEvidenceArtifactBoundFakeProvider
):
    def invoke(self, request: object) -> ProviderResponse:
        response = super().invoke(request)
        artifact = json.loads(response.output_text)
        artifact.pop("executed_protocol")
        return replace(
            response,
            output_text=json.dumps(
                artifact,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )


class _AuthorityEvidenceArtifactBoundFakeProvider(_BoundFakeProvider):
    def __init__(self, artifact: object) -> None:
        super().__init__()
        self._artifact = artifact

    def invoke(self, request: object) -> ProviderResponse:
        self._increment_call_count()
        self.last_request = request
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


class _UnknownUsageAuthorityEvidenceArtifactBoundFakeProvider(
    _AuthorityEvidenceArtifactBoundFakeProvider
):
    def invoke(self, request: object) -> ProviderResponse:
        return replace(super().invoke(request), raw_usage={})


class _UnknownUsageEvidenceArtifactBoundFakeProvider(
    _EvidenceArtifactBoundFakeProvider
):
    def invoke(self, request: object) -> ProviderResponse:
        return replace(super().invoke(request), raw_usage={})


class _UnboundEvidenceAdapter(EvidenceAdapter):
    pass


class _UnboundOperationalEvidenceReceipt(OperationalEvidenceReceipt):
    pass


class _UnboundCampaignLearningCommitSink(CampaignLearningCommitSink):
    def commit(self, *args, **kwargs) -> str:
        return "f" * 64


class _UnboundLearningCommitService(LearningCommitService):
    def commit(self, *args, **kwargs) -> str:
        return "f" * 64


class _EstimatedUsageBoundFakeProvider(_BoundFakeProvider):
    def invoke(self, request: object) -> ProviderResponse:
        return replace(
            super().invoke(request),
            usage_status=UsageStatus.ESTIMATED,
        )


class _OversizedOutputBoundFakeProvider(_BoundFakeProvider):
    def invoke(self, request: object) -> ProviderResponse:
        return replace(
            super().invoke(request),
            output_text=json.dumps({"payload": "x" * (48 * 1024)}),
        )


class _LargeIntegerOutputBoundFakeProvider(_BoundFakeProvider):
    def invoke(self, request: object) -> ProviderResponse:
        return replace(super().invoke(request), output_text="9" * 5_000)


class _FakeMonotonicClock:
    def __init__(self, *values: int) -> None:
        self._values = iter(values)

    def __call__(self) -> int:
        return next(self._values)


class _LeaseSwapMonotonicClock:
    def __init__(self, barrier: Barrier) -> None:
        self._barrier = barrier
        self._values = iter((100, 1_000_000, 2_000_000))
        self._calls = 0

    def __call__(self) -> int:
        self._calls += 1
        value = next(self._values)
        if self._calls == 3:
            self._barrier.wait(timeout=5)
            self._barrier.wait(timeout=5)
        return value


class _FlakyWriteUnitOfWork:
    """Delegates the real SQLite unit of work but deterministically raises
    SqliteStoreBusyError for a configured number of _write calls (every call
    when unlimited), tracking invocation and failure counts so tests can
    prove the bounded lock-wait retry behavior without any timing."""

    _skip_writes = 0
    _failures_remaining = 0
    _failures_unlimited = False
    _write_calls = 0
    _failures_raised = 0
    _failure_call_numbers: list[int] = []

    @classmethod
    def reset(
        cls,
        *,
        skip_writes: int = 0,
        failures_remaining: int = 0,
        failures_unlimited: bool = False,
    ) -> None:
        cls._skip_writes = skip_writes
        cls._failures_remaining = failures_remaining
        cls._failures_unlimited = failures_unlimited
        cls._write_calls = 0
        cls._failures_raised = 0
        cls._failure_call_numbers = []

    def __init__(self, spec, **kwargs) -> None:
        self._delegate = _SqliteUnitOfWork(spec, **kwargs)

    def _read(self, operation):
        return self._delegate._read(operation)

    def _write(self, operation):
        type(self)._write_calls += 1
        if type(self)._skip_writes > 0:
            type(self)._skip_writes -= 1
            return self._delegate._write(operation)
        if type(self)._failures_unlimited or type(self)._failures_remaining > 0:
            if not type(self)._failures_unlimited:
                type(self)._failures_remaining -= 1
            type(self)._failures_raised += 1
            type(self)._failure_call_numbers.append(type(self)._write_calls)
            raise SqliteStoreBusyError("control-plane store is busy")
        return self._delegate._write(operation)


_SPAWN_CALL_WALL_TIME_MS = 5_000
_SPAWN_DOUBLE_CALL_WALL_TIME_MS = 10_000
_SPAWN_CAMPAIGN_WALL_TIME_MS = 50_000


_FAKE_CAMPAIGN_LIMITS = CampaignBudgetLimits(
    currency="USD",
    max_cycles=1,
    max_input_tokens=100,
    max_output_tokens=50,
    max_cost="1",
    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
    max_tool_attempts=2,
)


_FAKE_CALL_LIMITS = OperationalModelCallLimits(
    currency="USD",
    max_input_tokens=20,
    max_output_tokens=10,
    max_cost="0.1",
    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
    max_attempts=2,
)


def _completed_evidence_model_call(
    root,
    journal,
    *,
    campaign_id: str,
    provider=None,
    max_cycles: int = 1,
    campaign_max_cost: str = "1",
    reservation_max_cost: str = "0.1",
    call_max_cost: str = "0.1",
    resource_observation: ResourceObservation | None = None,
    campaign_max_data_exposures: int = 0,
    campaign_max_disk_growth_bytes: int = 0,
    reservation_max_data_exposures: int = 0,
    reservation_max_disk_growth_bytes: int = 0,
):
    protocol = _protocol()
    execution_spec = compile_execution_spec(
        protocol,
        approved_protocol=protocol,
        approval=_approval(protocol),
        amendment=None,
    )
    prompt = {"instruction": "Return one synthetic runner artifact"}
    member = replace(
        _protocol_member(),
        prompt_sha256=operational_prompt_sha256(prompt),
    )
    task = ExperimentTask(
        task_id="cycle-001",
        strategy="b1",
        proposal={
            "hypothesis": "Synthetic output becomes bounded evidence",
            "scope": _scope(generation="generation-1"),
        },
        source="synthetic-test",
    )
    owner = ProcessIdentity("host-controller", 144, 44_000)
    controller = OperationalCampaignController(
        journal=journal,
        repository_root=root,
        budget_limits=CampaignBudgetLimits(
            currency="USD",
            max_cycles=max_cycles,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost=campaign_max_cost,
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
            max_data_exposures=campaign_max_data_exposures,
            max_disk_growth_bytes=campaign_max_disk_growth_bytes,
        ),
        identity_provider=_FakeProcessIdentityProvider(owner),
        monotonic_ns=_FakeMonotonicClock(
            100,
            1_000_000,
            2_000_000,
        ),
    )
    controller.prepare_cycle(
        task=task,
        cycle_number=1,
        execution_spec=execution_spec,
        roster_members=(member,),
        reservation_limits=CycleReservationLimits(
            currency="USD",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost=reservation_max_cost,
            max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
            max_tool_attempts=2,
            max_data_exposures=reservation_max_data_exposures,
            max_disk_growth_bytes=reservation_max_disk_growth_bytes,
        ),
    )
    execution = controller.start_execution(
        cycle_id=task.task_id,
        acquisition_id=f"execute-{campaign_id}",
    )
    controller.invoke_member_json(
        execution=execution,
        member_id=member.member_id,
        provider=provider or _EvidenceArtifactBoundFakeProvider(),
        prompt=prompt,
        limits=replace(_FAKE_CALL_LIMITS, max_cost=call_max_cost),
    )
    usage = controller.complete_model_execution(
        execution=execution,
        resource_observation=resource_observation,
    )
    return controller, execution, member, usage


def _settled_eligible_learning(
    root,
    journal,
    *,
    campaign_id: str,
    max_cycles: int = 2,
):
    claim = {
        "kind": "NEGATIVE",
        "summary": "Synthetic eligible finding.",
        "scope": _synthetic_claim_scope_text(),
    }
    packet_hash = "f" * 64
    controller, execution, member, usage = _completed_evidence_model_call(
        root,
        journal,
        campaign_id=campaign_id,
        provider=_EligibleEvidenceArtifactBoundFakeProvider(),
        max_cycles=max_cycles,
    )
    evidence = controller.record_model_evidence(
        execution=execution,
        member_id=member.member_id,
        evidence_adapter=EvidenceAdapter(
            known_runners={"fixture-runner": "1.0.0"},
            approved_protocol=_synthetic_protocol(),
            approved_claim=claim,
        ),
    )
    service = LearningCommitService(repository_root=root)
    with patch.object(
        LearningCommitService,
        "expected_packet_hash",
        return_value=packet_hash,
    ), patch.object(
        LearningCommitService,
        "commit",
        return_value=packet_hash,
    ):
        learning = controller.commit_learning(
            execution=execution,
            evidence_receipt=evidence,
            authority_task_report={"synthetic": "terminal-report"},
            learning_commit_sink=CampaignLearningCommitSink(
                journal=journal,
                service=service,
            ),
        )
    settlement = controller.settle_cycle(
        execution=execution,
        execution_usage=usage,
        learning_commit_receipt=learning,
    )
    return controller, execution, settlement, packet_hash


def _completed_eligible_information_gain(
    root,
    journal,
    *,
    test_case: unittest.TestCase,
    campaign_id: str,
    max_cycles: int = 2,
    foreign_packet_hash: str | None = None,
):
    controller, execution, settlement, packet_hash = (
        _settled_eligible_learning(
            root,
            journal,
            campaign_id=campaign_id,
            max_cycles=max_cycles,
        )
    )
    projection_history = [_projectable_learning_input()]
    if foreign_packet_hash is not None:
        projection_history.append(
            _projectable_learning_input(foreign_packet_hash)
        )
    projection_history.append(
        _projectable_learning_input(
            *(
                (packet_hash,)
                if foreign_packet_hash is None
                else (foreign_packet_hash, packet_hash)
            )
        )
    )
    checkpoint_patch = patch.object(
        CommittedLearningLedgerReader,
        "read_projection_checkpoints",
        return_value=(
            len(projection_history) - 1,
            projection_history[0],
            projection_history[-1],
        ),
    )
    checkpoint_patch.start()
    test_case.addCleanup(checkpoint_patch.stop)
    information_gain = controller.record_information_gain(
        execution=execution,
        settlement_receipt=settlement,
    )
    return controller, execution, information_gain


def _projectable_learning_input(
    *packet_hashes: str,
) -> dict[str, object]:
    return {
        "schema_version": "control_plane.committed_learning_input.v1",
        "claims": [
            {
                "claim_id": packet_hash,
                "kind": "NEGATIVE",
                "execution_identity": "synthetic-execution",
                "semantic_identity": "synthetic-semantic",
                "conclusion": "AVOID",
                "scope": _scope(generation="generation-1"),
                "audit_grade": "PASS",
                "evidence_grade": "UNSPECIFIED",
                "evidence_refs": [],
                "taint_refs": [],
                "invalidation_codes": [],
                "reopen_predicates": [],
                "parent_claim_ids": [],
                "directional_status": "avoid",
                "universal_factor_rejection": False,
            }
            for packet_hash in packet_hashes
        ],
        "excluded_claims": [],
    }


def _projectable_preflight_input(
    *claims: dict[str, object],
) -> dict[str, object]:
    guidance = {
        "NEGATIVE": ("AVOID", "avoid"),
        "PARTIAL": ("REGIME_CONDITIONAL", "regime_conditional"),
    }
    projected_claims = []
    for claim in claims:
        conclusion, directional_status = guidance[str(claim["kind"])]
        projected_claims.append(
            {
                **claim,
                "conclusion": conclusion,
                "evidence_refs": [],
                "reopen_predicates": [],
                "directional_status": directional_status,
            }
        )
    return {
        "schema_version": "control_plane.committed_learning_input.v1",
        "claims": projected_claims,
        "excluded_claims": [],
    }


def _excluded_learning_input(
    packet_hash: str,
    reason_code: str,
) -> dict[str, object]:
    return {
        "schema_version": "control_plane.committed_learning_input.v1",
        "claims": [],
        "excluded_claims": [
            {
                "claim_id": packet_hash,
                "reason_codes": [reason_code],
            }
        ],
    }


def _completed_no_material_information_gain(
    root,
    journal,
    *,
    campaign_id: str,
    max_cycles: int = 2,
):
    controller, execution, member, usage = _completed_evidence_model_call(
        root,
        journal,
        campaign_id=campaign_id,
        max_cycles=max_cycles,
    )
    evidence = controller.record_model_evidence(
        execution=execution,
        member_id=member.member_id,
        evidence_adapter=EvidenceAdapter(
            known_runners={"fixture-runner": "1.0.0"},
            approved_protocol={"label": "synthetic-only"},
        ),
    )
    settlement = controller.settle_cycle_without_learning(
        execution=execution,
        execution_usage=usage,
        evidence_receipt=evidence,
    )
    information_gain = controller.record_information_gain(
        execution=execution,
        settlement_receipt=settlement,
    )
    return controller, execution, information_gain


def _prepare_synthetic_cycle(
    controller: OperationalCampaignController,
    *,
    cycle_id: str,
    cycle_number: int,
    reservation_limits: CycleReservationLimits | None = None,
):
    protocol = _protocol()
    execution_spec = compile_execution_spec(
        protocol,
        approved_protocol=protocol,
        approval=_approval(protocol),
        amendment=None,
    )
    prompt = {"instruction": f"Return synthetic artifact {cycle_number}"}
    member = replace(
        _protocol_member(),
        prompt_sha256=operational_prompt_sha256(prompt),
    )
    return controller.prepare_cycle(
        task=ExperimentTask(
            task_id=cycle_id,
            strategy="b1",
            proposal={
                "hypothesis": f"Synthetic cycle {cycle_number}",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        ),
        cycle_number=cycle_number,
        execution_spec=execution_spec,
        roster_members=(member,),
        reservation_limits=(
            reservation_limits
            or CycleReservationLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                max_tool_attempts=1,
            )
        ),
    )


def _prepare_model_call_execution(
    root,
    journal,
    *,
    campaign_id: str,
    max_wall_time_ms: int = 5_000,
    max_attempts: int = 2,
):
    protocol = _protocol()
    execution_spec = compile_execution_spec(
        protocol,
        approved_protocol=protocol,
        approval=_approval(protocol),
        amendment=None,
    )
    prompt = {"instruction": "Return one bounded synthetic result"}
    member = replace(
        _protocol_member(),
        prompt_sha256=operational_prompt_sha256(prompt),
    )
    controller = OperationalCampaignController(
        journal=journal,
        repository_root=root,
        budget_limits=CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=max_wall_time_ms,
            max_tool_attempts=max_attempts,
        ),
        identity_provider=_FakeProcessIdentityProvider(
            ProcessIdentity("host-controller", 211, 61_000)
        ),
        monotonic_ns=lambda: 1_000_000,
    )
    controller.prepare_cycle(
        task=ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "A fake provider executes in a bounded worker",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        ),
        cycle_number=1,
        execution_spec=execution_spec,
        roster_members=(member,),
        reservation_limits=CycleReservationLimits(
            currency="USD",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=max_wall_time_ms,
            max_tool_attempts=max_attempts,
        ),
    )
    execution = controller.start_execution(
        cycle_id="cycle-001",
        acquisition_id=f"execute-{campaign_id}",
    )
    return controller, execution, member, prompt


def _controller_event_id(domain: bytes, *parts: str) -> str:
    return hashlib.sha256(
        domain + b"\0" + "\0".join(parts).encode("ascii")
    ).hexdigest()


def _campaign_event_rows(root, campaign_id: str) -> tuple[tuple[object, ...], ...]:
    connection = sqlite3.connect(root / "operational.sqlite3")
    try:
        return tuple(
            connection.execute(
                "SELECT event_id, namespace, campaign_id, cycle_id, "
                "aggregate_type, aggregate_id, event_type, payload_json, "
                "payload_sha256, occurred_at, sequence "
                "FROM campaign_events WHERE namespace = ? AND campaign_id = ? "
                "ORDER BY sequence",
                ("formal", campaign_id),
            ).fetchall()
        )
    finally:
        connection.close()


def _learning_preflight_prepare_inputs(root, journal, *, hypothesis: str):
    protocol = _protocol()
    execution_spec = compile_execution_spec(
        protocol,
        approved_protocol=protocol,
        approval=_approval(protocol),
        amendment=None,
    )
    task = ExperimentTask(
        task_id="cycle-001",
        strategy="b1",
        proposal={
            "hypothesis": hypothesis,
            "scope": _scope(generation="generation-1"),
        },
        source="synthetic-test",
    )
    controller = OperationalCampaignController(
        journal=journal,
        repository_root=root,
        budget_limits=CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=5_000,
            max_tool_attempts=4,
            max_data_exposures=1,
            max_disk_growth_bytes=10_000,
        ),
        identity_provider=_FakeProcessIdentityProvider(
            ProcessIdentity("host-learning-preflight", 141, 41_000)
        ),
        monotonic_ns=lambda: 100,
    )
    return (
        controller,
        task,
        execution_spec,
        (_protocol_member(),),
        CycleReservationLimits(
            currency="USD",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=1_000,
            max_tool_attempts=1,
            max_data_exposures=1,
            max_disk_growth_bytes=1_000,
        ),
    )


def _prepare_cycle_durable_snapshot(
    root,
    campaign_id: str,
    journal,
    controller: OperationalCampaignController,
    *,
    cycle_id: str,
) -> dict[str, object]:
    try:
        cycle = controller.cycle_snapshot(cycle_id)
    except CampaignLifecycleError:
        cycle = None
    return {
        "all_events": _campaign_event_rows(root, campaign_id),
        "budget": controller.budget_snapshot(),
        "cycle_budget": controller.cycle_budget_snapshot(),
        "cycle": cycle,
        "work_item": journal.list_events(
            cycle_id=cycle_id,
            aggregate_type="CAMPAIGN_WORK_ITEM",
            aggregate_id=cycle_id,
        ),
        "context": journal.list_events(
            cycle_id=cycle_id,
            aggregate_type="CYCLE_SAFE_CONTEXT",
            aggregate_id=cycle_id,
        ),
        "roster": journal.list_events(
            cycle_id=cycle_id,
            aggregate_type="CYCLE_ROSTER",
            aggregate_id=cycle_id,
        ),
        "freeze": journal.list_events(
            cycle_id=cycle_id,
            aggregate_type="CYCLE_INPUT_FREEZE",
            aggregate_id=cycle_id,
        ),
        "preparation": journal.list_events(
            cycle_id=cycle_id,
            aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
            aggregate_id=cycle_id,
        ),
        "usage": OperationalUsageJournal(
            journal=journal,
            cycle_id=cycle_id,
        ).list_attempts(),
    }


def _operational_table_bytes(root) -> tuple[bytes, ...]:
    connection = sqlite3.connect(root / "operational.sqlite3")
    try:
        return tuple(line.encode("utf-8") for line in connection.iterdump())
    finally:
        connection.close()


def _rewrite_campaign_event_payload(root, event, payload) -> None:
    payload_json = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    integrity = _event_integrity_sha256(
        event_id=event.event_id,
        namespace=event.namespace,
        campaign_id=event.campaign_id,
        cycle_id=event.cycle_id,
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        event_type=event.event_type,
        payload_json=payload_json,
        occurred_at=event.occurred_at.isoformat(),
        sequence=event.sequence,
    )
    connection = sqlite3.connect(root / "operational.sqlite3")
    try:
        connection.execute(
            "UPDATE campaign_events SET payload_json = ?, "
            "payload_sha256 = ? WHERE event_id = ?",
            (payload_json, integrity, event.event_id),
        )
        connection.commit()
    finally:
        connection.close()


def _graft_and_resign_preparation_context(
    root,
    journal,
    *,
    cycle_id: str,
    context_proposal: dict[str, object],
    freeze_proposal: dict[str, object],
    preparation_work_item: dict[str, object] | None = None,
) -> None:
    context_event = journal.list_events(
        cycle_id=cycle_id,
        aggregate_type="CYCLE_SAFE_CONTEXT",
        aggregate_id=cycle_id,
    )[0]
    context_payload = context_event.payload()
    context_payload["proposal"] = context_proposal
    proposal_text, _ = campaign_context_module._canonical_snapshot(
        context_payload["proposal"],
        "grafted context proposal",
        maximum_bytes=16 * 1024 * 1024,
    )
    context_payload["proposal_sha256"] = (
        campaign_context_module._content_sha256(
            b"control_plane.cycle_context_proposal.v2",
            proposal_text,
        )
    )
    context_identity = {
        key: value
        for key, value in context_payload.items()
        if key
        not in {
            "_authority_grant_id",
            "manifest_sha256",
            "safe_context",
            "projection_input",
            "proposal",
            "untrusted_sources",
        }
    }
    context_payload["manifest_sha256"] = (
        campaign_context_module._content_sha256(
            b"control_plane.cycle_context_receipt.v2",
            campaign_context_module._canonical_snapshot(
                context_identity,
                "grafted context identity",
                maximum_bytes=48 * 1024,
            )[0],
        )
    )
    _rewrite_campaign_event_payload(
        root,
        context_event,
        context_payload,
    )

    freeze_event = journal.list_events(
        cycle_id=cycle_id,
        aggregate_type="CYCLE_INPUT_FREEZE",
        aggregate_id=cycle_id,
    )[0]
    freeze_payload = freeze_event.payload()
    freeze_payload["proposal_sha256"] = (
        campaign_freeze_module._content_sha256(
            b"control_plane.campaign_proposal.v1",
            freeze_proposal,
            "grafted frozen proposal",
        )
    )
    freeze_payload["context_manifest_sha256"] = context_payload[
        "manifest_sha256"
    ]
    freeze_identity = {
        key: value
        for key, value in freeze_payload.items()
        if key not in {"_authority_grant_id", "manifest_sha256"}
    }
    freeze_payload["manifest_sha256"] = (
        campaign_freeze_module._content_sha256(
            campaign_freeze_module._CYCLE_FREEZE_MANIFEST_DOMAIN,
            freeze_identity,
            "grafted Cycle freeze identity",
        )
    )
    _rewrite_campaign_event_payload(
        root,
        freeze_event,
        freeze_payload,
    )

    preparation_event = journal.list_events(
        cycle_id=cycle_id,
        aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
        aggregate_id=cycle_id,
    )[0]
    preparation_payload = preparation_event.payload()
    if preparation_work_item is not None:
        preparation_payload["work_item_sha256"] = _controller_sha256(
            b"control_plane.controller_work_item_payload.v1",
            {
                key: value
                for key, value in preparation_work_item.items()
                if key != "_authority_grant_id"
            },
            "grafted stored Campaign work item",
        )
    preparation_payload["context_manifest_sha256"] = context_payload[
        "manifest_sha256"
    ]
    preparation_payload["freeze_manifest_sha256"] = freeze_payload[
        "manifest_sha256"
    ]
    preparation_identity = {
        key: value
        for key, value in preparation_payload.items()
        if key not in {"_authority_grant_id", "manifest_sha256"}
    }
    preparation_payload["manifest_sha256"] = _controller_sha256(
        b"control_plane.campaign_cycle_preparation.v2",
        preparation_identity,
        "grafted Cycle preparation identity",
    )
    _rewrite_campaign_event_payload(
        root,
        preparation_event,
        preparation_payload,
    )


class OperationalCampaignControllerTests(unittest.TestCase):
    def test_bound_fake_provider_counter_is_atomic_under_concurrency(
        self,
    ) -> None:
        provider = _BoundFakeProvider()
        with ThreadPoolExecutor(max_workers=16) as pool:
            observed = tuple(
                pool.map(
                    lambda _index: provider._increment_call_count(),
                    range(64),
                )
            )

        self.assertEqual(sorted(observed), list(range(1, 65)))
        self.assertEqual(provider.call_count, 64)

    def test_bound_fake_provider_counter_is_atomic_across_spawn_processes(
        self,
    ) -> None:
        provider = _BoundFakeProvider()
        with ProcessPoolExecutor(
            max_workers=8,
            mp_context=multiprocessing.get_context("spawn"),
        ) as pool:
            observed = tuple(
                pool.map(
                    _increment_bound_fake_provider_counter,
                    [provider] * 32,
                )
            )

        self.assertEqual(sorted(observed), list(range(1, 33)))
        self.assertEqual(provider.call_count, 32)

    def test_model_call_limits_require_positive_wall_time(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "max_wall_time_ms must be a positive integer",
        ):
            OperationalModelCallLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=0,
                max_attempts=2,
            )

    def test_model_call_limits_reject_wall_time_that_overflows_deadline(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "max_wall_time_ms exceeds the finite deadline range",
        ):
            OperationalModelCallLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=10**400,
                max_attempts=2,
            )

    def test_controller_executes_member_in_spawned_worker(self) -> None:
        campaign_id = "campaign-controller-spawn-boundary"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, prompt = _prepare_model_call_execution(
                root,
                journal,
                campaign_id=campaign_id,
            )
            marker_path = root / "provider-worker.pid"

            executed = controller.invoke_member_json(
                execution=execution,
                member_id=member.member_id,
                provider=_ProcessMarkerBoundFakeProvider(str(marker_path)),
                prompt=prompt,
                limits=OperationalModelCallLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=5_000,
                    max_attempts=1,
                ),
            )

            self.assertEqual(
                executed.output,
                {"source": "synthetic", "status": "ok"},
            )
            self.assertNotEqual(
                int(marker_path.read_text(encoding="ascii")),
                os.getpid(),
            )
            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            ).list_attempts()
            self.assertEqual(len(attempts), 1)
            self.assertEqual(
                attempts[0].final_outcome,
                InvocationOutcome.SUCCESS,
            )

    def test_unpicklable_provider_fails_before_model_call_start(self) -> None:
        campaign_id = "campaign-controller-spawn-preflight-failure"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, prompt = _prepare_model_call_execution(
                root,
                journal,
                campaign_id=campaign_id,
            )
            provider = _UnpicklableBoundFakeProvider()
            limits = OperationalModelCallLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=5_000,
                max_attempts=1,
            )
            call_id = controller._member_call_id("cycle-001", member.member_id)

            with self.assertRaisesRegex(ValueError, "not spawn-picklable"):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=limits,
                )

            self.assertEqual(provider.reduce_calls, 1)
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_MODEL_CALL",
                    aggregate_id=call_id,
                ),
                (),
            )
            self.assertEqual(
                OperationalUsageJournal(
                    journal=journal,
                    cycle_id="cycle-001",
                ).list_attempts(),
                (),
            )
            self.assertEqual(
                controller.campaign_snapshot().status,
                CampaignStatus.ACTIVE,
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )

    def test_provider_serialization_does_not_hold_the_operational_writer_lock(
        self,
    ) -> None:
        campaign_id = "campaign-controller-spawn-preflight-writer-lock"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, prompt = _prepare_model_call_execution(
                root,
                journal,
                campaign_id=campaign_id,
            )
            limits = OperationalModelCallLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=5_000,
                max_attempts=1,
            )
            serialization_started = Event()
            release_serialization = Event()
            provider = _BlockingSerializationBoundFakeProvider(
                serialization_started,
                release_serialization,
            )
            lease_journal = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-controller", 211, 61_000)
                ),
                monotonic_ns=lambda: 2_000_000,
            )

            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    controller.invoke_member_json,
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=limits,
                )
                try:
                    self.assertTrue(serialization_started.wait(timeout=2))
                    lease_journal.heartbeat(
                        lease=execution.lease,
                        heartbeat_id="heartbeat-during-provider-preflight",
                    )
                finally:
                    release_serialization.set()

                completed = future.result(timeout=5)

            self.assertEqual(provider.reduce_calls, 1)
            self.assertEqual(
                completed.output,
                {"source": "synthetic", "status": "ok"},
            )
            self.assertEqual(
                tuple(
                    event.event_type
                    for event in journal.list_events(
                        cycle_id="cycle-001",
                        aggregate_type="OPERATIONAL_MODEL_CALL",
                        aggregate_id=controller._member_call_id(
                            "cycle-001",
                            member.member_id,
                        ),
                    )
                ),
                (
                    "OPERATIONAL_MODEL_CALL_STARTED",
                    "OPERATIONAL_MODEL_CALL_COMPLETED",
                ),
            )

    def test_completed_concurrent_call_wins_over_stale_serialization_failure(
        self,
    ) -> None:
        campaign_id = "campaign-controller-spawn-preflight-race"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, prompt = _prepare_model_call_execution(
                root,
                journal,
                campaign_id=campaign_id,
            )
            limits = OperationalModelCallLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=5_000,
                max_attempts=1,
            )
            serialization_started = Event()
            release_serialization = Event()
            stale_provider = _BlockingFailingSerializationBoundFakeProvider(
                serialization_started,
                release_serialization,
            )

            with ThreadPoolExecutor(max_workers=1) as pool:
                stale_future = pool.submit(
                    controller.invoke_member_json,
                    execution=execution,
                    member_id=member.member_id,
                    provider=stale_provider,
                    prompt=prompt,
                    limits=limits,
                )
                try:
                    self.assertTrue(serialization_started.wait(timeout=2))
                    completed = controller.invoke_member_json(
                        execution=execution,
                        member_id=member.member_id,
                        provider=_BoundFakeProvider(),
                        prompt=prompt,
                        limits=limits,
                    )
                finally:
                    release_serialization.set()

                replayed = stale_future.result(timeout=5)

            self.assertEqual(stale_provider.reduce_calls, 1)
            self.assertEqual(replayed, completed)
            self.assertEqual(
                tuple(
                    event.event_type
                    for event in journal.list_events(
                        cycle_id="cycle-001",
                        aggregate_type="OPERATIONAL_MODEL_CALL",
                        aggregate_id=controller._member_call_id(
                            "cycle-001",
                            member.member_id,
                        ),
                    )
                ),
                (
                    "OPERATIONAL_MODEL_CALL_STARTED",
                    "OPERATIONAL_MODEL_CALL_COMPLETED",
                ),
            )

    def test_stale_false_probe_retries_preflight_once_before_start(self) -> None:
        campaign_id = "campaign-controller-spawn-preflight-stale-probe"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, prompt = _prepare_model_call_execution(
                root,
                journal,
                campaign_id=campaign_id,
            )
            limits = OperationalModelCallLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=5_000,
                max_attempts=1,
            )
            serialization_started = Event()
            release_serialization = Event()
            release_serialization.set()
            provider = _BlockingSerializationBoundFakeProvider(
                serialization_started,
                release_serialization,
            )
            original_probe = getattr(
                OperationalCampaignController,
                "_model_call_provider_preflight_required_in_transaction",
            )
            probe_calls = 0

            def stale_once(controller_self, connection, **kwargs):
                nonlocal probe_calls
                probe_calls += 1
                if controller_self is controller and probe_calls == 1:
                    return False
                return original_probe(controller_self, connection, **kwargs)

            with patch.object(
                OperationalCampaignController,
                "_model_call_provider_preflight_required_in_transaction",
                new=stale_once,
            ):
                completed = controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=limits,
                )

            self.assertEqual(probe_calls, 2)
            self.assertTrue(serialization_started.is_set())
            self.assertEqual(provider.reduce_calls, 1)
            self.assertEqual(
                completed.output,
                {"source": "synthetic", "status": "ok"},
            )
            self.assertEqual(
                tuple(
                    event.event_type
                    for event in journal.list_events(
                        cycle_id="cycle-001",
                        aggregate_type="OPERATIONAL_MODEL_CALL",
                        aggregate_id=controller._member_call_id(
                            "cycle-001",
                            member.member_id,
                        ),
                    )
                ),
                (
                    "OPERATIONAL_MODEL_CALL_STARTED",
                    "OPERATIONAL_MODEL_CALL_COMPLETED",
                ),
            )

    def test_completed_model_call_replays_before_provider_serialization(self) -> None:
        campaign_id = "campaign-controller-spawn-completed-replay"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, prompt = _prepare_model_call_execution(
                root,
                journal,
                campaign_id=campaign_id,
            )
            limits = OperationalModelCallLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=5_000,
                max_attempts=1,
            )
            marker_path = root / "completed-provider.pid"
            completed = controller.invoke_member_json(
                execution=execution,
                member_id=member.member_id,
                provider=_ProcessMarkerBoundFakeProvider(str(marker_path)),
                prompt=prompt,
                limits=limits,
            )
            replay_provider = _UnpicklableBoundFakeProvider()

            replayed = controller.invoke_member_json(
                execution=execution,
                member_id=member.member_id,
                provider=replay_provider,
                prompt=prompt,
                limits=limits,
            )

            self.assertEqual(replayed, completed)
            self.assertEqual(replay_provider.reduce_calls, 0)

    def test_in_doubt_model_call_blocks_before_provider_serialization(self) -> None:
        campaign_id = "campaign-controller-spawn-in-doubt-replay"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, prompt = _prepare_model_call_execution(
                root,
                journal,
                campaign_id=campaign_id,
            )
            limits = OperationalModelCallLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=5_000,
                max_attempts=1,
            )
            with patch(
                "research_automation.control_plane.campaign_controller."
                "RetryingModelInvocation.invoke_json_with_receipt",
                side_effect=RuntimeError("synthetic mid-call crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "mid-call crash"):
                    controller.invoke_member_json(
                        execution=execution,
                        member_id=member.member_id,
                        provider=_BoundFakeProvider(),
                        prompt=prompt,
                        limits=limits,
                    )
            replay_provider = _UnpicklableBoundFakeProvider()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "incomplete and in doubt",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=replay_provider,
                    prompt=prompt,
                    limits=limits,
                )

            self.assertEqual(replay_provider.reduce_calls, 0)
            call_events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=controller._member_call_id(
                    "cycle-001",
                    member.member_id,
                ),
            )
            self.assertEqual(
                tuple(event.event_type for event in call_events),
                ("OPERATIONAL_MODEL_CALL_STARTED",),
            )
            self.assertEqual(
                controller.campaign_snapshot().status,
                CampaignStatus.BLOCKED,
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )

    def test_hanging_member_times_out_blocks_and_replays_without_provider(self) -> None:
        campaign_id = "campaign-controller-spawn-timeout"
        max_wall_time_ms = 2_000
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, prompt = _prepare_model_call_execution(
                root,
                journal,
                campaign_id=campaign_id,
                max_wall_time_ms=max_wall_time_ms,
                max_attempts=1,
            )
            limits = OperationalModelCallLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=max_wall_time_ms,
                max_attempts=1,
            )
            marker_path = root / "hanging-provider.pid"

            with self.assertRaisesRegex(
                RosterDriftError,
                "REQUIRED_MEMBER_RESPONSE_INVALID",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=_HangingProcessMarkerBoundFakeProvider(
                        str(marker_path)
                    ),
                    prompt=prompt,
                    limits=limits,
                )

            worker_pid = int(marker_path.read_text(encoding="ascii"))
            self.assertNotEqual(worker_pid, os.getpid())
            self.assertNotIn(
                worker_pid,
                tuple(
                    child.pid
                    for child in multiprocessing.active_children()
                    if child.pid is not None
                ),
            )
            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            ).list_attempts()
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0].envelope.usage_status, UsageStatus.UNKNOWN)
            self.assertEqual(
                attempts[0].final_outcome,
                InvocationOutcome.TIMEOUT,
            )
            call_id = controller._member_call_id("cycle-001", member.member_id)
            call_events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=call_id,
            )
            self.assertEqual(
                tuple(event.event_type for event in call_events),
                ("OPERATIONAL_MODEL_CALL_STARTED",),
            )
            campaign = controller.campaign_snapshot()
            self.assertEqual(campaign.status, CampaignStatus.BLOCKED)
            self.assertEqual(
                campaign.block_reason_code,
                "REQUIRED_MEMBER_RESPONSE_INVALID",
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )
            replay_provider = _UnpicklableBoundFakeProvider()
            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=replay_provider,
                    prompt=prompt,
                    limits=limits,
                )
            self.assertEqual(replay_provider.reduce_calls, 0)

            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=max_wall_time_ms,
                    max_tool_attempts=1,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-controller", 211, 61_000)
                ),
                monotonic_ns=lambda: 1_000_000,
            )
            reopened_provider = _UnpicklableBoundFakeProvider()
            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                reopened.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=reopened_provider,
                    prompt=prompt,
                    limits=limits,
                )
            self.assertEqual(reopened_provider.reduce_calls, 0)
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_MODEL_CALL",
                    aggregate_id=call_id,
                ),
                call_events,
            )

    def test_pre_attempt_deadline_timeout_is_durable_and_blocks_replay(
        self,
    ) -> None:
        campaign_id = "campaign-controller-pre-attempt-timeout"
        max_wall_time_ms = 1
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, prompt = _prepare_model_call_execution(
                root,
                journal,
                campaign_id=campaign_id,
                max_wall_time_ms=max_wall_time_ms,
                max_attempts=1,
            )
            limits = OperationalModelCallLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=max_wall_time_ms,
                max_attempts=1,
            )
            provider = _BoundFakeProvider()
            monotonic_values = iter((100.0, 100.001))

            def monotonic() -> float:
                return next(monotonic_values, 100.001)

            with patch(
                "research_automation.control_plane.campaign.time.monotonic",
                side_effect=monotonic,
            ):
                with self.assertRaisesRegex(
                    RosterDriftError,
                    "REQUIRED_MEMBER_RESPONSE_INVALID",
                ):
                    controller.invoke_member_json(
                        execution=execution,
                        member_id=member.member_id,
                        provider=provider,
                        prompt=prompt,
                        limits=limits,
                    )

            self.assertEqual(provider.call_count, 0)
            call_id = controller._member_call_id("cycle-001", member.member_id)
            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            ).list_attempts(call_id=call_id)
            self.assertEqual(len(attempts), 1)
            self.assertEqual(
                attempts[0].envelope.attempt_id,
                f"{call_id}-attempt-001",
            )
            self.assertEqual(
                attempts[0].envelope.usage_status,
                UsageStatus.UNKNOWN,
            )
            self.assertEqual(
                attempts[0].final_outcome,
                InvocationOutcome.TIMEOUT,
            )
            call_events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=call_id,
            )
            self.assertEqual(
                tuple(event.event_type for event in call_events),
                ("OPERATIONAL_MODEL_CALL_STARTED",),
            )
            campaign = controller.campaign_snapshot()
            self.assertEqual(campaign.status, CampaignStatus.BLOCKED)
            self.assertEqual(
                campaign.block_reason_code,
                "REQUIRED_MEMBER_RESPONSE_INVALID",
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )
            events_before_replay = _campaign_event_rows(root, campaign_id)

            replay_provider = _UnpicklableBoundFakeProvider()
            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=replay_provider,
                    prompt=prompt,
                    limits=limits,
                )
            self.assertEqual(replay_provider.reduce_calls, 0)

            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=max_wall_time_ms,
                    max_tool_attempts=1,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-controller", 212, 62_000)
                ),
                monotonic_ns=lambda: 1_000_000,
            )
            reopened_provider = _UnpicklableBoundFakeProvider()
            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                reopened.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=reopened_provider,
                    prompt=prompt,
                    limits=limits,
                )
            self.assertEqual(reopened_provider.reduce_calls, 0)
            self.assertEqual(
                _campaign_event_rows(root, campaign_id),
                events_before_replay,
            )

    def test_usd_is_bound_through_the_operational_no_learning_path(self) -> None:
        campaign_id = "campaign-controller-currency-001"
        cycle_id = "cycle-currency-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one synthetic runner artifact"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        campaign_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        reservation_limits = CycleReservationLimits(
            currency="USD",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        call_limits = OperationalModelCallLimits(
            currency="USD",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
            max_attempts=2,
        )

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=campaign_limits,
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-currency", 101, 1_000)
                ),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            prepared = controller.prepare_cycle(
                task=ExperimentTask(
                    task_id=cycle_id,
                    strategy="b1",
                    proposal={
                        "hypothesis": "Currency remains auditable",
                        "scope": _scope(generation="generation-1"),
                    },
                    source="synthetic-test",
                ),
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=reservation_limits,
            )
            preparation_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                aggregate_id=cycle_id,
            )[0]
            preparation_payload = preparation_event.payload()
            preparation_payload.pop("_authority_grant_id")
            budget_events = journal.list_events(
                cycle_id=None,
                aggregate_type="CAMPAIGN_BUDGET",
                aggregate_id=controller._budget._budget_id,
            )
            reservation_event = next(
                event
                for event in budget_events
                if event.event_type == "BUDGET_RESERVED"
            )
            reservation_payload = reservation_event.payload()
            reservation_payload.pop("_authority_grant_id")

            self.assertEqual(campaign_limits.currency, "USD")
            self.assertEqual(reservation_limits.currency, "USD")
            self.assertEqual(call_limits.currency, "USD")
            self.assertEqual(prepared.reservation.currency, "USD")
            self.assertEqual(preparation_payload["currency"], "USD")
            self.assertEqual(
                preparation_payload["schema_version"],
                "control_plane.campaign_cycle_preparation.v2",
            )
            self.assertEqual(
                preparation_payload["reservation_sha256"],
                _controller_sha256(
                    b"control_plane.controller_cycle_reservation_bounds.v2",
                    reservation_payload,
                    "expected currency-bound Cycle reservation",
                ),
            )

            execution = controller.start_execution(
                cycle_id=cycle_id,
                acquisition_id="execute-currency-001",
            )
            provider = _EvidenceArtifactBoundFakeProvider()
            model_call = controller.invoke_member_json(
                execution=execution,
                member_id=member.member_id,
                provider=provider,
                prompt=prompt,
                limits=call_limits,
            )
            call_start = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=model_call.call_id,
            )[0]
            call_start_payload = call_start.payload()
            call_start_payload.pop("_authority_grant_id")
            usage = controller.complete_model_execution(execution=execution)
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settlement = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )

            self.assertEqual(provider.call_count, 1)
            self.assertEqual(
                call_start_payload["schema_version"],
                "control_plane.operational_model_call.v2",
            )
            self.assertEqual(call_start_payload["call_limits"], call_limits.to_payload())
            self.assertEqual(call_start_payload["call_limits"]["currency"], "USD")
            self.assertEqual(usage.currency, "USD")
            self.assertEqual(settlement.currency, "USD")
            settlement_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_CYCLE_SETTLEMENT",
                aggregate_id=cycle_id,
            )[0]
            settlement_payload = settlement_event.payload()
            settlement_payload.pop("_authority_grant_id")
            self.assertEqual(
                settlement_payload["schema_version"],
                "control_plane.operational_no_learning_settlement.v2",
            )
            self.assertEqual(settlement_payload["currency"], "USD")
            budget_settlement_event = next(
                event
                for event in journal.list_events(
                    cycle_id=None,
                    aggregate_type="CAMPAIGN_BUDGET",
                    aggregate_id=controller._budget._budget_id,
                )
                if event.event_type == "BUDGET_SETTLED"
            )
            budget_settlement_payload = budget_settlement_event.payload()
            budget_settlement_payload.pop("_authority_grant_id")
            self.assertEqual(budget_settlement_payload["currency"], "USD")
            self.assertEqual(controller.budget_snapshot().currency, "USD")

    def test_non_usd_observed_cost_keeps_full_usd_reservation(self) -> None:
        cases = (
            ("missing", None),
            ("foreign", "EUR"),
        )
        for label, observed_currency in cases:
            with self.subTest(observed_currency=observed_currency):
                campaign_id = f"campaign-controller-cost-{label}-001"
                provider = _ObservedCostEvidenceArtifactBoundFakeProvider(
                    reported_cost="100",
                    currency=observed_currency,
                )
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    controller, execution, member, usage = (
                        _completed_evidence_model_call(
                            root,
                            journal,
                            campaign_id=campaign_id,
                            provider=provider,
                        )
                    )
                    attempts = OperationalUsageJournal(
                        journal=journal,
                        cycle_id="cycle-001",
                    ).list_attempts()
                    evidence = controller.record_model_evidence(
                        execution=execution,
                        member_id=member.member_id,
                        evidence_adapter=EvidenceAdapter(
                            known_runners={"fixture-runner": "1.0.0"},
                            approved_protocol={"label": "synthetic-only"},
                        ),
                    )
                    settlement = controller.settle_cycle_without_learning(
                        execution=execution,
                        execution_usage=usage,
                        evidence_receipt=evidence,
                    )

                    self.assertEqual(provider.call_count, 1)
                    self.assertEqual(len(attempts), 1)
                    self.assertEqual(
                        attempts[0].envelope.usage_status,
                        UsageStatus.REPORTED,
                    )
                    self.assertEqual(attempts[0].envelope.reported_cost, "100")
                    self.assertEqual(
                        attempts[0].envelope.currency,
                        observed_currency,
                    )
                    self.assertEqual(
                        attempts[0].final_outcome,
                        InvocationOutcome.SUCCESS,
                    )
                    self.assertEqual(usage.usage_status, UsageStatus.UNKNOWN)
                    self.assertIsNone(usage.cost)
                    self.assertEqual(usage.currency, "USD")
                    self.assertEqual(
                        settlement.settlement_state,
                        "SETTLED_UNKNOWN",
                    )
                    snapshot = controller.budget_snapshot()
                    self.assertEqual(snapshot.currency, "USD")
                    self.assertEqual(snapshot.reserved_input_tokens, 20)
                    self.assertEqual(snapshot.reserved_output_tokens, 10)
                    self.assertEqual(snapshot.reserved_cost, "0.1")
                    self.assertEqual(snapshot.reserved_wall_time_ms, _SPAWN_CALL_WALL_TIME_MS)
                    self.assertEqual(snapshot.reserved_tool_attempts, 2)
                    self.assertEqual(snapshot.reserved_data_exposures, 0)
                    self.assertEqual(snapshot.reserved_disk_growth_bytes, 0)
                    self.assertEqual(snapshot.spent_input_tokens, 0)
                    self.assertEqual(snapshot.spent_output_tokens, 0)
                    self.assertEqual(snapshot.spent_cost, "0")
                    self.assertEqual(snapshot.spent_wall_time_ms, 0)
                    self.assertEqual(snapshot.spent_tool_attempts, 0)
                    self.assertEqual(snapshot.spent_data_exposures, 0)
                    self.assertEqual(snapshot.spent_disk_growth_bytes, 0)

    def test_mixed_currency_retry_keeps_raw_attempts_and_usd_reservation(
        self,
    ) -> None:
        campaign_id = "campaign-controller-cost-mixed-retry-001"
        provider = _MixedCurrencyRetryEvidenceArtifactBoundFakeProvider()
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=provider,
                )
            )
            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            ).list_attempts()
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settlement = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )

            self.assertEqual(provider.call_count, 2)
            self.assertEqual(len(attempts), 2)
            self.assertEqual(
                tuple(attempt.envelope.reported_cost for attempt in attempts),
                ("100", "0.02"),
            )
            self.assertEqual(
                tuple(attempt.envelope.currency for attempt in attempts),
                ("JPY", "USD"),
            )
            self.assertEqual(
                tuple(attempt.envelope.usage_status for attempt in attempts),
                (UsageStatus.REPORTED, UsageStatus.REPORTED),
            )
            self.assertEqual(
                tuple(attempt.final_outcome for attempt in attempts),
                (InvocationOutcome.INVALID_JSON, InvocationOutcome.SUCCESS),
            )
            self.assertEqual(usage.input_tokens, 14)
            self.assertEqual(usage.output_tokens, 6)
            self.assertEqual(usage.usage_status, UsageStatus.UNKNOWN)
            self.assertIsNone(usage.cost)
            self.assertEqual(usage.currency, "USD")
            self.assertEqual(settlement.settlement_state, "SETTLED_UNKNOWN")
            snapshot = controller.budget_snapshot()
            self.assertEqual(snapshot.currency, "USD")
            self.assertEqual(snapshot.reserved_input_tokens, 20)
            self.assertEqual(snapshot.reserved_output_tokens, 10)
            self.assertEqual(snapshot.reserved_cost, "0.1")
            self.assertEqual(snapshot.reserved_wall_time_ms, _SPAWN_CALL_WALL_TIME_MS)
            self.assertEqual(snapshot.reserved_tool_attempts, 2)
            self.assertEqual(snapshot.spent_input_tokens, 0)
            self.assertEqual(snapshot.spent_output_tokens, 0)
            self.assertEqual(snapshot.spent_cost, "0")
            self.assertEqual(snapshot.spent_wall_time_ms, 0)
            self.assertEqual(snapshot.spent_tool_attempts, 0)

    def test_foreign_retry_cannot_hide_later_usd_cost_overrun(self) -> None:
        campaign_id = "campaign-controller-cost-usd-overrun-001"
        provider = _MixedCurrencyRetryEvidenceArtifactBoundFakeProvider(
            success_reported_cost="0.2"
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            with self.assertRaisesRegex(
                BudgetExceededError,
                "known usage exceeds its call limits",
            ):
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=provider,
                )

            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            ).list_attempts()
            self.assertEqual(provider.call_count, 2)
            self.assertEqual(
                tuple(attempt.envelope.reported_cost for attempt in attempts),
                ("100", "0.2"),
            )
            self.assertEqual(
                tuple(attempt.envelope.currency for attempt in attempts),
                ("JPY", "USD"),
            )
            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=_FAKE_CAMPAIGN_LIMITS,
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-controller", 144, 44_000)
                ),
                monotonic_ns=lambda: 3_000_000,
            )
            self.assertEqual(
                reopened.campaign_snapshot().status,
                CampaignStatus.BLOCKED,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                    aggregate_id="cycle-001",
                ),
                (),
            )

    def test_same_currency_retry_exact_cost_overrun_blocks_atomically(
        self,
    ) -> None:
        campaign_id = "campaign-controller-cost-exact-overrun-001"
        reported_costs = (
            "0.99999999999999999999999999995",
            "0.00000000000000000000000000006",
        )
        provider = _CanonicalCurrencyRetryEvidenceArtifactBoundFakeProvider(
            first_reported_cost=reported_costs[0],
            success_reported_cost=reported_costs[1],
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            with self.assertRaisesRegex(
                BudgetExceededError,
                "known usage exceeds its call limits",
            ):
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=provider,
                    campaign_max_cost="1",
                    reservation_max_cost="1",
                    call_max_cost="1",
                )

            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            ).list_attempts()
            self.assertEqual(provider.call_count, 2)
            self.assertEqual(
                tuple(attempt.envelope.reported_cost for attempt in attempts),
                reported_costs,
            )
            self.assertEqual(
                tuple(attempt.envelope.currency for attempt in attempts),
                ("USD", "USD"),
            )
            self.assertEqual(
                tuple(attempt.final_outcome for attempt in attempts),
                (InvocationOutcome.INVALID_JSON, InvocationOutcome.SUCCESS),
            )
            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=replace(_FAKE_CAMPAIGN_LIMITS, max_cost="1"),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-controller", 144, 44_000)
                ),
                monotonic_ns=lambda: 3_000_000,
            )
            self.assertEqual(
                reopened.campaign_snapshot().status,
                CampaignStatus.BLOCKED,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                    aggregate_id="cycle-001",
                ),
                (),
            )

    def test_execution_usage_preserves_exact_same_currency_retry_cost(
        self,
    ) -> None:
        campaign_id = "campaign-controller-cost-exact-freeze-001"
        expected_cost = "1.00000000000000000000000000001"
        provider = _CanonicalCurrencyRetryEvidenceArtifactBoundFakeProvider(
            first_reported_cost="0.99999999999999999999999999995",
            success_reported_cost="0.00000000000000000000000000006",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, _, _, usage = _completed_evidence_model_call(
                root,
                journal,
                campaign_id=campaign_id,
                provider=provider,
                campaign_max_cost="2",
                reservation_max_cost="2",
                call_max_cost="2",
            )

            self.assertEqual(provider.call_count, 2)
            self.assertEqual(usage.usage_status, UsageStatus.REPORTED)
            self.assertEqual(usage.cost, expected_cost)
            self.assertEqual(usage.currency, "USD")
            usage_event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                aggregate_id="cycle-001",
            )[0]
            payload = usage_event.payload()
            payload.pop("_authority_grant_id")
            identity = {
                key: value
                for key, value in payload.items()
                if key != "manifest_sha256"
            }
            self.assertEqual(payload["cost"], expected_cost)
            self.assertEqual(
                payload["manifest_sha256"],
                _controller_sha256(
                    b"control_plane.operational_execution_usage.v2",
                    identity,
                    "expected exact operational execution usage",
                ),
            )
            self.assertEqual(
                usage.manifest_sha256,
                payload["manifest_sha256"],
            )

    def test_unrepresentable_exact_retry_cost_replays_unknown_and_settles(
        self,
    ) -> None:
        campaign_id = "campaign-controller-cost-unrepresentable-retry-001"
        cycle_id = "cycle-001"
        max_cost = "2e128"
        reported_costs = ("1e128", "1e-128")
        provider = _CanonicalCurrencyRetryEvidenceArtifactBoundFakeProvider(
            first_reported_cost=reported_costs[0],
            success_reported_cost=reported_costs[1],
        )

        with _authorized_campaign(campaign_id) as (root, grant, journal):
            controller, _, member, frozen_usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=provider,
                    campaign_max_cost=max_cost,
                    reservation_max_cost=max_cost,
                    call_max_cost=max_cost,
                )
            )
            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id=cycle_id,
            ).list_attempts()
            usage_events = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                aggregate_id=cycle_id,
            )
            usage_payload = usage_events[0].payload()

            self.assertEqual(provider.call_count, 2)
            self.assertEqual(len(attempts), 2)
            self.assertEqual(
                tuple(attempt.envelope.reported_cost for attempt in attempts),
                reported_costs,
            )
            self.assertEqual(
                tuple(attempt.envelope.currency for attempt in attempts),
                ("USD", "USD"),
            )
            self.assertEqual(
                tuple(attempt.envelope.usage_status for attempt in attempts),
                (UsageStatus.REPORTED, UsageStatus.REPORTED),
            )
            self.assertEqual(
                tuple(attempt.envelope.outcome for attempt in attempts),
                (
                    InvocationOutcome.RESPONSE_RECEIVED,
                    InvocationOutcome.RESPONSE_RECEIVED,
                ),
            )
            self.assertEqual(
                tuple(attempt.final_outcome for attempt in attempts),
                (InvocationOutcome.INVALID_JSON, InvocationOutcome.SUCCESS),
            )
            self.assertEqual(len(usage_events), 1)
            self.assertEqual(
                usage_events[0].event_type,
                "OPERATIONAL_EXECUTION_USAGE_FROZEN",
            )
            self.assertEqual(
                usage_payload["schema_version"],
                "control_plane.operational_execution_usage.v2",
            )
            self.assertEqual(frozen_usage.usage_status, UsageStatus.UNKNOWN)
            self.assertIsNone(frozen_usage.cost)
            self.assertEqual(frozen_usage.currency, "USD")
            self.assertEqual(
                usage_payload["usage_status"],
                "UNKNOWN",
            )
            self.assertIsNone(usage_payload["cost"])
            self.assertEqual(usage_payload["currency"], "USD")

            rows_before_replay = _campaign_event_rows(root, campaign_id)
            del controller
            replay_journal = OperationalCampaignJournal(
                root_secret=ROOT_SECRET,
                grant=grant,
                namespace="formal",
                campaign_id=campaign_id,
                clock=lambda: NOW,
            )
            reopened = OperationalCampaignController(
                journal=replay_journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost=max_cost,
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-controller", 144, 44_000)
                ),
                monotonic_ns=lambda: 3_000_000,
            )
            replay_execution = reopened.start_execution(
                cycle_id=cycle_id,
                acquisition_id=f"execute-{campaign_id}",
            )
            replay_provider = _EvidenceArtifactBoundFakeProvider()
            replayed_call = reopened.invoke_member_json(
                execution=replay_execution,
                member_id=member.member_id,
                provider=replay_provider,
                prompt={
                    "instruction": "Return one synthetic runner artifact"
                },
                limits=replace(_FAKE_CALL_LIMITS, max_cost=max_cost),
            )
            replayed_usage = reopened.complete_model_execution(
                execution=replay_execution,
            )

            self.assertEqual(replay_provider.call_count, 0)
            self.assertEqual(replayed_call, frozen_usage.model_calls[0])
            self.assertEqual(replayed_usage, frozen_usage)
            self.assertEqual(
                _campaign_event_rows(root, campaign_id),
                rows_before_replay,
            )
            self.assertEqual(
                len(
                    replay_journal.list_events(
                        cycle_id=cycle_id,
                        aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                        aggregate_id=cycle_id,
                    )
                ),
                1,
            )
            self.assertEqual(
                len(
                    OperationalUsageJournal(
                        journal=replay_journal,
                        cycle_id=cycle_id,
                    ).list_attempts()
                ),
                2,
            )

            evidence = reopened.record_model_evidence(
                execution=replay_execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            self.assertEqual(
                reopened.cycle_snapshot(cycle_id).status,
                CycleStatus.EVIDENCE_READY,
            )
            settled = reopened.settle_cycle_without_learning(
                execution=replay_execution,
                execution_usage=replayed_usage,
                evidence_receipt=evidence,
            )

            self.assertEqual(settled.settlement_state, "SETTLED_UNKNOWN")
            self.assertEqual(
                reopened.cycle_snapshot(cycle_id).status,
                CycleStatus.SETTLED,
            )
            snapshot = reopened.budget_snapshot()
            self.assertEqual(snapshot.currency, "USD")
            self.assertEqual(snapshot.reserved_input_tokens, 20)
            self.assertEqual(snapshot.reserved_output_tokens, 10)
            self.assertEqual(snapshot.reserved_cost, "2e+128")
            self.assertEqual(snapshot.reserved_wall_time_ms, _SPAWN_CALL_WALL_TIME_MS)
            self.assertEqual(snapshot.reserved_tool_attempts, 2)
            self.assertEqual(snapshot.reserved_data_exposures, 0)
            self.assertEqual(snapshot.reserved_disk_growth_bytes, 0)
            self.assertEqual(snapshot.spent_input_tokens, 0)
            self.assertEqual(snapshot.spent_output_tokens, 0)
            self.assertEqual(snapshot.spent_cost, "0")
            self.assertEqual(snapshot.spent_wall_time_ms, 0)
            self.assertEqual(snapshot.spent_tool_attempts, 0)
            self.assertEqual(snapshot.spent_data_exposures, 0)
            self.assertEqual(snapshot.spent_disk_growth_bytes, 0)
            evidence_events = replay_journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_MODEL_EVIDENCE",
                aggregate_id=cycle_id,
            )
            cycle_settlement_events = replay_journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_CYCLE_SETTLEMENT",
                aggregate_id=cycle_id,
            )
            self.assertEqual(len(evidence_events), 1)
            self.assertEqual(
                evidence_events[0].event_type,
                "OPERATIONAL_MODEL_EVIDENCE_RECORDED",
            )
            self.assertEqual(len(cycle_settlement_events), 1)
            self.assertEqual(
                cycle_settlement_events[0].event_type,
                "OPERATIONAL_CYCLE_SETTLEMENT_RECORDED",
            )

            rows_before_settlement_replay = _campaign_event_rows(
                root,
                campaign_id,
            )
            replayed_settlement = reopened.settle_cycle_without_learning(
                execution=replay_execution,
                execution_usage=replayed_usage,
                evidence_receipt=evidence,
            )

            self.assertEqual(replayed_settlement, settled)
            self.assertEqual(reopened.budget_snapshot(), snapshot)
            self.assertEqual(
                _campaign_event_rows(root, campaign_id),
                rows_before_settlement_replay,
            )
            self.assertEqual(
                len(
                    OperationalUsageJournal(
                        journal=replay_journal,
                        cycle_id=cycle_id,
                    ).list_attempts()
                ),
                2,
            )

    def test_controller_reopen_currency_mismatch_fails_before_any_event_write(
        self,
    ) -> None:
        campaign_id = "campaign-controller-currency-reopen-001"
        owner = ProcessIdentity("host-currency-reopen", 101, 1_001)

        with _authorized_campaign(campaign_id) as (root, grant, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=_FAKE_CAMPAIGN_LIMITS,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 100,
            )
            events_before = _campaign_event_rows(root, campaign_id)
            reopened_journal = OperationalCampaignJournal(
                root_secret=ROOT_SECRET,
                grant=grant,
                namespace="formal",
                campaign_id=campaign_id,
                clock=lambda: NOW,
            )

            with self.assertRaisesRegex(
                BudgetConflictError,
                "campaign budget configuration conflicts",
            ):
                OperationalCampaignController(
                    journal=reopened_journal,
                    repository_root=root,
                    budget_limits=replace(
                        _FAKE_CAMPAIGN_LIMITS,
                        currency="EUR",
                    ),
                    identity_provider=_FakeProcessIdentityProvider(owner),
                    monotonic_ns=lambda: 100,
                )

            self.assertEqual(
                _campaign_event_rows(root, campaign_id),
                events_before,
            )
            self.assertEqual(
                controller.campaign_snapshot().status,
                CampaignStatus.CREATED,
            )
            self.assertEqual(controller.budget_snapshot().currency, "USD")

    def test_prepare_cycle_currency_mismatch_fails_before_cycle_or_budget_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-currency-reservation-001"
        cycle_id = "cycle-currency-reservation-001"
        owner = ProcessIdentity("host-currency-reservation", 102, 1_002)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=_FAKE_CAMPAIGN_LIMITS,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 100,
            )
            events_before = _campaign_event_rows(root, campaign_id)
            budget_before = controller.budget_snapshot()
            cycle_budget_before = controller.cycle_budget_snapshot()

            with self.assertRaisesRegex(
                BudgetConflictError,
                "Cycle reservation currency conflicts with Campaign budget",
            ):
                _prepare_synthetic_cycle(
                    controller,
                    cycle_id=cycle_id,
                    cycle_number=1,
                    reservation_limits=CycleReservationLimits(
                        currency="EUR",
                        max_input_tokens=20,
                        max_output_tokens=10,
                        max_cost="0.1",
                        max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                        max_tool_attempts=1,
                    ),
                )

            events_after = _campaign_event_rows(root, campaign_id)
            self.assertEqual(events_after, events_before)
            self.assertEqual(controller.budget_snapshot(), budget_before)
            self.assertEqual(
                controller.cycle_budget_snapshot(),
                cycle_budget_before,
            )
            self.assertEqual(
                controller.cycle_budget_snapshot().reserved_cycle_ids,
                (),
            )
            self.assertEqual(
                controller.campaign_snapshot().status,
                CampaignStatus.CREATED,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id=cycle_id,
                    aggregate_type="CAMPAIGN_WORK_ITEM",
                    aggregate_id=cycle_id,
                ),
                (),
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id=cycle_id,
                    aggregate_type="CYCLE_STATE",
                    aggregate_id=cycle_id,
                ),
                (),
            )
            with self.assertRaises(CampaignLifecycleError):
                controller.cycle_snapshot(cycle_id)

    def test_model_call_currency_mismatch_fails_before_start_or_provider_invocation(
        self,
    ) -> None:
        campaign_id = "campaign-controller-currency-call-001"
        cycle_id = "cycle-currency-call-001"
        owner = ProcessIdentity("host-currency-call", 103, 1_003)
        prompt = {"instruction": "Return synthetic artifact 1"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=_FAKE_CAMPAIGN_LIMITS,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 100,
            )
            _prepare_synthetic_cycle(
                controller,
                cycle_id=cycle_id,
                cycle_number=1,
            )
            execution = controller.start_execution(
                cycle_id=cycle_id,
                acquisition_id="execute-currency-call-001",
            )
            provider = _BoundFakeProvider()
            call_id = controller._member_call_id(cycle_id, member.member_id)
            call_events_before = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=call_id,
            )
            events_before = _campaign_event_rows(root, campaign_id)

            with self.assertRaisesRegex(
                BudgetConflictError,
                "model call currency conflicts with Campaign budget",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=replace(_FAKE_CALL_LIMITS, currency="EUR"),
                )

            call_events_after = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=call_id,
            )
            self.assertEqual(provider.call_count, 0)
            self.assertEqual(
                _campaign_event_rows(root, campaign_id),
                events_before,
            )
            self.assertEqual(call_events_after, call_events_before)
            self.assertEqual(call_events_after, ())
            self.assertFalse(
                any(
                    event.event_type == "OPERATIONAL_MODEL_CALL_STARTED"
                    for event in call_events_after
                )
            )
            self.assertEqual(
                controller.cycle_snapshot(cycle_id).status,
                CycleStatus.EXECUTING,
            )

    def _rewrite_model_call_event_as_authentic_currencyless_v1(
        self,
        *,
        root,
        journal,
        cycle_id: str,
        call_id: str,
        legacy_event_type: str,
    ) -> None:
        event_types = (
            "OPERATIONAL_MODEL_CALL_STARTED",
            "OPERATIONAL_MODEL_CALL_COMPLETED",
        )
        manifest_domains = {
            "OPERATIONAL_MODEL_CALL_STARTED": (
                b"control_plane.operational_model_call_start.v1"
            ),
            "OPERATIONAL_MODEL_CALL_COMPLETED": (
                b"control_plane.operational_model_call_receipt.v1"
            ),
        }
        call_events = journal.list_events(
            cycle_id=cycle_id,
            aggregate_type="OPERATIONAL_MODEL_CALL",
            aggregate_id=call_id,
        )
        self.assertEqual(
            tuple(event.event_type for event in call_events),
            event_types,
        )
        legacy_event_index = event_types.index(legacy_event_type)
        untouched_event_index = 1 - legacy_event_index
        legacy_event = call_events[legacy_event_index]
        untouched_event = call_events[untouched_event_index]
        legacy_metadata = (
            legacy_event.event_id,
            legacy_event.namespace,
            legacy_event.campaign_id,
            legacy_event.cycle_id,
            legacy_event.aggregate_type,
            legacy_event.aggregate_id,
            legacy_event.event_type,
            legacy_event.occurred_at,
            legacy_event.sequence,
        )
        legacy_payload = legacy_event.payload()
        authority_grant_id = legacy_payload.pop("_authority_grant_id")
        self.assertEqual(
            legacy_payload["schema_version"],
            "control_plane.operational_model_call.v2",
        )
        legacy_payload["schema_version"] = (
            "control_plane.operational_model_call.v1"
        )
        legacy_call_limits = dict(legacy_payload["call_limits"])
        self.assertEqual(legacy_call_limits.pop("currency"), "USD")
        legacy_payload["call_limits"] = legacy_call_limits
        legacy_identity = {
            key: value
            for key, value in legacy_payload.items()
            if key != "manifest_sha256"
        }
        legacy_payload["manifest_sha256"] = _controller_sha256(
            manifest_domains[legacy_event_type],
            legacy_identity,
            "legacy operational model call",
        )
        legacy_payload["_authority_grant_id"] = authority_grant_id
        _rewrite_campaign_event_payload(root, legacy_event, legacy_payload)

        rewritten_events = journal.list_events(
            cycle_id=cycle_id,
            aggregate_type="OPERATIONAL_MODEL_CALL",
            aggregate_id=call_id,
        )
        rewritten_event = rewritten_events[legacy_event_index]
        rewritten_payload = rewritten_event.payload()
        rewritten_identity = {
            key: value
            for key, value in rewritten_payload.items()
            if key not in {"manifest_sha256", "_authority_grant_id"}
        }
        self.assertEqual(
            rewritten_payload["schema_version"],
            "control_plane.operational_model_call.v1",
        )
        self.assertNotIn("currency", rewritten_payload["call_limits"])
        self.assertEqual(
            rewritten_payload["manifest_sha256"],
            _controller_sha256(
                manifest_domains[legacy_event_type],
                rewritten_identity,
                "stored legacy operational model call",
            ),
        )
        self.assertEqual(
            rewritten_payload["_authority_grant_id"],
            authority_grant_id,
        )
        self.assertEqual(
            (
                rewritten_event.event_id,
                rewritten_event.namespace,
                rewritten_event.campaign_id,
                rewritten_event.cycle_id,
                rewritten_event.aggregate_type,
                rewritten_event.aggregate_id,
                rewritten_event.event_type,
                rewritten_event.occurred_at,
                rewritten_event.sequence,
            ),
            legacy_metadata,
        )
        self.assertEqual(
            rewritten_event.payload_sha256,
            _event_integrity_sha256(
                event_id=rewritten_event.event_id,
                namespace=rewritten_event.namespace,
                campaign_id=rewritten_event.campaign_id,
                cycle_id=rewritten_event.cycle_id,
                aggregate_type=rewritten_event.aggregate_type,
                aggregate_id=rewritten_event.aggregate_id,
                event_type=rewritten_event.event_type,
                payload_json=rewritten_event.payload_json,
                occurred_at=rewritten_event.occurred_at.isoformat(),
                sequence=rewritten_event.sequence,
            ),
        )
        untouched_rewritten_event = rewritten_events[untouched_event_index]
        self.assertEqual(untouched_rewritten_event, untouched_event)
        self.assertEqual(
            untouched_rewritten_event.payload()["schema_version"],
            "control_plane.operational_model_call.v2",
        )
        self.assertEqual(
            untouched_rewritten_event.payload()["call_limits"]["currency"],
            "USD",
        )

    def _assert_currencyless_model_call_public_replay_fails_closed(
        self,
        *,
        root,
        grant,
        campaign_id: str,
        cycle_id: str,
        member_id: str,
        prompt: dict[str, str],
        error_pattern: str,
    ) -> None:
        replay_journal = OperationalCampaignJournal(
            root_secret=ROOT_SECRET,
            grant=grant,
            namespace="formal",
            campaign_id=campaign_id,
            clock=lambda: NOW,
        )
        reopened = OperationalCampaignController(
            journal=replay_journal,
            repository_root=root,
            budget_limits=_FAKE_CAMPAIGN_LIMITS,
            identity_provider=_FakeProcessIdentityProvider(
                ProcessIdentity("host-controller", 144, 44_000)
            ),
            monotonic_ns=lambda: 3_000_000,
        )
        replay_execution = reopened.start_execution(
            cycle_id=cycle_id,
            acquisition_id=f"execute-{campaign_id}",
        )
        campaign_status_before = reopened.campaign_snapshot().status
        cycle_status_before = reopened.cycle_snapshot(cycle_id).status
        rows_before = _campaign_event_rows(root, campaign_id)
        count_before = len(rows_before)
        hashes_before = tuple((row[0], row[8]) for row in rows_before)
        provider = _EvidenceArtifactBoundFakeProvider()

        with self.assertRaisesRegex(CampaignJournalError, error_pattern):
            reopened.invoke_member_json(
                execution=replay_execution,
                member_id=member_id,
                provider=provider,
                prompt=prompt,
                limits=_FAKE_CALL_LIMITS,
            )

        rows_after = _campaign_event_rows(root, campaign_id)
        self.assertEqual(provider.call_count, 0)
        self.assertEqual(rows_after, rows_before)
        self.assertEqual(len(rows_after), count_before)
        self.assertEqual(
            tuple((row[0], row[8]) for row in rows_after),
            hashes_before,
        )
        self.assertEqual(campaign_status_before, CampaignStatus.ACTIVE)
        self.assertEqual(cycle_status_before, CycleStatus.EXECUTING)
        self.assertEqual(
            reopened.campaign_snapshot().status,
            campaign_status_before,
        )
        self.assertEqual(
            reopened.cycle_snapshot(cycle_id).status,
            cycle_status_before,
        )

    def _rewrite_cycle_settlement_as_authentic_currencyless_v1(
        self,
        *,
        root,
        journal,
        campaign_id: str,
        cycle_id: str,
        current_schema: str,
        legacy_schema: str,
        manifest_domain: bytes,
    ) -> None:
        rows_before = _campaign_event_rows(root, campaign_id)
        settlement_events = journal.list_events(
            cycle_id=cycle_id,
            aggregate_type="OPERATIONAL_CYCLE_SETTLEMENT",
            aggregate_id=cycle_id,
        )
        self.assertEqual(len(settlement_events), 1)
        current_event = settlement_events[0]
        self.assertEqual(
            current_event.event_type,
            "OPERATIONAL_CYCLE_SETTLEMENT_RECORDED",
        )
        current_payload = current_event.payload()
        self.assertEqual(current_payload["schema_version"], current_schema)
        self.assertEqual(current_payload["currency"], "USD")

        legacy_payload = dict(current_payload)
        authority_grant_id = legacy_payload.pop("_authority_grant_id")
        legacy_payload["schema_version"] = legacy_schema
        self.assertEqual(legacy_payload.pop("currency"), "USD")
        legacy_identity = {
            key: value
            for key, value in legacy_payload.items()
            if key != "manifest_sha256"
        }
        legacy_payload["manifest_sha256"] = _controller_sha256(
            manifest_domain,
            legacy_identity,
            "legacy operational Cycle settlement",
        )
        legacy_payload["_authority_grant_id"] = authority_grant_id
        _rewrite_campaign_event_payload(root, current_event, legacy_payload)

        rewritten_events = journal.list_events(
            cycle_id=cycle_id,
            aggregate_type="OPERATIONAL_CYCLE_SETTLEMENT",
            aggregate_id=cycle_id,
        )
        self.assertEqual(len(rewritten_events), 1)
        rewritten_event = rewritten_events[0]
        rewritten_payload = rewritten_event.payload()
        rewritten_identity = {
            key: value
            for key, value in rewritten_payload.items()
            if key not in {"manifest_sha256", "_authority_grant_id"}
        }
        self.assertEqual(rewritten_payload, legacy_payload)
        self.assertEqual(rewritten_payload["schema_version"], legacy_schema)
        self.assertNotIn("currency", rewritten_payload)
        self.assertEqual(
            rewritten_payload["manifest_sha256"],
            _controller_sha256(
                manifest_domain,
                rewritten_identity,
                "stored legacy operational Cycle settlement",
            ),
        )
        self.assertEqual(
            rewritten_event.payload_sha256,
            _event_integrity_sha256(
                event_id=rewritten_event.event_id,
                namespace=rewritten_event.namespace,
                campaign_id=rewritten_event.campaign_id,
                cycle_id=rewritten_event.cycle_id,
                aggregate_type=rewritten_event.aggregate_type,
                aggregate_id=rewritten_event.aggregate_id,
                event_type=rewritten_event.event_type,
                payload_json=rewritten_event.payload_json,
                occurred_at=rewritten_event.occurred_at.isoformat(),
                sequence=rewritten_event.sequence,
            ),
        )

        rows_after = _campaign_event_rows(root, campaign_id)
        self.assertEqual(len(rows_after), len(rows_before))
        for before, after in zip(rows_before, rows_after, strict=True):
            if before[0] == current_event.event_id:
                self.assertEqual(before[:7] + before[9:], after[:7] + after[9:])
                self.assertNotEqual(before[7:9], after[7:9])
            else:
                self.assertEqual(after, before)

    def _assert_currencyless_settlement_replay_conflict(
        self,
        *,
        root,
        grant,
        campaign_id: str,
        cycle_id: str,
        replay,
        expected_error: str,
    ) -> None:
        replay_journal = OperationalCampaignJournal(
            root_secret=ROOT_SECRET,
            grant=grant,
            namespace="formal",
            campaign_id=campaign_id,
            clock=lambda: NOW,
        )
        reopened = OperationalCampaignController(
            journal=replay_journal,
            repository_root=root,
            budget_limits=_FAKE_CAMPAIGN_LIMITS,
            identity_provider=_FakeProcessIdentityProvider(
                ProcessIdentity("host-controller", 144, 44_000)
            ),
            monotonic_ns=lambda: 3_000_000,
        )
        campaign_status_before = reopened.campaign_snapshot().status
        cycle_status_before = reopened.cycle_snapshot(cycle_id).status
        self.assertEqual(campaign_status_before, CampaignStatus.ACTIVE)
        self.assertEqual(cycle_status_before, CycleStatus.SETTLED)
        rows_before_replay = _campaign_event_rows(root, campaign_id)
        count_before_replay = len(rows_before_replay)
        hashes_before_replay = tuple(
            (row[0], row[8]) for row in rows_before_replay
        )

        with self.assertRaises(CampaignJournalError) as raised:
            replay(reopened)
        self.assertEqual(str(raised.exception), expected_error)

        rows_after_replay = _campaign_event_rows(root, campaign_id)
        self.assertEqual(rows_after_replay, rows_before_replay)
        self.assertEqual(len(rows_after_replay), count_before_replay)
        self.assertEqual(
            tuple((row[0], row[8]) for row in rows_after_replay),
            hashes_before_replay,
        )
        self.assertEqual(
            reopened.campaign_snapshot().status,
            campaign_status_before,
        )
        self.assertEqual(
            reopened.cycle_snapshot(cycle_id).status,
            cycle_status_before,
        )

    def test_legacy_currencyless_model_call_start_replay_fails_closed_before_provider_without_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-legacy-currencyless-call-start"
        cycle_id = "cycle-001"
        prompt = {"instruction": "Return one synthetic runner artifact"}

        with _authorized_campaign(campaign_id) as (root, grant, journal):
            controller, _, member, usage = _completed_evidence_model_call(
                root,
                journal,
                campaign_id=campaign_id,
            )
            call_id = usage.model_calls[0].call_id
            self._rewrite_model_call_event_as_authentic_currencyless_v1(
                root=root,
                journal=journal,
                cycle_id=cycle_id,
                call_id=call_id,
                legacy_event_type="OPERATIONAL_MODEL_CALL_STARTED",
            )
            del controller
            self._assert_currencyless_model_call_public_replay_fails_closed(
                root=root,
                grant=grant,
                campaign_id=campaign_id,
                cycle_id=cycle_id,
                member_id=member.member_id,
                prompt=prompt,
                error_pattern="^model call limits are invalid$",
            )

    def test_legacy_currencyless_model_call_completion_replay_fails_closed_before_provider_without_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-legacy-currencyless-call-completion"
        cycle_id = "cycle-001"
        prompt = {"instruction": "Return one synthetic runner artifact"}

        with _authorized_campaign(campaign_id) as (root, grant, journal):
            controller, _, member, usage = _completed_evidence_model_call(
                root,
                journal,
                campaign_id=campaign_id,
            )
            call_id = usage.model_calls[0].call_id
            self._rewrite_model_call_event_as_authentic_currencyless_v1(
                root=root,
                journal=journal,
                cycle_id=cycle_id,
                call_id=call_id,
                legacy_event_type="OPERATIONAL_MODEL_CALL_COMPLETED",
            )
            del controller
            self._assert_currencyless_model_call_public_replay_fails_closed(
                root=root,
                grant=grant,
                campaign_id=campaign_id,
                cycle_id=cycle_id,
                member_id=member.member_id,
                prompt=prompt,
                error_pattern="^operational model call identity conflicts$",
            )

    def test_authentic_legacy_execution_usage_currency_conflict_fails_closed_without_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-legacy-execution-usage-currency"
        cycle_id = "cycle-001"
        prompt = {"instruction": "Return one synthetic runner artifact"}

        with _authorized_campaign(campaign_id) as (root, grant, journal):
            controller, _, member, frozen_usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            call_id = frozen_usage.model_calls[0].call_id
            model_call_events_before = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=call_id,
            )
            usage_attempts_before = OperationalUsageJournal(
                journal=journal,
                cycle_id=cycle_id,
            ).list_attempts(call_id=call_id)
            usage_events = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                aggregate_id=cycle_id,
            )

            self.assertEqual(
                tuple(event.event_type for event in model_call_events_before),
                (
                    "OPERATIONAL_MODEL_CALL_STARTED",
                    "OPERATIONAL_MODEL_CALL_COMPLETED",
                ),
            )
            self.assertTrue(
                all(
                    event.payload()["schema_version"]
                    == "control_plane.operational_model_call.v2"
                    for event in model_call_events_before
                )
            )
            self.assertTrue(
                all(
                    event.payload()["call_limits"]["currency"] == "USD"
                    for event in model_call_events_before
                )
            )
            self.assertEqual(len(usage_attempts_before), 1)
            self.assertEqual(
                usage_attempts_before[0].envelope.currency,
                "USD",
            )
            self.assertEqual(
                usage_attempts_before[0].envelope.usage_status,
                UsageStatus.REPORTED,
            )
            self.assertEqual(
                usage_attempts_before[0].final_outcome,
                InvocationOutcome.SUCCESS,
            )
            self.assertEqual(len(usage_events), 1)
            current_event = usage_events[0]
            current_payload = current_event.payload()
            self.assertEqual(
                current_event.event_type,
                "OPERATIONAL_EXECUTION_USAGE_FROZEN",
            )
            self.assertEqual(
                current_payload["schema_version"],
                "control_plane.operational_execution_usage.v2",
            )
            self.assertEqual(current_payload["currency"], "USD")

            legacy_payload = dict(current_payload)
            legacy_payload["schema_version"] = (
                "control_plane.operational_execution_usage.v1"
            )
            legacy_payload["currency"] = None
            legacy_identity = {
                key: value
                for key, value in legacy_payload.items()
                if key not in {"manifest_sha256", "_authority_grant_id"}
            }
            legacy_payload["manifest_sha256"] = _controller_sha256(
                b"control_plane.operational_execution_usage.v1",
                legacy_identity,
                "legacy operational execution usage",
            )
            _rewrite_campaign_event_payload(root, current_event, legacy_payload)

            rewritten_events = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                aggregate_id=cycle_id,
            )
            self.assertEqual(len(rewritten_events), 1)
            rewritten_event = rewritten_events[0]
            rewritten_payload = rewritten_event.payload()
            rewritten_identity = {
                key: value
                for key, value in rewritten_payload.items()
                if key not in {"manifest_sha256", "_authority_grant_id"}
            }
            self.assertEqual(rewritten_payload, legacy_payload)
            self.assertIn("currency", rewritten_payload)
            self.assertIsNone(rewritten_payload["currency"])
            self.assertEqual(
                rewritten_payload["manifest_sha256"],
                _controller_sha256(
                    b"control_plane.operational_execution_usage.v1",
                    rewritten_identity,
                    "stored legacy operational execution usage",
                ),
            )
            self.assertEqual(
                rewritten_event.payload_sha256,
                _event_integrity_sha256(
                    event_id=rewritten_event.event_id,
                    namespace=rewritten_event.namespace,
                    campaign_id=rewritten_event.campaign_id,
                    cycle_id=rewritten_event.cycle_id,
                    aggregate_type=rewritten_event.aggregate_type,
                    aggregate_id=rewritten_event.aggregate_id,
                    event_type=rewritten_event.event_type,
                    payload_json=rewritten_event.payload_json,
                    occurred_at=rewritten_event.occurred_at.isoformat(),
                    sequence=rewritten_event.sequence,
                ),
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id=cycle_id,
                    aggregate_type="OPERATIONAL_MODEL_CALL",
                    aggregate_id=call_id,
                ),
                model_call_events_before,
            )
            self.assertEqual(
                OperationalUsageJournal(
                    journal=journal,
                    cycle_id=cycle_id,
                ).list_attempts(call_id=call_id),
                usage_attempts_before,
            )

            rows_before_replay = _campaign_event_rows(root, campaign_id)
            count_before_replay = len(rows_before_replay)
            hashes_before_replay = tuple(
                (row[0], row[8]) for row in rows_before_replay
            )
            del controller
            replay_journal = OperationalCampaignJournal(
                root_secret=ROOT_SECRET,
                grant=grant,
                namespace="formal",
                campaign_id=campaign_id,
                clock=lambda: NOW,
            )
            reopened = OperationalCampaignController(
                journal=replay_journal,
                repository_root=root,
                budget_limits=_FAKE_CAMPAIGN_LIMITS,
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-controller", 144, 44_000)
                ),
                monotonic_ns=lambda: 3_000_000,
            )
            replay_execution = reopened.start_execution(
                cycle_id=cycle_id,
                acquisition_id=f"execute-{campaign_id}",
            )
            self.assertEqual(
                reopened.campaign_snapshot().status,
                CampaignStatus.ACTIVE,
            )
            self.assertEqual(
                reopened.cycle_snapshot(cycle_id).status,
                CycleStatus.EXECUTING,
            )
            replay_provider = _EvidenceArtifactBoundFakeProvider()
            replayed_call = reopened.invoke_member_json(
                execution=replay_execution,
                member_id=member.member_id,
                provider=replay_provider,
                prompt=prompt,
                limits=_FAKE_CALL_LIMITS,
            )
            self.assertEqual(replay_provider.call_count, 0)
            self.assertEqual(replayed_call, frozen_usage.model_calls[0])

            with self.assertRaises(CampaignJournalError) as raised:
                reopened.complete_model_execution(execution=replay_execution)
            self.assertEqual(
                str(raised.exception),
                "operational execution usage conflicts",
            )

            rows_after_failure = _campaign_event_rows(root, campaign_id)
            self.assertEqual(rows_after_failure, rows_before_replay)
            self.assertEqual(len(rows_after_failure), count_before_replay)
            self.assertEqual(
                tuple((row[0], row[8]) for row in rows_after_failure),
                hashes_before_replay,
            )
            self.assertEqual(
                replay_journal.list_events(
                    cycle_id=cycle_id,
                    aggregate_type="OPERATIONAL_MODEL_CALL",
                    aggregate_id=call_id,
                ),
                model_call_events_before,
            )
            self.assertEqual(
                OperationalUsageJournal(
                    journal=replay_journal,
                    cycle_id=cycle_id,
                ).list_attempts(call_id=call_id),
                usage_attempts_before,
            )
            self.assertEqual(
                reopened.campaign_snapshot().status,
                CampaignStatus.ACTIVE,
            )
            self.assertEqual(
                reopened.cycle_snapshot(cycle_id).status,
                CycleStatus.EXECUTING,
            )

    def test_authentic_legacy_learned_settlement_currency_conflict_fails_closed_without_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-legacy-learned-settlement-currency"
        cycle_id = "cycle-001"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }

        with _authorized_campaign(campaign_id) as (root, grant, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            self.assertTrue(evidence.evidence.promotion_eligible)
            service = LearningCommitService(repository_root=root)
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value="f" * 64,
            ):
                learning = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "legacy-settlement-terminal-report"
                    },
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )
            settlement = controller.settle_cycle(
                execution=execution,
                execution_usage=usage,
                learning_commit_receipt=learning,
            )
            self.assertEqual(settlement.currency, "USD")
            self.assertEqual(
                controller.cycle_snapshot(cycle_id).status,
                CycleStatus.SETTLED,
            )

            call_events = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=usage.model_calls[0].call_id,
            )
            self.assertEqual(len(call_events), 2)
            self.assertTrue(
                all(
                    event.payload()["schema_version"]
                    == "control_plane.operational_model_call.v2"
                    and event.payload()["call_limits"]["currency"] == "USD"
                    for event in call_events
                )
            )
            usage_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                aggregate_id=cycle_id,
            )[0]
            self.assertEqual(
                usage_event.payload()["schema_version"],
                "control_plane.operational_execution_usage.v2",
            )
            self.assertEqual(usage_event.payload()["currency"], "USD")
            evidence_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_MODEL_EVIDENCE",
                aggregate_id=cycle_id,
            )[0]
            self.assertEqual(
                evidence_event.payload()["schema_version"],
                "control_plane.operational_model_evidence.v1",
            )
            learning_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                aggregate_id=cycle_id,
            )[0]
            self.assertEqual(
                learning_event.payload()["schema_version"],
                "control_plane.operational_learning_commit.v1",
            )
            self._rewrite_cycle_settlement_as_authentic_currencyless_v1(
                root=root,
                journal=journal,
                campaign_id=campaign_id,
                cycle_id=cycle_id,
                current_schema=(
                    "control_plane.operational_cycle_settlement.v2"
                ),
                legacy_schema=(
                    "control_plane.operational_cycle_settlement.v1"
                ),
                manifest_domain=(
                    b"control_plane.operational_cycle_settlement.v1"
                ),
            )

            del controller
            self._assert_currencyless_settlement_replay_conflict(
                root=root,
                grant=grant,
                campaign_id=campaign_id,
                cycle_id=cycle_id,
                replay=lambda reopened: reopened.settle_cycle(
                    execution=execution,
                    execution_usage=usage,
                    learning_commit_receipt=learning,
                ),
                expected_error="operational Cycle settlement conflicts",
            )

    def test_authentic_legacy_no_learning_settlement_currency_conflict_fails_closed_without_writes(
        self,
    ) -> None:
        campaign_id = (
            "campaign-controller-legacy-no-learning-settlement-currency"
        )
        cycle_id = "cycle-001"

        with _authorized_campaign(campaign_id) as (root, grant, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            self.assertFalse(evidence.evidence.promotion_eligible)
            self.assertEqual(
                evidence.evidence.verdict,
                "NO_MATERIAL_FINDING",
            )
            settlement = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )
            self.assertEqual(settlement.currency, "USD")
            self.assertEqual(
                controller.cycle_snapshot(cycle_id).status,
                CycleStatus.SETTLED,
            )

            call_events = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=usage.model_calls[0].call_id,
            )
            self.assertEqual(len(call_events), 2)
            self.assertTrue(
                all(
                    event.payload()["schema_version"]
                    == "control_plane.operational_model_call.v2"
                    and event.payload()["call_limits"]["currency"] == "USD"
                    for event in call_events
                )
            )
            usage_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                aggregate_id=cycle_id,
            )[0]
            self.assertEqual(
                usage_event.payload()["schema_version"],
                "control_plane.operational_execution_usage.v2",
            )
            self.assertEqual(usage_event.payload()["currency"], "USD")
            evidence_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_MODEL_EVIDENCE",
                aggregate_id=cycle_id,
            )[0]
            self.assertEqual(
                evidence_event.payload()["schema_version"],
                "control_plane.operational_model_evidence.v1",
            )
            disposition_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="OPERATIONAL_NO_LEARNING_DISPOSITION",
                aggregate_id=cycle_id,
            )[0]
            self.assertEqual(
                disposition_event.payload()["schema_version"],
                "control_plane.operational_no_learning_disposition.v1",
            )
            self._rewrite_cycle_settlement_as_authentic_currencyless_v1(
                root=root,
                journal=journal,
                campaign_id=campaign_id,
                cycle_id=cycle_id,
                current_schema=(
                    "control_plane.operational_no_learning_settlement.v2"
                ),
                legacy_schema=(
                    "control_plane.operational_no_learning_settlement.v1"
                ),
                manifest_domain=(
                    b"control_plane.operational_no_learning_settlement.v1"
                ),
            )

            del controller
            self._assert_currencyless_settlement_replay_conflict(
                root=root,
                grant=grant,
                campaign_id=campaign_id,
                cycle_id=cycle_id,
                replay=lambda reopened: reopened.settle_cycle_without_learning(
                    execution=execution,
                    execution_usage=usage,
                    evidence_receipt=evidence,
                ),
                expected_error=(
                    "operational no-Learning settlement conflicts"
                ),
            )

    def test_committed_hard_block_rejects_before_any_prepare_cycle_write(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-hard-preflight"
        hypothesis = "Committed negative Learning blocks an exact execution"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=hypothesis,
            )
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                controller,
                cycle_id=task.task_id,
            )
            self.assertGreater(len(baseline["all_events"]), 0)
            committed = _claim(
                claim_id="committed-negative",
                hypothesis=hypothesis,
                scope=task.proposal["scope"],
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_preflight_input(committed),
            ) as reader:
                with self.assertRaisesRegex(
                    CycleFreezeError,
                    "LEARNING_HARD_BLOCK",
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            reader.assert_called_once_with()
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                baseline,
            )
            self.assertEqual(baseline["usage"], ())

    def test_committed_partial_rejects_before_any_prepare_cycle_write(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-scoped-preflight"
        hypothesis = "Committed partial Learning blocks its covered scope"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=hypothesis,
            )
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                controller,
                cycle_id=task.task_id,
            )
            committed = _claim(
                claim_id="committed-partial",
                hypothesis=hypothesis,
                scope=task.proposal["scope"],
                kind="PARTIAL",
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_preflight_input(committed),
            ) as reader:
                with self.assertRaisesRegex(
                    CycleFreezeError,
                    "LEARNING_SCOPED_BLOCK",
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            reader.assert_called_once_with()
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                baseline,
            )
            self.assertEqual(baseline["usage"], ())

    def test_missing_work_item_cannot_adopt_a_created_cycle_shell(
        self,
    ) -> None:
        campaign_id = "campaign-controller-created-shell-admission"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="A CREATED shell cannot be adopted by the controller",
            )
            controller._lifecycle.activate()
            controller._cycle_budget.open_cycle(
                lifecycle=controller._lifecycle,
                cycle_id=task.task_id,
                cycle_number=1,
            )
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.CREATED,
            )
            before = _operational_table_bytes(root)

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ):
                with self.assertRaises(CampaignJournalError):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertEqual(_operational_table_bytes(root), before)

    def test_missing_work_item_cannot_adopt_valid_v2_context_ready_history(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-ready-without-work-item"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=(
                    "A valid durable context cannot backfill controller admission"
                ),
            )
            controller._lifecycle.activate()
            controller._cycle_budget.open_cycle(
                lifecycle=controller._lifecycle,
                cycle_id=task.task_id,
                cycle_number=1,
            )
            controller._lifecycle.advance_cycle(
                cycle_id=task.task_id,
                expected_status=CycleStatus.CREATED,
                next_status=CycleStatus.BUDGET_RESERVED,
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ):
                controller._context.prepare(
                    cycle_id=task.task_id,
                    proposal=task.proposal,
                    roles=tuple(
                        sorted({member.role for member in roster_members})
                    ),
                )
            context_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CYCLE_SAFE_CONTEXT",
                aggregate_id=task.task_id,
            )[0]
            self.assertEqual(
                context_event.payload()["schema_version"],
                "control_plane.cycle_context_receipt.v2",
            )
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.CONTEXT_READY,
            )
            before = _operational_table_bytes(root)

            with self.assertRaises(CampaignJournalError):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )

            self.assertEqual(_operational_table_bytes(root), before)

    def test_admission_lock_replays_a_late_conflicting_work_item_before_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-late-conflicting-work-item"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Admission must bind the work item under its write lock",
            )
            _, conflicting_work_item = campaign_controller_module._canonical_task(
                ExperimentTask(
                    task_id=task.task_id,
                    strategy=task.strategy,
                    proposal={
                        **task.proposal,
                        "hypothesis": "A conflicting late-writer mechanism",
                    },
                    source=task.source,
                    priority=task.priority,
                ),
                cycle_number=1,
            )
            conflicting_work_item["proposal"] = (
                campaign_controller_module.canonical_campaign_proposal(
                    conflicting_work_item["proposal"]
                )
            )
            original_write = campaign_controller_module._SqliteUnitOfWork._write
            original_preflight = campaign_controller_module.run_campaign_preflight
            original_activate = OperationalCampaignLifecycle._activate_in_transaction
            preflight_completed = False
            inserted_baseline: list[tuple[bytes, ...]] = []
            activation_calls = 0

            def observed_preflight(**kwargs):
                nonlocal preflight_completed
                result = original_preflight(**kwargs)
                self.assertEqual(result["verdict"], "WOULD_ACCEPT")
                preflight_completed = True
                return result

            def late_writer_before_lock(unit_of_work, operation):
                if operation.__name__ == "reserve_and_open":
                    self.assertTrue(preflight_completed)
                    self.assertEqual(
                        controller.campaign_snapshot().status,
                        CampaignStatus.CREATED,
                    )
                    journal.append(
                        event_id=controller._work_item_event_id(task.task_id),
                        cycle_id=task.task_id,
                        aggregate_type="CAMPAIGN_WORK_ITEM",
                        aggregate_id=task.task_id,
                        event_type="CAMPAIGN_WORK_ITEM_ADOPTED",
                        payload=conflicting_work_item,
                    )
                    inserted_baseline.append(_operational_table_bytes(root))
                return original_write(unit_of_work, operation)

            def observed_activate(lifecycle, connection):
                nonlocal activation_calls
                activation_calls += 1
                return original_activate(lifecycle, connection)

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                side_effect=observed_preflight,
            ), patch.object(
                campaign_controller_module._SqliteUnitOfWork,
                "_write",
                new=late_writer_before_lock,
            ), patch.object(
                OperationalCampaignLifecycle,
                "_activate_in_transaction",
                new=observed_activate,
            ):
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "^Campaign work item conflicts$",
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertEqual(len(inserted_baseline), 1)
            self.assertEqual(_operational_table_bytes(root), inserted_baseline[0])
            self.assertEqual(activation_calls, 0)

    def test_late_writer_resource_prefix_cannot_be_adopted_after_preflight(
        self,
    ) -> None:
        campaign_id = "campaign-controller-late-resource-prefix"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=(
                    "A late resource reservation cannot skip Cycle-slot admission"
                ),
            )
            _, work_item = campaign_controller_module._canonical_task(
                task,
                cycle_number=1,
            )
            work_item["proposal"] = (
                campaign_controller_module.canonical_campaign_proposal(
                    work_item["proposal"]
                )
            )
            original_write = campaign_controller_module._SqliteUnitOfWork._write
            original_preflight = campaign_controller_module.run_campaign_preflight
            candidate_before_lock = Event()
            late_writer_committed = Event()
            release_candidate = Event()
            candidate_thread_id: list[int] = []
            late_writer_baseline: list[tuple[bytes, ...]] = []

            def observed_preflight(**kwargs):
                result = original_preflight(**kwargs)
                self.assertEqual(result["verdict"], "WOULD_ACCEPT")
                return result

            def gated_write(unit_of_work, operation):
                if (
                    operation.__name__ == "reserve_and_open"
                    and get_ident() == candidate_thread_id[0]
                ):
                    candidate_before_lock.set()
                    if not release_candidate.wait(timeout=10):
                        raise AssertionError("late writer did not release candidate")
                    self.assertTrue(late_writer_committed.is_set())
                return original_write(unit_of_work, operation)

            def prepare_candidate():
                candidate_thread_id.append(get_ident())
                return controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )

            def write_illegal_prefix() -> None:
                if not candidate_before_lock.wait(timeout=10):
                    raise AssertionError("candidate did not reach admission boundary")

                def append_prefix(connection) -> None:
                    controller._adopt_work_item_in_transaction(
                        connection,
                        cycle_id=task.task_id,
                        payload=work_item,
                    )
                    controller._budget._reserve_in_transaction(
                        connection,
                        reservation_id=controller._reservation_id(task.task_id),
                        call_id=task.task_id,
                        currency=reservation_limits.currency,
                        max_input_tokens=reservation_limits.max_input_tokens,
                        max_output_tokens=reservation_limits.max_output_tokens,
                        max_cost=reservation_limits.max_cost,
                        max_wall_time_ms=reservation_limits.max_wall_time_ms,
                        max_tool_attempts=reservation_limits.max_tool_attempts,
                        max_data_exposures=reservation_limits.max_data_exposures,
                        max_disk_growth_bytes=(
                            reservation_limits.max_disk_growth_bytes
                        ),
                    )

                try:
                    original_write(
                        campaign_controller_module._SqliteUnitOfWork(
                            campaign_controller_module.stores._operational_spec()
                        ),
                        append_prefix,
                    )
                    late_writer_baseline.append(_operational_table_bytes(root))
                    late_writer_committed.set()
                finally:
                    release_candidate.set()

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                side_effect=observed_preflight,
            ) as preflight, patch.object(
                campaign_controller_module._SqliteUnitOfWork,
                "_write",
                new=gated_write,
            ), ThreadPoolExecutor(max_workers=2) as executor:
                candidate = executor.submit(prepare_candidate)
                late_writer = executor.submit(write_illegal_prefix)
                late_writer.result(timeout=10)
                with self.assertRaises(CampaignJournalError):
                    candidate.result(timeout=10)

            preflight.assert_called_once()
            self.assertTrue(late_writer_committed.is_set())
            self.assertEqual(len(late_writer_baseline), 1)
            self.assertEqual(
                controller.cycle_budget_snapshot().reserved_cycle_ids,
                (),
            )
            self.assertEqual(
                _operational_table_bytes(root),
                late_writer_baseline[0],
            )

    def test_settled_complete_admission_bundle_is_rejected_before_prepare_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-settled-admission-bundle"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=(
                    "A settled reservation cannot resume Cycle preparation"
                ),
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch(
                "research_automation.control_plane.campaign_context."
                "OperationalCycleContextJournal._prepare_assembled",
                side_effect=RuntimeError("synthetic crash before context"),
            ):
                with self.assertRaisesRegex(RuntimeError, "before context"):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.BUDGET_RESERVED,
            )
            reservation_id = controller._reservation_id(task.task_id)
            controller._budget.settle(
                reservation_id,
                currency=reservation_limits.currency,
                input_tokens=0,
                output_tokens=0,
                cost="0",
                wall_time_ms=0,
                tool_attempts=0,
                data_exposures=0,
                disk_growth_bytes=0,
            )
            before = _operational_table_bytes(root)

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ):
                with self.assertRaises(CampaignJournalError):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertEqual(_operational_table_bytes(root), before)
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.BUDGET_RESERVED,
            )

    def test_settlement_after_admission_blocks_context_write_without_pollution(
        self,
    ) -> None:
        campaign_id = "campaign-controller-settlement-before-context-write"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=(
                    "Every preparation write revalidates its reservation"
                ),
            )
            original_prepare = OperationalCycleContextJournal._prepare_assembled
            settled_baseline: list[tuple[bytes, ...]] = []

            def settle_before_context(context, assembled, **kwargs):
                controller._budget.settle(
                    controller._reservation_id(task.task_id),
                    currency=reservation_limits.currency,
                    input_tokens=0,
                    output_tokens=0,
                    cost="0",
                    wall_time_ms=0,
                    tool_attempts=0,
                    data_exposures=0,
                    disk_growth_bytes=0,
                )
                settled_baseline.append(_operational_table_bytes(root))
                return original_prepare(context, assembled, **kwargs)

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch.object(
                OperationalCycleContextJournal,
                "_prepare_assembled",
                new=settle_before_context,
            ):
                with self.assertRaises(CampaignJournalError):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertEqual(len(settled_baseline), 1)
            self.assertEqual(_operational_table_bytes(root), settled_baseline[0])
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.BUDGET_RESERVED,
            )

    def test_settlement_between_preparation_stages_blocks_the_next_write(
        self,
    ) -> None:
        cases = (
            (
                "before-roster",
                OperationalRosterJournal,
                0,
            ),
            (
                "before-freeze",
                OperationalCycleFreezeJournal,
                1,
            ),
        )
        for label, stage_owner, expected_roster_count in cases:
            with self.subTest(boundary=label):
                campaign_id = f"campaign-controller-settlement-{label}"
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    (
                        controller,
                        task,
                        execution_spec,
                        roster_members,
                        reservation_limits,
                    ) = _learning_preflight_prepare_inputs(
                        root,
                        journal,
                        hypothesis=(
                            "Every durable preparation stage revalidates budget"
                        ),
                    )
                    original_freeze = stage_owner.freeze
                    settled_baseline: list[tuple[bytes, ...]] = []

                    def settle_before_stage(stage, **kwargs):
                        controller._budget.settle(
                            controller._reservation_id(task.task_id),
                            currency=reservation_limits.currency,
                            input_tokens=0,
                            output_tokens=0,
                            cost="0",
                            wall_time_ms=0,
                            tool_attempts=0,
                            data_exposures=0,
                            disk_growth_bytes=0,
                        )
                        settled_baseline.append(_operational_table_bytes(root))
                        return original_freeze(stage, **kwargs)

                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        return_value=_projectable_learning_input(),
                    ), patch.object(
                        stage_owner,
                        "freeze",
                        new=settle_before_stage,
                    ):
                        with self.assertRaises(CampaignJournalError):
                            controller.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )

                    self.assertEqual(len(settled_baseline), 1)
                    self.assertEqual(
                        _operational_table_bytes(root),
                        settled_baseline[0],
                    )
                    self.assertEqual(
                        controller.cycle_snapshot(task.task_id).status,
                        CycleStatus.CONTEXT_READY,
                    )
                    self.assertEqual(
                        len(
                            journal.list_events(
                                cycle_id=task.task_id,
                                aggregate_type="CYCLE_ROSTER",
                                aggregate_id=task.task_id,
                            )
                        ),
                        expected_roster_count,
                    )
                    self.assertEqual(
                        journal.list_events(
                            cycle_id=task.task_id,
                            aggregate_type="CYCLE_INPUT_FREEZE",
                            aggregate_id=task.task_id,
                        ),
                        (),
                    )

    def test_early_artifact_prefix_is_rejected_before_context_write(
        self,
    ) -> None:
        cases = (
            (
                "roster",
                "CYCLE_ROSTER",
                "CYCLE_ROSTER_FROZEN",
            ),
            (
                "freeze",
                "CYCLE_INPUT_FREEZE",
                "CYCLE_INPUTS_FROZEN",
            ),
        )
        for label, aggregate_type, event_type in cases:
            with self.subTest(prefix=label):
                campaign_id = f"campaign-controller-early-{label}-prefix"
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    (
                        controller,
                        task,
                        execution_spec,
                        roster_members,
                        reservation_limits,
                    ) = _learning_preflight_prepare_inputs(
                        root,
                        journal,
                        hypothesis=(
                            "Artifacts cannot precede their preparation stage"
                        ),
                    )
                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        return_value=_projectable_learning_input(),
                    ), patch.object(
                        OperationalCycleContextJournal,
                        "_prepare_assembled",
                        side_effect=RuntimeError("synthetic pre-context crash"),
                    ):
                        with self.assertRaisesRegex(
                            RuntimeError,
                            "pre-context crash",
                        ):
                            controller.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )

                    event_id = (
                        controller._roster._event_id(task.task_id, "freeze")
                        if label == "roster"
                        else controller._freeze._freeze_event_id(task.task_id)
                    )
                    journal.append(
                        event_id=event_id,
                        cycle_id=task.task_id,
                        aggregate_type=aggregate_type,
                        aggregate_id=task.task_id,
                        event_type=event_type,
                        payload={"invalid_early_prefix": label},
                    )
                    before = _operational_table_bytes(root)

                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        return_value=_projectable_learning_input(),
                    ):
                        with self.assertRaises(RuntimeError):
                            controller.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )

                    self.assertEqual(_operational_table_bytes(root), before)
                    self.assertEqual(
                        controller.cycle_snapshot(task.task_id).status,
                        CycleStatus.BUDGET_RESERVED,
                    )

    def test_context_ready_roster_prefix_order_rejects_before_preflight(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-ready-roster-order"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Roster prefixes follow durable context readiness",
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch.object(
                OperationalCycleFreezeJournal,
                "freeze",
                side_effect=RuntimeError("synthetic pre-freeze crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "pre-freeze crash"):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            context_ready = next(
                event
                for event in journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CYCLE_STATE",
                    aggregate_id=task.task_id,
                )
                if event.payload().get("to_status") == CycleStatus.CONTEXT_READY.value
            )
            roster_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CYCLE_ROSTER",
                aggregate_id=task.task_id,
            )[0]
            _swap_and_resign_event_sequences(context_ready, roster_event)
            before = _operational_table_bytes(root)
            original_preflight = campaign_controller_module.run_campaign_preflight

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=AssertionError(
                    "CONTEXT_READY recovery must not read live Learning"
                ),
            ) as live_reader, patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                wraps=original_preflight,
            ) as controller_preflight:
                with self.assertRaises(
                    (CampaignJournalError, CycleFreezeIntegrityError)
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            live_reader.assert_not_called()
            controller_preflight.assert_not_called()
            self.assertEqual(_operational_table_bytes(root), before)

    def test_context_ready_early_freeze_prefix_rejects_without_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-ready-early-freeze"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Freeze history cannot precede its freeze stage",
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch.object(
                OperationalCycleFreezeJournal,
                "freeze",
                side_effect=RuntimeError("synthetic pre-freeze crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "pre-freeze crash"):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            journal.append(
                event_id=controller._freeze._freeze_event_id(task.task_id),
                cycle_id=task.task_id,
                aggregate_type="CYCLE_INPUT_FREEZE",
                aggregate_id=task.task_id,
                event_type="CYCLE_INPUTS_FROZEN",
                payload={"invalid_early_prefix": "freeze"},
            )
            before = _operational_table_bytes(root)
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=AssertionError(
                    "invalid durable prefixes must reject before live Learning"
                ),
            ) as live_reader, patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                side_effect=AssertionError(
                    "invalid durable prefixes must reject before preflight"
                ),
            ) as controller_preflight:
                with self.assertRaises(CampaignJournalError):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            live_reader.assert_not_called()
            controller_preflight.assert_not_called()
            self.assertEqual(_operational_table_bytes(root), before)

    def test_context_ready_recovery_preflights_durable_claims_before_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-ready-durable-preflight"
        hypothesis = "Durable blocked Learning cannot be adopted by recovery"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=hypothesis,
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch.object(
                OperationalCycleContextJournal,
                "_prepare_assembled",
                side_effect=RuntimeError("synthetic pre-context crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "pre-context crash"):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            blocked_claim = _claim(
                claim_id="durable-context-hard-block",
                hypothesis=hypothesis,
                scope=task.proposal["scope"],
            )
            durable_projection = _projectable_preflight_input(blocked_claim)
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=durable_projection,
            ):
                controller._context.prepare(
                    cycle_id=task.task_id,
                    proposal=task.proposal,
                    roles=tuple(
                        sorted({member.role for member in roster_members})
                    ),
                )
            before = _operational_table_bytes(root)
            observed_claims: list[object] = []
            original_preflight = campaign_controller_module.run_campaign_preflight

            def observe_durable_preflight(**kwargs):
                observed_claims.append(kwargs["committed_claims"])
                return original_preflight(**kwargs)

            live_read_error = AssertionError(
                "CONTEXT_READY recovery must not read live Learning"
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=live_read_error,
            ) as live_reader, patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                side_effect=observe_durable_preflight,
            ) as controller_preflight, patch.object(
                campaign_freeze_module,
                "run_campaign_preflight",
                side_effect=AssertionError(
                    "freeze preflight must not follow a durable rejection"
                ),
            ) as freeze_preflight:
                with self.assertRaises(CycleFreezeConflictError) as caught:
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertIn("LEARNING_HARD_BLOCK", str(caught.exception))
            live_reader.assert_not_called()
            controller_preflight.assert_called_once()
            freeze_preflight.assert_not_called()
            self.assertEqual(observed_claims, [durable_projection["claims"]])
            self.assertEqual(_operational_table_bytes(root), before)
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CYCLE_ROSTER",
                    aggregate_id=task.task_id,
                ),
                (),
            )

    def test_executing_without_preparation_receipt_is_rejected_without_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-executing-without-preparation"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=(
                    "Execution cannot precede its durable preparation receipt"
                ),
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch.object(
                OperationalCampaignController,
                "_record_cycle_preparation",
                side_effect=RuntimeError("synthetic post-freeze crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "post-freeze crash"):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                    aggregate_id=task.task_id,
                ),
                (),
            )
            controller._lifecycle.advance_cycle(
                cycle_id=task.task_id,
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            before = _operational_table_bytes(root)

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ):
                with self.assertRaises(CampaignJournalError):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertEqual(_operational_table_bytes(root), before)
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.EXECUTING,
            )

    def test_preparation_writer_revalidates_lifecycle_after_freeze(self) -> None:
        cases = ("campaign-blocked", "cycle-executing")
        for label in cases:
            with self.subTest(boundary=label):
                campaign_id = f"campaign-controller-preparation-{label}"
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    (
                        controller,
                        task,
                        execution_spec,
                        roster_members,
                        reservation_limits,
                    ) = _learning_preflight_prepare_inputs(
                        root,
                        journal,
                        hypothesis=(
                            "Preparation receipts require the frozen active boundary"
                        ),
                    )
                    original_write = (
                        campaign_controller_module._SqliteUnitOfWork._write
                    )
                    mutated = False
                    mutation_baseline: list[tuple[bytes, ...]] = []

                    def mutate_before_preparation_writer(unit_of_work, operation):
                        nonlocal mutated
                        if not mutated and operation.__name__ == "record":
                            mutated = True
                            if label == "campaign-blocked":
                                controller._lifecycle.block(
                                    reason_code="synthetic_post_freeze_block",
                                    source_ref="test:preparation-boundary",
                                )
                            else:
                                controller._lifecycle.advance_cycle(
                                    cycle_id=task.task_id,
                                    expected_status=CycleStatus.FROZEN,
                                    next_status=CycleStatus.EXECUTING,
                                )
                            mutation_baseline.append(
                                _operational_table_bytes(root)
                            )
                        return original_write(unit_of_work, operation)

                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        return_value=_projectable_learning_input(),
                    ), patch.object(
                        campaign_controller_module._SqliteUnitOfWork,
                        "_write",
                        new=mutate_before_preparation_writer,
                    ):
                        with self.assertRaises(CampaignJournalError):
                            controller.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )

                    self.assertTrue(mutated)
                    self.assertEqual(len(mutation_baseline), 1)
                    self.assertEqual(
                        _operational_table_bytes(root),
                        mutation_baseline[0],
                    )
                    self.assertEqual(
                        journal.list_events(
                            cycle_id=task.task_id,
                            aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                            aggregate_id=task.task_id,
                        ),
                        (),
                    )

    def test_preparation_writer_replays_freeze_after_freeze_boundary(self) -> None:
        campaign_id = "campaign-controller-preparation-freeze-replay"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Preparation receipts bind the durable frozen inputs",
            )
            original_write = campaign_controller_module._SqliteUnitOfWork._write
            mutated = False
            mutation_baseline: list[tuple[bytes, ...]] = []

            def resign_freeze_before_preparation_writer(unit_of_work, operation):
                nonlocal mutated
                if not mutated and operation.__name__ == "record":
                    mutated = True
                    freeze_event = journal.list_events(
                        cycle_id=task.task_id,
                        aggregate_type="CYCLE_INPUT_FREEZE",
                        aggregate_id=task.task_id,
                    )[0]
                    payload = freeze_event.payload()
                    payload["preflight_sha256"] = "0" * 64
                    freeze_identity = {
                        key: value
                        for key, value in payload.items()
                        if key not in {"_authority_grant_id", "manifest_sha256"}
                    }
                    payload["manifest_sha256"] = (
                        campaign_freeze_module._content_sha256(
                            campaign_freeze_module._CYCLE_FREEZE_MANIFEST_DOMAIN,
                            freeze_identity,
                            "resigned Cycle freeze identity",
                        )
                    )
                    _rewrite_campaign_event_payload(root, freeze_event, payload)
                    mutation_baseline.append(_operational_table_bytes(root))
                return original_write(unit_of_work, operation)

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch.object(
                campaign_controller_module._SqliteUnitOfWork,
                "_write",
                new=resign_freeze_before_preparation_writer,
            ):
                with self.assertRaises(
                    (CampaignJournalError, CycleFreezeIntegrityError)
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertTrue(mutated)
            self.assertEqual(len(mutation_baseline), 1)
            self.assertEqual(_operational_table_bytes(root), mutation_baseline[0])
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                    aggregate_id=task.task_id,
                ),
                (),
            )

    def test_blocked_campaign_cannot_recover_missing_preparation_receipt(
        self,
    ) -> None:
        campaign_id = "campaign-controller-blocked-preparation-recovery"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Blocked Campaigns cannot append recovery receipts",
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch.object(
                OperationalCampaignController,
                "_record_cycle_preparation",
                side_effect=RuntimeError("synthetic post-freeze crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "post-freeze crash"):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            controller._lifecycle.block(
                reason_code="synthetic_post_freeze_block",
                source_ref="test:preparation-recovery",
            )
            before = _operational_table_bytes(root)
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=AssertionError(
                    "frozen recovery must not reopen live Learning"
                ),
            ) as live_reader:
                with self.assertRaises(CampaignJournalError):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            live_reader.assert_not_called()
            self.assertEqual(_operational_table_bytes(root), before)
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                    aggregate_id=task.task_id,
                ),
                (),
            )

    def test_late_lease_cannot_precede_recovered_preparation_receipt(
        self,
    ) -> None:
        campaign_id = "campaign-controller-lease-before-recovered-preparation"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="A lease cannot precede controller preparation",
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ), patch.object(
                OperationalCampaignController,
                "_record_cycle_preparation",
                side_effect=RuntimeError("synthetic post-freeze crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "post-freeze crash"):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            original_write = campaign_controller_module._SqliteUnitOfWork._write
            lease_baseline: list[tuple[bytes, ...]] = []
            lease_inserted = False

            def insert_lease_before_recovery_write(unit_of_work, operation):
                nonlocal lease_inserted
                if not lease_inserted:
                    lease_inserted = True
                    controller._leases.acquire(
                        cycle_id=task.task_id,
                        acquisition_id="late-recovery-lease",
                    )
                    lease_baseline.append(_operational_table_bytes(root))
                return original_write(unit_of_work, operation)

            with patch.object(
                campaign_controller_module._SqliteUnitOfWork,
                "_write",
                new=insert_lease_before_recovery_write,
            ):
                with self.assertRaises(CampaignJournalError):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertTrue(lease_inserted)
            self.assertEqual(len(lease_baseline), 1)
            self.assertEqual(_operational_table_bytes(root), lease_baseline[0])
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                    aggregate_id=task.task_id,
                ),
                (),
            )

    def test_complete_replay_rejects_resigned_lease_before_preparation(
        self,
    ) -> None:
        campaign_id = "campaign-controller-resigned-lease-before-preparation"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Preparation receipts precede every execution lease",
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ):
                prepared = controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )
            controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="resigned-order-lease",
            )
            preparation_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                aggregate_id=task.task_id,
            )[0]
            lease_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CYCLE_LEASE",
                aggregate_id=task.task_id,
            )[0]
            self.assertLess(preparation_event.sequence, lease_event.sequence)
            _swap_and_resign_event_sequences(preparation_event, lease_event)
            before = _operational_table_bytes(root)

            with self.assertRaisesRegex(
                CampaignJournalError,
                "preparation receipt conflicts",
            ):
                controller._preparation_snapshot(
                    cycle_id=task.task_id,
                    frozen=prepared.frozen,
                )

            self.assertEqual(_operational_table_bytes(root), before)

    def test_missing_work_item_rejects_all_cycle_bound_and_budget_prefixes(
        self,
    ) -> None:
        cases = (
            "aggregate-id-only",
            "cycle-budget-only",
            "resource-reservation-only",
        )
        for label in cases:
            with self.subTest(prefix=label):
                campaign_id = f"campaign-controller-prefix-{label}"
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    (
                        controller,
                        task,
                        execution_spec,
                        roster_members,
                        reservation_limits,
                    ) = _learning_preflight_prepare_inputs(
                        root,
                        journal,
                        hypothesis="Admission rejects history without its work item",
                    )
                    if label == "aggregate-id-only":
                        journal.append(
                            event_id=hashlib.sha256(
                                f"{campaign_id}:aggregate-prefix".encode("ascii")
                            ).hexdigest(),
                            cycle_id=None,
                            aggregate_type="FOREIGN_CYCLE_PREFIX",
                            aggregate_id=task.task_id,
                            event_type="FOREIGN_CYCLE_PREFIX_WRITTEN",
                            payload={"cycle_id": task.task_id},
                        )
                    elif label == "cycle-budget-only":
                        controller._cycle_budget.reserve(cycle_id=task.task_id)
                    else:
                        controller._budget.reserve(
                            reservation_id=controller._reservation_id(task.task_id),
                            call_id=task.task_id,
                            currency=reservation_limits.currency,
                            max_input_tokens=reservation_limits.max_input_tokens,
                            max_output_tokens=reservation_limits.max_output_tokens,
                            max_cost=reservation_limits.max_cost,
                            max_wall_time_ms=reservation_limits.max_wall_time_ms,
                            max_tool_attempts=reservation_limits.max_tool_attempts,
                            max_data_exposures=reservation_limits.max_data_exposures,
                            max_disk_growth_bytes=(
                                reservation_limits.max_disk_growth_bytes
                            ),
                        )
                    before = _operational_table_bytes(root)

                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        return_value=_projectable_learning_input(),
                    ):
                        with self.assertRaises(CampaignJournalError):
                            controller.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )

                    self.assertEqual(_operational_table_bytes(root), before)

    def test_work_item_only_partial_runs_live_preflight_and_can_continue(
        self,
    ) -> None:
        campaign_id = "campaign-controller-work-item-only-continues"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="A work-item-only partial remains admissible",
            )
            _, work_item = campaign_controller_module._canonical_task(
                task,
                cycle_number=1,
            )
            work_item["proposal"] = (
                campaign_controller_module.canonical_campaign_proposal(
                    work_item["proposal"]
                )
            )
            journal.append(
                event_id=controller._work_item_event_id(task.task_id),
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_WORK_ITEM",
                aggregate_id=task.task_id,
                event_type="CAMPAIGN_WORK_ITEM_ADOPTED",
                payload=work_item,
            )
            original_preflight = campaign_controller_module.run_campaign_preflight

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ) as reader, patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                wraps=original_preflight,
            ) as preflight:
                prepared = controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )

            reader.assert_called_once_with()
            preflight.assert_called_once()
            self.assertEqual(prepared.cycle_id, task.task_id)
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )

    def test_work_item_only_shell_cannot_bypass_learning_preflight(self) -> None:
        campaign_id = "campaign-controller-learning-preflight-work-shell"
        hypothesis = "An isolated work item cannot confer preflight admission"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=hypothesis,
            )
            _, work_item = campaign_controller_module._canonical_task(
                task,
                cycle_number=1,
            )
            work_item["proposal"] = (
                campaign_controller_module.canonical_campaign_proposal(
                    work_item["proposal"]
                )
            )
            journal.append(
                event_id=controller._work_item_event_id(task.task_id),
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_WORK_ITEM",
                aggregate_id=task.task_id,
                event_type="CAMPAIGN_WORK_ITEM_ADOPTED",
                payload=work_item,
            )
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                controller,
                cycle_id=task.task_id,
            )
            committed = _claim(
                claim_id="committed-work-shell-block",
                hypothesis=hypothesis,
                scope=task.proposal["scope"],
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_preflight_input(committed),
            ) as reader:
                with self.assertRaisesRegex(
                    CycleFreezeError,
                    "LEARNING_HARD_BLOCK",
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            reader.assert_called_once_with()
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                baseline,
            )

    def test_learning_journal_open_failure_is_domain_closed_without_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-preflight-journal-open"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="An unreadable Learning journal must fail closed",
            )
            learning_journal = (
                root
                / "research_state/control_plane/learning_commit.sqlite3"
            )
            learning_journal.mkdir(parents=True)
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                controller,
                cycle_id=task.task_id,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "Learning preflight projection is unavailable",
            ):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )

            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                baseline,
            )

    def test_exact_prepare_cycle_replay_does_not_reopen_learning_preflight(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-preflight-replay"
        hypothesis = "A prepared Cycle remains exactly replayable"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=hypothesis,
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ) as first_reader:
                prepared = controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )
            first_reader.assert_called_once_with()
            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=5_000,
                    max_tool_attempts=4,
                    max_data_exposures=1,
                    max_disk_growth_bytes=10_000,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-learning-preflight", 142, 42_000)
                ),
                monotonic_ns=lambda: 200,
            )
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                reopened,
                cycle_id=task.task_id,
            )

            external_read_error = AssertionError(
                "exact durable replay must use only durable artifacts"
            )
            original_preflight = campaign_controller_module.run_campaign_preflight
            original_freeze_preflight = campaign_freeze_module.run_campaign_preflight
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=external_read_error,
            ) as replay_reader, patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                wraps=original_preflight,
            ) as controller_preflight, patch.object(
                campaign_freeze_module,
                "run_campaign_preflight",
                wraps=original_freeze_preflight,
            ) as freeze_preflight:
                replayed = reopened.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )

            replay_reader.assert_not_called()
            controller_preflight.assert_called_once()
            freeze_preflight.assert_called_once()
            self.assertEqual(replayed, prepared)
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    reopened,
                    cycle_id=task.task_id,
                ),
                baseline,
            )

    def test_exact_prepare_cycle_replay_remains_durable_after_execution_starts(
        self,
    ) -> None:
        campaign_id = "campaign-controller-post-execution-preparation-replay"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="An executing Cycle retains its exact preparation",
            )
            prepare_kwargs = {
                "task": task,
                "cycle_number": 1,
                "execution_spec": execution_spec,
                "roster_members": roster_members,
                "reservation_limits": reservation_limits,
            }
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ) as first_reader:
                prepared = controller.prepare_cycle(**prepare_kwargs)
            first_reader.assert_called_once_with()
            controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="post-execution-preparation-replay",
            )
            baseline = _operational_table_bytes(root)
            external_read_error = AssertionError(
                "post-execution exact replay must not reopen Learning"
            )
            original_preflight = campaign_controller_module.run_campaign_preflight
            original_freeze_preflight = campaign_freeze_module.run_campaign_preflight

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=external_read_error,
            ) as replay_reader, patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                wraps=original_preflight,
            ) as controller_preflight, patch.object(
                campaign_freeze_module,
                "run_campaign_preflight",
                wraps=original_freeze_preflight,
            ) as freeze_preflight:
                replayed = controller.prepare_cycle(**prepare_kwargs)

            replay_reader.assert_not_called()
            controller_preflight.assert_called_once()
            freeze_preflight.assert_called_once()
            self.assertEqual(replayed, prepared)
            self.assertEqual(_operational_table_bytes(root), baseline)

    def test_complete_replay_recomputes_resigned_preflight_semantics(
        self,
    ) -> None:
        campaign_id = "campaign-controller-resigned-preflight-semantics"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Durable preflight semantics remain reproducible",
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )

            freeze_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CYCLE_INPUT_FREEZE",
                aggregate_id=task.task_id,
            )[0]
            freeze_payload = freeze_event.payload()
            forged_preflight = json.loads(freeze_payload["preflight_json"])
            forged_preflight["learning_verdict"]["warning_codes"] = [
                "FORGED_WARNING"
            ]
            freeze_payload["preflight_json"] = json.dumps(
                forged_preflight,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            freeze_payload["preflight_sha256"] = (
                campaign_freeze_module._content_sha256(
                    b"control_plane.campaign_preflight.v1",
                    forged_preflight,
                    "forged preflight",
                )
            )
            freeze_identity = {
                key: value
                for key, value in freeze_payload.items()
                if key not in {"_authority_grant_id", "manifest_sha256"}
            }
            freeze_payload["manifest_sha256"] = (
                campaign_freeze_module._content_sha256(
                    campaign_freeze_module._CYCLE_FREEZE_MANIFEST_DOMAIN,
                    freeze_identity,
                    "forged Cycle freeze identity",
                )
            )
            _rewrite_campaign_event_payload(root, freeze_event, freeze_payload)

            preparation_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                aggregate_id=task.task_id,
            )[0]
            preparation_payload = preparation_event.payload()
            preparation_payload["freeze_manifest_sha256"] = freeze_payload[
                "manifest_sha256"
            ]
            preparation_identity = {
                key: value
                for key, value in preparation_payload.items()
                if key not in {"_authority_grant_id", "manifest_sha256"}
            }
            preparation_payload["manifest_sha256"] = _controller_sha256(
                b"control_plane.campaign_cycle_preparation.v2",
                preparation_identity,
                "forged Cycle preparation identity",
            )
            _rewrite_campaign_event_payload(
                root,
                preparation_event,
                preparation_payload,
            )
            before = _operational_table_bytes(root)
            original_preflight = campaign_controller_module.run_campaign_preflight
            original_freeze_preflight = campaign_freeze_module.run_campaign_preflight

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=AssertionError(
                    "strict replay must use durable context claims"
                ),
            ) as live_reader, patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                wraps=original_preflight,
            ) as replay_preflight, patch.object(
                campaign_freeze_module,
                "run_campaign_preflight",
                wraps=original_freeze_preflight,
            ) as freeze_preflight:
                with self.assertRaisesRegex(
                    CycleFreezeIntegrityError,
                    "preflight",
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            live_reader.assert_not_called()
            replay_preflight.assert_not_called()
            freeze_preflight.assert_called_once()
            self.assertEqual(_operational_table_bytes(root), before)

    def test_start_execution_rejects_resigned_preflight_semantics(self) -> None:
        campaign_id = "campaign-controller-execution-preflight-semantics"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Execution leases require authentic frozen preflight",
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )

            freeze_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CYCLE_INPUT_FREEZE",
                aggregate_id=task.task_id,
            )[0]
            freeze_payload = freeze_event.payload()
            forged_preflight = json.loads(freeze_payload["preflight_json"])
            forged_preflight["learning_verdict"]["warning_codes"] = [
                "FORGED_WARNING"
            ]
            freeze_payload["preflight_json"] = json.dumps(
                forged_preflight,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            freeze_payload["preflight_sha256"] = (
                campaign_freeze_module._content_sha256(
                    b"control_plane.campaign_preflight.v1",
                    forged_preflight,
                    "forged preflight",
                )
            )
            freeze_identity = {
                key: value
                for key, value in freeze_payload.items()
                if key not in {"_authority_grant_id", "manifest_sha256"}
            }
            freeze_payload["manifest_sha256"] = (
                campaign_freeze_module._content_sha256(
                    campaign_freeze_module._CYCLE_FREEZE_MANIFEST_DOMAIN,
                    freeze_identity,
                    "forged Cycle freeze identity",
                )
            )
            _rewrite_campaign_event_payload(root, freeze_event, freeze_payload)

            preparation_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                aggregate_id=task.task_id,
            )[0]
            preparation_payload = preparation_event.payload()
            preparation_payload["freeze_manifest_sha256"] = freeze_payload[
                "manifest_sha256"
            ]
            preparation_identity = {
                key: value
                for key, value in preparation_payload.items()
                if key not in {"_authority_grant_id", "manifest_sha256"}
            }
            preparation_payload["manifest_sha256"] = _controller_sha256(
                b"control_plane.campaign_cycle_preparation.v2",
                preparation_identity,
                "forged Cycle preparation identity",
            )
            _rewrite_campaign_event_payload(
                root,
                preparation_event,
                preparation_payload,
            )
            before = _operational_table_bytes(root)

            with self.assertRaises(
                (CampaignJournalError, CycleFreezeIntegrityError)
            ):
                controller.start_execution(
                    cycle_id=task.task_id,
                    acquisition_id="forged-preflight-execution",
                )

            self.assertEqual(_operational_table_bytes(root), before)
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CYCLE_LEASE",
                    aggregate_id=task.task_id,
                ),
                (),
            )

    def test_complete_replay_rejects_self_consistent_context_proposal_graft(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-proposal-graft"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Exact replay binds the full context proposal",
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=roster_members,
                reservation_limits=reservation_limits,
            )

            work_item_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_WORK_ITEM",
                aggregate_id=task.task_id,
            )[0]
            work_item_payload = work_item_event.payload()
            work_item_payload["proposal"]["self_consistent_graft"] = "grafted"
            _rewrite_campaign_event_payload(
                root,
                work_item_event,
                work_item_payload,
            )

            context_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CYCLE_SAFE_CONTEXT",
                aggregate_id=task.task_id,
            )[0]
            context_payload = context_event.payload()
            context_payload["proposal"]["self_consistent_graft"] = "grafted"
            proposal_text, _ = campaign_context_module._canonical_snapshot(
                context_payload["proposal"],
                "grafted context proposal",
                maximum_bytes=16 * 1024 * 1024,
            )
            context_payload["proposal_sha256"] = (
                campaign_context_module._content_sha256(
                    b"control_plane.cycle_context_proposal.v2",
                    proposal_text,
                )
            )
            context_identity = {
                key: value
                for key, value in context_payload.items()
                if key
                not in {
                    "_authority_grant_id",
                    "manifest_sha256",
                    "safe_context",
                    "projection_input",
                    "proposal",
                    "untrusted_sources",
                }
            }
            context_payload["manifest_sha256"] = (
                campaign_context_module._content_sha256(
                    b"control_plane.cycle_context_receipt.v2",
                    campaign_context_module._canonical_snapshot(
                        context_identity,
                        "grafted context identity",
                        maximum_bytes=48 * 1024,
                    )[0],
                )
            )
            _rewrite_campaign_event_payload(
                root,
                context_event,
                context_payload,
            )

            freeze_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CYCLE_INPUT_FREEZE",
                aggregate_id=task.task_id,
            )[0]
            freeze_payload = freeze_event.payload()
            freeze_payload["proposal_sha256"] = (
                campaign_freeze_module._content_sha256(
                    b"control_plane.campaign_proposal.v1",
                    work_item_payload["proposal"],
                    "grafted frozen proposal",
                )
            )
            freeze_payload["context_manifest_sha256"] = context_payload[
                "manifest_sha256"
            ]
            forged_preflight = campaign_freeze_module.run_campaign_preflight(
                execution_spec=execution_spec,
                proposal=work_item_payload["proposal"],
                committed_claims=context_payload["projection_input"]["claims"],
            )
            freeze_payload["preflight_json"] = json.dumps(
                forged_preflight,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            freeze_payload["preflight_sha256"] = (
                campaign_freeze_module._content_sha256(
                    b"control_plane.campaign_preflight.v1",
                    forged_preflight,
                    "grafted preflight",
                )
            )
            freeze_identity = {
                key: value
                for key, value in freeze_payload.items()
                if key not in {"_authority_grant_id", "manifest_sha256"}
            }
            freeze_payload["manifest_sha256"] = (
                campaign_freeze_module._content_sha256(
                    campaign_freeze_module._CYCLE_FREEZE_MANIFEST_DOMAIN,
                    freeze_identity,
                    "grafted Cycle freeze identity",
                )
            )
            _rewrite_campaign_event_payload(
                root,
                freeze_event,
                freeze_payload,
            )

            preparation_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                aggregate_id=task.task_id,
            )[0]
            preparation_payload = preparation_event.payload()
            preparation_payload["work_item_sha256"] = _controller_sha256(
                b"control_plane.controller_work_item_payload.v1",
                {
                    key: value
                    for key, value in work_item_payload.items()
                    if key != "_authority_grant_id"
                },
                "grafted stored Campaign work item",
            )
            preparation_payload["context_manifest_sha256"] = context_payload[
                "manifest_sha256"
            ]
            preparation_payload["freeze_manifest_sha256"] = freeze_payload[
                "manifest_sha256"
            ]
            preparation_identity = {
                key: value
                for key, value in preparation_payload.items()
                if key not in {"_authority_grant_id", "manifest_sha256"}
            }
            preparation_payload["manifest_sha256"] = _controller_sha256(
                b"control_plane.campaign_cycle_preparation.v2",
                preparation_identity,
                "grafted Cycle preparation identity",
            )
            _rewrite_campaign_event_payload(
                root,
                preparation_event,
                preparation_payload,
            )
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                controller,
                cycle_id=task.task_id,
            )
            external_read_error = AssertionError(
                "durable graft rejection must not reopen external Learning"
            )
            original_freeze_preflight = campaign_freeze_module.run_campaign_preflight

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=external_read_error,
            ) as replay_reader, patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                side_effect=external_read_error,
            ) as controller_preflight, patch.object(
                campaign_freeze_module,
                "run_campaign_preflight",
                wraps=original_freeze_preflight,
            ) as freeze_preflight:
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "Campaign durable preparation conflicts",
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            replay_reader.assert_not_called()
            controller_preflight.assert_not_called()
            freeze_preflight.assert_called_once()
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                baseline,
            )

    def test_complete_replay_rejects_context_only_proposal_graft(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-only-proposal-graft"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Exact replay binds context to the current proposal",
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=roster_members,
                reservation_limits=reservation_limits,
            )

            work_item_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_WORK_ITEM",
                aggregate_id=task.task_id,
            )[0]
            original_work_item_row = next(
                row
                for row in _campaign_event_rows(root, campaign_id)
                if row[0] == work_item_event.event_id
            )
            original_work_item_payload = work_item_event.payload()
            grafted_context_proposal = {
                **task.proposal,
                "context_only_graft": "grafted",
            }
            _graft_and_resign_preparation_context(
                root,
                journal,
                cycle_id=task.task_id,
                context_proposal=grafted_context_proposal,
                freeze_proposal=original_work_item_payload["proposal"],
            )
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                controller,
                cycle_id=task.task_id,
            )
            self.assertEqual(
                next(
                    row
                    for row in baseline["all_events"]
                    if row[0] == work_item_event.event_id
                ),
                original_work_item_row,
            )
            external_read_error = AssertionError(
                "context graft rejection must not reopen external Learning"
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=external_read_error,
            ) as replay_reader, patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                side_effect=external_read_error,
            ) as controller_preflight, patch.object(
                campaign_freeze_module,
                "run_campaign_preflight",
                side_effect=external_read_error,
            ) as freeze_preflight:
                with self.assertRaisesRegex(
                    CycleFreezeIntegrityError,
                    "Cycle freeze proposal binding is invalid",
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            replay_reader.assert_not_called()
            controller_preflight.assert_not_called()
            freeze_preflight.assert_not_called()
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                baseline,
            )

    def test_stale_negative_replay_returns_concurrent_exact_preparation_before_learning(
        self,
    ) -> None:
        campaign_id = "campaign-controller-stale-negative-replay"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="A stale negative replay must converge durably",
            )
            winner = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=5_000,
                    max_tool_attempts=4,
                    max_data_exposures=1,
                    max_disk_growth_bytes=10_000,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-learning-preflight", 142, 42_000)
                ),
                monotonic_ns=lambda: 200,
            )
            loser_negative_seen = Event()
            release_loser = Event()
            loser_thread_id: list[int] = []
            learning_reads = {"winner": 0, "loser": 0}
            original_read = campaign_controller_module._SqliteUnitOfWork._read

            def gated_read(unit_of_work, operation):
                result = original_read(unit_of_work, operation)
                if (
                    operation.__name__ == "replay_complete_preparation"
                    and result is None
                    and not loser_thread_id
                ):
                    loser_thread_id.append(get_ident())
                    loser_negative_seen.set()
                    if not release_loser.wait(timeout=10):
                        raise AssertionError("winner did not release stale replay")
                return result

            def read_projection():
                if loser_thread_id and get_ident() == loser_thread_id[0]:
                    learning_reads["loser"] += 1
                    raise OSError("loser must not reopen committed Learning")
                learning_reads["winner"] += 1
                return _projectable_learning_input()

            prepare_kwargs = {
                "task": task,
                "cycle_number": 1,
                "execution_spec": execution_spec,
                "roster_members": roster_members,
                "reservation_limits": reservation_limits,
            }
            with patch.object(
                campaign_controller_module._SqliteUnitOfWork,
                "_read",
                new=gated_read,
            ), patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=read_projection,
            ), ThreadPoolExecutor(max_workers=2) as executor:
                loser_future = executor.submit(
                    controller.prepare_cycle,
                    **prepare_kwargs,
                )
                self.assertTrue(loser_negative_seen.wait(timeout=10))
                winning = executor.submit(
                    winner.prepare_cycle,
                    **prepare_kwargs,
                ).result(timeout=10)
                durable_after_winner = _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    winner,
                    cycle_id=task.task_id,
                )
                release_loser.set()
                losing = loser_future.result(timeout=10)

            self.assertEqual(losing, winning)
            self.assertEqual(learning_reads, {"winner": 1, "loser": 0})
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                durable_after_winner,
            )

    def test_learning_read_failure_returns_concurrent_exact_preparation(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-failure-convergence"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="A failed Learning read must converge durably",
            )
            winner = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=5_000,
                    max_tool_attempts=4,
                    max_data_exposures=1,
                    max_disk_growth_bytes=10_000,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-learning-preflight", 142, 42_000)
                ),
                monotonic_ns=lambda: 200,
            )
            loser_learning_entered = Event()
            release_learning_failure = Event()
            loser_thread_id: list[int] = []
            learning_reads = {"winner": 0, "loser": 0}

            def read_projection():
                thread_id = get_ident()
                if not loser_thread_id:
                    loser_thread_id.append(thread_id)
                    learning_reads["loser"] += 1
                    loser_learning_entered.set()
                    if not release_learning_failure.wait(timeout=10):
                        raise AssertionError(
                            "winner did not release failing Learning read"
                        )
                    raise OSError("synthetic concurrent Learning failure")
                if thread_id == loser_thread_id[0]:
                    raise AssertionError("loser reopened Learning more than once")
                learning_reads["winner"] += 1
                return _projectable_learning_input()

            prepare_kwargs = {
                "task": task,
                "cycle_number": 1,
                "execution_spec": execution_spec,
                "roster_members": roster_members,
                "reservation_limits": reservation_limits,
            }
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=read_projection,
            ), ThreadPoolExecutor(max_workers=2) as executor:
                loser_future = executor.submit(
                    controller.prepare_cycle,
                    **prepare_kwargs,
                )
                self.assertTrue(loser_learning_entered.wait(timeout=10))
                winning = executor.submit(
                    winner.prepare_cycle,
                    **prepare_kwargs,
                ).result(timeout=10)
                durable_after_winner = _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    winner,
                    cycle_id=task.task_id,
                )
                release_learning_failure.set()
                losing = loser_future.result(timeout=10)

            self.assertEqual(losing, winning)
            self.assertEqual(learning_reads, {"winner": 1, "loser": 1})
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                durable_after_winner,
            )

    def test_accepted_preflight_converges_when_exact_preparation_wins_before_admission(
        self,
    ) -> None:
        campaign_id = "campaign-controller-post-preflight-convergence"
        hypothesis = "Post-preflight admission must converge durably"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                loser,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=hypothesis,
            )
            winner = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=5_000,
                    max_tool_attempts=4,
                    max_data_exposures=1,
                    max_disk_growth_bytes=10_000,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-learning-preflight", 142, 42_000)
                ),
                monotonic_ns=lambda: 200,
            )
            before = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                loser,
                cycle_id=task.task_id,
            )
            loser_admission_write_entered = Event()
            release_loser = Event()
            loser_thread_id: list[int] = []
            learning_reads = {"winner": 0, "loser": 0}
            unrelated_claim = _claim(
                claim_id="accepted-concurrent-snapshot",
                hypothesis="Unrelated committed Learning remains admissible",
                scope=task.proposal["scope"],
            )
            original_write = campaign_controller_module._SqliteUnitOfWork._write

            def read_projection():
                thread_id = get_ident()
                if not loser_thread_id:
                    loser_thread_id.append(thread_id)
                    learning_reads["loser"] += 1
                    return _projectable_learning_input()
                if thread_id == loser_thread_id[0]:
                    raise AssertionError("loser reopened Learning more than once")
                learning_reads["winner"] += 1
                return _projectable_preflight_input(unrelated_claim)

            def gated_write(unit_of_work, operation):
                if (
                    operation.__name__ == "reserve_and_open"
                    and get_ident() == loser_thread_id[0]
                ):
                    loser_admission_write_entered.set()
                    if not release_loser.wait(timeout=10):
                        raise AssertionError(
                            "winner did not release the admission write"
                        )
                return original_write(unit_of_work, operation)

            prepare_kwargs = {
                "task": task,
                "cycle_number": 1,
                "execution_spec": execution_spec,
                "roster_members": roster_members,
                "reservation_limits": reservation_limits,
            }
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=read_projection,
            ), patch.object(
                campaign_controller_module._SqliteUnitOfWork,
                "_write",
                new=gated_write,
            ), ThreadPoolExecutor(max_workers=2) as executor:
                loser_future = executor.submit(
                    loser.prepare_cycle,
                    **prepare_kwargs,
                )
                self.assertTrue(
                    loser_admission_write_entered.wait(timeout=10)
                )
                winning = executor.submit(
                    winner.prepare_cycle,
                    **prepare_kwargs,
                ).result(timeout=10)
                after_winner = _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    winner,
                    cycle_id=task.task_id,
                )
                self.assertNotEqual(after_winner, before)
                release_loser.set()
                losing = loser_future.result(timeout=10)

            self.assertEqual(losing, winning)
            self.assertEqual(learning_reads, {"winner": 1, "loser": 1})
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    loser,
                    cycle_id=task.task_id,
                ),
                after_winner,
            )

    def test_blocked_preflight_converges_when_exact_preparation_wins_during_preflight(
        self,
    ) -> None:
        cases = (
            ("hard", "NEGATIVE", "LEARNING_HARD_BLOCK"),
            ("scoped", "PARTIAL", "LEARNING_SCOPED_BLOCK"),
        )
        for label, kind, rejection_code in cases:
            with self.subTest(case=label):
                campaign_id = (
                    f"campaign-controller-blocked-preflight-race-{label}"
                )
                hypothesis = (
                    "A concurrent exact preparation supersedes a stale block"
                )
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    (
                        loser,
                        task,
                        execution_spec,
                        roster_members,
                        reservation_limits,
                    ) = _learning_preflight_prepare_inputs(
                        root,
                        journal,
                        hypothesis=hypothesis,
                    )
                    winner = OperationalCampaignController(
                        journal=journal,
                        repository_root=root,
                        budget_limits=CampaignBudgetLimits(
                            currency="USD",
                            max_cycles=1,
                            max_input_tokens=100,
                            max_output_tokens=50,
                            max_cost="1",
                            max_wall_time_ms=5_000,
                            max_tool_attempts=4,
                            max_data_exposures=1,
                            max_disk_growth_bytes=10_000,
                        ),
                        identity_provider=_FakeProcessIdentityProvider(
                            ProcessIdentity(
                                "host-learning-preflight",
                                142,
                                42_000,
                            )
                        ),
                        monotonic_ns=lambda: 200,
                    )
                    before = _prepare_cycle_durable_snapshot(
                        root,
                        campaign_id,
                        journal,
                        loser,
                        cycle_id=task.task_id,
                    )
                    loser_preflight_complete = Event()
                    release_loser = Event()
                    loser_thread_id: list[int] = []
                    loser_preflight_calls = 0
                    learning_reads = {"winner": 0, "loser": 0}
                    blocked_claim = _claim(
                        claim_id=f"concurrent-{label}-block",
                        hypothesis=hypothesis,
                        scope=task.proposal["scope"],
                        kind=kind,
                    )
                    original_preflight = (
                        campaign_controller_module.run_campaign_preflight
                    )

                    def read_projection():
                        thread_id = get_ident()
                        if not loser_thread_id:
                            loser_thread_id.append(thread_id)
                            learning_reads["loser"] += 1
                            return _projectable_preflight_input(blocked_claim)
                        if thread_id == loser_thread_id[0]:
                            raise AssertionError(
                                "loser reopened Learning more than once"
                            )
                        learning_reads["winner"] += 1
                        return _projectable_learning_input()

                    def gated_preflight(**kwargs):
                        nonlocal loser_preflight_calls
                        preflight = original_preflight(**kwargs)
                        if get_ident() == loser_thread_id[0]:
                            loser_preflight_calls += 1
                            if loser_preflight_calls == 1:
                                self.assertEqual(
                                    preflight["verdict"],
                                    "WOULD_REJECT",
                                )
                                self.assertIn(
                                    rejection_code,
                                    preflight["rejection_codes"],
                                )
                                loser_preflight_complete.set()
                                if not release_loser.wait(timeout=10):
                                    raise AssertionError(
                                        "winner did not release blocked preflight"
                                    )
                        return preflight

                    prepare_kwargs = {
                        "task": task,
                        "cycle_number": 1,
                        "execution_spec": execution_spec,
                        "roster_members": roster_members,
                        "reservation_limits": reservation_limits,
                    }
                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        side_effect=read_projection,
                    ), patch.object(
                        campaign_controller_module,
                        "run_campaign_preflight",
                        side_effect=gated_preflight,
                    ), ThreadPoolExecutor(max_workers=2) as executor:
                        loser_future = executor.submit(
                            loser.prepare_cycle,
                            **prepare_kwargs,
                        )
                        self.assertTrue(
                            loser_preflight_complete.wait(timeout=10)
                        )
                        winning = executor.submit(
                            winner.prepare_cycle,
                            **prepare_kwargs,
                        ).result(timeout=10)
                        after_winner = _prepare_cycle_durable_snapshot(
                            root,
                            campaign_id,
                            journal,
                            winner,
                            cycle_id=task.task_id,
                        )
                        self.assertNotEqual(after_winner, before)
                        release_loser.set()
                        losing = loser_future.result(timeout=10)

                    self.assertEqual(losing, winning)
                    self.assertEqual(
                        learning_reads,
                        {"winner": 1, "loser": 1},
                    )
                    self.assertEqual(loser_preflight_calls, 2)
                    self.assertEqual(
                        _prepare_cycle_durable_snapshot(
                            root,
                            campaign_id,
                            journal,
                            loser,
                            cycle_id=task.task_id,
                        ),
                        after_winner,
                    )

    def test_router_conflict_converges_when_exact_preparation_wins_during_assembly(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-router-race"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                loser,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Exact completion supersedes an assembly conflict",
            )
            winner = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=5_000,
                    max_tool_attempts=4,
                    max_data_exposures=1,
                    max_disk_growth_bytes=10_000,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity(
                        "host-learning-preflight",
                        144,
                        44_000,
                    )
                ),
                monotonic_ns=lambda: 400,
            )
            losing_router_entered = Event()
            release_losing_router = Event()
            original_build_messages = (
                loser._context._router.build_messages
            )
            first_losing_call = True

            def losing_router(*args, **kwargs):
                nonlocal first_losing_call
                if not first_losing_call:
                    return original_build_messages(*args, **kwargs)
                first_losing_call = False
                losing_router_entered.set()
                if not release_losing_router.wait(timeout=10):
                    raise AssertionError(
                        "winner did not release the losing router"
                    )
                raise CycleContextConflictError(
                    "synthetic losing router conflict"
                )

            prepare_kwargs = {
                "task": task,
                "cycle_number": 1,
                "execution_spec": execution_spec,
                "roster_members": roster_members,
                "reservation_limits": reservation_limits,
            }
            with patch.object(
                loser._context._router,
                "build_messages",
                side_effect=losing_router,
            ), ThreadPoolExecutor(max_workers=2) as executor:
                loser_future = executor.submit(
                    loser.prepare_cycle,
                    **prepare_kwargs,
                )
                self.assertTrue(losing_router_entered.wait(timeout=10))
                winning = executor.submit(
                    winner.prepare_cycle,
                    **prepare_kwargs,
                ).result(timeout=10)
                after_winner = _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    winner,
                    cycle_id=task.task_id,
                )
                release_losing_router.set()
                losing = loser_future.result(timeout=10)

            self.assertEqual(losing, winning)
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    loser,
                    cycle_id=task.task_id,
                ),
                after_winner,
            )

    def test_validation_conflict_preserves_original_when_replay_is_invalid(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-validation-invalid-winner"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                loser,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Validation conflict keeps its original identity",
            )
            winner = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=5_000,
                    max_tool_attempts=4,
                    max_data_exposures=1,
                    max_disk_growth_bytes=10_000,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity(
                        "host-learning-preflight",
                        147,
                        47_000,
                    )
                ),
                monotonic_ns=lambda: 700,
            )
            validation_router_entered = Event()
            release_validation_router = Event()
            original_build_messages = loser._context._router.build_messages
            original_conflict = CycleContextConflictError(
                "synthetic original validation router conflict"
            )
            router_calls = 0

            def validation_router(*args, **kwargs):
                nonlocal router_calls
                router_calls += 1
                if router_calls == 2:
                    validation_router_entered.set()
                    if not release_validation_router.wait(timeout=10):
                        raise AssertionError(
                            "winner did not release validation router"
                        )
                    raise original_conflict
                return original_build_messages(*args, **kwargs)

            prepare_kwargs = {
                "task": task,
                "cycle_number": 1,
                "execution_spec": execution_spec,
                "roster_members": roster_members,
                "reservation_limits": reservation_limits,
            }
            with patch.object(
                loser._context._router,
                "build_messages",
                side_effect=validation_router,
            ), ThreadPoolExecutor(max_workers=2) as executor:
                loser_future = executor.submit(
                    loser.prepare_cycle,
                    **prepare_kwargs,
                )
                self.assertTrue(validation_router_entered.wait(timeout=10))
                executor.submit(
                    winner.prepare_cycle,
                    **prepare_kwargs,
                ).result(timeout=10)
                freeze_event = journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CYCLE_INPUT_FREEZE",
                    aggregate_id=task.task_id,
                )[0]
                freeze_payload = json.loads(freeze_event.payload_json)
                freeze_payload["manifest_sha256"] = "0" * 64
                _rewrite_campaign_event_payload(
                    root,
                    freeze_event,
                    freeze_payload,
                )
                release_validation_router.set()
                with self.assertRaises(CycleContextConflictError) as raised:
                    loser_future.result(timeout=10)

            self.assertEqual(router_calls, 2)
            self.assertIs(raised.exception, original_conflict)

    def test_router_conflict_re_raises_original_when_partial_winner_is_invalid(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-router-invalid-winner"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                loser,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Invalid partial completion cannot replace conflict",
            )
            winner = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=5_000,
                    max_tool_attempts=4,
                    max_data_exposures=1,
                    max_disk_growth_bytes=10_000,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity(
                        "host-learning-preflight",
                        145,
                        45_000,
                    )
                ),
                monotonic_ns=lambda: 500,
            )
            losing_router_entered = Event()
            release_losing_router = Event()
            original_build_messages = loser._context._router.build_messages
            original_conflict = CycleContextConflictError(
                "synthetic original losing router conflict"
            )
            first_losing_call = True

            def losing_router(*args, **kwargs):
                nonlocal first_losing_call
                if not first_losing_call:
                    return original_build_messages(*args, **kwargs)
                first_losing_call = False
                losing_router_entered.set()
                if not release_losing_router.wait(timeout=10):
                    raise AssertionError(
                        "winner did not release the losing router"
                    )
                raise original_conflict

            prepare_kwargs = {
                "task": task,
                "cycle_number": 1,
                "execution_spec": execution_spec,
                "roster_members": roster_members,
                "reservation_limits": reservation_limits,
            }
            with patch.object(
                loser._context._router,
                "build_messages",
                side_effect=losing_router,
            ), ThreadPoolExecutor(max_workers=2) as executor:
                loser_future = executor.submit(
                    loser.prepare_cycle,
                    **prepare_kwargs,
                )
                self.assertTrue(losing_router_entered.wait(timeout=10))
                executor.submit(
                    winner.prepare_cycle,
                    **prepare_kwargs,
                ).result(timeout=10)
                freeze_event = journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CYCLE_INPUT_FREEZE",
                    aggregate_id=task.task_id,
                )[0]
                freeze_payload = json.loads(freeze_event.payload_json)
                freeze_payload["manifest_sha256"] = "0" * 64
                _rewrite_campaign_event_payload(
                    root,
                    freeze_event,
                    freeze_payload,
                )
                release_losing_router.set()
                with self.assertRaises(CycleContextConflictError) as raised:
                    loser_future.result(timeout=10)

            self.assertIs(raised.exception, original_conflict)

    def test_post_reservation_conflict_preserves_original_when_replay_is_invalid(
        self,
    ) -> None:
        campaign_id = "campaign-controller-post-reservation-invalid-winner"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                loser,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Post-reservation recovery preserves conflict",
            )
            winner = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=5_000,
                    max_tool_attempts=4,
                    max_data_exposures=1,
                    max_disk_growth_bytes=10_000,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity(
                        "host-learning-preflight",
                        146,
                        46_000,
                    )
                ),
                monotonic_ns=lambda: 600,
            )
            losing_prepare_entered = Event()
            release_losing_prepare = Event()
            original_conflict = CycleContextConflictError(
                "synthetic original post-reservation context conflict"
            )
            original_prepare = (
                OperationalCycleContextJournal._prepare_assembled
            )

            def gated_prepare(context_journal, assembled, **kwargs):
                if context_journal is loser._context:
                    losing_prepare_entered.set()
                    if not release_losing_prepare.wait(timeout=10):
                        raise AssertionError(
                            "winner did not release losing context preparation"
                        )
                    raise original_conflict
                return original_prepare(context_journal, assembled, **kwargs)

            prepare_kwargs = {
                "task": task,
                "cycle_number": 1,
                "execution_spec": execution_spec,
                "roster_members": roster_members,
                "reservation_limits": reservation_limits,
            }
            with patch.object(
                OperationalCycleContextJournal,
                "_prepare_assembled",
                new=gated_prepare,
            ), ThreadPoolExecutor(max_workers=2) as executor:
                loser_future = executor.submit(
                    loser.prepare_cycle,
                    **prepare_kwargs,
                )
                self.assertTrue(losing_prepare_entered.wait(timeout=10))
                executor.submit(
                    winner.prepare_cycle,
                    **prepare_kwargs,
                ).result(timeout=10)
                freeze_event = journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CYCLE_INPUT_FREEZE",
                    aggregate_id=task.task_id,
                )[0]
                freeze_payload = json.loads(freeze_event.payload_json)
                freeze_payload["manifest_sha256"] = "0" * 64
                _rewrite_campaign_event_payload(
                    root,
                    freeze_event,
                    freeze_payload,
                )
                release_losing_prepare.set()
                with self.assertRaises(CycleContextConflictError) as raised:
                    loser_future.result(timeout=10)

            self.assertIs(raised.exception, original_conflict)

    def test_budget_reserved_recovery_rechecks_learning_before_any_write(
        self,
    ) -> None:
        cases = (
            ("hard", "NEGATIVE", "LEARNING_HARD_BLOCK"),
            ("scoped", "PARTIAL", "LEARNING_SCOPED_BLOCK"),
        )
        for label, kind, rejection_code in cases:
            with self.subTest(case=label):
                campaign_id = (
                    f"campaign-controller-learning-recovery-{label}"
                )
                hypothesis = (
                    "Incomplete admission must recheck current Learning"
                )
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    (
                        controller,
                        task,
                        execution_spec,
                        roster_members,
                        reservation_limits,
                    ) = _learning_preflight_prepare_inputs(
                        root,
                        journal,
                        hypothesis=hypothesis,
                    )
                    with patch.object(
                        OperationalCycleContextJournal,
                        "_prepare_assembled",
                        side_effect=RuntimeError("synthetic crash boundary"),
                    ), patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        return_value=_projectable_learning_input(),
                    ):
                        with self.assertRaisesRegex(
                            RuntimeError,
                            "synthetic crash boundary",
                        ):
                            controller.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )

                    self.assertEqual(
                        controller.cycle_snapshot(task.task_id).status,
                        CycleStatus.BUDGET_RESERVED,
                    )
                    reopened = OperationalCampaignController(
                        journal=journal,
                        repository_root=root,
                        budget_limits=CampaignBudgetLimits(
                            currency="USD",
                            max_cycles=1,
                            max_input_tokens=100,
                            max_output_tokens=50,
                            max_cost="1",
                            max_wall_time_ms=5_000,
                            max_tool_attempts=4,
                            max_data_exposures=1,
                            max_disk_growth_bytes=10_000,
                        ),
                        identity_provider=_FakeProcessIdentityProvider(
                            ProcessIdentity(
                                "host-learning-preflight",
                                143,
                                43_000,
                            )
                        ),
                        monotonic_ns=lambda: 300,
                    )
                    baseline = _prepare_cycle_durable_snapshot(
                        root,
                        campaign_id,
                        journal,
                        reopened,
                        cycle_id=task.task_id,
                    )
                    committed = _claim(
                        claim_id=f"committed-recovery-{label}",
                        hypothesis=hypothesis,
                        scope=task.proposal["scope"],
                        kind=kind,
                    )

                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        return_value=_projectable_preflight_input(committed),
                    ) as reader:
                        with self.assertRaisesRegex(
                            CycleFreezeError,
                            rejection_code,
                        ):
                            reopened.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )

                    reader.assert_called_once_with()
                    self.assertEqual(
                        _prepare_cycle_durable_snapshot(
                            root,
                            campaign_id,
                            journal,
                            reopened,
                            cycle_id=task.task_id,
                        ),
                        baseline,
                    )

    def test_invalid_assembled_context_fails_before_admission_without_writes(
        self,
    ) -> None:
        class ForgedAssembledContext:
            def __init__(self, genuine) -> None:
                self.preview = genuine.preview
                self.projection_input_json = genuine.projection_input_json

        def drift(genuine, field_name: str):
            return replace(
                genuine,
                preview=replace(
                    genuine.preview,
                    **{field_name: "0" * 64},
                ),
            )

        cases = (
            (
                "forged-type",
                lambda genuine: ForgedAssembledContext(genuine),
                CampaignJournalError,
            ),
            (
                "request-hash-drift",
                lambda genuine: drift(genuine, "request_sha256"),
                CycleContextIntegrityError,
            ),
            (
                "context-hash-drift",
                lambda genuine: drift(genuine, "context_sha256"),
                CycleContextIntegrityError,
            ),
            (
                "manifest-hash-drift",
                lambda genuine: drift(genuine, "manifest_sha256"),
                CycleContextIntegrityError,
            ),
            (
                "projection-input-noncanonical-bytes",
                lambda genuine: replace(
                    genuine,
                    projection_input_json=(
                        " " + genuine.projection_input_json
                    ),
                ),
                CycleContextIntegrityError,
            ),
            (
                "proposal-noncanonical-bytes",
                lambda genuine: replace(
                    genuine,
                    proposal_json=" " + genuine.proposal_json,
                ),
                CycleContextIntegrityError,
            ),
            (
                "untrusted-sources-noncanonical-bytes",
                lambda genuine: replace(
                    genuine,
                    untrusted_sources_json=(
                        " " + genuine.untrusted_sources_json
                    ),
                ),
                CycleContextIntegrityError,
            ),
        )
        for label, mutate, expected_error in cases:
            with self.subTest(case=label):
                campaign_id = f"campaign-controller-context-{label}"
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    (
                        controller,
                        task,
                        execution_spec,
                        roster_members,
                        reservation_limits,
                    ) = _learning_preflight_prepare_inputs(
                        root,
                        journal,
                        hypothesis="Invalid assembly must precede admission",
                    )
                    genuine = controller._context._assemble(
                        cycle_id=task.task_id,
                        proposal=task.proposal,
                        roles=tuple(
                            member.role for member in roster_members
                        ),
                    )
                    candidate = mutate(genuine)
                    baseline = _prepare_cycle_durable_snapshot(
                        root,
                        campaign_id,
                        journal,
                        controller,
                        cycle_id=task.task_id,
                    )

                    with patch.object(
                        OperationalCycleContextJournal,
                        "_assemble",
                        return_value=candidate,
                    ):
                        with self.assertRaises(expected_error):
                            controller.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )

                    self.assertEqual(
                        _prepare_cycle_durable_snapshot(
                            root,
                            campaign_id,
                            journal,
                            controller,
                            cycle_id=task.task_id,
                        ),
                        baseline,
                    )

    def test_self_consistent_assembled_request_drift_fails_before_admission(
        self,
    ) -> None:
        cases = (
            "cycle",
            "roles",
            "learning-budget",
            "control-budget",
            "target-scope",
            "untrusted-sources",
        )
        for label in cases:
            with self.subTest(case=label):
                campaign_id = f"campaign-controller-context-binding-{label}"
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    (
                        controller,
                        task,
                        execution_spec,
                        roster_members,
                        reservation_limits,
                    ) = _learning_preflight_prepare_inputs(
                        root,
                        journal,
                        hypothesis=(
                            "Assembly must bind the current prepare request"
                        ),
                    )
                    assembly_inputs = {
                        "cycle_id": task.task_id,
                        "proposal": task.proposal,
                        "roles": tuple(
                            member.role for member in roster_members
                        ),
                        "learning_token_budget": 1500,
                        "control_token_budget": 500,
                        "untrusted_sources": None,
                    }
                    if label == "cycle":
                        assembly_inputs["cycle_id"] = "cycle-999"
                    elif label == "roles":
                        assembly_inputs["roles"] = ("source_librarian",)
                    elif label == "learning-budget":
                        assembly_inputs["learning_token_budget"] = 1499
                    elif label == "control-budget":
                        assembly_inputs["control_token_budget"] = 499
                    elif label == "target-scope":
                        assembly_inputs["proposal"] = {
                            **task.proposal,
                            "scope": _scope(generation="generation-2"),
                        }
                    else:
                        assembly_inputs["untrusted_sources"] = (
                            {
                                "source_ref": "self-consistent-drift",
                                "content": "Quoted drifted source material",
                            },
                        )
                    candidate = controller._context._assemble(
                        **assembly_inputs,
                    )
                    baseline = _prepare_cycle_durable_snapshot(
                        root,
                        campaign_id,
                        journal,
                        controller,
                        cycle_id=task.task_id,
                    )

                    with patch.object(
                        OperationalCycleContextJournal,
                        "_assemble",
                        return_value=candidate,
                    ):
                        with self.assertRaises(CycleContextIntegrityError):
                            controller.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )

                    self.assertEqual(
                        _prepare_cycle_durable_snapshot(
                            root,
                            campaign_id,
                            journal,
                            controller,
                            cycle_id=task.task_id,
                        ),
                        baseline,
                    )

    def test_learning_projection_failure_is_domain_closed_without_writes(
        self,
    ) -> None:
        failures = (
            ValueError("malformed committed Learning projection"),
            OverflowError("committed Learning scope overflow"),
        )
        for index, failure in enumerate(failures, start=1):
            with self.subTest(error=type(failure).__name__):
                campaign_id = (
                    f"campaign-controller-learning-preflight-invalid-{index}"
                )
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    (
                        controller,
                        task,
                        execution_spec,
                        roster_members,
                        reservation_limits,
                    ) = _learning_preflight_prepare_inputs(
                        root,
                        journal,
                        hypothesis="Malformed Learning must fail closed",
                    )
                    baseline = _prepare_cycle_durable_snapshot(
                        root,
                        campaign_id,
                        journal,
                        controller,
                        cycle_id=task.task_id,
                    )

                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        side_effect=failure,
                    ) as reader:
                        with self.assertRaisesRegex(
                            CampaignJournalError,
                            "Learning preflight projection is unavailable",
                        ):
                            controller.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )

                    reader.assert_called_once_with()
                    self.assertEqual(
                        _prepare_cycle_durable_snapshot(
                            root,
                            campaign_id,
                            journal,
                            controller,
                            cycle_id=task.task_id,
                        ),
                        baseline,
                    )
                    self.assertEqual(baseline["usage"], ())

    def test_learning_failure_preserves_original_cause_when_fallback_replay_is_invalid(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-fallback-replay-invalid"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Observational replay cannot replace Learning failure",
            )
            before = _operational_table_bytes(root)
            original_error = OSError("original committed Learning read failure")
            fallback_error = CampaignJournalError(
                "invalid observational durable replay"
            )
            original_uow_read = (
                campaign_controller_module._SqliteUnitOfWork._read
            )
            strict_replay_reads = 0

            def fail_invalid_fallback_replay(unit_of_work, operation):
                nonlocal strict_replay_reads
                if operation.__name__ == "replay_complete_preparation":
                    strict_replay_reads += 1
                    if strict_replay_reads == 3:
                        raise fallback_error
                return original_uow_read(unit_of_work, operation)

            with patch.object(
                campaign_controller_module._SqliteUnitOfWork,
                "_read",
                new=fail_invalid_fallback_replay,
            ), patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=original_error,
            ) as reader:
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "^Committed Learning preflight projection is unavailable$",
                ) as caught:
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            reader.assert_called_once_with()
            self.assertEqual(strict_replay_reads, 3)
            self.assertIs(caught.exception.__cause__, original_error)
            self.assertEqual(_operational_table_bytes(root), before)

    def test_rejected_preflight_survives_an_invalid_observational_replay(
        self,
    ) -> None:
        campaign_id = "campaign-controller-rejected-observational-replay"
        hypothesis = "A rejected preflight remains the public failure"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis=hypothesis,
            )
            blocked_claim = _claim(
                claim_id="rejected-observational-replay",
                hypothesis=hypothesis,
                scope=task.proposal["scope"],
            )
            original_preflight = campaign_controller_module.run_campaign_preflight
            inserted_baseline: list[tuple[bytes, ...]] = []

            def reject_then_insert_invalid_partial(**kwargs):
                result = original_preflight(**kwargs)
                self.assertEqual(result["verdict"], "WOULD_REJECT")
                self.assertIn(
                    "LEARNING_HARD_BLOCK",
                    result["rejection_codes"],
                )
                journal.append(
                    event_id=controller._preparation_event_id(task.task_id),
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                    aggregate_id=task.task_id,
                    event_type="CAMPAIGN_CYCLE_PREPARED",
                    payload={"invalid_partial_history": True},
                )
                inserted_baseline.append(_operational_table_bytes(root))
                return result

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_preflight_input(blocked_claim),
            ), patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                side_effect=reject_then_insert_invalid_partial,
            ):
                with self.assertRaises(CycleFreezeConflictError) as caught:
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertIn("LEARNING_HARD_BLOCK", str(caught.exception))
            self.assertEqual(len(inserted_baseline), 1)
            self.assertEqual(_operational_table_bytes(root), inserted_baseline[0])

    def test_prior_cycle_continuation_rejects_before_learning_preflight(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-preflight-order"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Continuation admission precedes Learning preflight",
            )
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                controller,
                cycle_id=task.task_id,
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=AssertionError(
                    "invalid continuation must not read Learning projection"
                ),
            ) as reader:
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "previous Cycle did not authorize continuation",
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=2,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            reader.assert_not_called()
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                baseline,
            )

    def test_context_overflow_precedes_reservation_and_writes_nothing(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-overflow-admission"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Context overflow must precede durable admission",
            )
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                controller,
                cycle_id=task.task_id,
            )
            self.assertIsNone(baseline["cycle"])
            self.assertEqual(baseline["cycle_budget"].reserved_cycle_ids, ())
            self.assertEqual(baseline["budget"].reserved_input_tokens, 0)
            overflow_projection = {
                "schema_version": "control_plane.committed_learning_input.v1",
                "claims": [],
                "excluded_claims": [
                    {
                        "claim_id": f"excluded-overflow-{index:04d}",
                        "reason_codes": ["P5_PACKET_NOT_PROJECTABLE"],
                    }
                    for index in range(64)
                ],
            }

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=overflow_projection,
            ):
                with self.assertRaisesRegex(
                    CycleContextConflictError,
                    "safe context is not ready",
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                baseline,
            )

    def test_large_valid_projection_payload_fails_before_admission_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-context-event-payload-overflow"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Large valid projection fails before admission",
            )
            projection = _projectable_learning_input(
                *(
                    hashlib.sha256(
                        f"large-context-claim-{index}".encode("ascii")
                    ).hexdigest()
                    for index in range(256)
                )
            )
            for claim in projection["claims"]:
                claim["scope"] = _scope(generation="foreign-generation")
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                controller,
                cycle_id=task.task_id,
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=projection,
            ) as reader:
                with self.assertRaisesRegex(
                    CycleContextConflictError,
                    "durable event payload exceeds the bounded size",
                ):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )

            reader.assert_called_once_with()
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                baseline,
            )

    def test_oversized_freeze_payload_fails_before_admission_writes(self) -> None:
        campaign_id = "campaign-controller-freeze-event-payload-overflow"
        base_protocol = _protocol()
        protocol = base_protocol.model_copy(
            update={
                "metadata": base_protocol.metadata.model_copy(
                    update={"notes": "x" * 24_300}
                )
            }
        )
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                _,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Oversized freeze bytes must not consume admission",
            )
            baseline = _prepare_cycle_durable_snapshot(
                root,
                campaign_id,
                journal,
                controller,
                cycle_id=task.task_id,
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ) as reader, self.assertRaises(Exception) as caught:
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )

            reader.assert_called_once_with()
            self.assertEqual(
                _prepare_cycle_durable_snapshot(
                    root,
                    campaign_id,
                    journal,
                    controller,
                    cycle_id=task.task_id,
                ),
                baseline,
            )
            self.assertIsInstance(caught.exception, CycleFreezeConflictError)

    def test_new_cycle_admission_reads_learning_projection_once(self) -> None:
        campaign_id = "campaign-controller-one-context-projection-read"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="One projection snapshot drives full admission",
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ) as projection_reader:
                prepared = controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )

            self.assertEqual(prepared.cycle_id, task.task_id)
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(projection_reader.call_count, 1)

    def test_controller_prepares_one_budgeted_context_bound_cycle(self) -> None:
        campaign_id = "campaign-controller-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "A bounded offline controller mechanism",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
            priority=10,
        )
        owner = ProcessIdentity(
            host_id="host-controller",
            pid=101,
            process_started_at_ns=1_000,
        )
        budget = CampaignBudgetLimits(
            currency="USD",
            max_cycles=2,
            max_input_tokens=10_000,
            max_output_tokens=5_000,
            max_cost="10.00",
            max_wall_time_ms=60_000,
            max_tool_attempts=20,
            max_data_exposures=4,
            max_disk_growth_bytes=1_000_000,
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=1_000,
            max_output_tokens=500,
            max_cost="1.00",
            max_wall_time_ms=5_000,
            max_tool_attempts=4,
            max_data_exposures=1,
            max_disk_growth_bytes=10_000,
        )

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 100,
            )

            prepared = controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(_protocol_member(),),
                reservation_limits=reservation,
            )
            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 100,
            )
            replay = reopened.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(_protocol_member(),),
                reservation_limits=reservation,
            )

            self.assertEqual(replay, prepared)
            self.assertEqual(prepared.cycle_id, task.task_id)
            self.assertEqual(
                reopened.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(
                prepared.context_manifest_sha256,
                prepared.frozen.context_manifest_sha256,
            )
            self.assertEqual(
                prepared.roster_manifest_sha256,
                prepared.frozen.roster_manifest_sha256,
            )
            self.assertEqual(
                controller.cycle_budget_snapshot().reserved_cycle_ids,
                (task.task_id,),
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                reservation.max_input_tokens,
            )
            work_items = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_WORK_ITEM",
                aggregate_id=task.task_id,
            )
            self.assertEqual(len(work_items), 1)

    def test_controller_starts_execution_only_through_a_fenced_lease(self) -> None:
        campaign_id = "campaign-controller-002"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Execution starts only after a durable freeze",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 102, 2_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=1_000,
                    max_output_tokens=500,
                    max_cost="1",
                    max_wall_time_ms=5_000,
                    max_tool_attempts=4,
                    max_data_exposures=1,
                    max_disk_growth_bytes=10_000,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 200,
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(_protocol_member(),),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="0.1",
                    max_wall_time_ms=500,
                    max_tool_attempts=1,
                    max_data_exposures=0,
                    max_disk_growth_bytes=1_000,
                ),
            )

            executing = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="acquire-cycle-001",
            )
            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=1_000,
                    max_output_tokens=500,
                    max_cost="1",
                    max_wall_time_ms=5_000,
                    max_tool_attempts=4,
                    max_data_exposures=1,
                    max_disk_growth_bytes=10_000,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 200,
            )
            replay = reopened.start_execution(
                cycle_id=task.task_id,
                acquisition_id="acquire-cycle-001",
            )

            self.assertEqual(replay, executing)
            self.assertEqual(executing.cycle.status, CycleStatus.EXECUTING)
            self.assertEqual(executing.lease.fencing_token, 1)
            self.assertEqual(executing.lease.owner, owner)

    def test_start_execution_rejects_shadow_freeze_before_any_lease_write(
        self,
    ) -> None:
        campaign_id = "campaign-controller-shadow-freeze-before-execution"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Execution must linearize its durable freeze",
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )
            freeze_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CYCLE_INPUT_FREEZE",
                aggregate_id=task.task_id,
            )[0]
            original_write = campaign_controller_module._SqliteUnitOfWork._write
            shadow_written = False
            after_shadow: list[tuple[bytes, ...]] = []

            def inject_shadow_before_start(unit_of_work, operation):
                nonlocal shadow_written
                if not shadow_written:
                    shadow_written = True
                    journal.append(
                        event_id="shadow-freeze-before-execution",
                        cycle_id=task.task_id,
                        aggregate_type="CYCLE_INPUT_FREEZE",
                        aggregate_id=task.task_id,
                        event_type="CYCLE_INPUTS_FROZEN",
                        payload=_event_domain_payload(freeze_event),
                    )
                    after_shadow.append(_operational_table_bytes(root))
                return original_write(unit_of_work, operation)

            error: Exception | None = None
            with patch.object(
                campaign_controller_module._SqliteUnitOfWork,
                "_write",
                new=inject_shadow_before_start,
            ):
                try:
                    controller.start_execution(
                        cycle_id=task.task_id,
                        acquisition_id="shadow-freeze-execution",
                    )
                except Exception as caught:
                    error = caught

            self.assertEqual(len(after_shadow), 1, repr(error))
            self.assertEqual(_operational_table_bytes(root), after_shadow[0])
            self.assertIsInstance(error, CycleFreezeIntegrityError)

    def test_start_execution_durably_blocks_a_poisoned_lease_prefix(self) -> None:
        campaign_id = "campaign-controller-poisoned-lease-before-execution"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="A poisoned lease prefix must block execution",
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=roster_members,
                    reservation_limits=reservation_limits,
                )
            poisoned_event_id = "poisoned-controller-start-lease-prefix"
            journal.append(
                event_id=poisoned_event_id,
                cycle_id=task.task_id,
                aggregate_type="CYCLE_LEASE",
                aggregate_id=task.task_id,
                event_type="UNKNOWN_CYCLE_LEASE_EVENT",
                payload={"cycle_id": task.task_id},
            )

            with self.assertRaises(CycleLeaseIntegrityError):
                controller.start_execution(
                    cycle_id=task.task_id,
                    acquisition_id="poisoned-lease-execution",
                )

            campaign = controller.campaign_snapshot()
            self.assertEqual(campaign.status, CampaignStatus.BLOCKED)
            self.assertEqual(campaign.block_source_ref, poisoned_event_id)
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(
                tuple(
                    event.event_id
                    for event in journal.list_events(
                        cycle_id=task.task_id,
                        aggregate_type="CYCLE_LEASE",
                        aggregate_id=task.task_id,
                    )
                ),
                (poisoned_event_id,),
            )

    def test_resource_budget_failure_rolls_back_cycle_slot_and_open(self) -> None:
        campaign_id = "campaign-controller-003"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Budget failure cannot consume a Cycle slot",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 103, 3_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=10,
                    max_output_tokens=10,
                    max_cost="1",
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 300,
            )

            with self.assertRaises(BudgetExceededError):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=CycleReservationLimits(
                        currency="USD",
                        max_input_tokens=11,
                        max_output_tokens=1,
                        max_cost="0.1",
                    ),
                )

            self.assertEqual(
                controller.campaign_snapshot().status,
                CampaignStatus.CREATED,
            )
            self.assertEqual(
                controller.cycle_budget_snapshot().reserved_cycle_ids,
                (),
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                0,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_WORK_ITEM",
                    aggregate_id=task.task_id,
                ),
                (),
            )
            with self.assertRaises(CampaignLifecycleError):
                controller.cycle_snapshot(task.task_id)

    def test_reused_task_identity_cannot_change_the_work_item(self) -> None:
        campaign_id = "campaign-controller-004"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "The original bounded work item",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 104, 4_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=5,
            max_cost="0.1",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 400,
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(_protocol_member(),),
                reservation_limits=reservation,
            )
            changed = ExperimentTask(
                task_id=task.task_id,
                strategy=task.strategy,
                proposal={
                    **task.proposal,
                    "hypothesis": "A conflicting replacement work item",
                },
                source=task.source,
                priority=task.priority,
            )

            with self.assertRaises(CampaignJournalError):
                controller.prepare_cycle(
                    task=changed,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=reservation,
                )

            self.assertEqual(
                len(
                    journal.list_events(
                        cycle_id=task.task_id,
                        aggregate_type="CAMPAIGN_WORK_ITEM",
                        aggregate_id=task.task_id,
                    )
                ),
                1,
            )

    def test_shadow_work_item_stream_blocks_cycle_preparation(self) -> None:
        campaign_id = "campaign-controller-005"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "A shadow work-item stream must fail closed",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 105, 5_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            journal.append(
                event_id="shadow-work-item-event",
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_WORK_ITEM",
                aggregate_id="shadow-cycle-001",
                event_type="CAMPAIGN_WORK_ITEM_ADOPTED",
                payload={"shadow": True},
            )
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 500,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "work item stream conflicts",
            ):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=CycleReservationLimits(
                        currency="USD",
                        max_input_tokens=10,
                        max_output_tokens=5,
                        max_cost="0.1",
                    ),
                )

            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_WORK_ITEM",
                    aggregate_id=task.task_id,
                ),
                (),
            )

    def test_reopen_resumes_after_budget_reservation_before_context(self) -> None:
        campaign_id = "campaign-controller-006"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Crash recovery resumes the missing context step",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 106, 6_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=5,
            max_cost="0.1",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 500,
            )
            with patch(
                "research_automation.control_plane.campaign_context."
                "OperationalCycleContextJournal._prepare_assembled",
                side_effect=RuntimeError("synthetic crash boundary"),
            ):
                with self.assertRaisesRegex(RuntimeError, "synthetic crash"):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=(_protocol_member(),),
                        reservation_limits=reservation,
                    )

            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.BUDGET_RESERVED,
            )
            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 500,
            )
            prepared = reopened.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(_protocol_member(),),
                reservation_limits=reservation,
            )

            self.assertEqual(
                reopened.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(
                prepared.reservation.max_input_tokens,
                reservation.max_input_tokens,
            )

    def test_concurrent_identical_preparation_returns_one_frozen_cycle(
        self,
    ) -> None:
        campaign_id = "campaign-controller-007"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Concurrent preparation has one identity",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 107, 7_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=5,
            max_cost="0.1",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controllers = tuple(
                OperationalCampaignController(
                    journal=journal,
                    repository_root=root,
                    budget_limits=limits,
                    identity_provider=_FakeProcessIdentityProvider(owner),
                    monotonic_ns=lambda: 700,
                )
                for _ in range(2)
            )
            barrier = Barrier(2)

            def prepare(index: int) -> object:
                barrier.wait()
                return controllers[index].prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=reservation,
                )

            with ThreadPoolExecutor(max_workers=2) as executor:
                prepared = tuple(executor.map(prepare, range(2)))

            self.assertEqual(prepared[0], prepared[1])
            self.assertEqual(
                controllers[0].cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(
                controllers[0].cycle_budget_snapshot().reserved_cycle_ids,
                (task.task_id,),
            )
            self.assertEqual(
                controllers[0].budget_snapshot().reserved_input_tokens,
                reservation.max_input_tokens,
            )

    def test_concurrent_different_reservations_have_one_winner(self) -> None:
        campaign_id = "campaign-controller-008"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "One reservation identity has one bound",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 108, 8_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservations = (
            CycleReservationLimits(
                currency="USD",
                max_input_tokens=10,
                max_output_tokens=5,
                max_cost="0.1",
            ),
            CycleReservationLimits(
                currency="USD",
                max_input_tokens=11,
                max_output_tokens=6,
                max_cost="0.2",
            ),
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controllers = tuple(
                OperationalCampaignController(
                    journal=journal,
                    repository_root=root,
                    budget_limits=limits,
                    identity_provider=_FakeProcessIdentityProvider(owner),
                    monotonic_ns=lambda: 800,
                )
                for _ in range(2)
            )
            barrier = Barrier(2)

            def prepare(index: int) -> object:
                barrier.wait()
                try:
                    return controllers[index].prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=(_protocol_member(),),
                        reservation_limits=reservations[index],
                    )
                except Exception as error:
                    return error

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = tuple(executor.map(prepare, range(2)))

            winners = tuple(
                item for item in outcomes if not isinstance(item, Exception)
            )
            self.assertEqual(len(winners), 1)
            self.assertEqual(
                sum(isinstance(item, BudgetConflictError) for item in outcomes),
                1,
            )
            self.assertEqual(
                controllers[0].budget_snapshot().reserved_input_tokens,
                winners[0].reservation.max_input_tokens,
            )
            self.assertEqual(
                controllers[0].cycle_budget_snapshot().reserved_cycle_ids,
                (task.task_id,),
            )

    def test_duplicate_completed_preparation_reports_different_reservation_as_budget_conflict(
        self,
    ) -> None:
        campaign_id = "campaign-controller-008b"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "One reservation identity has one bound",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 108, 8_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservations = (
            CycleReservationLimits(
                currency="USD",
                max_input_tokens=10,
                max_output_tokens=5,
                max_cost="0.1",
            ),
            CycleReservationLimits(
                currency="USD",
                max_input_tokens=11,
                max_output_tokens=6,
                max_cost="0.2",
            ),
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 800,
            )
            prepared = controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(_protocol_member(),),
                reservation_limits=reservations[0],
            )
            with self.assertRaisesRegex(
                BudgetConflictError,
                "reservation_id was reused with different bounds",
            ):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=reservations[1],
                )
            self.assertEqual(
                prepared.reservation.max_input_tokens,
                reservations[0].max_input_tokens,
            )
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                reservations[0].max_input_tokens,
            )

    def test_combined_work_item_and_reservation_difference_reports_identity_conflict(
        self,
    ) -> None:
        campaign_id = "campaign-controller-008d"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "One reservation identity has one bound",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 108, 8_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservations = (
            CycleReservationLimits(
                currency="USD",
                max_input_tokens=10,
                max_output_tokens=5,
                max_cost="0.1",
            ),
            CycleReservationLimits(
                currency="USD",
                max_input_tokens=11,
                max_output_tokens=6,
                max_cost="0.2",
            ),
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 800,
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(_protocol_member(),),
                reservation_limits=reservations[0],
            )
            grafted_task = replace(
                task,
                proposal={
                    "hypothesis": "A different hypothesis is a graft",
                    "scope": _scope(generation="generation-1"),
                },
            )
            with self.assertRaisesRegex(
                CampaignJournalError,
                "Campaign durable preparation conflicts",
            ):
                controller.prepare_cycle(
                    task=grafted_task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=reservations[1],
                )
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                reservations[0].max_input_tokens,
            )

    def test_transient_store_busy_during_cycle_preparation_write_is_retried(
        self,
    ) -> None:
        campaign_id = "campaign-controller-008c"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "One reservation identity has one bound",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 108, 8_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=5,
            max_cost="0.1",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 800,
            )
            _FlakyWriteUnitOfWork.reset(failures_remaining=1)
            with patch.object(
                campaign_controller_module,
                "_SqliteUnitOfWork",
                _FlakyWriteUnitOfWork,
            ):
                prepared = controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=reservation,
                )
            self.assertEqual(
                prepared.reservation.max_input_tokens,
                reservation.max_input_tokens,
            )
            self.assertEqual(
                _FlakyWriteUnitOfWork._failures_raised,
                1,
            )
            self.assertEqual(
                _FlakyWriteUnitOfWork._failure_call_numbers,
                [1],
            )
            # Recovery write fails once and retries (2 calls), then the
            # admission write (1) and preparation record write (1).
            self.assertEqual(
                _FlakyWriteUnitOfWork._write_calls,
                4,
            )
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                reservation.max_input_tokens,
            )

    def test_transient_store_busy_during_cycle_admission_write_is_retried(
        self,
    ) -> None:
        campaign_id = "campaign-controller-008e"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "One reservation identity has one bound",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 108, 8_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=5,
            max_cost="0.1",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 800,
            )
            _FlakyWriteUnitOfWork.reset(
                skip_writes=1,
                failures_remaining=1,
            )
            with patch.object(
                campaign_controller_module,
                "_SqliteUnitOfWork",
                _FlakyWriteUnitOfWork,
            ):
                prepared = controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=reservation,
                )
            self.assertEqual(
                prepared.reservation.max_input_tokens,
                reservation.max_input_tokens,
            )
            self.assertEqual(
                _FlakyWriteUnitOfWork._failures_raised,
                1,
            )
            self.assertEqual(
                _FlakyWriteUnitOfWork._failure_call_numbers,
                [2],
            )
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                reservation.max_input_tokens,
            )

    def test_all_busy_preparation_writes_fail_closed_with_exactly_three_attempts(
        self,
    ) -> None:
        campaign_id = "campaign-controller-008f"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "One reservation identity has one bound",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 108, 8_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=5,
            max_cost="0.1",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 800,
            )
            baseline_rows = _campaign_event_rows(root, campaign_id)
            _FlakyWriteUnitOfWork.reset(failures_unlimited=True)
            with patch.object(
                campaign_controller_module,
                "_SqliteUnitOfWork",
                _FlakyWriteUnitOfWork,
            ):
                with self.assertRaises(SqliteStoreBusyError):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=(_protocol_member(),),
                        reservation_limits=reservation,
                    )
            self.assertEqual(_FlakyWriteUnitOfWork._write_calls, 3)
            self.assertEqual(_FlakyWriteUnitOfWork._failures_raised, 3)
            self.assertEqual(
                _FlakyWriteUnitOfWork._failure_call_numbers,
                [1, 2, 3],
            )
            self.assertEqual(
                _campaign_event_rows(root, campaign_id),
                baseline_rows,
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                0,
            )
            self.assertEqual(
                controller.cycle_budget_snapshot().reserved_cycle_ids,
                (),
            )

    def test_all_busy_admission_writes_fail_closed_with_exactly_three_attempts(
        self,
    ) -> None:
        campaign_id = "campaign-controller-008g"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "One reservation identity has one bound",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 108, 8_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=5,
            max_cost="0.1",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 800,
            )
            baseline_rows = _campaign_event_rows(root, campaign_id)
            _FlakyWriteUnitOfWork.reset(
                skip_writes=1,
                failures_remaining=(
                    campaign_controller_module._PREPARATION_LOCK_RETRY_ATTEMPTS
                ),
            )
            with patch.object(
                campaign_controller_module,
                "_SqliteUnitOfWork",
                _FlakyWriteUnitOfWork,
            ):
                with self.assertRaises(SqliteStoreBusyError):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=(_protocol_member(),),
                        reservation_limits=reservation,
                    )
            # The recovery write succeeds (1), then the admission write
            # exhausts its bounded retries (3), so no further writes run.
            self.assertEqual(_FlakyWriteUnitOfWork._write_calls, 4)
            self.assertEqual(
                _FlakyWriteUnitOfWork._failures_raised,
                campaign_controller_module._PREPARATION_LOCK_RETRY_ATTEMPTS,
            )
            self.assertEqual(
                _FlakyWriteUnitOfWork._failure_call_numbers,
                [2, 3, 4],
            )
            self.assertEqual(
                _campaign_event_rows(root, campaign_id),
                baseline_rows,
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                0,
            )
            self.assertEqual(
                controller.cycle_budget_snapshot().reserved_cycle_ids,
                (),
            )

    def test_concurrent_different_work_items_have_one_winner(self) -> None:
        campaign_id = "campaign-controller-009"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        tasks = tuple(
            ExperimentTask(
                task_id="cycle-001",
                strategy="b1",
                proposal={
                    "hypothesis": hypothesis,
                    "scope": _scope(generation="generation-1"),
                },
                source="synthetic-test",
            )
            for hypothesis in (
                "The first immutable work item",
                "A conflicting immutable work item",
            )
        )
        owner = ProcessIdentity("host-controller", 109, 9_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=5,
            max_cost="0.1",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controllers = tuple(
                OperationalCampaignController(
                    journal=journal,
                    repository_root=root,
                    budget_limits=limits,
                    identity_provider=_FakeProcessIdentityProvider(owner),
                    monotonic_ns=lambda: 900,
                )
                for _ in range(2)
            )
            losing_recovery_completed = Event()
            release_losing_recovery = Event()
            losing_thread_id: list[int] = []
            original_write = campaign_controller_module._SqliteUnitOfWork._write

            def gated_write(unit_of_work, operation):
                result = original_write(unit_of_work, operation)
                if (
                    losing_thread_id
                    and get_ident() == losing_thread_id[0]
                    and result is None
                    and not losing_recovery_completed.is_set()
                ):
                    losing_recovery_completed.set()
                    if not release_losing_recovery.wait(timeout=10):
                        raise AssertionError(
                            "winning work item did not release recovery boundary"
                        )
                return result

            def prepare(index: int) -> object:
                if index == 0:
                    losing_thread_id.append(get_ident())
                try:
                    return controllers[index].prepare_cycle(
                        task=tasks[index],
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=(_protocol_member(),),
                        reservation_limits=reservation,
                    )
                except Exception as error:
                    return error

            with patch.object(
                campaign_controller_module._SqliteUnitOfWork,
                "_write",
                new=gated_write,
            ), ThreadPoolExecutor(max_workers=2) as executor:
                losing_future = executor.submit(prepare, 0)
                self.assertTrue(losing_recovery_completed.wait(timeout=10))
                winning = executor.submit(prepare, 1).result(timeout=10)
                release_losing_recovery.set()
                losing = losing_future.result(timeout=10)

            outcomes = (losing, winning)

            self.assertEqual(
                sum(not isinstance(item, Exception) for item in outcomes),
                1,
            )
            self.assertEqual(
                sum(isinstance(item, CampaignJournalError) for item in outcomes),
                1,
            )
            self.assertEqual(
                len(
                    journal.list_events(
                        cycle_id="cycle-001",
                        aggregate_type="CAMPAIGN_WORK_ITEM",
                        aggregate_id="cycle-001",
                    )
                ),
                1,
            )
            self.assertEqual(
                controllers[0].budget_snapshot().reserved_input_tokens,
                reservation.max_input_tokens,
            )

    def test_blocked_campaign_cannot_adopt_a_new_work_item(self) -> None:
        campaign_id = "campaign-controller-010"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "A terminal Campaign cannot accept new work",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 110, 10_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 1_000,
            )
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.block(
                reason_code="synthetic_block",
                source_ref="test:blocked-campaign",
            )

            with self.assertRaises(CampaignLifecycleError):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=CycleReservationLimits(
                        currency="USD",
                        max_input_tokens=10,
                        max_output_tokens=5,
                        max_cost="0.1",
                    ),
                )

            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_WORK_ITEM",
                    aggregate_id=task.task_id,
                ),
                (),
            )

    def test_raw_frozen_lifecycle_cannot_start_execution(self) -> None:
        campaign_id = "campaign-controller-011"
        cycle_id = "cycle-001"
        owner = ProcessIdentity("host-controller", 111, 11_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 1_100,
            )
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            journal.append(
                event_id=lifecycle._cycle_event_id(
                    cycle_id,
                    CycleStatus.CREATED.value,
                ),
                cycle_id=cycle_id,
                aggregate_type="CYCLE_STATE",
                aggregate_id=cycle_id,
                event_type="CYCLE_OPENED",
                payload={
                    "cycle_id": cycle_id,
                    "cycle_number": 1,
                    "status": CycleStatus.CREATED.value,
                },
            )
            current = CycleStatus.CREATED
            for next_status in (
                CycleStatus.BUDGET_RESERVED,
                CycleStatus.CONTEXT_READY,
                CycleStatus.FROZEN,
            ):
                journal.append(
                    event_id=lifecycle._cycle_event_id(
                        cycle_id,
                        next_status.value,
                    ),
                    cycle_id=cycle_id,
                    aggregate_type="CYCLE_STATE",
                    aggregate_id=cycle_id,
                    event_type="CYCLE_TRANSITIONED",
                    payload={
                        "cycle_id": cycle_id,
                        "cycle_number": 1,
                        "from_status": current.value,
                        "to_status": next_status.value,
                    },
                )
                current = next_status

            with self.assertRaises(CycleFreezeError):
                controller.start_execution(
                    cycle_id=cycle_id,
                    acquisition_id="raw-freeze-acquisition",
                )

            self.assertEqual(
                journal.list_events(
                    cycle_id=cycle_id,
                    aggregate_type="CYCLE_LEASE",
                    aggregate_id=cycle_id,
                ),
                (),
            )
            self.assertEqual(
                controller.cycle_snapshot(cycle_id).status,
                CycleStatus.FROZEN,
            )

    def test_lower_level_freeze_without_controller_work_cannot_execute(
        self,
    ) -> None:
        campaign_id = "campaign-controller-012"
        cycle_id = "cycle-001"
        proposal = {
            "hypothesis": "Lower-level freeze is not controller preparation",
            "scope": _scope(generation="generation-1"),
        }
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        owner = ProcessIdentity("host-controller", 112, 12_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 1_200,
            )
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            controller._cycle_budget.open_cycle(
                lifecycle=lifecycle,
                cycle_id=cycle_id,
                cycle_number=1,
            )
            lifecycle.advance_cycle(
                cycle_id=cycle_id,
                expected_status=CycleStatus.CREATED,
                next_status=CycleStatus.BUDGET_RESERVED,
            )
            context = OperationalCycleContextJournal(
                journal=journal,
                lifecycle=lifecycle,
                repository_root=root,
            )
            context.prepare(
                cycle_id=cycle_id,
                proposal=proposal,
                roles=("factor_engineer",),
            )
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster_manifest = roster.freeze(
                cycle_id=cycle_id,
                members=(_protocol_member(),),
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )
            freeze.freeze(
                cycle_id=cycle_id,
                proposal=proposal,
                execution_spec=execution_spec,
                expected_roster=roster_manifest,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "controller preparation is incomplete",
            ):
                controller.start_execution(
                    cycle_id=cycle_id,
                    acquisition_id="lower-level-freeze-acquisition",
                )

            self.assertEqual(
                journal.list_events(
                    cycle_id=cycle_id,
                    aggregate_type="CYCLE_LEASE",
                    aggregate_id=cycle_id,
                ),
                (),
            )

    def test_invalid_proposal_has_no_persistent_side_effects(self) -> None:
        campaign_id = "campaign-controller-013"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={},
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 113, 13_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 1_300,
            )

            with self.assertRaisesRegex(
                ValueError,
                "proposal.hypothesis must be canonical",
            ):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=CycleReservationLimits(
                        currency="USD",
                        max_input_tokens=10,
                        max_output_tokens=5,
                        max_cost="0.1",
                    ),
                )

            self.assertEqual(controller.campaign_snapshot().status.value, "CREATED")
            self.assertEqual(
                controller.cycle_budget_snapshot().reserved_cycle_ids,
                (),
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                0,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_WORK_ITEM",
                    aggregate_id=task.task_id,
                ),
                (),
            )
            with self.assertRaises(CampaignLifecycleError):
                controller.cycle_snapshot(task.task_id)

    def test_roster_protocol_mismatch_has_no_persistent_side_effects(self) -> None:
        campaign_id = "campaign-controller-014"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Roster drift must fail before reservation",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 114, 14_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 1_400,
            )

            with self.assertRaisesRegex(
                ValueError,
                "ExecutionSpec roster conflicts",
            ):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(
                        replace(_protocol_member(), model="drifted-model"),
                    ),
                    reservation_limits=CycleReservationLimits(
                        currency="USD",
                        max_input_tokens=10,
                        max_output_tokens=5,
                        max_cost="0.1",
                    ),
                )

            self.assertEqual(controller.campaign_snapshot().status.value, "CREATED")
            self.assertEqual(
                controller.cycle_budget_snapshot().reserved_cycle_ids,
                (),
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                0,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_WORK_ITEM",
                    aggregate_id=task.task_id,
                ),
                (),
            )

    def test_work_item_replay_is_type_sensitive(self) -> None:
        campaign_id = "campaign-controller-015"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        original = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Typed work-item identity is immutable",
                "scope": _scope(generation="generation-1"),
                "flag": True,
            },
            source="synthetic-test",
        )
        changed = ExperimentTask(
            task_id=original.task_id,
            strategy=original.strategy,
            proposal={**original.proposal, "flag": 1},
            source=original.source,
            priority=original.priority,
        )
        owner = ProcessIdentity("host-controller", 115, 15_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=5,
            max_cost="0.1",
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 1_500,
            )
            with patch(
                "research_automation.control_plane.campaign_context."
                "OperationalCycleContextJournal._prepare_assembled",
                side_effect=RuntimeError("synthetic crash boundary"),
            ):
                with self.assertRaisesRegex(RuntimeError, "synthetic crash"):
                    controller.prepare_cycle(
                        task=original,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=(_protocol_member(),),
                        reservation_limits=reservation,
                    )

            with self.assertRaises(CampaignJournalError):
                controller.prepare_cycle(
                    task=changed,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=reservation,
                )

            events = journal.list_events(
                cycle_id=original.task_id,
                aggregate_type="CAMPAIGN_WORK_ITEM",
                aggregate_id=original.task_id,
            )
            self.assertEqual(len(events), 1)
            self.assertIn('"flag":true', events[0].payload_json)
            self.assertNotIn('"flag":1', events[0].payload_json)

    def test_reopen_records_missing_preparation_receipt_after_freeze(self) -> None:
        campaign_id = "campaign-controller-016"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            (
                controller,
                task,
                execution_spec,
                roster_members,
                reservation_limits,
            ) = _learning_preflight_prepare_inputs(
                root,
                journal,
                hypothesis="Preparation receipt recovers after freeze",
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=_projectable_learning_input(),
            ) as initial_reader, patch.object(
                OperationalCampaignController,
                "_record_cycle_preparation",
                side_effect=RuntimeError("synthetic post-freeze crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "post-freeze crash"):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=roster_members,
                        reservation_limits=reservation_limits,
                    )
            initial_reader.assert_called_once_with()

            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                    aggregate_id=task.task_id,
                ),
                (),
            )
            with self.assertRaisesRegex(
                CampaignJournalError,
                "receipt is missing",
            ):
                controller.start_execution(
                    cycle_id=task.task_id,
                    acquisition_id="pre-recovery-acquisition",
                )

            frozen_context = controller._context.snapshot(cycle_id=task.task_id)
            frozen_inputs = controller._freeze.snapshot(cycle_id=task.task_id)

            def reopen(pid: int) -> OperationalCampaignController:
                return OperationalCampaignController(
                    journal=journal,
                    repository_root=root,
                    budget_limits=CampaignBudgetLimits(
                        currency="USD",
                        max_cycles=1,
                        max_input_tokens=100,
                        max_output_tokens=50,
                        max_cost="1",
                        max_wall_time_ms=5_000,
                        max_tool_attempts=4,
                        max_data_exposures=1,
                        max_disk_growth_bytes=10_000,
                    ),
                    identity_provider=_FakeProcessIdentityProvider(
                        ProcessIdentity(
                            "host-learning-preflight",
                            pid,
                            42_000,
                        )
                    ),
                    monotonic_ns=lambda: 200,
                )

            reopened = reopen(142)
            concurrent_reopened = reopen(143)
            rows_before_recovery = _campaign_event_rows(root, campaign_id)
            external_read_error = AssertionError(
                "frozen recovery must use only validated durable artifacts"
            )
            recovery_write_barrier = Barrier(2)
            original_write = campaign_controller_module._SqliteUnitOfWork._write
            original_preflight = campaign_controller_module.run_campaign_preflight
            original_freeze_preflight = campaign_freeze_module.run_campaign_preflight

            def synchronized_recovery_write(unit_of_work, operation):
                recovery_write_barrier.wait(timeout=10)
                return original_write(unit_of_work, operation)

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                side_effect=external_read_error,
            ) as replay_reader, patch.object(
                campaign_controller_module,
                "run_campaign_preflight",
                wraps=original_preflight,
            ) as controller_preflight, patch.object(
                campaign_freeze_module,
                "run_campaign_preflight",
                wraps=original_freeze_preflight,
            ) as freeze_preflight, patch.object(
                campaign_controller_module._SqliteUnitOfWork,
                "_write",
                new=synchronized_recovery_write,
            ):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = tuple(
                        executor.submit(
                            candidate.prepare_cycle,
                            task=task,
                            cycle_number=1,
                            execution_spec=execution_spec,
                            roster_members=roster_members,
                            reservation_limits=reservation_limits,
                        )
                        for candidate in (reopened, concurrent_reopened)
                    )
                    prepared, concurrent_prepared = tuple(
                        future.result() for future in futures
                    )

            replay_reader.assert_not_called()
            self.assertEqual(controller_preflight.call_count, 2)
            self.assertEqual(freeze_preflight.call_count, 2)
            self.assertEqual(concurrent_prepared, prepared)
            self.assertEqual(prepared.context, frozen_context)
            self.assertEqual(prepared.frozen, frozen_inputs)

            preparation_events = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                aggregate_id=task.task_id,
            )
            self.assertEqual(len(preparation_events), 1)
            self.assertIn(
                prepared.preparation_manifest_sha256,
                preparation_events[0].payload_json,
            )
            rows_after_recovery = _campaign_event_rows(root, campaign_id)
            self.assertEqual(rows_after_recovery[:-1], rows_before_recovery)
            self.assertEqual(
                rows_after_recovery[-1][4],
                "CAMPAIGN_CYCLE_PREPARATION",
            )

            replayed = reopened.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=roster_members,
                reservation_limits=reservation_limits,
            )
            self.assertEqual(replayed, prepared)
            self.assertEqual(
                _campaign_event_rows(root, campaign_id),
                rows_after_recovery,
            )

    def test_context_ready_recovery_uses_durable_learning_for_preflight(
        self,
    ) -> None:
        crash_cases = (
            (
                "after-context-before-roster",
                OperationalRosterJournal,
                "freeze",
                0,
                (
                    "CYCLE_ROSTER",
                    "CYCLE_INPUT_FREEZE",
                    "CYCLE_STATE",
                    "CAMPAIGN_CYCLE_PREPARATION",
                ),
            ),
            (
                "after-roster-before-freeze",
                OperationalCycleFreezeJournal,
                "freeze",
                1,
                (
                    "CYCLE_INPUT_FREEZE",
                    "CYCLE_STATE",
                    "CAMPAIGN_CYCLE_PREPARATION",
                ),
            ),
        )
        original_controller_preflight = (
            campaign_controller_module.run_campaign_preflight
        )
        original_freeze_preflight = campaign_freeze_module.run_campaign_preflight
        for (
            label,
            crash_owner,
            crash_method,
            expected_roster_count,
            expected_new_aggregate_types,
        ) in crash_cases:
            with self.subTest(crash_boundary=label):
                campaign_id = f"campaign-controller-context-ready-{label}"
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    (
                        controller,
                        task,
                        execution_spec,
                        roster_members,
                        reservation_limits,
                    ) = _learning_preflight_prepare_inputs(
                        root,
                        journal,
                        hypothesis=(
                            "Durable context must recover without live Learning"
                        ),
                    )
                    stored_projection = _projectable_learning_input(
                        f"stored-context-ready-{label}"
                    )
                    stored_projection["claims"][0]["scope"] = _scope(
                        generation="stored-foreign-generation"
                    )
                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        return_value=stored_projection,
                    ) as initial_reader, patch.object(
                        crash_owner,
                        crash_method,
                        side_effect=RuntimeError(
                            f"synthetic {label} crash boundary"
                        ),
                    ):
                        with self.assertRaisesRegex(
                            RuntimeError,
                            f"synthetic {label}",
                        ):
                            controller.prepare_cycle(
                                task=task,
                                cycle_number=1,
                                execution_spec=execution_spec,
                                roster_members=roster_members,
                                reservation_limits=reservation_limits,
                            )
                    initial_reader.assert_called_once_with()

                    crash_snapshot = _prepare_cycle_durable_snapshot(
                        root,
                        campaign_id,
                        journal,
                        controller,
                        cycle_id=task.task_id,
                    )
                    self.assertEqual(
                        crash_snapshot["cycle"].status,
                        CycleStatus.CONTEXT_READY,
                    )
                    self.assertEqual(len(crash_snapshot["context"]), 1)
                    self.assertEqual(
                        len(crash_snapshot["roster"]),
                        expected_roster_count,
                    )
                    self.assertEqual(crash_snapshot["freeze"], ())
                    self.assertEqual(crash_snapshot["preparation"], ())
                    stored_context_payload = json.loads(
                        crash_snapshot["context"][0].payload_json
                    )
                    self.assertEqual(
                        stored_context_payload["projection_input"],
                        stored_projection,
                    )
                    self.assertEqual(
                        stored_context_payload["proposal"],
                        task.proposal,
                    )
                    self.assertEqual(
                        stored_context_payload["untrusted_sources"],
                        [],
                    )
                    rows_before_recovery = crash_snapshot["all_events"]

                    reopened = OperationalCampaignController(
                        journal=journal,
                        repository_root=root,
                        budget_limits=CampaignBudgetLimits(
                            currency="USD",
                            max_cycles=1,
                            max_input_tokens=100,
                            max_output_tokens=50,
                            max_cost="1",
                            max_wall_time_ms=5_000,
                            max_tool_attempts=4,
                            max_data_exposures=1,
                            max_disk_growth_bytes=10_000,
                        ),
                        identity_provider=_FakeProcessIdentityProvider(
                            ProcessIdentity(
                                "host-context-ready-recovery",
                                144,
                                44_000,
                            )
                        ),
                        monotonic_ns=lambda: 400,
                    )
                    external_read_error = AssertionError(
                        "CONTEXT_READY recovery must not read live Learning"
                    )
                    controller_claims = []
                    freeze_claims = []

                    def verify_stored_controller_preflight(**kwargs):
                        controller_claims.append(kwargs["committed_claims"])
                        self.assertEqual(
                            kwargs["committed_claims"],
                            stored_projection["claims"],
                        )
                        return original_controller_preflight(**kwargs)

                    def verify_stored_freeze_preflight(**kwargs):
                        freeze_claims.append(kwargs["committed_claims"])
                        self.assertEqual(
                            kwargs["committed_claims"],
                            stored_projection["claims"],
                        )
                        return original_freeze_preflight(**kwargs)

                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        side_effect=external_read_error,
                    ) as recovery_reader, patch.object(
                        campaign_controller_module,
                        "run_campaign_preflight",
                        side_effect=verify_stored_controller_preflight,
                    ) as controller_preflight, patch.object(
                        campaign_freeze_module,
                        "run_campaign_preflight",
                        side_effect=verify_stored_freeze_preflight,
                    ) as freeze_preflight:
                        prepared = reopened.prepare_cycle(
                            task=task,
                            cycle_number=1,
                            execution_spec=execution_spec,
                            roster_members=roster_members,
                            reservation_limits=reservation_limits,
                        )

                    recovery_reader.assert_not_called()
                    controller_preflight.assert_called_once()
                    self.assertEqual(freeze_preflight.call_count, 3)
                    self.assertEqual(
                        controller_claims,
                        [stored_projection["claims"]],
                    )
                    self.assertEqual(
                        freeze_claims,
                        [stored_projection["claims"]] * 3,
                    )
                    self.assertEqual(
                        reopened.cycle_snapshot(task.task_id).status,
                        CycleStatus.FROZEN,
                    )
                    self.assertEqual(prepared.context.event_id,
                                     crash_snapshot["context"][0].event_id)
                    rows_after_recovery = _campaign_event_rows(
                        root,
                        campaign_id,
                    )
                    self.assertEqual(
                        rows_after_recovery[: len(rows_before_recovery)],
                        rows_before_recovery,
                    )
                    self.assertEqual(
                        tuple(
                            row[4]
                            for row in rows_after_recovery[
                                len(rows_before_recovery) :
                            ]
                        ),
                        expected_new_aggregate_types,
                    )

                    with patch.object(
                        CommittedLearningLedgerReader,
                        "read_projection_input",
                        side_effect=external_read_error,
                    ) as replay_reader, patch.object(
                        campaign_controller_module,
                        "run_campaign_preflight",
                        side_effect=verify_stored_controller_preflight,
                    ) as replay_controller_preflight, patch.object(
                        campaign_freeze_module,
                        "run_campaign_preflight",
                        side_effect=verify_stored_freeze_preflight,
                    ) as replay_freeze_preflight:
                        replayed = reopened.prepare_cycle(
                            task=task,
                            cycle_number=1,
                            execution_spec=execution_spec,
                            roster_members=roster_members,
                            reservation_limits=reservation_limits,
                        )
                    self.assertEqual(replayed, prepared)
                    replay_reader.assert_not_called()
                    replay_controller_preflight.assert_called_once()
                    replay_freeze_preflight.assert_called_once()
                    self.assertEqual(
                        _campaign_event_rows(root, campaign_id),
                        rows_after_recovery,
                    )

    def test_unapproved_execution_spec_has_no_persistent_side_effects(self) -> None:
        campaign_id = "campaign-controller-017"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=None,
            approval=None,
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Unapproved execution cannot reserve a Cycle",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 117, 17_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 1_700,
            )

            with self.assertRaises(MaterialProtocolChangeError):
                controller.prepare_cycle(
                    task=task,
                    cycle_number=1,
                    execution_spec=execution_spec,
                    roster_members=(_protocol_member(),),
                    reservation_limits=CycleReservationLimits(
                        currency="USD",
                        max_input_tokens=10,
                        max_output_tokens=5,
                        max_cost="0.1",
                    ),
                )

            self.assertEqual(controller.campaign_snapshot().status.value, "CREATED")
            self.assertEqual(
                controller.cycle_budget_snapshot().reserved_cycle_ids,
                (),
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                0,
            )

    def test_shadow_preparation_stream_blocks_execution(self) -> None:
        campaign_id = "campaign-controller-018"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Preparation replay rejects shadow streams",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 118, 18_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 1_800,
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(_protocol_member(),),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=10,
                    max_output_tokens=5,
                    max_cost="0.1",
                ),
            )
            journal.append(
                event_id="shadow-preparation-event",
                cycle_id=task.task_id,
                aggregate_type="CAMPAIGN_CYCLE_PREPARATION",
                aggregate_id="shadow-cycle-001",
                event_type="CAMPAIGN_CYCLE_PREPARED",
                payload={"shadow": True},
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "preparation stream conflicts",
            ):
                controller.start_execution(
                    cycle_id=task.task_id,
                    acquisition_id="shadow-preparation-acquisition",
                )
            self.assertEqual(
                journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="CYCLE_LEASE",
                    aggregate_id=task.task_id,
                ),
                (),
            )

    def test_shadow_usage_attempt_blocks_provider_invocation(self) -> None:
        campaign_id = "campaign-controller-shadow-attempt"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Shadow usage cannot authorize a provider call",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 130, 30_000)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 100,
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-shadow-attempt",
            )
            OperationalUsageJournal(
                journal=journal,
                cycle_id=task.task_id,
            ).begin(
                UsageEnvelope(
                    provider=member.provider,
                    profile=member.profile,
                    request_model=member.model,
                    response_model=None,
                    call_id="shadow-call",
                    attempt_id="shadow-call-attempt-001",
                    usage_status=UsageStatus.UNKNOWN,
                    input_tokens=None,
                    output_tokens=None,
                    total_tokens=None,
                    cache_read_tokens=None,
                    cache_write_tokens=None,
                    reasoning_tokens=None,
                    reported_cost=None,
                    currency=None,
                    fallback=False,
                    streamed=False,
                    outcome=InvocationOutcome.TIMEOUT,
                    raw_usage_sha256="4" * 64,
                )
            )
            provider = _BoundFakeProvider()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "attempt inventory",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )

            self.assertEqual(provider.call_count, 0)

    def test_controller_invokes_one_frozen_fake_member_and_records_usage(
        self,
    ) -> None:
        campaign_id = "campaign-controller-019"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "A bound fake member records durable usage",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 119, 19_000)
        provider = _BoundFakeProvider(timeouts_before_success=1)
        monotonic = _FakeMonotonicClock(100, 1_000_000, 6_000_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=monotonic,
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            executing = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-fake-member",
            )

            with self.assertRaisesRegex(ValueError, "prompt conflicts"):
                controller.invoke_member_json(
                    execution=executing,
                    member_id=member.member_id,
                    provider=provider,
                    prompt={"instruction": "A drifted prompt"},
                    limits=_FAKE_CALL_LIMITS,
                )
            drifted_provider = _BoundFakeProvider()
            drifted_provider.profile = "drifted-profile"
            with self.assertRaisesRegex(
                ValueError,
                "provider binding conflicts",
            ):
                controller.invoke_member_json(
                    execution=executing,
                    member_id=member.member_id,
                    provider=drifted_provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )
            self.assertEqual(provider.call_count, 0)
            self.assertEqual(drifted_provider.call_count, 0)
            self.assertEqual(
                OperationalUsageJournal(
                    journal=journal,
                    cycle_id=task.task_id,
                ).list_attempts(),
                (),
            )

            executed = controller.invoke_member_json(
                execution=executing,
                member_id=member.member_id,
                provider=provider,
                prompt=prompt,
                limits=_FAKE_CALL_LIMITS,
            )
            replay_provider = _BoundFakeProvider()
            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 1_900,
            )
            replay_execution = reopened.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-fake-member",
            )
            replay = reopened.invoke_member_json(
                execution=replay_execution,
                member_id=member.member_id,
                provider=replay_provider,
                prompt=prompt,
                limits=_FAKE_CALL_LIMITS,
            )
            execution_usage = reopened.complete_model_execution(
                execution=replay_execution,
            )
            usage_replay = reopened.complete_model_execution(
                execution=replay_execution,
            )

            self.assertEqual(
                executed.output,
                {"source": "synthetic", "status": "ok"},
            )
            self.assertEqual(replay, executed)
            self.assertEqual(executed.member_id, member.member_id)
            self.assertEqual(executed.attempt_count, 2)
            self.assertEqual(executed.wall_time_ms, 5)
            self.assertEqual(provider.call_count, 2)
            self.assertEqual(replay_provider.call_count, 0)
            self.assertEqual(usage_replay, execution_usage)
            self.assertEqual(execution_usage.usage_status, UsageStatus.UNKNOWN)
            self.assertIsNone(execution_usage.input_tokens)
            self.assertIsNone(execution_usage.output_tokens)
            self.assertIsNone(execution_usage.cost)
            self.assertEqual(execution_usage.currency, "USD")
            self.assertEqual(execution_usage.wall_time_ms, 5)
            self.assertEqual(execution_usage.tool_attempts, 2)
            self.assertEqual(
                reopened.budget_snapshot().reserved_input_tokens,
                20,
            )
            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id=task.task_id,
            ).list_attempts(call_id=executed.call_id)
            self.assertEqual(len(attempts), 2)
            self.assertIsNone(attempts[0].envelope.total_tokens)
            self.assertEqual(attempts[1].envelope.total_tokens, 10)
            self.assertEqual(
                attempts[0].final_outcome.value,
                "TIMEOUT",
            )
            self.assertEqual(
                attempts[1].final_outcome.value,
                "SUCCESS",
            )
            self.assertTrue(executed.verified_response.event_id)

    def test_controller_requires_invocation_binding_before_provider_construction(
        self,
    ) -> None:
        campaign_id = "campaign-controller-invocation-binding"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Provider identity must bind before provider construction",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 131, 31_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 100,
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-binding-attempt",
            )

            class _BareProvider:
                def invoke(self, request: object) -> object:
                    return request

            with self.assertRaisesRegex(
                ValueError,
                "provider binding identity is invalid",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=_BareProvider(),
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )
            self.assertEqual(
                OperationalUsageJournal(
                    journal=journal,
                    cycle_id=task.task_id,
                ).list_attempts(),
                (),
            )

    def test_provider_request_model_drift_persists_and_atomically_blocks(
        self,
    ) -> None:
        campaign_id = "campaign-controller-request-model-drift"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Provider request identity drift blocks closed",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 119, 19_001)
        provider = _RequestModelDriftBoundFakeProvider()
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(100, 1_000_000),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            executing = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-request-model-drift",
            )

            with self.assertRaises(RosterDriftError):
                controller.invoke_member_json(
                    execution=executing,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )

            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id=task.task_id,
            ).list_attempts()
            self.assertEqual(len(attempts), 1)
            self.assertEqual(
                attempts[0].envelope.request_model,
                "provider-attributed-drift-model",
            )
            self.assertEqual(attempts[0].final_outcome.value, "SUCCESS")
            blocked = controller.campaign_snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_IDENTITY_DRIFT")
            call_id = controller._member_call_id(task.task_id, member.member_id)
            self.assertEqual(
                tuple(
                    event.event_type
                    for event in journal.list_events(
                        cycle_id=task.task_id,
                        aggregate_type="OPERATIONAL_MODEL_CALL",
                        aggregate_id=call_id,
                    )
                ),
                ("OPERATIONAL_MODEL_CALL_STARTED",),
            )
            replay_provider = _BoundFakeProvider()
            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.invoke_member_json(
                    execution=executing,
                    member_id=member.member_id,
                    provider=replay_provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )
            self.assertEqual(replay_provider.call_count, 0)

    def test_exhausted_fake_member_blocks_without_retrying_after_failure(
        self,
    ) -> None:
        campaign_id = "campaign-controller-020"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Exhausted required member fails closed",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 120, 20_000)
        provider = _BoundFakeProvider(timeouts_before_success=2)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(100, 1_000_000),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            executing = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-failing-member",
            )

            with self.assertRaises(RosterDriftError):
                controller.invoke_member_json(
                    execution=executing,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )

            self.assertEqual(provider.call_count, 2)
            self.assertEqual(
                controller.campaign_snapshot().status.value,
                "BLOCKED",
            )
            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id=task.task_id,
            ).list_attempts()
            self.assertEqual(len(attempts), 2)
            self.assertTrue(
                all(
                    attempt.final_outcome.value == "TIMEOUT"
                    for attempt in attempts
                )
            )
            replay_provider = _BoundFakeProvider()
            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.invoke_member_json(
                    execution=executing,
                    member_id=member.member_id,
                    provider=replay_provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )
            self.assertEqual(replay_provider.call_count, 0)
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                20,
            )

    def test_invalid_model_output_blocks_required_member_without_replay(
        self,
    ) -> None:
        cases = (
            (
                "canonical-expansion",
                '{"payload":"' + "\u4e2d" * 10_000 + '"}',
            ),
            ("nan", "NaN"),
            ("overflow", "1e10000"),
        )
        for label, output_text in cases:
            with self.subTest(label=label):
                campaign_id = f"campaign-controller-invalid-output-{label}"
                protocol = _protocol()
                execution_spec = compile_execution_spec(
                    protocol,
                    approved_protocol=protocol,
                    approval=_approval(protocol),
                    amendment=None,
                )
                prompt = {"instruction": "Return one bounded synthetic result"}
                member = replace(
                    _protocol_member(),
                    prompt_sha256=operational_prompt_sha256(prompt),
                )
                task = ExperimentTask(
                    task_id="cycle-001",
                    strategy="b1",
                    proposal={
                        "hypothesis": "Invalid required member blocks closed",
                        "scope": _scope(generation="generation-1"),
                    },
                    source="synthetic-test",
                )
                provider = _InvalidJsonBoundFakeProvider(output_text)
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    controller = OperationalCampaignController(
                        journal=journal,
                        repository_root=root,
                        budget_limits=CampaignBudgetLimits(
                            currency="USD",
                            max_cycles=1,
                            max_input_tokens=100,
                            max_output_tokens=50,
                            max_cost="1",
                            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                            max_tool_attempts=1,
                        ),
                        identity_provider=_FakeProcessIdentityProvider(
                            ProcessIdentity("host-controller", 122, 22_000)
                        ),
                        monotonic_ns=lambda: 100,
                    )
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=(member,),
                        reservation_limits=CycleReservationLimits(
                            currency="USD",
                            max_input_tokens=20,
                            max_output_tokens=10,
                            max_cost="0.1",
                            max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                            max_tool_attempts=1,
                        ),
                    )
                    execution = controller.start_execution(
                        cycle_id=task.task_id,
                        acquisition_id=f"execute-invalid-output-{label}",
                    )

                    with self.assertRaises(RosterDriftError):
                        controller.invoke_member_json(
                            execution=execution,
                            member_id=member.member_id,
                            provider=provider,
                            prompt=prompt,
                            limits=OperationalModelCallLimits(
                                currency="USD",
                                max_input_tokens=20,
                                max_output_tokens=10,
                                max_cost="0.1",
                                max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                                max_attempts=1,
                            ),
                        )

                    self.assertEqual(provider.call_count, 1)
                    self.assertEqual(
                        controller.campaign_snapshot().status,
                        CampaignStatus.BLOCKED,
                    )
                    attempts = OperationalUsageJournal(
                        journal=journal,
                        cycle_id=task.task_id,
                    ).list_attempts()
                    self.assertEqual(len(attempts), 1)
                    self.assertEqual(
                        attempts[0].final_outcome,
                        InvocationOutcome.INVALID_JSON,
                    )

                    replay_provider = _BoundFakeProvider()
                    with self.assertRaisesRegex(
                        CampaignJournalError,
                        "execution receipt is stale",
                    ):
                        controller.invoke_member_json(
                            execution=execution,
                            member_id=member.member_id,
                            provider=replay_provider,
                            prompt=prompt,
                            limits=OperationalModelCallLimits(
                                currency="USD",
                                max_input_tokens=20,
                                max_output_tokens=10,
                                max_cost="0.1",
                                max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                                max_attempts=1,
                            ),
                        )
                    self.assertEqual(replay_provider.call_count, 0)

    def test_finish_journal_typed_errors_bypass_controller_compensation(
        self,
    ) -> None:
        error_types = (
            InvalidModelResponseError,
            ModelInvocationProviderError,
            ModelInvocationTimeoutError,
        )
        for error_type in error_types:
            with self.subTest(error_type=error_type.__name__):
                campaign_id = (
                    "campaign-controller-finish-journal-origin-"
                    f"{error_type.__name__.lower()}"
                )
                failure = error_type("synthetic usage journal finish failure")
                provider = _BoundFakeProvider()
                compensation_calls = {"list_attempts": 0, "verify_response": 0}
                original_list_attempts = OperationalUsageJournal.list_attempts

                def tracked_list_attempts(journal, *args, **kwargs):
                    compensation_calls["list_attempts"] += 1
                    return original_list_attempts(journal, *args, **kwargs)

                def tracked_verify_response(*args, **kwargs):
                    compensation_calls["verify_response"] += 1
                    return None

                with _authorized_campaign(campaign_id) as (root, _, journal):
                    with patch.object(
                        campaign_controller_module._FencedOperationalUsageJournal,
                        "finish",
                        autospec=True,
                        side_effect=failure,
                    ) as finish, patch.object(
                        OperationalUsageJournal,
                        "list_attempts",
                        new=tracked_list_attempts,
                    ), patch.object(
                        OperationalRosterJournal,
                        "verify_response",
                        new=tracked_verify_response,
                    ):
                        with self.assertRaises(Exception) as raised:
                            _completed_evidence_model_call(
                                root,
                                journal,
                                campaign_id=campaign_id,
                                provider=provider,
                            )

                self.assertIs(raised.exception, failure)
                self.assertTrue(_is_usage_journal_error(failure))
                self.assertEqual(provider.call_count, 1)
                self.assertEqual(finish.call_count, 1)
                self.assertEqual(compensation_calls["list_attempts"], 0)
                self.assertEqual(compensation_calls["verify_response"], 0)

    def test_begin_journal_typed_error_bypasses_controller_compensation(
        self,
    ) -> None:
        campaign_id = "campaign-controller-begin-journal-origin-invalid-response"
        failure = InvalidModelResponseError(
            "synthetic usage journal begin failure"
        )
        provider = _BoundFakeProvider()
        compensation_calls = {"list_attempts": 0, "verify_response": 0}
        original_list_attempts = OperationalUsageJournal.list_attempts

        def tracked_list_attempts(journal, *args, **kwargs):
            compensation_calls["list_attempts"] += 1
            return original_list_attempts(journal, *args, **kwargs)

        def tracked_verify_response(*args, **kwargs):
            compensation_calls["verify_response"] += 1
            return None

        with _authorized_campaign(campaign_id) as (root, _, journal):
            with patch.object(
                campaign_controller_module._FencedOperationalUsageJournal,
                "begin",
                autospec=True,
                side_effect=failure,
            ) as begin, patch.object(
                OperationalUsageJournal,
                "list_attempts",
                new=tracked_list_attempts,
            ), patch.object(
                OperationalRosterJournal,
                "verify_response",
                new=tracked_verify_response,
            ):
                with self.assertRaises(Exception) as raised:
                    _completed_evidence_model_call(
                        root,
                        journal,
                        campaign_id=campaign_id,
                        provider=provider,
                    )

        self.assertIs(raised.exception, failure)
        self.assertTrue(_is_usage_journal_error(failure))
        self.assertEqual(provider.call_count, 1)
        self.assertEqual(begin.call_count, 1)
        self.assertEqual(compensation_calls["list_attempts"], 0)
        self.assertEqual(compensation_calls["verify_response"], 0)

    def test_mid_call_crash_is_fenced_without_second_provider_call(self) -> None:
        campaign_id = "campaign-controller-021"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "A mid-call crash is fenced at-most-once",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 121, 21_000)
        limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        reservation = CycleReservationLimits(
            currency="USD",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(100, 1_000_000),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=reservation,
            )
            executing = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-crashing-member",
            )
            provider = _BoundFakeProvider()
            with patch(
                "research_automation.control_plane.campaign_controller."
                "RetryingModelInvocation.invoke_json_with_receipt",
                side_effect=RuntimeError("synthetic mid-call crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "mid-call crash"):
                    controller.invoke_member_json(
                        execution=executing,
                        member_id=member.member_id,
                        provider=provider,
                        prompt=prompt,
                        limits=_FAKE_CALL_LIMITS,
                    )

            self.assertEqual(provider.call_count, 0)
            self.assertEqual(
                controller.campaign_snapshot().status.value,
                "ACTIVE",
            )
            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 2_000_000,
            )
            replay_execution = reopened.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-crashing-member",
            )
            replay_provider = _BoundFakeProvider()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "incomplete and in doubt",
            ):
                reopened.invoke_member_json(
                    execution=replay_execution,
                    member_id=member.member_id,
                    provider=replay_provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )

            self.assertEqual(replay_provider.call_count, 0)
            self.assertEqual(
                reopened.campaign_snapshot().status.value,
                "BLOCKED",
            )
            call_events = tuple(
                event
                for event in journal.list_events(
                    cycle_id=task.task_id,
                    aggregate_type="OPERATIONAL_MODEL_CALL",
                    aggregate_id=reopened._member_call_id(
                        task.task_id,
                        member.member_id,
                    ),
                )
            )
            self.assertEqual(len(call_events), 1)
            self.assertEqual(
                call_events[0].event_type,
                "OPERATIONAL_MODEL_CALL_STARTED",
            )

    def test_oversized_output_blocks_campaign_during_first_logical_call(self) -> None:
        campaign_id = "campaign-controller-oversized-output"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Oversized output remains at-most-once",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 133, 33_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-oversized-output",
            )
            provider = _OversizedOutputBoundFakeProvider()
            with self.assertRaisesRegex(
                RosterDriftError,
                "REQUIRED_MEMBER_RESPONSE_INVALID",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )

            self.assertEqual(provider.call_count, _FAKE_CALL_LIMITS.max_attempts)
            self.assertEqual(
                controller.campaign_snapshot().status.value,
                "BLOCKED",
            )
            campaign_events = journal.list_events(
                cycle_id=None,
                aggregate_type="CAMPAIGN_STATE",
                aggregate_id=campaign_id,
            )
            self.assertEqual(
                json.loads(campaign_events[-1].payload_json)["reason_code"],
                "REQUIRED_MEMBER_RESPONSE_INVALID",
            )

    def test_large_integer_output_blocks_campaign_during_first_logical_call(
        self,
    ) -> None:
        if not (
            hasattr(sys, "get_int_max_str_digits")
            and hasattr(sys, "set_int_max_str_digits")
        ):
            self.skipTest("CPython integer digit-limit API is unavailable")
        campaign_id = "campaign-controller-large-integer-output"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Large JSON integers remain terminal failures",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 134, 34_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-large-integer-output",
            )
            provider = _LargeIntegerOutputBoundFakeProvider()
            original_limit = sys.get_int_max_str_digits()
            try:
                sys.set_int_max_str_digits(0)
                with self.assertRaisesRegex(
                    RosterDriftError,
                    "REQUIRED_MEMBER_RESPONSE_INVALID",
                ):
                    controller.invoke_member_json(
                        execution=execution,
                        member_id=member.member_id,
                        provider=provider,
                        prompt=prompt,
                        limits=_FAKE_CALL_LIMITS,
                    )
            finally:
                sys.set_int_max_str_digits(original_limit)

            self.assertEqual(provider.call_count, _FAKE_CALL_LIMITS.max_attempts)
            self.assertEqual(
                controller.campaign_snapshot().status.value,
                "BLOCKED",
            )
            campaign_events = journal.list_events(
                cycle_id=None,
                aggregate_type="CAMPAIGN_STATE",
                aggregate_id=campaign_id,
            )
            self.assertEqual(
                json.loads(campaign_events[-1].payload_json)["reason_code"],
                "REQUIRED_MEMBER_RESPONSE_INVALID",
            )
            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id=task.task_id,
            ).list_attempts()
            self.assertEqual(len(attempts), _FAKE_CALL_LIMITS.max_attempts)
            self.assertTrue(
                all(
                    attempt.final_outcome is InvocationOutcome.INVALID_JSON
                    for attempt in attempts
                )
            )
            replay_provider = _BoundFakeProvider()
            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=replay_provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )
            self.assertEqual(replay_provider.call_count, 0)

    def test_in_doubt_member_blocks_other_provider_calls(self) -> None:
        campaign_id = "campaign-controller-cross-member-in-doubt"
        base_protocol = _protocol()
        second_protocol_member = base_protocol.roster[0].model_copy(
            update={
                "role": "source_librarian",
                "provider_profile_id": "offline-local-2",
                "model_id": "deterministic-reviewer-2",
                "public_identity_sha256": "c" * 64,
            }
        )
        protocol = base_protocol.model_copy(
            update={
                "roster": tuple(
                    sorted(
                        (*base_protocol.roster, second_protocol_member),
                        key=lambda item: item.role,
                    )
                )
            }
        )
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompts = {
            "factor_engineer": {"instruction": "Return factor fixture"},
            "source_librarian": {"instruction": "Return source fixture"},
        }
        factor_member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(
                prompts["factor_engineer"]
            ),
        )
        source_member = replace(
            _protocol_member(),
            member_id="source-librarian",
            profile="offline-local-2",
            model="deterministic-reviewer-2",
            role="source_librarian",
            prompt_sha256=operational_prompt_sha256(
                prompts["source_librarian"]
            ),
            config_sha256="4" * 64,
            capability_sha256="5" * 64,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "One in-doubt call stops the frozen roster",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 130, 30_000)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=4,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                    3_000_000,
                ),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(factor_member, source_member),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=40,
                    max_output_tokens=20,
                    max_cost="0.2",
                    max_wall_time_ms=_SPAWN_DOUBLE_CALL_WALL_TIME_MS,
                    max_tool_attempts=4,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-cross-member-in-doubt",
            )
            with patch(
                "research_automation.control_plane.campaign_controller."
                "RetryingModelInvocation.invoke_json_with_receipt",
                side_effect=RuntimeError("synthetic mid-call crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "mid-call crash"):
                    controller.invoke_member_json(
                        execution=execution,
                        member_id=factor_member.member_id,
                        provider=_BoundFakeProvider(),
                        prompt=prompts[factor_member.role],
                        limits=_FAKE_CALL_LIMITS,
                    )

            provider = _BoundFakeProvider()
            provider.profile = source_member.profile
            provider.model = source_member.model
            provider.config_sha256 = source_member.config_sha256
            provider.capability_sha256 = source_member.capability_sha256
            with self.assertRaisesRegex(
                CampaignJournalError,
                "incomplete and in doubt",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=source_member.member_id,
                    provider=provider,
                    prompt=prompts[source_member.role],
                    limits=_FAKE_CALL_LIMITS,
                )

            self.assertEqual(provider.call_count, 0)
            self.assertEqual(
                controller.campaign_snapshot().status.value,
                "BLOCKED",
            )

    def test_invalid_completed_member_blocks_other_provider_calls(self) -> None:
        campaign_id = "campaign-controller-invalid-completed-member"
        base_protocol = _protocol()
        second_protocol_member = base_protocol.roster[0].model_copy(
            update={
                "role": "source_librarian",
                "provider_profile_id": "offline-local-2",
                "model_id": "deterministic-reviewer-2",
                "public_identity_sha256": "c" * 64,
            }
        )
        protocol = base_protocol.model_copy(
            update={
                "roster": tuple(
                    sorted(
                        (*base_protocol.roster, second_protocol_member),
                        key=lambda item: item.role,
                    )
                )
            }
        )
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompts = {
            "factor_engineer": {"instruction": "Return factor fixture"},
            "source_librarian": {"instruction": "Return source fixture"},
        }
        factor_member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(
                prompts["factor_engineer"]
            ),
        )
        source_member = replace(
            _protocol_member(),
            member_id="source-librarian",
            profile="offline-local-2",
            model="deterministic-reviewer-2",
            role="source_librarian",
            prompt_sha256=operational_prompt_sha256(
                prompts["source_librarian"]
            ),
            config_sha256="4" * 64,
            capability_sha256="5" * 64,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Invalid completion cannot unlock another call",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 142, 42_000)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=4,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(factor_member, source_member),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=40,
                    max_output_tokens=20,
                    max_cost="0.2",
                    max_wall_time_ms=_SPAWN_DOUBLE_CALL_WALL_TIME_MS,
                    max_tool_attempts=4,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-invalid-completed-member",
            )
            with patch(
                "research_automation.control_plane.campaign_controller."
                "RetryingModelInvocation.invoke_json_with_receipt",
                side_effect=RuntimeError("synthetic mid-call crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "mid-call crash"):
                    controller.invoke_member_json(
                        execution=execution,
                        member_id=factor_member.member_id,
                        provider=_BoundFakeProvider(),
                        prompt=prompts[factor_member.role],
                        limits=_FAKE_CALL_LIMITS,
                    )

            factor_call_id = _controller_event_id(
                b"control_plane.controller_member_call.v1",
                journal.namespace,
                campaign_id,
                task.task_id,
                factor_member.member_id,
            )
            journal.append(
                event_id=_controller_event_id(
                    b"control_plane.controller_model_call_result.v1",
                    journal.namespace,
                    campaign_id,
                    task.task_id,
                    factor_call_id,
                    "complete",
                ),
                cycle_id=task.task_id,
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=factor_call_id,
                event_type="OPERATIONAL_MODEL_CALL_COMPLETED",
                payload={"synthetic": "not-a-call-receipt"},
            )
            provider = _BoundFakeProvider()
            provider.profile = source_member.profile
            provider.model = source_member.model
            provider.config_sha256 = source_member.config_sha256
            provider.capability_sha256 = source_member.capability_sha256

            with self.assertRaisesRegex(
                CampaignJournalError,
                "incomplete and in doubt",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=source_member.member_id,
                    provider=provider,
                    prompt=prompts[source_member.role],
                    limits=_FAKE_CALL_LIMITS,
                )

            self.assertEqual(provider.call_count, 0)
            self.assertEqual(
                controller.campaign_snapshot().status.value,
                "BLOCKED",
            )

    def test_reconstructed_execution_receipt_cannot_cross_local_owner(self) -> None:
        campaign_id = "campaign-controller-local-owner-fence"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "A persisted receipt cannot transfer lease ownership",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        first_owner = ProcessIdentity("host-controller", 140, 40_000)
        other_owner = ProcessIdentity("host-controller", 141, 41_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=_FakeProcessIdentityProvider(first_owner),
                monotonic_ns=lambda: 100,
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-original-owner",
            )

            other_identity = _FakeProcessIdentityProvider(other_owner)
            other_controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=other_identity,
                monotonic_ns=_FakeMonotonicClock(1_000_000, 2_000_000),
            )
            observed_lease = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=other_identity,
                monotonic_ns=lambda: 3_000_000,
            ).snapshot(cycle_id=task.task_id)
            reconstructed = ExecutingOperationalCycle(
                cycle=other_controller.cycle_snapshot(task.task_id),
                lease=observed_lease,
            )
            provider = _BoundFakeProvider()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                other_controller.invoke_member_json(
                    execution=reconstructed,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )

            self.assertEqual(provider.call_count, 0)

    def test_replaced_lease_blocks_provider_before_model_call_start(self) -> None:
        campaign_id = "campaign-controller-lease-swap"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "A replaced lease fences the provider call",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        first_owner = ProcessIdentity("host-controller", 124, 24_000)
        recovered_owner = ProcessIdentity("host-controller", 125, 25_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        reservation_limits = CycleReservationLimits(
            currency="USD",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        barrier = Barrier(2)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=_FakeProcessIdentityProvider(first_owner),
                monotonic_ns=lambda: 100,
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=reservation_limits,
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-first-generation",
            )
            provider = _LeaseSwapBoundFakeProvider(barrier)
            recovery_identity = _FakeProcessIdentityProvider(
                recovered_owner,
                process_starts={
                    (first_owner.host_id, first_owner.pid): None,
                },
            )
            recovery_leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 1_000_000,
            )

            with ThreadPoolExecutor(max_workers=1) as pool:
                invocation = pool.submit(
                    controller.invoke_member_json,
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )
                barrier.wait(timeout=5)
                replacement = recovery_leases.recover(
                    cycle_id=task.task_id,
                    acquisition_id="execute-recovered-generation",
                    stale_after_ns=1,
                )
                barrier.wait(timeout=5)
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "execution receipt is stale",
                ):
                    invocation.result(timeout=5)

            self.assertEqual(provider.call_count, 0)
            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=recovery_identity,
                monotonic_ns=_FakeMonotonicClock(2_000_000, 3_000_000),
            )
            recovered_execution = ExecutingOperationalCycle(
                cycle=recovered.cycle_snapshot(task.task_id),
                lease=replacement,
            )
            replacement_provider = _BoundFakeProvider()

            recovered.invoke_member_json(
                execution=recovered_execution,
                member_id=member.member_id,
                provider=replacement_provider,
                prompt=prompt,
                limits=_FAKE_CALL_LIMITS,
            )

            self.assertEqual(replacement_provider.call_count, 1)

    def test_replaced_lease_blocks_post_provider_completion_writes(self) -> None:
        campaign_id = "campaign-controller-post-provider-lease-swap"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Post-provider writes retain lease fencing",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        first_owner = ProcessIdentity("host-controller", 128, 28_000)
        recovered_owner = ProcessIdentity("host-controller", 129, 29_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        barrier = Barrier(2)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=_FakeProcessIdentityProvider(first_owner),
                monotonic_ns=_LeaseSwapMonotonicClock(barrier),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-first-generation",
            )
            provider = _BoundFakeProvider()
            recovery_identity = _FakeProcessIdentityProvider(
                recovered_owner,
                process_starts={
                    (first_owner.host_id, first_owner.pid): None,
                },
            )
            recovery_leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 3_000_000,
            )

            with ThreadPoolExecutor(max_workers=1) as pool:
                invocation = pool.submit(
                    controller.invoke_member_json,
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )
                barrier.wait(timeout=5)
                replacement = recovery_leases.recover(
                    cycle_id=task.task_id,
                    acquisition_id="execute-recovered-generation",
                    stale_after_ns=1,
                )
                barrier.wait(timeout=5)
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "execution receipt is stale",
                ):
                    invocation.result(timeout=5)

            self.assertEqual(provider.call_count, 1)
            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 4_000_000,
            )
            recovered_execution = ExecutingOperationalCycle(
                cycle=recovered.cycle_snapshot(task.task_id),
                lease=replacement,
            )
            replay_provider = _BoundFakeProvider()
            with self.assertRaisesRegex(
                CampaignJournalError,
                "incomplete and in doubt",
            ):
                recovered.invoke_member_json(
                    execution=recovered_execution,
                    member_id=member.member_id,
                    provider=replay_provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )

            self.assertEqual(replay_provider.call_count, 0)
            self.assertEqual(
                recovered.campaign_snapshot().status.value,
                "BLOCKED",
            )

    def test_replaced_lease_fences_response_and_call_completion(self) -> None:
        campaign_id = "campaign-controller-lease-swap-after-response"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "A replaced lease fences provider-side results",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        first_owner = ProcessIdentity("host-controller", 128, 28_000)
        recovered_owner = ProcessIdentity("host-controller", 129, 29_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        reservation_limits = CycleReservationLimits(
            currency="USD",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        barrier = Barrier(2)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=_FakeProcessIdentityProvider(first_owner),
                monotonic_ns=_LeaseSwapMonotonicClock(barrier),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=reservation_limits,
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-first-generation",
            )
            provider = _BoundFakeProvider()
            recovery_identity = _FakeProcessIdentityProvider(
                recovered_owner,
                process_starts={
                    (first_owner.host_id, first_owner.pid): None,
                },
            )
            recovery_leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 3_000_000,
            )

            with ThreadPoolExecutor(max_workers=1) as pool:
                invocation = pool.submit(
                    controller.invoke_member_json,
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )
                barrier.wait(timeout=5)
                recovery_leases.recover(
                    cycle_id=task.task_id,
                    acquisition_id="execute-recovered-generation",
                    stale_after_ns=1,
                )
                barrier.wait(timeout=5)
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "execution receipt is stale",
                ):
                    invocation.result(timeout=5)

            self.assertEqual(provider.call_count, 1)
            self.assertEqual(
                OperationalRosterJournal(
                    journal=journal,
                    lifecycle=OperationalCampaignLifecycle(journal=journal),
                ).snapshot(cycle_id=task.task_id).verified_member_ids,
                (),
            )
            call_id = controller._member_call_id(
                task.task_id,
                member.member_id,
            )
            self.assertEqual(
                tuple(
                    event.event_type
                    for event in journal.list_events(
                        cycle_id=task.task_id,
                        aggregate_type="OPERATIONAL_MODEL_CALL",
                        aggregate_id=call_id,
                    )
                ),
                ("OPERATIONAL_MODEL_CALL_STARTED",),
            )

    def test_two_frozen_fake_members_complete_one_usage_inventory(self) -> None:
        campaign_id = "campaign-controller-022"
        base_protocol = _protocol()
        second_protocol_member = base_protocol.roster[0].model_copy(
            update={
                "role": "source_librarian",
                "provider_profile_id": "offline-local-2",
                "model_id": "deterministic-reviewer-2",
                "public_identity_sha256": "c" * 64,
            }
        )
        protocol = base_protocol.model_copy(
            update={
                "roster": tuple(
                    sorted(
                        (*base_protocol.roster, second_protocol_member),
                        key=lambda item: item.role,
                    )
                )
            }
        )
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompts = {
            "factor_engineer": {"instruction": "Return factor fixture"},
            "source_librarian": {"instruction": "Return source fixture"},
        }
        factor_member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(
                prompts["factor_engineer"]
            ),
        )
        source_member = replace(
            _protocol_member(),
            member_id="source-librarian",
            profile="offline-local-2",
            model="deterministic-reviewer-2",
            role="source_librarian",
            prompt_sha256=operational_prompt_sha256(
                prompts["source_librarian"]
            ),
            config_sha256="4" * 64,
            capability_sha256="5" * 64,
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Two fake roles keep disjoint call streams",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 122, 22_000)
        monotonic = _FakeMonotonicClock(
            100,
            1_000_000,
            3_000_000,
            4_000_000,
            7_000_000,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=4,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=monotonic,
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(source_member, factor_member),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=40,
                    max_output_tokens=20,
                    max_cost="0.2",
                    max_wall_time_ms=_SPAWN_DOUBLE_CALL_WALL_TIME_MS,
                    max_tool_attempts=4,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-two-fake-members",
            )
            providers = {}
            for member in (factor_member, source_member):
                provider = _BoundFakeProvider()
                provider.profile = member.profile
                provider.model = member.model
                provider.config_sha256 = member.config_sha256
                provider.capability_sha256 = member.capability_sha256
                providers[member.member_id] = provider
                if member is source_member:
                    with self.assertRaises(BudgetExceededError):
                        controller.invoke_member_json(
                            execution=execution,
                            member_id=member.member_id,
                            provider=provider,
                            prompt=prompts[member.role],
                            limits=OperationalModelCallLimits(
                                currency="USD",
                                max_input_tokens=21,
                                max_output_tokens=10,
                                max_cost="0.1",
                                max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                                max_attempts=2,
                            ),
                        )
                    self.assertEqual(provider.call_count, 0)
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompts[member.role],
                    limits=_FAKE_CALL_LIMITS,
                )

            usage = controller.complete_model_execution(execution=execution)

            self.assertEqual(len(usage.model_calls), 2)
            self.assertEqual(
                tuple(call.member_id for call in usage.model_calls),
                (factor_member.member_id, source_member.member_id),
            )
            self.assertEqual(
                len({call.call_id for call in usage.model_calls}),
                2,
            )
            self.assertEqual(usage.usage_status, UsageStatus.REPORTED)
            self.assertEqual(usage.input_tokens, 14)
            self.assertEqual(usage.output_tokens, 6)
            self.assertEqual(usage.cost, "0.04")
            self.assertEqual(usage.wall_time_ms, 5)
            self.assertEqual(usage.tool_attempts, 2)
            self.assertTrue(
                all(provider.call_count == 1 for provider in providers.values())
            )

    def test_model_call_cost_allocations_are_summed_exactly(self) -> None:
        base_protocol = _protocol()
        second_protocol_member = base_protocol.roster[0].model_copy(
            update={
                "role": "source_librarian",
                "provider_profile_id": "offline-local-2",
                "model_id": "deterministic-reviewer-2",
                "public_identity_sha256": "c" * 64,
            }
        )
        protocol = base_protocol.model_copy(
            update={
                "roster": tuple(
                    sorted(
                        (*base_protocol.roster, second_protocol_member),
                        key=lambda item: item.role,
                    )
                )
            }
        )
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompts = {
            "factor_engineer": {"instruction": "Return factor fixture"},
            "source_librarian": {"instruction": "Return source fixture"},
        }
        factor_member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(
                prompts["factor_engineer"]
            ),
        )
        source_member = replace(
            _protocol_member(),
            member_id="source-librarian",
            profile="offline-local-2",
            model="deterministic-reviewer-2",
            role="source_librarian",
            prompt_sha256=operational_prompt_sha256(
                prompts["source_librarian"]
            ),
            config_sha256="4" * 64,
            capability_sha256="5" * 64,
        )
        hostile_context = Context(prec=2)
        hostile_context.traps[Inexact] = True
        hostile_context.traps[Rounded] = True
        cases = (
            ("default", Context()),
            ("hostile", hostile_context),
        )

        for label, decimal_context in cases:
            with self.subTest(decimal_context=label), localcontext(
                decimal_context
            ):
                campaign_id = f"campaign-controller-allocation-cost-{label}"
                cycle_id = f"cycle-allocation-cost-{label}"
                task = ExperimentTask(
                    task_id=cycle_id,
                    strategy="b1",
                    proposal={
                        "hypothesis": "Call allocations are summed exactly",
                        "scope": _scope(generation="generation-1"),
                    },
                    source="synthetic-test",
                )
                owner = ProcessIdentity(
                    f"host-allocation-cost-{label}",
                    150,
                    50_000,
                )
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    controller = OperationalCampaignController(
                        journal=journal,
                        repository_root=root,
                        budget_limits=CampaignBudgetLimits(
                            currency="USD",
                            max_cycles=1,
                            max_input_tokens=40,
                            max_output_tokens=20,
                            max_cost="1",
                            max_wall_time_ms=_SPAWN_DOUBLE_CALL_WALL_TIME_MS,
                            max_tool_attempts=4,
                        ),
                        identity_provider=_FakeProcessIdentityProvider(owner),
                        monotonic_ns=_FakeMonotonicClock(
                            100,
                            1_000_000,
                            3_000_000,
                            4_000_000,
                            7_000_000,
                        ),
                    )
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=(source_member, factor_member),
                        reservation_limits=CycleReservationLimits(
                            currency="USD",
                            max_input_tokens=40,
                            max_output_tokens=20,
                            max_cost="1",
                            max_wall_time_ms=_SPAWN_DOUBLE_CALL_WALL_TIME_MS,
                            max_tool_attempts=4,
                        ),
                    )
                    execution = controller.start_execution(
                        cycle_id=cycle_id,
                        acquisition_id=f"execute-allocation-cost-{label}",
                    )
                    factor_provider = _BoundFakeProvider()
                    source_provider = _BoundFakeProvider()
                    for member, provider in (
                        (factor_member, factor_provider),
                        (source_member, source_provider),
                    ):
                        provider.profile = member.profile
                        provider.model = member.model
                        provider.config_sha256 = member.config_sha256
                        provider.capability_sha256 = member.capability_sha256

                    first_call = controller.invoke_member_json(
                        execution=execution,
                        member_id=factor_member.member_id,
                        provider=factor_provider,
                        prompt=prompts[factor_member.role],
                        limits=OperationalModelCallLimits(
                            currency="USD",
                            max_input_tokens=20,
                            max_output_tokens=10,
                            max_cost=(
                                "0.99999999999999999999999999995"
                            ),
                            max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                            max_attempts=2,
                        ),
                    )
                    usage = OperationalUsageJournal(
                        journal=journal,
                        cycle_id=cycle_id,
                    )
                    first_events_before = journal.list_events(
                        cycle_id=cycle_id,
                        aggregate_type="OPERATIONAL_MODEL_CALL",
                        aggregate_id=first_call.call_id,
                    )
                    first_attempts_before = usage.list_attempts(
                        call_id=first_call.call_id
                    )
                    self.assertEqual(factor_provider.call_count, 1)
                    self.assertEqual(
                        tuple(
                            event.event_type for event in first_events_before
                        ),
                        (
                            "OPERATIONAL_MODEL_CALL_STARTED",
                            "OPERATIONAL_MODEL_CALL_COMPLETED",
                        ),
                    )
                    self.assertEqual(len(first_attempts_before), 1)
                    self.assertEqual(
                        first_attempts_before[0].final_outcome.value,
                        "SUCCESS",
                    )

                    source_call_id = controller._member_call_id(
                        cycle_id,
                        source_member.member_id,
                    )
                    with self.assertRaises(BudgetExceededError) as rejected:
                        controller.invoke_member_json(
                            execution=execution,
                            member_id=source_member.member_id,
                            provider=source_provider,
                            prompt=prompts[source_member.role],
                            limits=OperationalModelCallLimits(
                                currency="USD",
                                max_input_tokens=20,
                                max_output_tokens=10,
                                max_cost=(
                                    "0.00000000000000000000000000006"
                                ),
                                max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                                max_attempts=2,
                            ),
                        )

                    self.assertEqual(source_provider.call_count, 0)
                    self.assertEqual(
                        str(rejected.exception),
                        "model call allocations exceed the Cycle reservation",
                    )
                    source_events = journal.list_events(
                        cycle_id=cycle_id,
                        aggregate_type="OPERATIONAL_MODEL_CALL",
                        aggregate_id=source_call_id,
                    )
                    self.assertEqual(source_events, ())
                    self.assertEqual(
                        journal.list_events(
                            cycle_id=cycle_id,
                            aggregate_type="OPERATIONAL_MODEL_CALL",
                            aggregate_id=first_call.call_id,
                        ),
                        first_events_before,
                    )
                    self.assertEqual(
                        usage.list_attempts(call_id=first_call.call_id),
                        first_attempts_before,
                    )
                    self.assertEqual(
                        controller.campaign_snapshot().status,
                        CampaignStatus.ACTIVE,
                    )
                    self.assertEqual(
                        controller.cycle_snapshot(cycle_id).status,
                        CycleStatus.EXECUTING,
                    )

    def test_known_call_usage_above_its_limits_blocks_immediately(
        self,
    ) -> None:
        campaign_id = "campaign-controller-023"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Known usage stays inside its call allocation",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 123, 23_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(100, 1_000_000, 2_000_000),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-over-limit-member",
            )
            provider = _BoundFakeProvider()
            with self.assertRaisesRegex(
                BudgetExceededError,
                "known usage exceeds its call limits",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=OperationalModelCallLimits(
                        currency="USD",
                        max_input_tokens=6,
                        max_output_tokens=10,
                        max_cost="0.1",
                        max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                        max_attempts=2,
                    ),
                )

            self.assertEqual(provider.call_count, 1)
            self.assertEqual(
                controller.campaign_snapshot().status.value,
                "BLOCKED",
            )

    def test_completed_call_replay_rechecks_known_usage_limits(self) -> None:
        campaign_id = "campaign-controller-completed-limit-replay"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Replay enforces the persisted call allocation",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 143, 43_000)
        limits = OperationalModelCallLimits(
            currency="USD",
            max_input_tokens=6,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
            max_attempts=2,
        )

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(100, 1_000_000, 2_000_000),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-completed-limit-replay",
            )
            with patch.object(
                OperationalCampaignController,
                "_require_known_model_call_usage_within_limits",
                return_value=None,
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=_BoundFakeProvider(),
                    prompt=prompt,
                    limits=limits,
                )

            replay_provider = _BoundFakeProvider()
            with self.assertRaisesRegex(
                BudgetExceededError,
                "known usage exceeds its call limits",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=replay_provider,
                    prompt=prompt,
                    limits=limits,
                )

            self.assertEqual(replay_provider.call_count, 0)
            self.assertEqual(
                controller.campaign_snapshot().status.value,
                "BLOCKED",
            )

    def test_unknown_attempt_cannot_hide_a_known_usage_overrun(self) -> None:
        campaign_id = "campaign-controller-unknown-known-overrun"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Unknown usage cannot erase a known lower bound",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 131, 31_000)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-unknown-known-overrun",
            )
            provider = _BoundFakeProvider(timeouts_before_success=1)

            with self.assertRaisesRegex(
                BudgetExceededError,
                "known usage exceeds its call limits",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=OperationalModelCallLimits(
                        currency="USD",
                        max_input_tokens=6,
                        max_output_tokens=10,
                        max_cost="0.1",
                        max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                        max_attempts=2,
                    ),
                )

            attempts = OperationalUsageJournal(
                journal=journal,
                cycle_id=task.task_id,
            ).list_attempts()
            self.assertEqual(provider.call_count, 2)
            self.assertEqual(len(attempts), 2)
            self.assertIsNone(attempts[0].envelope.input_tokens)
            self.assertEqual(attempts[1].envelope.input_tokens, 7)
            self.assertEqual(
                controller.campaign_snapshot().status.value,
                "BLOCKED",
            )

    def test_execution_usage_without_cost_currency_is_canonical_unknown(
        self,
    ) -> None:
        campaign_id = "campaign-controller-missing-currency"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Cost without currency stays unknown",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 126, 26_000)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-missing-currency",
            )
            controller.invoke_member_json(
                execution=execution,
                member_id=member.member_id,
                provider=_MissingCurrencyBoundFakeProvider(),
                prompt=prompt,
                limits=_FAKE_CALL_LIMITS,
            )

            usage = controller.complete_model_execution(execution=execution)

            self.assertEqual(usage.usage_status, UsageStatus.UNKNOWN)
            self.assertIsNone(usage.cost)
            self.assertEqual(usage.currency, "USD")
            usage_event = journal.list_events(
                cycle_id=task.task_id,
                aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                aggregate_id=task.task_id,
            )[0]
            payload = usage_event.payload()
            payload.pop("_authority_grant_id")
            identity = {
                key: value
                for key, value in payload.items()
                if key != "manifest_sha256"
            }
            self.assertEqual(
                payload["schema_version"],
                "control_plane.operational_execution_usage.v2",
            )
            self.assertEqual(payload["currency"], "USD")
            self.assertEqual(
                payload["manifest_sha256"],
                _controller_sha256(
                    b"control_plane.operational_execution_usage.v2",
                    identity,
                    "expected operational execution usage",
                ),
            )
            self.assertEqual(
                usage.event_id,
                _controller_event_id(
                    b"control_plane.controller_execution_usage.v1",
                    "formal",
                    campaign_id,
                    task.task_id,
                ),
            )

    def test_execution_usage_preserves_estimated_attempt_status(self) -> None:
        campaign_id = "campaign-controller-estimated-usage"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Estimated attempts stay estimated in aggregate",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 127, 27_000)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-estimated-usage",
            )
            controller.invoke_member_json(
                execution=execution,
                member_id=member.member_id,
                provider=_EstimatedUsageBoundFakeProvider(),
                prompt=prompt,
                limits=_FAKE_CALL_LIMITS,
            )

            usage = controller.complete_model_execution(execution=execution)

            self.assertEqual(usage.usage_status, UsageStatus.ESTIMATED)
            self.assertEqual(usage.input_tokens, 7)
            self.assertEqual(usage.output_tokens, 3)
            self.assertEqual(usage.cost, "0.02")
            self.assertEqual(usage.currency, "USD")

    def test_replaced_lease_fences_execution_usage_after_roster_completion(
        self,
    ) -> None:
        campaign_id = "campaign-controller-usage-lease-swap"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one bounded synthetic result"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Execution usage retains its lease fence",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        first_owner = ProcessIdentity("host-controller", 131, 31_000)
        recovered_owner = ProcessIdentity("host-controller", 132, 32_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        barrier = Barrier(2)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=_FakeProcessIdentityProvider(first_owner),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-first-generation",
            )
            controller.invoke_member_json(
                execution=execution,
                member_id=member.member_id,
                provider=_BoundFakeProvider(),
                prompt=prompt,
                limits=_FAKE_CALL_LIMITS,
            )
            recovery_identity = _FakeProcessIdentityProvider(
                recovered_owner,
                process_starts={
                    (first_owner.host_id, first_owner.pid): None,
                },
            )
            recovery_leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 3_000_000,
            )
            original_complete = OperationalRosterJournal.complete_responses

            def complete_then_wait(roster, *, cycle_id, **kwargs):
                result = original_complete(
                    roster,
                    cycle_id=cycle_id,
                    **kwargs,
                )
                barrier.wait(timeout=5)
                barrier.wait(timeout=5)
                return result

            with patch.object(
                OperationalRosterJournal,
                "complete_responses",
                complete_then_wait,
            ), ThreadPoolExecutor(max_workers=1) as pool:
                completion = pool.submit(
                    controller.complete_model_execution,
                    execution=execution,
                )
                barrier.wait(timeout=5)
                replacement = recovery_leases.recover(
                    cycle_id=task.task_id,
                    acquisition_id="execute-recovered-generation",
                    stale_after_ns=1,
                )
                barrier.wait(timeout=5)
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "execution receipt is stale",
                ):
                    completion.result(timeout=5)

            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 4_000_000,
            )
            recovered_execution = ExecutingOperationalCycle(
                cycle=recovered.cycle_snapshot(task.task_id),
                lease=replacement,
            )

            usage = recovered.complete_model_execution(
                execution=recovered_execution,
            )

            self.assertEqual(usage.usage_status, UsageStatus.REPORTED)

    def test_model_output_evidence_advances_the_fenced_cycle(self) -> None:
        campaign_id = "campaign-controller-model-evidence"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        prompt = {"instruction": "Return one synthetic runner artifact"}
        member = replace(
            _protocol_member(),
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        task = ExperimentTask(
            task_id="cycle-001",
            strategy="b1",
            proposal={
                "hypothesis": "Synthetic output becomes bounded evidence",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 144, 44_000)

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            controller.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(member,),
                reservation_limits=CycleReservationLimits(
                    currency="USD",
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=_SPAWN_CALL_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-model-evidence",
            )
            controller.invoke_member_json(
                execution=execution,
                member_id=member.member_id,
                provider=_EvidenceArtifactBoundFakeProvider(),
                prompt=prompt,
                limits=_FAKE_CALL_LIMITS,
            )
            usage = controller.complete_model_execution(execution=execution)

            receipt = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )

            self.assertEqual(receipt.cycle_id, task.task_id)
            self.assertEqual(receipt.member_id, member.member_id)
            self.assertEqual(
                receipt.execution_usage_manifest_sha256,
                usage.manifest_sha256,
            )
            self.assertEqual(
                receipt.model_call_manifest_sha256,
                usage.model_calls[0].manifest_sha256,
            )
            self.assertEqual(receipt.evidence.verdict, "NO_MATERIAL_FINDING")
            self.assertEqual(
                controller.cycle_snapshot(task.task_id).status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_model_output_evidence_exact_replay_is_idempotent(self) -> None:
        campaign_id = "campaign-controller-model-evidence-replay"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            adapter = EvidenceAdapter(
                known_runners={"fixture-runner": "1.0.0"},
                approved_protocol={"label": "synthetic-only"},
            )
            first = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=adapter,
            )
            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-controller", 144, 44_000)
                ),
                monotonic_ns=lambda: 3_000_000,
            )

            replayed = reopened.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=adapter,
            )

            self.assertEqual(replayed, first)

    def test_evidence_receipt_recovers_from_current_state_and_new_lease(
        self,
    ) -> None:
        campaign_id = "campaign-controller-model-evidence-durable-recovery"
        recovered_owner = ProcessIdentity("host-controller", 146, 46_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            adapter = EvidenceAdapter(
                known_runners={"fixture-runner": "1.0.0"},
                approved_protocol={"label": "synthetic-only"},
            )
            original = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=adapter,
            )
            recovery_identity = _FakeProcessIdentityProvider(
                recovered_owner,
                process_starts={("host-controller", 144): None},
            )
            replacement = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 3_000_000,
            ).recover(
                cycle_id="cycle-001",
                acquisition_id="recover-completed-evidence",
                stale_after_ns=1,
            )
            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 4_000_000,
            )
            recovered_execution = ExecutingOperationalCycle(
                cycle=recovered.cycle_snapshot("cycle-001"),
                lease=replacement,
            )

            replayed = recovered.record_model_evidence(
                execution=recovered_execution,
                member_id=member.member_id,
                evidence_adapter=adapter,
            )

            self.assertEqual(replayed, original)
            self.assertEqual(
                recovered_execution.cycle.status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_model_output_evidence_rejects_adapter_configuration_drift(
        self,
    ) -> None:
        campaign_id = "campaign-controller-model-evidence-adapter-drift"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "operational model evidence conflicts",
            ):
                controller.record_model_evidence(
                    execution=execution,
                    member_id=member.member_id,
                    evidence_adapter=EvidenceAdapter(
                        known_runners={
                            "fixture-runner": "1.0.0",
                            "unused-runner": "9.9.9",
                        },
                        approved_protocol={"label": "synthetic-only"},
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_replaced_lease_fences_model_evidence_recording(self) -> None:
        campaign_id = "campaign-controller-model-evidence-lease-swap"
        recovered_owner = ProcessIdentity("host-controller", 145, 45_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            recovery_identity = _FakeProcessIdentityProvider(
                recovered_owner,
                process_starts={("host-controller", 144): None},
            )
            replacement = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 3_000_000,
            ).recover(
                cycle_id="cycle-001",
                acquisition_id="execute-evidence-recovered-generation",
                stale_after_ns=1,
            )
            adapter = EvidenceAdapter(
                known_runners={"fixture-runner": "1.0.0"},
                approved_protocol={"label": "synthetic-only"},
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.record_model_evidence(
                    execution=execution,
                    member_id=member.member_id,
                    evidence_adapter=adapter,
                )

            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 4_000_000,
            )
            receipt = recovered.record_model_evidence(
                execution=ExecutingOperationalCycle(
                    cycle=recovered.cycle_snapshot("cycle-001"),
                    lease=replacement,
                ),
                member_id=member.member_id,
                evidence_adapter=adapter,
            )

            self.assertEqual(receipt.cycle_id, "cycle-001")
            self.assertEqual(
                recovered.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_blocked_campaign_fences_model_evidence_recording(self) -> None:
        campaign_id = "campaign-controller-model-evidence-blocked"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            OperationalCampaignLifecycle(journal=journal).block(
                reason_code="SYNTHETIC_EVIDENCE_BLOCK",
                source_ref="fixture-block",
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.record_model_evidence(
                    execution=execution,
                    member_id=member.member_id,
                    evidence_adapter=EvidenceAdapter(
                        known_runners={"fixture-runner": "1.0.0"},
                        approved_protocol={"label": "synthetic-only"},
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )

    def test_shadow_model_call_after_usage_freeze_blocks_model_evidence(
        self,
    ) -> None:
        campaign_id = "campaign-controller-model-evidence-shadow-call"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            journal.append(
                event_id="shadow-model-call-after-usage-freeze",
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id="shadow-model-call",
                event_type="OPERATIONAL_MODEL_CALL_STARTED",
                payload={"shadow": True},
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution usage model call inventory conflicts",
            ):
                controller.record_model_evidence(
                    execution=execution,
                    member_id=member.member_id,
                    evidence_adapter=EvidenceAdapter(
                        known_runners={"fixture-runner": "1.0.0"},
                        approved_protocol={"label": "synthetic-only"},
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )

    def test_shadow_usage_attempt_after_freeze_blocks_model_evidence(
        self,
    ) -> None:
        campaign_id = "campaign-controller-model-evidence-shadow-attempt"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            ).begin(
                UsageEnvelope(
                    provider=member.provider,
                    profile=member.profile,
                    request_model=member.model,
                    response_model=None,
                    call_id="shadow-call",
                    attempt_id="shadow-call-attempt-001",
                    usage_status=UsageStatus.UNKNOWN,
                    input_tokens=None,
                    output_tokens=None,
                    total_tokens=None,
                    cache_read_tokens=None,
                    cache_write_tokens=None,
                    reasoning_tokens=None,
                    reported_cost=None,
                    currency=None,
                    fallback=False,
                    streamed=False,
                    outcome=InvocationOutcome.TIMEOUT,
                    raw_usage_sha256="4" * 64,
                )
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution usage attempt inventory conflicts",
            ):
                controller.record_model_evidence(
                    execution=execution,
                    member_id=member.member_id,
                    evidence_adapter=EvidenceAdapter(
                        known_runners={"fixture-runner": "1.0.0"},
                        approved_protocol={"label": "synthetic-only"},
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )

    def test_forged_frozen_usage_totals_block_model_evidence(self) -> None:
        campaign_id = "campaign-controller-model-evidence-forged-usage"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            usage_event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                aggregate_id="cycle-001",
            )[0]
            payload = json.loads(usage_event.payload_json)
            payload["input_tokens"] = 8
            identity = {
                key: value
                for key, value in payload.items()
                if key not in {"manifest_sha256", "_authority_grant_id"}
            }
            payload["manifest_sha256"] = _controller_sha256(
                b"control_plane.operational_execution_usage.v2",
                identity,
                "forged operational execution usage",
            )
            payload_json = json.dumps(
                payload,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            forged_integrity = _event_integrity_sha256(
                event_id=usage_event.event_id,
                namespace=usage_event.namespace,
                campaign_id=usage_event.campaign_id,
                cycle_id=usage_event.cycle_id,
                aggregate_type=usage_event.aggregate_type,
                aggregate_id=usage_event.aggregate_id,
                event_type=usage_event.event_type,
                payload_json=payload_json,
                occurred_at=usage_event.occurred_at.isoformat(),
                sequence=usage_event.sequence,
            )
            connection = sqlite3.connect(root / "operational.sqlite3")
            try:
                connection.execute(
                    "UPDATE campaign_events SET payload_json = ?, "
                    "payload_sha256 = ? WHERE event_id = ?",
                    (payload_json, forged_integrity, usage_event.event_id),
                )
                connection.commit()
            finally:
                connection.close()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "frozen operational execution usage conflicts",
            ):
                controller.record_model_evidence(
                    execution=execution,
                    member_id=member.member_id,
                    evidence_adapter=EvidenceAdapter(
                        known_runners={"fixture-runner": "1.0.0"},
                        approved_protocol={"label": "synthetic-only"},
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )

    def test_shadow_evidence_stream_blocks_model_evidence(self) -> None:
        campaign_id = "campaign-controller-model-evidence-shadow-stream"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            journal.append(
                event_id="shadow-model-evidence-event",
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_MODEL_EVIDENCE",
                aggregate_id="shadow-cycle-001",
                event_type="OPERATIONAL_MODEL_EVIDENCE_RECORDED",
                payload={"shadow": True},
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "operational model evidence stream conflicts",
            ):
                controller.record_model_evidence(
                    execution=execution,
                    member_id=member.member_id,
                    evidence_adapter=EvidenceAdapter(
                        known_runners={"fixture-runner": "1.0.0"},
                        approved_protocol={"label": "synthetic-only"},
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )

    def test_invalid_model_artifact_records_ineligible_evidence(self) -> None:
        campaign_id = "campaign-controller-model-evidence-invalid"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_InvalidEvidenceArtifactBoundFakeProvider(),
                )
            )

            receipt = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )

            self.assertEqual(receipt.evidence.verdict, "EVIDENCE_INVALID")
            self.assertFalse(receipt.evidence.promotion_eligible)
            self.assertEqual(
                receipt.evidence.invalidation_codes,
                ("UNKNOWN_RUNNER_SCHEMA",),
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_non_object_model_artifact_records_ineligible_evidence(self) -> None:
        campaign_id = "campaign-controller-model-evidence-non-object"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_NonObjectEvidenceArtifactBoundFakeProvider(),
                )
            )

            receipt = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )

            self.assertEqual(receipt.evidence.verdict, "EVIDENCE_INVALID")
            self.assertEqual(
                receipt.evidence.invalidation_codes,
                ("INVALID_ARTIFACT_TYPE",),
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_unbound_evidence_adapter_subclass_is_rejected(self) -> None:
        campaign_id = "campaign-controller-model-evidence-adapter-subclass"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )

            with self.assertRaisesRegex(
                TypeError,
                "evidence_adapter must be an EvidenceAdapter",
            ):
                controller.record_model_evidence(
                    execution=execution,
                    member_id=member.member_id,
                    evidence_adapter=_UnboundEvidenceAdapter(
                        known_runners={"fixture-runner": "1.0.0"},
                        approved_protocol={"label": "synthetic-only"},
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )

    def test_model_evidence_and_state_transition_are_atomic(self) -> None:
        campaign_id = "campaign-controller-model-evidence-atomic"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            adapter = EvidenceAdapter(
                known_runners={"fixture-runner": "1.0.0"},
                approved_protocol={"label": "synthetic-only"},
            )
            with patch.object(
                OperationalCampaignLifecycle,
                "_advance_cycle_in_transaction",
                side_effect=RuntimeError("synthetic transition crash"),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "synthetic transition crash",
                ):
                    controller.record_model_evidence(
                        execution=execution,
                        member_id=member.member_id,
                        evidence_adapter=adapter,
                    )

            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_MODEL_EVIDENCE",
                    aggregate_id="cycle-001",
                ),
                (),
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )

            receipt = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=adapter,
            )

            self.assertEqual(receipt.cycle_id, "cycle-001")
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_model_artifact_stream_drift_blocks_evidence(self) -> None:
        campaign_id = "campaign-controller-model-evidence-artifact-drift"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            call_id = controller._member_call_id(
                "cycle-001",
                member.member_id,
            )
            journal.append(
                event_id="drifted-model-artifact-event",
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_MODEL_CALL",
                aggregate_id=call_id,
                event_type="OPERATIONAL_MODEL_CALL_COMPLETED",
                payload={"output": {"status": "drifted"}},
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "required operational model call is missing",
            ):
                controller.record_model_evidence(
                    execution=execution,
                    member_id=member.member_id,
                    evidence_adapter=EvidenceAdapter(
                        known_runners={"fixture-runner": "1.0.0"},
                        approved_protocol={"label": "synthetic-only"},
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EXECUTING,
            )

    def test_eligible_evidence_commits_learning_and_advances_cycle(self) -> None:
        campaign_id = "campaign-controller-learning-commit"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value="f" * 64,
            ):
                receipt = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={"synthetic": "terminal-report"},
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )

            self.assertEqual(receipt.cycle_id, "cycle-001")
            self.assertEqual(
                receipt.evidence_manifest_sha256,
                evidence.manifest_sha256,
            )
            self.assertEqual(receipt.packet_hash, "f" * 64)
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )

    def test_authority_bound_learning_packet_advances_the_cycle(self) -> None:
        campaign_id = "campaign-controller-learning-authority"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            protocol = _synthetic_protocol()
            report, binding, artifact, expected_evidence, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root,
                    protocol=protocol,
                )
            )
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_AuthorityEvidenceArtifactBoundFakeProvider(
                        artifact
                    ),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            service = LearningCommitService(repository_root=root)

            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                receipt = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report=report,
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )
                ledger = service.rebuild_ledger()

            self.assertEqual(evidence.evidence, expected_evidence)
            self.assertEqual(
                ledger["packet_hashes"],
                [receipt.packet_hash],
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )

    def _record_protocol_binding_evidence(
        self, root, journal, campaign_id, provider, claim, protocol
    ):
        controller, execution, member, _ = _completed_evidence_model_call(
            root, journal, campaign_id=campaign_id, provider=provider
        )
        adapter_kwargs = (
            {"approved_protocol": protocol}
            if protocol is not None
            else {}
        )
        evidence = controller.record_model_evidence(
            execution=execution,
            member_id=member.member_id,
            evidence_adapter=EvidenceAdapter(
                known_runners={"fixture-runner": "1.0.0"},
                approved_claim=claim,
                **adapter_kwargs,
            ),
        )
        return controller, execution, evidence

    def _assert_protocol_binding_rejected(
        self,
        root,
        journal,
        controller,
        execution,
        evidence,
        report,
        intent_count=0,
    ):
        service = LearningCommitService(repository_root=root)
        with patch.object(
            LearningCommitService,
            "expected_packet_hash",
            side_effect=AssertionError("protocol binding reached packet hash"),
        ), patch.object(
            LearningCommitService,
            "commit",
            side_effect=AssertionError("protocol binding reached commit"),
        ), self.assertRaisesRegex(
            CampaignJournalError,
            "runner protocol conflicts with frozen Cycle",
        ):
            controller.commit_learning(
                execution=execution,
                evidence_receipt=evidence,
                authority_task_report=report,
                learning_commit_sink=CampaignLearningCommitSink(
                    journal=journal,
                    service=service,
                ),
            )
        intents = journal.list_events(
            cycle_id="cycle-001",
            aggregate_type="OPERATIONAL_LEARNING_COMMIT_INTENT",
            aggregate_id="cycle-001",
        )
        self.assertEqual(len(intents), intent_count)
        self.assertEqual(
            journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                aggregate_id="cycle-001",
            ),
            (),
        )
        for path in (
            "research_state/control_plane/learning_commit.sqlite3",
            "research_state/control_plane/learning_packets",
        ):
            self.assertFalse((root / path).exists())
        self.assertEqual(
            controller.cycle_snapshot("cycle-001").status,
            CycleStatus.EVIDENCE_READY,
        )
        return intents

    def _assert_learning_scope_rejected(
        self,
        *,
        campaign_id: str,
        claim_scope: object,
        include_scope: bool = True,
        seed_intent: bool = False,
    ) -> None:
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
        }
        if include_scope:
            claim["scope"] = claim_scope
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, _, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root,
                    claim=claim,
                    protocol=_synthetic_protocol(),
                )
            )
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_AuthorityEvidenceArtifactBoundFakeProvider(
                        artifact
                    ),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            expected_intent_payload = None
            if seed_intent:
                expected_intent_payload = (
                    controller._learning_commit_intent_payload(
                        cycle_id="cycle-001",
                        evidence_receipt=evidence,
                        authority_task_report_sha256=_controller_sha256(
                            b"control_plane.operational_learning_task_report.v1",
                            report,
                            "Authority TaskReport",
                        ),
                        packet_hash="f" * 64,
                    )
                )
                journal.append(
                    event_id=controller._learning_commit_intent_event_id(
                        "cycle-001"
                    ),
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_LEARNING_COMMIT_INTENT",
                    aggregate_id="cycle-001",
                    event_type="OPERATIONAL_LEARNING_COMMIT_INTENT_RECORDED",
                    payload=expected_intent_payload,
                )

            service = LearningCommitService(repository_root=root)
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                side_effect=AssertionError("scope check reached packet hash"),
            ) as expected_packet_hash, patch.object(
                LearningCommitService,
                "commit",
                side_effect=AssertionError("scope check reached commit"),
            ) as commit, self.assertRaisesRegex(
                CampaignJournalError,
                "runner claim scope conflicts",
            ):
                controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report=report,
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )

            intents = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_LEARNING_COMMIT_INTENT",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(intents), int(seed_intent))
            if expected_intent_payload is not None:
                stored_intent = intents[0].payload()
                self.assertEqual(
                    {key: stored_intent[key] for key in expected_intent_payload},
                    expected_intent_payload,
                )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                    aggregate_id="cycle-001",
                ),
                (),
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )
            self.assertFalse(
                (root / "research_state/control_plane/learning_commit.sqlite3").exists()
            )
            self.assertFalse(
                (root / "research_state/control_plane/learning_packets").exists()
            )
            self.assertEqual(expected_packet_hash.call_count, 0)
            self.assertEqual(commit.call_count, 0)

    def test_learning_commit_rejects_valid_different_protocol_before_intent(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-protocol-mismatch"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            protocol = _synthetic_protocol_with_notes(
                "valid protocol from another frozen generation"
            )
            report, _, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root, protocol=protocol
                )
            )
            controller, execution, evidence = (
                self._record_protocol_binding_evidence(
                    root,
                    journal,
                    campaign_id,
                    _AuthorityEvidenceArtifactBoundFakeProvider(artifact),
                    artifact["claim"],
                    protocol,
                )
            )
            self._assert_protocol_binding_rejected(
                root, journal, controller, execution, evidence, report
            )

    def test_learning_commit_rejects_cross_generation_claim_scope_before_intent(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-scope-generation-mismatch"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": json.dumps(
                _scope(generation="generation-2"),
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, binding, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root,
                    claim=claim,
                    protocol=_synthetic_protocol(),
                )
            )
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_AuthorityEvidenceArtifactBoundFakeProvider(
                        artifact
                    ),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            service = LearningCommitService(repository_root=root)
            caught = None
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ), patch.object(
                LearningCommitService,
                "expected_packet_hash",
                wraps=LearningCommitService.expected_packet_hash,
            ) as expected_packet_hash, patch.object(
                LearningCommitService,
                "commit",
                wraps=LearningCommitService.commit,
            ) as commit:
                try:
                    controller.commit_learning(
                        execution=execution,
                        evidence_receipt=evidence,
                        authority_task_report=report,
                        learning_commit_sink=CampaignLearningCommitSink(
                            journal=journal,
                            service=service,
                        ),
                    )
                except CampaignJournalError as error:
                    caught = error

            self.assertIsInstance(caught, CampaignJournalError)
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_LEARNING_COMMIT_INTENT",
                    aggregate_id="cycle-001",
                ),
                (),
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                    aggregate_id="cycle-001",
                ),
                (),
            )
            self.assertFalse(
                (root / "research_state/control_plane/learning_commit.sqlite3").exists()
            )
            self.assertFalse(
                (root / "research_state/control_plane/learning_packets").exists()
            )
            self.assertEqual(expected_packet_hash.call_count, 0)
            self.assertEqual(commit.call_count, 0)

    def test_learning_commit_rejects_missing_claim_scope_before_intent(
        self,
    ) -> None:
        self._assert_learning_scope_rejected(
            campaign_id="campaign-controller-learning-scope-missing",
            claim_scope=None,
            include_scope=False,
        )

    def test_learning_commit_rejects_invalid_claim_scope_encodings(
        self,
    ) -> None:
        scope = _scope(generation="generation-1")
        cases = (
            ("unparseable", "{"),
            (
                "noncanonical",
                json.dumps(scope, sort_keys=True, indent=2),
            ),
            ("non-string", scope),
        )
        for label, claim_scope in cases:
            with self.subTest(label=label):
                self._assert_learning_scope_rejected(
                    campaign_id=(
                        "campaign-controller-learning-scope-encoding-"
                        f"{label}"
                    ),
                    claim_scope=claim_scope,
                )

    def test_learning_commit_rejects_overflowing_claim_scope_before_intent(
        self,
    ) -> None:
        overflowing_scope = _scope(generation="generation-1")
        overflowing_scope["time_windows"] = [
            {"start": "0001-01-01", "end": "9999-12-31"},
            {"start": "9999-12-31", "end": "9999-12-31"},
        ]
        self._assert_learning_scope_rejected(
            campaign_id="campaign-controller-learning-scope-overflow",
            claim_scope=json.dumps(
                overflowing_scope,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    def test_learning_commit_rejects_expanded_and_overlapping_claim_scopes(
        self,
    ) -> None:
        expanded_scope = _scope(generation="generation-1")
        expanded_scope["generation_families"] = [
            "generation-1",
            "generation-2",
        ]
        overlapping_scope = _scope(generation="generation-1")
        overlapping_scope["time_windows"] = [
            {"start": "2019-01-01", "end": "2021-12-31"}
        ]
        for label, scope in (
            ("expanded", expanded_scope),
            ("overlap", overlapping_scope),
        ):
            with self.subTest(label=label):
                self._assert_learning_scope_rejected(
                    campaign_id=(
                        "campaign-controller-learning-scope-relation-"
                        f"{label}"
                    ),
                    claim_scope=json.dumps(
                        scope,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )

    def test_learning_commit_existing_intent_cannot_bypass_claim_scope(
        self,
    ) -> None:
        self._assert_learning_scope_rejected(
            campaign_id="campaign-controller-learning-scope-intent-replay",
            claim_scope=_synthetic_claim_scope_text(
                generation="generation-2"
            ),
            seed_intent=True,
        )

    def test_learning_commit_allows_strict_subset_claim_scope(self) -> None:
        campaign_id = "campaign-controller-learning-scope-subset"
        subset_scope = _scope(generation="generation-1")
        subset_scope["time_windows"] = [
            {"start": "2022-01-01", "end": "2023-12-31"}
        ]
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": json.dumps(
                subset_scope,
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, binding, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root,
                    claim=claim,
                    protocol=_synthetic_protocol(),
                )
            )
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_AuthorityEvidenceArtifactBoundFakeProvider(
                        artifact
                    ),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ), patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ) as expected_packet_hash, patch.object(
                LearningCommitService,
                "commit",
                return_value="f" * 64,
            ) as commit:
                receipt = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report=report,
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )

            self.assertEqual(receipt.packet_hash, "f" * 64)
            self.assertEqual(expected_packet_hash.call_count, 1)
            self.assertEqual(commit.call_count, 1)
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )

    def test_learning_commit_rejects_missing_executed_protocol_before_intent(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-protocol-missing"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, evidence = (
                self._record_protocol_binding_evidence(
                    root,
                    journal,
                    campaign_id,
                    _MissingExecutedProtocolEligibleFakeProvider(),
                    claim,
                    None,
                )
            )
            self.assertTrue(evidence.evidence.promotion_eligible)
            self._assert_protocol_binding_rejected(
                root,
                journal,
                controller,
                execution,
                evidence,
                {"synthetic": "terminal-report"},
            )

    def test_learning_commit_rejects_malformed_executed_protocol_before_intent(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-protocol-malformed"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, _, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(root)
            )
            controller, execution, evidence = (
                self._record_protocol_binding_evidence(
                    root,
                    journal,
                    campaign_id,
                    _AuthorityEvidenceArtifactBoundFakeProvider(artifact),
                    artifact["claim"],
                    artifact["executed_protocol"],
                )
            )
            self.assertTrue(evidence.evidence.promotion_eligible)
            self._assert_protocol_binding_rejected(
                root, journal, controller, execution, evidence, report
            )

    def test_learning_commit_existing_intent_cannot_bypass_protocol_binding(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-protocol-intent-replay"
        report = {"synthetic": "terminal-report"}
        with _authorized_campaign(campaign_id) as (root, _, journal):
            protocol = _synthetic_protocol_with_notes(
                "valid replay protocol from another frozen generation"
            )
            _, _, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root, protocol=protocol
                )
            )
            controller, execution, evidence = (
                self._record_protocol_binding_evidence(
                    root,
                    journal,
                    campaign_id,
                    _AuthorityEvidenceArtifactBoundFakeProvider(artifact),
                    artifact["claim"],
                    protocol,
                )
            )
            authority_hash = _controller_sha256(
                b"control_plane.operational_learning_task_report.v1",
                report,
                "Authority TaskReport",
            )
            intent_payload = controller._learning_commit_intent_payload(
                cycle_id="cycle-001",
                evidence_receipt=evidence,
                authority_task_report_sha256=authority_hash,
                packet_hash="f" * 64,
            )
            journal.append(
                event_id=controller._learning_commit_intent_event_id("cycle-001"),
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_LEARNING_COMMIT_INTENT",
                aggregate_id="cycle-001",
                event_type="OPERATIONAL_LEARNING_COMMIT_INTENT_RECORDED",
                payload=intent_payload,
            )
            intents = self._assert_protocol_binding_rejected(
                root,
                journal,
                controller,
                execution,
                evidence,
                report,
                intent_count=1,
            )
            stored_intent = intents[0].payload()
            self.assertEqual(
                {key: stored_intent[key] for key in intent_payload},
                intent_payload,
            )

    def test_learning_sink_from_another_repository_is_rejected_before_write(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-foreign-root"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            foreign_root = root / "foreign-repository"
            foreign_root.mkdir()
            service = LearningCommitService(repository_root=foreign_root)

            with patch.object(
                LearningCommitService,
                "commit",
                side_effect=AssertionError("foreign root reached Learning"),
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "same repository root",
                ):
                    controller.commit_learning(
                        execution=execution,
                        evidence_receipt=evidence,
                        authority_task_report={
                            "synthetic": "terminal-report"
                        },
                        learning_commit_sink=CampaignLearningCommitSink(
                            journal=journal,
                            service=service,
                        ),
                    )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_exact_learning_service_rejects_an_unbound_task_report(self) -> None:
        campaign_id = "campaign-controller-learning-unbound-report"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)

            with self.assertRaisesRegex(
                LearningCommitAuthorizationError,
                "TaskReport authority is unavailable",
            ):
                controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "terminal-report"
                    },
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )

            self.assertEqual(service.rebuild_ledger()["packet_hashes"], [])
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_LEARNING_COMMIT_INTENT",
                    aggregate_id="cycle-001",
                ),
                (),
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_no_material_evidence_cannot_enter_learning(self) -> None:
        campaign_id = "campaign-controller-learning-ineligible"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch.object(
                LearningCommitService,
                "commit",
                side_effect=AssertionError("ineligible sink invocation"),
            ):
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "not Learning eligible",
                ):
                    controller.commit_learning(
                        execution=execution,
                        evidence_receipt=evidence,
                        authority_task_report={
                            "synthetic": "terminal-report"
                        },
                        learning_commit_sink=CampaignLearningCommitSink(
                            journal=journal,
                            service=service,
                        ),
                    )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_unbound_evidence_receipt_subclass_is_rejected_before_learning(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-receipt-subclass"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            unbound_evidence = _UnboundOperationalEvidenceReceipt(
                cycle_id=evidence.cycle_id,
                member_id=evidence.member_id,
                preparation_manifest_sha256=(
                    evidence.preparation_manifest_sha256
                ),
                execution_usage_manifest_sha256=(
                    evidence.execution_usage_manifest_sha256
                ),
                model_call_manifest_sha256=(
                    evidence.model_call_manifest_sha256
                ),
                artifact_sha256=evidence.artifact_sha256,
                adapter_manifest_sha256=(
                    evidence.adapter_manifest_sha256
                ),
                evidence=evidence.evidence,
                manifest_sha256=evidence.manifest_sha256,
                event_id=evidence.event_id,
            )
            service = LearningCommitService(repository_root=root)

            with patch.object(
                LearningCommitService,
                "commit",
                side_effect=AssertionError("receipt subclass reached Learning"),
            ):
                with self.assertRaisesRegex(
                    TypeError,
                    "evidence_receipt must be an OperationalEvidenceReceipt",
                ):
                    controller.commit_learning(
                        execution=execution,
                        evidence_receipt=unbound_evidence,
                        authority_task_report={
                            "synthetic": "terminal-report"
                        },
                        learning_commit_sink=CampaignLearningCommitSink(
                            journal=journal,
                            service=service,
                        ),
                    )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_unbound_learning_sink_subclass_is_rejected(self) -> None:
        campaign_id = "campaign-controller-learning-sink-subclass"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )

            with self.assertRaisesRegex(
                TypeError,
                "learning_commit_sink must be a CampaignLearningCommitSink",
            ):
                controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "terminal-report"
                    },
                    learning_commit_sink=(
                        _UnboundCampaignLearningCommitSink(
                            journal=journal,
                            service=LearningCommitService(
                                repository_root=root
                            ),
                        )
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_unbound_learning_service_subclass_is_rejected(self) -> None:
        campaign_id = "campaign-controller-learning-service-subclass"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )

            with self.assertRaisesRegex(
                TypeError,
                "service must be a LearningCommitService",
            ):
                controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "terminal-report"
                    },
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=_UnboundLearningCommitService(
                            repository_root=root
                        ),
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_shadow_learning_stream_blocks_sink_invocation(self) -> None:
        campaign_id = "campaign-controller-learning-shadow-stream"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            journal.append(
                event_id="shadow-learning-commit-event",
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                aggregate_id="shadow-cycle-001",
                event_type="OPERATIONAL_LEARNING_COMMIT_RECORDED",
                payload={"shadow": True},
            )
            service = LearningCommitService(repository_root=root)

            with patch.object(
                LearningCommitService,
                "commit",
                side_effect=AssertionError("shadow stream reached sink"),
            ):
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "operational Learning Commit stream conflicts",
                ):
                    controller.commit_learning(
                        execution=execution,
                        evidence_receipt=evidence,
                        authority_task_report={
                            "synthetic": "terminal-report"
                        },
                        learning_commit_sink=CampaignLearningCommitSink(
                            journal=journal,
                            service=service,
                        ),
                    )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_learning_receipt_event_collision_blocks_formal_packet_write(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-receipt-collision"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, binding, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root,
                    protocol=_synthetic_protocol(),
                )
            )
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_AuthorityEvidenceArtifactBoundFakeProvider(
                        artifact
                    ),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            journal.append(
                event_id=controller._learning_commit_event_id("cycle-001"),
                cycle_id="cycle-001",
                aggregate_type="COLLIDING_OPERATIONAL_STREAM",
                aggregate_id="cycle-001",
                event_type="COLLIDING_OPERATIONAL_EVENT",
                payload={"collision": "synthetic"},
            )
            service = LearningCommitService(repository_root=root)

            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaises(CampaignJournalError):
                    controller.commit_learning(
                        execution=execution,
                        evidence_receipt=evidence,
                        authority_task_report=report,
                        learning_commit_sink=CampaignLearningCommitSink(
                            journal=journal,
                            service=service,
                        ),
                    )
                ledger = service.rebuild_ledger()

            self.assertEqual(ledger["packet_hashes"], [])
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_learning_transition_event_collision_blocks_formal_packet_write(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-transition-collision"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, binding, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root,
                    protocol=_synthetic_protocol(),
                )
            )
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_AuthorityEvidenceArtifactBoundFakeProvider(
                        artifact
                    ),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            journal.append(
                event_id=controller._lifecycle._cycle_event_id(
                    "cycle-001",
                    CycleStatus.LEARNING_COMMITTED.value,
                ),
                cycle_id="cycle-001",
                aggregate_type="COLLIDING_OPERATIONAL_STREAM",
                aggregate_id="cycle-001",
                event_type="COLLIDING_OPERATIONAL_EVENT",
                payload={"collision": "synthetic"},
            )
            service = LearningCommitService(repository_root=root)

            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with self.assertRaises(CampaignJournalError):
                    controller.commit_learning(
                        execution=execution,
                        evidence_receipt=evidence,
                        authority_task_report=report,
                        learning_commit_sink=CampaignLearningCommitSink(
                            journal=journal,
                            service=service,
                        ),
                    )
                ledger = service.rebuild_ledger()

            self.assertEqual(ledger["packet_hashes"], [])
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_learning_commit_exact_replay_is_idempotent(self) -> None:
        campaign_id = "campaign-controller-learning-replay"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        report = {"synthetic": "terminal-report"}
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            sink = CampaignLearningCommitSink(
                journal=journal,
                service=service,
            )
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value="f" * 64,
            ):
                first = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report=report,
                    learning_commit_sink=sink,
                )

                replayed = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report=report,
                    learning_commit_sink=sink,
                )

            self.assertEqual(replayed, first)
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )

    def test_learning_replay_rejects_task_report_drift_before_sink(self) -> None:
        campaign_id = "campaign-controller-learning-report-drift"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            sink = CampaignLearningCommitSink(
                journal=journal,
                service=service,
            )
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value="f" * 64,
            ):
                controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={"synthetic": "report-a"},
                    learning_commit_sink=sink,
                )
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ), patch.object(
                LearningCommitService,
                "commit",
                side_effect=AssertionError("drift reached Learning sink"),
            ):
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "operational Learning Commit intent conflicts",
                ):
                    controller.commit_learning(
                        execution=execution,
                        evidence_receipt=evidence,
                        authority_task_report={"synthetic": "report-b"},
                        learning_commit_sink=sink,
                    )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )

    def test_learning_receipt_and_state_transition_are_atomic(self) -> None:
        campaign_id = "campaign-controller-learning-atomic"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            sink = CampaignLearningCommitSink(
                journal=journal,
                service=service,
            )
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value="f" * 64,
            ):
                with patch.object(
                    OperationalCampaignLifecycle,
                    "_advance_cycle_in_transaction",
                    side_effect=RuntimeError("synthetic Learning crash"),
                ):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "synthetic Learning crash",
                    ):
                        controller.commit_learning(
                            execution=execution,
                            evidence_receipt=evidence,
                            authority_task_report={
                                "synthetic": "terminal-report"
                            },
                            learning_commit_sink=sink,
                        )

                self.assertEqual(
                    journal.list_events(
                        cycle_id="cycle-001",
                        aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                        aggregate_id="cycle-001",
                    ),
                    (),
                )
                self.assertEqual(
                    controller.cycle_snapshot("cycle-001").status,
                    CycleStatus.EVIDENCE_READY,
                )

                receipt = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "terminal-report"
                    },
                    learning_commit_sink=sink,
                )

            self.assertEqual(receipt.packet_hash, "f" * 64)
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )

    def test_learning_commit_recovers_when_formal_packet_precedes_cycle_commit(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-orphan-recovery"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, binding, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root,
                    protocol=_synthetic_protocol(),
                )
            )
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_AuthorityEvidenceArtifactBoundFakeProvider(
                        artifact
                    ),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            service = LearningCommitService(repository_root=root)
            sink = CampaignLearningCommitSink(
                journal=journal,
                service=service,
            )

            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                with patch.object(
                    OperationalCampaignLifecycle,
                    "_advance_cycle_in_transaction",
                    side_effect=RuntimeError("synthetic post-packet crash"),
                ):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "synthetic post-packet crash",
                    ):
                        controller.commit_learning(
                            execution=execution,
                            evidence_receipt=evidence,
                            authority_task_report=report,
                            learning_commit_sink=sink,
                        )

                packet_hashes_after_crash = service.rebuild_ledger()[
                    "packet_hashes"
                ]
                self.assertEqual(
                    controller.cycle_snapshot("cycle-001").status,
                    CycleStatus.EVIDENCE_READY,
                )
                self.assertEqual(len(packet_hashes_after_crash), 1)

                recovered = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report=report,
                    learning_commit_sink=sink,
                )
                packet_hashes_after_recovery = service.rebuild_ledger()[
                    "packet_hashes"
                ]

            self.assertEqual(
                packet_hashes_after_recovery,
                packet_hashes_after_crash,
            )
            self.assertEqual(
                recovered.packet_hash,
                packet_hashes_after_recovery[0],
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )

    def test_post_packet_crash_cannot_retry_with_another_task_report(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-report-crash-drift"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report_a, binding_a, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root,
                    protocol=_synthetic_protocol(),
                )
            )
            report_b_draft = json.loads(json.dumps(report_a))
            for computed_field in (
                "schema_version",
                "unexpected_changes",
                "outcome",
                "reason_codes",
                "report_payload_sha256",
            ):
                report_b_draft.pop(computed_field)
            report_b_draft.update(
                {
                    "ticket_id": "ticket-learning-002",
                    "idempotency_key": "p4-learning-commit-002",
                    "started_at": "2026-07-30T09:00:00Z",
                    "completed_at": "2026-07-30T09:01:00Z",
                }
            )
            report_b = build_task_report_v2(report_b_draft)
            binding_b = type(binding_a)(**vars(binding_a))
            binding_b.ticket_id = report_b["ticket_id"]
            binding_b.report_payload_sha256 = report_b[
                "report_payload_sha256"
            ]

            def binding_for_report(candidate):
                if candidate["ticket_id"] == report_a["ticket_id"]:
                    return binding_a
                if candidate["ticket_id"] == report_b["ticket_id"]:
                    return binding_b
                raise AssertionError("unexpected synthetic TaskReport")

            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_AuthorityEvidenceArtifactBoundFakeProvider(
                        artifact
                    ),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            service = LearningCommitService(repository_root=root)
            sink = CampaignLearningCommitSink(
                journal=journal,
                service=service,
            )

            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                side_effect=binding_for_report,
            ):
                with patch.object(
                    OperationalCampaignLifecycle,
                    "_advance_cycle_in_transaction",
                    side_effect=RuntimeError("synthetic post-packet crash"),
                ):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "synthetic post-packet crash",
                    ):
                        controller.commit_learning(
                            execution=execution,
                            evidence_receipt=evidence,
                            authority_task_report=report_a,
                            learning_commit_sink=sink,
                        )

                packet_hashes_after_crash = service.rebuild_ledger()[
                    "packet_hashes"
                ]
                intent_events_after_crash = journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_LEARNING_COMMIT_INTENT",
                    aggregate_id="cycle-001",
                )
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "Learning Commit intent conflicts",
                ):
                    controller.commit_learning(
                        execution=execution,
                        evidence_receipt=evidence,
                        authority_task_report=report_b,
                        learning_commit_sink=sink,
                    )
                packet_hashes_after_drift = service.rebuild_ledger()[
                    "packet_hashes"
                ]

            self.assertEqual(len(packet_hashes_after_crash), 1)
            self.assertEqual(len(intent_events_after_crash), 1)
            self.assertEqual(
                packet_hashes_after_drift,
                packet_hashes_after_crash,
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_learning_commit_holds_fence_through_the_durable_sink(self) -> None:
        campaign_id = "campaign-controller-learning-fenced-sink"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        recovered_owner = ProcessIdentity("host-controller", 148, 48_000)
        sink_entered = Event()
        release_sink = Event()
        recovery_completed = Event()
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            sink = CampaignLearningCommitSink(
                journal=journal,
                service=service,
            )
            recovery_leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=_FakeProcessIdentityProvider(
                    recovered_owner,
                    process_starts={("host-controller", 144): None},
                ),
                monotonic_ns=lambda: 3_000_000,
            )

            def commit_and_wait(*args, **kwargs):
                sink_entered.set()
                if not release_sink.wait(timeout=5):
                    raise RuntimeError("synthetic Learning sink timed out")
                return "f" * 64

            def recover_lease():
                try:
                    return recovery_leases.recover(
                        cycle_id="cycle-001",
                        acquisition_id="recover-during-learning",
                        stale_after_ns=1,
                    )
                finally:
                    recovery_completed.set()

            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ), patch.object(
                LearningCommitService,
                "commit",
                commit_and_wait,
            ), (
                ThreadPoolExecutor(max_workers=2)
            ) as pool:
                learning = pool.submit(
                    controller.commit_learning,
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "terminal-report"
                    },
                    learning_commit_sink=sink,
                )
                self.assertTrue(sink_entered.wait(timeout=5))
                recovery = pool.submit(recover_lease)
                try:
                    self.assertFalse(recovery_completed.wait(timeout=0.5))
                finally:
                    release_sink.set()

                receipt = learning.result(timeout=5)
                replacement = recovery.result(timeout=5)

            self.assertEqual(receipt.packet_hash, "f" * 64)
            self.assertGreater(replacement.fencing_token, execution.lease.fencing_token)
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )

    def test_learning_receipt_recovers_from_current_state_and_new_lease(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-durable-recovery"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        report = {"synthetic": "terminal-report"}
        recovered_owner = ProcessIdentity("host-controller", 147, 47_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, _ = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            sink = CampaignLearningCommitSink(
                journal=journal,
                service=service,
            )
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value="f" * 64,
            ):
                original = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report=report,
                    learning_commit_sink=sink,
                )
                recovery_identity = _FakeProcessIdentityProvider(
                    recovered_owner,
                    process_starts={("host-controller", 144): None},
                )
                replacement = OperationalCycleLeaseJournal(
                    journal=journal,
                    lifecycle=OperationalCampaignLifecycle(
                        journal=journal
                    ),
                    identity_provider=recovery_identity,
                    monotonic_ns=lambda: 3_000_000,
                ).recover(
                    cycle_id="cycle-001",
                    acquisition_id="recover-completed-learning",
                    stale_after_ns=1,
                )
                recovered = OperationalCampaignController(
                    journal=journal,
                    repository_root=root,
                    budget_limits=budget_limits,
                    identity_provider=recovery_identity,
                    monotonic_ns=lambda: 4_000_000,
                )

                replayed = recovered.commit_learning(
                    execution=ExecutingOperationalCycle(
                        cycle=recovered.cycle_snapshot("cycle-001"),
                        lease=replacement,
                    ),
                    evidence_receipt=evidence,
                    authority_task_report=report,
                    learning_commit_sink=sink,
                )

            self.assertEqual(replayed, original)

    def test_cycle_settlement_rejects_learning_receipt_after_transition(
        self,
    ) -> None:
        campaign_id = "campaign-controller-settlement-learning-order"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        report = {"synthetic": "terminal-report"}
        packet_hash = "f" * 64
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value=packet_hash,
            ), patch.object(
                LearningCommitService,
                "commit",
                side_effect=RuntimeError("synthetic pre-packet crash"),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "synthetic pre-packet crash",
                ):
                    controller.commit_learning(
                        execution=execution,
                        evidence_receipt=evidence,
                        authority_task_report=report,
                        learning_commit_sink=CampaignLearningCommitSink(
                            journal=journal,
                            service=service,
                        ),
                    )

            journal.append(
                event_id=controller._lifecycle._cycle_event_id(
                    "cycle-001",
                    CycleStatus.LEARNING_COMMITTED.value,
                ),
                cycle_id="cycle-001",
                aggregate_type="CYCLE_STATE",
                aggregate_id="cycle-001",
                event_type="CYCLE_TRANSITIONED",
                payload={
                    "cycle_id": "cycle-001",
                    "cycle_number": 1,
                    "from_status": CycleStatus.EVIDENCE_READY.value,
                    "to_status": CycleStatus.LEARNING_COMMITTED.value,
                },
            )
            authority_task_report_sha256 = _controller_sha256(
                b"control_plane.operational_learning_task_report.v1",
                report,
                "Authority TaskReport",
            )
            identity = {
                "schema_version": "control_plane.operational_learning_commit.v1",
                "cycle_id": "cycle-001",
                "member_id": member.member_id,
                "evidence_manifest_sha256": evidence.manifest_sha256,
                "authority_task_report_sha256": (
                    authority_task_report_sha256
                ),
                "packet_hash": packet_hash,
            }
            manifest_sha256 = _controller_sha256(
                b"control_plane.operational_learning_commit.v1",
                identity,
                "operational Learning Commit",
            )
            learning = OperationalLearningCommitReceipt(
                cycle_id="cycle-001",
                member_id=member.member_id,
                evidence_manifest_sha256=evidence.manifest_sha256,
                authority_task_report_sha256=authority_task_report_sha256,
                packet_hash=packet_hash,
                manifest_sha256=manifest_sha256,
                event_id=controller._learning_commit_event_id("cycle-001"),
            )
            journal.append(
                event_id=learning.event_id,
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                aggregate_id="cycle-001",
                event_type="OPERATIONAL_LEARNING_COMMIT_RECORDED",
                payload={**identity, "manifest_sha256": manifest_sha256},
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "Cycle settlement event order conflicts",
            ):
                controller.settle_cycle(
                    execution=execution,
                    execution_usage=usage,
                    learning_commit_receipt=learning,
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                20,
            )

    def test_no_material_cycle_settles_without_learning_packet(self) -> None:
        campaign_id = "campaign-controller-settlement-no-material"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )

            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )
            replayed = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )

            self.assertEqual(evidence.evidence.verdict, "NO_MATERIAL_FINDING")
            self.assertEqual(replayed, settled)
            self.assertEqual(settled.disposition_reason, "NO_MATERIAL_FINDING")
            self.assertEqual(settled.settlement_state, "SETTLED")
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_LEARNING_COMMIT_INTENT",
                    aggregate_id="cycle-001",
                ),
                (),
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                    aggregate_id="cycle-001",
                ),
                (),
            )
            snapshot = controller.budget_snapshot()
            self.assertEqual(snapshot.reserved_input_tokens, 0)
            self.assertEqual(snapshot.reserved_output_tokens, 0)
            self.assertEqual(snapshot.reserved_cost, "0")
            self.assertEqual(snapshot.spent_input_tokens, usage.input_tokens)
            self.assertEqual(snapshot.spent_output_tokens, usage.output_tokens)
            self.assertEqual(snapshot.spent_cost, usage.cost)

    def test_no_material_unknown_usage_keeps_full_reservation(self) -> None:
        campaign_id = "campaign-controller-settlement-no-material-unknown"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_UnknownUsageEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )

            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )

            self.assertEqual(usage.usage_status, UsageStatus.UNKNOWN)
            self.assertEqual(settled.settlement_state, "SETTLED_UNKNOWN")
            snapshot = controller.budget_snapshot()
            self.assertEqual(snapshot.reserved_input_tokens, 20)
            self.assertEqual(snapshot.reserved_output_tokens, 10)
            self.assertEqual(snapshot.reserved_cost, "0.1")
            self.assertEqual(snapshot.reserved_wall_time_ms, _SPAWN_CALL_WALL_TIME_MS)
            self.assertEqual(snapshot.reserved_tool_attempts, 2)
            self.assertEqual(snapshot.spent_input_tokens, 0)
            self.assertEqual(snapshot.spent_output_tokens, 0)
            self.assertEqual(snapshot.spent_cost, "0")
            self.assertEqual(snapshot.spent_wall_time_ms, 0)
            self.assertEqual(snapshot.spent_tool_attempts, 0)

    def test_invalid_evidence_cycle_settles_without_learning_packet(
        self,
    ) -> None:
        campaign_id = "campaign-controller-settlement-invalid-evidence"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_InvalidEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )

            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )

            self.assertEqual(evidence.evidence.verdict, "EVIDENCE_INVALID")
            self.assertEqual(settled.disposition_reason, "EVIDENCE_INVALID")
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                    aggregate_id="cycle-001",
                ),
                (),
            )

    def test_tainted_evidence_cycle_settles_without_learning_packet(
        self,
    ) -> None:
        campaign_id = "campaign-controller-settlement-tainted-evidence"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_TaintedEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )

            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )

            self.assertEqual(evidence.evidence.verdict, "EVIDENCE_INVALID")
            self.assertEqual(
                evidence.evidence.taint_refs,
                ("taint:synthetic-test",),
            )
            self.assertEqual(settled.disposition_reason, "TAINTED_EVIDENCE")
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                    aggregate_id="cycle-001",
                ),
                (),
            )

    def test_learning_eligible_evidence_cannot_skip_learning_commit(
        self,
    ) -> None:
        campaign_id = "campaign-controller-settlement-skip-eligible"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "requires Learning Commit",
            ):
                controller.settle_cycle_without_learning(
                    execution=execution,
                    execution_usage=usage,
                    evidence_receipt=evidence,
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                20,
            )

    def test_no_learning_settlement_is_atomic(self) -> None:
        campaign_id = "campaign-controller-settlement-no-learning-atomic"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            advance = (
                OperationalCampaignLifecycle._advance_cycle_in_transaction
            )

            def fail_settled(lifecycle, connection, **kwargs):
                if kwargs["next_status"] is CycleStatus.SETTLED:
                    raise RuntimeError("synthetic no-Learning crash")
                return advance(lifecycle, connection, **kwargs)

            with patch.object(
                OperationalCampaignLifecycle,
                "_advance_cycle_in_transaction",
                new=fail_settled,
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "synthetic no-Learning crash",
                ):
                    controller.settle_cycle_without_learning(
                        execution=execution,
                        execution_usage=usage,
                        evidence_receipt=evidence,
                    )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_NO_LEARNING_DISPOSITION",
                    aggregate_id="cycle-001",
                ),
                (),
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_CYCLE_SETTLEMENT",
                    aggregate_id="cycle-001",
                ),
                (),
            )
            snapshot = controller.budget_snapshot()
            self.assertEqual(snapshot.reserved_input_tokens, 20)
            self.assertEqual(snapshot.spent_input_tokens, 0)

    def test_replacement_lease_recovers_no_learning_settlement(self) -> None:
        campaign_id = "campaign-controller-no-learning-lease-recovery"
        recovered_owner = ProcessIdentity("host-controller", 149, 49_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            recovery_identity = _FakeProcessIdentityProvider(
                recovered_owner,
                process_starts={("host-controller", 144): None},
            )
            replacement = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 3_000_000,
            ).recover(
                cycle_id="cycle-001",
                acquisition_id="recover-no-learning-settlement",
                stale_after_ns=1,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.settle_cycle_without_learning(
                    execution=execution,
                    execution_usage=usage,
                    evidence_receipt=evidence,
                )

            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 4_000_000,
            )
            settled = recovered.settle_cycle_without_learning(
                execution=ExecutingOperationalCycle(
                    cycle=recovered.cycle_snapshot("cycle-001"),
                    lease=replacement,
                ),
                execution_usage=usage,
                evidence_receipt=evidence,
            )

            self.assertEqual(settled.disposition_reason, "NO_MATERIAL_FINDING")
            self.assertEqual(
                recovered.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )
            self.assertEqual(
                recovered.budget_snapshot().spent_input_tokens,
                usage.input_tokens,
            )

    def test_known_usage_settles_reserved_budget_after_learning(self) -> None:
        campaign_id = "campaign-controller-settlement-known"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, binding, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root,
                    protocol=_synthetic_protocol(),
                )
            )
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_AuthorityEvidenceArtifactBoundFakeProvider(
                        artifact
                    ),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
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

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )
            settled = controller.settle_cycle(
                execution=execution,
                execution_usage=usage,
                learning_commit_receipt=learning,
            )
            replayed = controller.settle_cycle(
                execution=execution,
                execution_usage=usage,
                learning_commit_receipt=learning,
            )

            self.assertEqual(replayed, settled)
            self.assertEqual(settled.cycle_id, "cycle-001")
            self.assertEqual(settled.settlement_state, "SETTLED")
            self.assertEqual(
                settled.execution_usage_manifest_sha256,
                usage.manifest_sha256,
            )
            self.assertEqual(
                settled.learning_commit_manifest_sha256,
                learning.manifest_sha256,
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )
            snapshot = controller.budget_snapshot()
            self.assertEqual(snapshot.reserved_input_tokens, 0)
            self.assertEqual(snapshot.reserved_output_tokens, 0)
            self.assertEqual(snapshot.reserved_cost, "0")
            self.assertEqual(snapshot.reserved_wall_time_ms, 0)
            self.assertEqual(snapshot.reserved_tool_attempts, 0)
            self.assertEqual(snapshot.reserved_data_exposures, 0)
            self.assertEqual(snapshot.reserved_disk_growth_bytes, 0)
            self.assertEqual(snapshot.spent_input_tokens, usage.input_tokens)
            self.assertEqual(snapshot.spent_output_tokens, usage.output_tokens)
            self.assertEqual(snapshot.spent_cost, usage.cost)
            self.assertEqual(snapshot.spent_wall_time_ms, usage.wall_time_ms)
            self.assertEqual(snapshot.spent_tool_attempts, usage.tool_attempts)
            self.assertEqual(snapshot.spent_data_exposures, usage.data_exposures)
            self.assertEqual(
                snapshot.spent_disk_growth_bytes,
                usage.disk_growth_bytes,
            )
            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution usage receipt conflicts",
            ):
                controller.settle_cycle(
                    execution=execution,
                    execution_usage=replace(
                        usage,
                        input_tokens=int(usage.input_tokens) + 1,
                    ),
                    learning_commit_receipt=learning,
                )
            with self.assertRaisesRegex(
                CampaignJournalError,
                "Learning Commit receipt conflicts",
            ):
                controller.settle_cycle(
                    execution=execution,
                    execution_usage=usage,
                    learning_commit_receipt=replace(
                        learning,
                        packet_hash="e" * 64,
                    ),
                )

    def test_unknown_usage_keeps_the_full_cycle_reservation(self) -> None:
        campaign_id = "campaign-controller-settlement-unknown"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, binding, artifact, _, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(
                    root,
                    protocol=_synthetic_protocol(),
                )
            )
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=(
                        _UnknownUsageAuthorityEvidenceArtifactBoundFakeProvider(
                            artifact
                        )
                    ),
                )
            )
            self.assertEqual(usage.usage_status, UsageStatus.UNKNOWN)
            self.assertIsNone(usage.input_tokens)
            self.assertIsNone(usage.output_tokens)
            self.assertIsNone(usage.cost)
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
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

            settled = controller.settle_cycle(
                execution=execution,
                execution_usage=usage,
                learning_commit_receipt=learning,
            )

            self.assertEqual(settled.settlement_state, "SETTLED_UNKNOWN")
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )
            snapshot = controller.budget_snapshot()
            self.assertEqual(snapshot.reserved_input_tokens, 20)
            self.assertEqual(snapshot.reserved_output_tokens, 10)
            self.assertEqual(snapshot.reserved_cost, "0.1")
            self.assertEqual(snapshot.reserved_wall_time_ms, _SPAWN_CALL_WALL_TIME_MS)
            self.assertEqual(snapshot.reserved_tool_attempts, 2)
            self.assertEqual(snapshot.spent_input_tokens, 0)
            self.assertEqual(snapshot.spent_output_tokens, 0)
            self.assertEqual(snapshot.spent_cost, "0")
            self.assertEqual(snapshot.spent_wall_time_ms, 0)
            self.assertEqual(snapshot.spent_tool_attempts, 0)

    def test_cycle_cannot_settle_before_learning_is_committed(self) -> None:
        campaign_id = "campaign-controller-settlement-before-learning"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            fake_learning = OperationalLearningCommitReceipt(
                cycle_id="cycle-001",
                member_id=member.member_id,
                evidence_manifest_sha256=evidence.manifest_sha256,
                authority_task_report_sha256="a" * 64,
                packet_hash="b" * 64,
                manifest_sha256="c" * 64,
                event_id="d" * 64,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "requires LEARNING_COMMITTED",
            ):
                controller.settle_cycle(
                    execution=execution,
                    execution_usage=usage,
                    learning_commit_receipt=fake_learning,
                )

            snapshot = controller.budget_snapshot()
            self.assertEqual(snapshot.reserved_input_tokens, 20)
            self.assertEqual(snapshot.reserved_output_tokens, 10)
            self.assertEqual(snapshot.reserved_cost, "0.1")
            self.assertEqual(snapshot.spent_input_tokens, 0)
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

    def test_cycle_settlement_and_lifecycle_transition_are_atomic(self) -> None:
        campaign_id = "campaign-controller-settlement-atomic"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value="f" * 64,
            ):
                learning = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "terminal-report"
                    },
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )

            with patch.object(
                OperationalCampaignLifecycle,
                "_advance_cycle_in_transaction",
                side_effect=RuntimeError("synthetic settlement crash"),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "synthetic settlement crash",
                ):
                    controller.settle_cycle(
                        execution=execution,
                        execution_usage=usage,
                        learning_commit_receipt=learning,
                    )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.LEARNING_COMMITTED,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_CYCLE_SETTLEMENT",
                    aggregate_id="cycle-001",
                ),
                (),
            )
            snapshot = controller.budget_snapshot()
            self.assertEqual(snapshot.reserved_input_tokens, 20)
            self.assertEqual(snapshot.reserved_output_tokens, 10)
            self.assertEqual(snapshot.reserved_cost, "0.1")
            self.assertEqual(snapshot.spent_input_tokens, 0)

            recovered = controller.settle_cycle(
                execution=execution,
                execution_usage=usage,
                learning_commit_receipt=learning,
            )
            self.assertEqual(recovered.settlement_state, "SETTLED")
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )

    def test_replacement_lease_recovers_cycle_settlement(self) -> None:
        campaign_id = "campaign-controller-settlement-lease-recovery"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        recovered_owner = ProcessIdentity("host-controller", 149, 49_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value="f" * 64,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value="f" * 64,
            ):
                learning = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "terminal-report"
                    },
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )

            recovery_identity = _FakeProcessIdentityProvider(
                recovered_owner,
                process_starts={("host-controller", 144): None},
            )
            replacement = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 3_000_000,
            ).recover(
                cycle_id="cycle-001",
                acquisition_id="recover-settlement",
                stale_after_ns=1,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.settle_cycle(
                    execution=execution,
                    execution_usage=usage,
                    learning_commit_receipt=learning,
                )
            self.assertEqual(
                controller.budget_snapshot().reserved_input_tokens,
                20,
            )

            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 4_000_000,
            )
            settled = recovered.settle_cycle(
                execution=ExecutingOperationalCycle(
                    cycle=recovered.cycle_snapshot("cycle-001"),
                    lease=replacement,
                ),
                execution_usage=usage,
                learning_commit_receipt=learning,
            )

            self.assertEqual(settled.settlement_state, "SETTLED")
            self.assertEqual(
                recovered.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )
            self.assertEqual(
                recovered.budget_snapshot().spent_input_tokens,
                usage.input_tokens,
            )

    def test_learned_settlement_records_information_gain(self) -> None:
        campaign_id = "campaign-controller-information-gain-learned"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        packet_hash = "f" * 64
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value=packet_hash,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value=packet_hash,
            ):
                learning = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "terminal-report"
                    },
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )
            settled = controller.settle_cycle(
                execution=execution,
                execution_usage=usage,
                learning_commit_receipt=learning,
            )
            checkpoint_patch = patch.object(
                CommittedLearningLedgerReader,
                "read_projection_checkpoints",
                return_value=(
                    1,
                    _projectable_learning_input(),
                    _projectable_learning_input(packet_hash),
                ),
            )
            checkpoint_patch.start()
            self.addCleanup(checkpoint_patch.stop)

            information_gain = controller.record_information_gain(
                execution=execution,
                settlement_receipt=settled,
            )
            replayed = controller.record_information_gain(
                execution=execution,
                settlement_receipt=settled,
            )
            replayed_from_current = controller.record_information_gain(
                execution=ExecutingOperationalCycle(
                    cycle=controller.cycle_snapshot("cycle-001"),
                    lease=execution.lease,
                ),
                settlement_receipt=settled,
            )

            self.assertEqual(replayed, information_gain)
            self.assertEqual(replayed_from_current, information_gain)
            self.assertEqual(
                information_gain.information_gain_status,
                "ELIGIBLE_LEARNING_COMMITTED",
            )
            self.assertTrue(information_gain.continuation_eligible)
            self.assertEqual(information_gain.learning_packet_hash, packet_hash)
            self.assertIsNone(information_gain.disposition_reason)
            self.assertEqual(
                information_gain.settlement_manifest_sha256,
                settled.manifest_sha256,
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.INFORMATION_GAIN_RECORDED,
            )
            with self.assertRaisesRegex(
                CampaignJournalError,
                "settlement receipt conflicts",
            ):
                controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=replace(
                        settled,
                        learning_commit_manifest_sha256="a" * 64,
                    ),
                )

    def test_foreign_learning_after_freeze_does_not_hide_new_information(
        self,
    ) -> None:
        campaign_id = "campaign-controller-information-gain-foreign-prefix"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            _, _, information_gain = _completed_eligible_information_gain(
                root,
                journal,
                test_case=self,
                campaign_id=campaign_id,
                foreign_packet_hash="e" * 64,
            )

        self.assertEqual(
            "ELIGIBLE_LEARNING_COMMITTED",
            information_gain.information_gain_status,
        )
        self.assertTrue(information_gain.continuation_eligible)
        self.assertIsNone(information_gain.disposition_reason)

    def test_token_omitted_frozen_packet_is_still_known_information(
        self,
    ) -> None:
        campaign_id = "campaign-controller-information-gain-token-omitted"
        packet_hash = "f" * 64
        prior_hashes = tuple(f"{index:064x}" for index in range(20))
        frozen_projection = _projectable_learning_input(
            *prior_hashes,
            packet_hash,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_input",
                return_value=frozen_projection,
            ):
                controller, execution, settlement, committed_packet_hash = (
                    _settled_eligible_learning(
                        root,
                        journal,
                        campaign_id=campaign_id,
                    )
                )
            context = controller._context.snapshot(
                cycle_id=execution.cycle.cycle_id,
            )
            trusted_context = json.loads(
                context.messages_for(_protocol_member().role)["system_message"][
                    "content"
                ]
            )
            selected_ids = {
                claim["claim_id"]
                for claim in trusted_context["learning_memory"]["claims"]
            }
            self.assertEqual(packet_hash, committed_packet_hash)
            self.assertNotIn(packet_hash, selected_ids)
            self.assertGreater(
                trusted_context["control_metadata"]["omitted_claim_count"],
                0,
            )

            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_checkpoints",
                return_value=(21, frozen_projection, frozen_projection),
            ) as checkpoint_reader:
                information_gain = controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=settlement,
                )
            checkpoint_reader.assert_called_once_with(
                baseline_prefix_length=21,
                packet_hash=packet_hash,
            )

        self.assertEqual(
            "LEARNING_PACKET_NOT_NOVEL",
            information_gain.information_gain_status,
        )
        self.assertFalse(information_gain.continuation_eligible)
        self.assertEqual(
            "DUPLICATE_LEARNING_PACKET",
            information_gain.disposition_reason,
        )

    def test_new_learning_stays_stable_after_foreign_tail_append(self) -> None:
        campaign_id = "campaign-controller-information-gain-replay-tail"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, settlement, packet_hash = (
                _settled_eligible_learning(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            checkpoint = (
                1,
                _projectable_learning_input(),
                _projectable_learning_input(packet_hash),
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_checkpoints",
                side_effect=(checkpoint, checkpoint),
            ):
                information_gain = controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=settlement,
                )
                replayed = controller.record_information_gain(
                    execution=execution,
                )

        self.assertEqual(replayed, information_gain)
        self.assertEqual(
            "ELIGIBLE_LEARNING_COMMITTED",
            information_gain.information_gain_status,
        )
        self.assertTrue(information_gain.continuation_eligible)

    def test_later_parent_commit_cannot_reclassify_cycle_learning(
        self,
    ) -> None:
        campaign_id = "campaign-controller-information-gain-parent-prefix"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, settlement, packet_hash = (
                _settled_eligible_learning(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            checkpoint = (
                1,
                _projectable_learning_input(),
                _excluded_learning_input(
                    packet_hash,
                    "P5_PARENT_UNAVAILABLE",
                ),
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_checkpoints",
                side_effect=(checkpoint, checkpoint),
            ):
                information_gain = controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=settlement,
                )
                replayed = controller.record_information_gain(
                    execution=execution,
                )

        self.assertEqual(replayed, information_gain)
        self.assertEqual(
            "LEARNING_PACKET_NOT_PROJECTABLE",
            information_gain.information_gain_status,
        )
        self.assertFalse(information_gain.continuation_eligible)
        self.assertEqual(
            "P5_PARENT_UNAVAILABLE",
            information_gain.disposition_reason,
        )

    def test_transient_projection_history_failure_rolls_back_for_retry(
        self,
    ) -> None:
        campaign_id = "campaign-controller-information-gain-history-retry"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, settlement, packet_hash = (
                _settled_eligible_learning(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            checkpoint = (
                1,
                _projectable_learning_input(),
                _projectable_learning_input(packet_hash),
            )
            with patch.object(
                CommittedLearningLedgerReader,
                "read_projection_checkpoints",
                side_effect=(ValueError("transient Learning head"), checkpoint),
            ):
                with self.assertRaisesRegex(
                    CampaignJournalError,
                    "projection history",
                ):
                    controller.record_information_gain(
                        execution=execution,
                        settlement_receipt=settlement,
                    )
                self.assertEqual(
                    CycleStatus.SETTLED,
                    controller.cycle_snapshot(execution.cycle.cycle_id).status,
                )
                information_gain = controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=settlement,
                )

        self.assertEqual(
            "ELIGIBLE_LEARNING_COMMITTED",
            information_gain.information_gain_status,
        )
        self.assertTrue(information_gain.continuation_eligible)
        self.assertIsNone(information_gain.disposition_reason)

    def test_no_material_settlement_records_no_information_gain(self) -> None:
        campaign_id = "campaign-controller-information-gain-no-material"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )

            information_gain = controller.record_information_gain(
                execution=execution,
                settlement_receipt=settled,
            )
            replayed = controller.record_information_gain(
                execution=execution,
                settlement_receipt=settled,
            )

            self.assertEqual(replayed, information_gain)
            self.assertEqual(
                information_gain.information_gain_status,
                "NO_MATERIAL_FINDING",
            )
            self.assertFalse(information_gain.continuation_eligible)
            self.assertIsNone(information_gain.learning_packet_hash)
            self.assertEqual(
                information_gain.disposition_reason,
                "NO_MATERIAL_FINDING",
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.INFORMATION_GAIN_RECORDED,
            )

    def test_information_gain_rejects_foreign_learning_payload(self) -> None:
        campaign_id = "campaign-controller-information-gain-foreign-learning"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        packet_hash = "f" * 64
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value=packet_hash,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value=packet_hash,
            ):
                learning = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "terminal-report"
                    },
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )
            settled = controller.settle_cycle(
                execution=execution,
                execution_usage=usage,
                learning_commit_receipt=learning,
            )
            learning_event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_LEARNING_COMMIT",
                aggregate_id="cycle-001",
            )[0]
            learning_payload = json.loads(learning_event.payload_json)
            learning_payload["cycle_id"] = "cycle-foreign"
            learning_identity = {
                key: value
                for key, value in learning_payload.items()
                if key not in {"manifest_sha256", "_authority_grant_id"}
            }
            learning_payload["manifest_sha256"] = _controller_sha256(
                b"control_plane.operational_learning_commit.v1",
                learning_identity,
                "forged foreign Learning Commit",
            )
            settlement_event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_CYCLE_SETTLEMENT",
                aggregate_id="cycle-001",
            )[0]
            settlement_payload = json.loads(settlement_event.payload_json)
            settlement_payload["learning_commit_manifest_sha256"] = (
                learning_payload["manifest_sha256"]
            )
            settlement_identity = {
                key: value
                for key, value in settlement_payload.items()
                if key not in {"manifest_sha256", "_authority_grant_id"}
            }
            settlement_payload["manifest_sha256"] = _controller_sha256(
                b"control_plane.operational_cycle_settlement.v2",
                settlement_identity,
                "forged foreign Learning settlement",
            )

            connection = sqlite3.connect(root / "operational.sqlite3")
            try:
                for event, payload in (
                    (learning_event, learning_payload),
                    (settlement_event, settlement_payload),
                ):
                    payload_json = json.dumps(
                        payload,
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    integrity = _event_integrity_sha256(
                        event_id=event.event_id,
                        namespace=event.namespace,
                        campaign_id=event.campaign_id,
                        cycle_id=event.cycle_id,
                        aggregate_type=event.aggregate_type,
                        aggregate_id=event.aggregate_id,
                        event_type=event.event_type,
                        payload_json=payload_json,
                        occurred_at=event.occurred_at.isoformat(),
                        sequence=event.sequence,
                    )
                    connection.execute(
                        "UPDATE campaign_events SET payload_json = ?, "
                        "payload_sha256 = ? WHERE event_id = ?",
                        (payload_json, integrity, event.event_id),
                    )
                connection.commit()
            finally:
                connection.close()
            forged_settlement = replace(
                settled,
                learning_commit_manifest_sha256=(
                    learning_payload["manifest_sha256"]
                ),
                manifest_sha256=settlement_payload["manifest_sha256"],
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "settlement receipt conflicts",
            ):
                controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=forged_settlement,
                )

    def test_information_gain_rejects_foreign_budget_settlement(self) -> None:
        campaign_id = "campaign-controller-information-gain-foreign-budget"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
            "scope": _synthetic_claim_scope_text(),
        }
        packet_hash = "f" * 64
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_EligibleEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=_synthetic_protocol(),
                    approved_claim=claim,
                ),
            )
            service = LearningCommitService(repository_root=root)
            with patch.object(
                LearningCommitService,
                "expected_packet_hash",
                return_value=packet_hash,
            ), patch.object(
                LearningCommitService,
                "commit",
                return_value=packet_hash,
            ):
                learning = controller.commit_learning(
                    execution=execution,
                    evidence_receipt=evidence,
                    authority_task_report={
                        "synthetic": "terminal-report"
                    },
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=service,
                    ),
                )
            foreign_reservation_id = "foreign-reservation"
            controller._budget.reserve(
                currency="USD",
                reservation_id=foreign_reservation_id,
                call_id="foreign-call",
                max_input_tokens=0,
                max_output_tokens=0,
                max_cost="0",
            )
            controller._budget.settle(
                foreign_reservation_id,
                currency="USD",
                input_tokens=0,
                output_tokens=0,
                cost="0",
            )
            foreign_budget_event_id = controller._budget._event_id(
                "settle",
                reservation_id=foreign_reservation_id,
            )
            settled = controller.settle_cycle(
                execution=execution,
                execution_usage=usage,
                learning_commit_receipt=learning,
            )
            settlement_event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_CYCLE_SETTLEMENT",
                aggregate_id="cycle-001",
            )[0]
            settlement_payload = json.loads(settlement_event.payload_json)
            settlement_payload["budget_settlement_event_id"] = (
                foreign_budget_event_id
            )
            settlement_identity = {
                key: value
                for key, value in settlement_payload.items()
                if key not in {"manifest_sha256", "_authority_grant_id"}
            }
            settlement_payload["manifest_sha256"] = _controller_sha256(
                b"control_plane.operational_cycle_settlement.v2",
                settlement_identity,
                "forged foreign budget settlement",
            )
            payload_json = json.dumps(
                settlement_payload,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            integrity = _event_integrity_sha256(
                event_id=settlement_event.event_id,
                namespace=settlement_event.namespace,
                campaign_id=settlement_event.campaign_id,
                cycle_id=settlement_event.cycle_id,
                aggregate_type=settlement_event.aggregate_type,
                aggregate_id=settlement_event.aggregate_id,
                event_type=settlement_event.event_type,
                payload_json=payload_json,
                occurred_at=settlement_event.occurred_at.isoformat(),
                sequence=settlement_event.sequence,
            )
            connection = sqlite3.connect(root / "operational.sqlite3")
            try:
                connection.execute(
                    "UPDATE campaign_events SET payload_json = ?, "
                    "payload_sha256 = ? WHERE event_id = ?",
                    (payload_json, integrity, settlement_event.event_id),
                )
                connection.execute(
                    "DELETE FROM campaign_events WHERE event_id = ?",
                    (settled.budget_settlement_event_id,),
                )
                connection.commit()
            finally:
                connection.close()
            forged_settlement = replace(
                settled,
                budget_settlement_event_id=foreign_budget_event_id,
                manifest_sha256=settlement_payload["manifest_sha256"],
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "settlement receipt conflicts",
            ):
                controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=forged_settlement,
                )

    def test_information_gain_replays_execution_usage_content(self) -> None:
        campaign_id = "campaign-controller-information-gain-usage-content"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )
            usage_event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_EXECUTION_USAGE",
                aggregate_id="cycle-001",
            )[0]
            budget_event = next(
                event
                for event in journal.list_events(
                    cycle_id=None,
                    aggregate_type="CAMPAIGN_BUDGET",
                    aggregate_id=controller._budget._budget_id,
                )
                if event.event_id == settled.budget_settlement_event_id
            )
            usage_payload = json.loads(usage_event.payload_json)
            budget_payload = json.loads(budget_event.payload_json)
            usage_payload["input_tokens"] = int(
                usage_payload["input_tokens"]
            ) - 1
            budget_payload["input_tokens"] = usage_payload["input_tokens"]
            _rewrite_campaign_event_payload(root, usage_event, usage_payload)
            _rewrite_campaign_event_payload(root, budget_event, budget_payload)

            with self.assertRaisesRegex(
                CampaignJournalError,
                "settlement receipt conflicts",
            ):
                controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=settled,
                )

    def test_information_gain_replays_no_learning_evidence_content(self) -> None:
        campaign_id = "campaign-controller-information-gain-evidence-content"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )
            evidence_event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_MODEL_EVIDENCE",
                aggregate_id="cycle-001",
            )[0]
            evidence_payload = json.loads(evidence_event.payload_json)
            evidence_payload["evidence"]["verdict"] = "EVIDENCE_INVALID"
            evidence_payload["evidence"]["audit_grade"] = "INVALID"
            evidence_payload["evidence"]["scientific_outcome"] = "UNKNOWN"
            evidence_payload["evidence"]["invalidation_codes"] = [
                "FORGED_EVIDENCE"
            ]
            _rewrite_campaign_event_payload(
                root,
                evidence_event,
                evidence_payload,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "settlement receipt conflicts",
            ):
                controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=settled,
                )

    def test_information_gain_rejects_extended_disposition_schema(self) -> None:
        campaign_id = "campaign-controller-information-gain-disposition-schema"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )
            disposition_event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_NO_LEARNING_DISPOSITION",
                aggregate_id="cycle-001",
            )[0]
            disposition_payload = json.loads(disposition_event.payload_json)
            disposition_payload["unknown_v1_field"] = "forged"
            disposition_identity = {
                key: value
                for key, value in disposition_payload.items()
                if key not in {"manifest_sha256", "_authority_grant_id"}
            }
            disposition_payload["manifest_sha256"] = _controller_sha256(
                b"control_plane.operational_no_learning_disposition.v1",
                disposition_identity,
                "extended no-Learning disposition",
            )
            _rewrite_campaign_event_payload(
                root,
                disposition_event,
                disposition_payload,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "settlement receipt conflicts",
            ):
                controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=settled,
                )

    def test_tainted_settlement_records_ineligible_information_gain(
        self,
    ) -> None:
        campaign_id = "campaign-controller-information-gain-tainted"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    provider=_TaintedEvidenceArtifactBoundFakeProvider(),
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )

            information_gain = controller.record_information_gain(
                execution=execution,
                settlement_receipt=settled,
            )

            self.assertEqual(
                information_gain.information_gain_status,
                "INELIGIBLE_EVIDENCE",
            )
            self.assertFalse(information_gain.continuation_eligible)
            self.assertIsNone(information_gain.learning_packet_hash)
            self.assertEqual(
                information_gain.disposition_reason,
                "TAINTED_EVIDENCE",
            )

    def test_non_promoted_material_keeps_its_information_gain_status(
        self,
    ) -> None:
        legacy_protocol = {"label": "synthetic-only"}
        cases = (
            (
                "MATERIAL_UNAPPROVED",
                {
                    "kind": "NEGATIVE",
                    "summary": "Different approved claim.",
                },
            ),
            ("RESEARCH_ONLY", None),
        )
        for expected_status, approved_claim in cases:
            campaign_id = (
                "campaign-controller-information-gain-"
                + expected_status.lower().replace("_", "-")
            )
            with self.subTest(expected_status=expected_status):
                with _authorized_campaign(campaign_id) as (
                    root,
                    _,
                    journal,
                ):
                    controller, execution, member, usage = (
                        _completed_evidence_model_call(
                            root,
                            journal,
                            campaign_id=campaign_id,
                            provider=(
                                _EligibleEvidenceArtifactBoundFakeProvider(
                                    executed_protocol=legacy_protocol
                                )
                            ),
                        )
                    )
                    evidence = controller.record_model_evidence(
                        execution=execution,
                        member_id=member.member_id,
                        evidence_adapter=EvidenceAdapter(
                            known_runners={"fixture-runner": "1.0.0"},
                            approved_protocol=legacy_protocol,
                            approved_claim=approved_claim,
                        ),
                    )
                    settled = controller.settle_cycle_without_learning(
                        execution=execution,
                        execution_usage=usage,
                        evidence_receipt=evidence,
                    )

                    information_gain = controller.record_information_gain(
                        execution=execution,
                        settlement_receipt=settled,
                    )

                    self.assertEqual(evidence.evidence.verdict, expected_status)
                    self.assertEqual(
                        information_gain.information_gain_status,
                        expected_status,
                    )
                    self.assertFalse(
                        information_gain.continuation_eligible
                    )

    def test_information_gain_and_lifecycle_transition_are_atomic(self) -> None:
        campaign_id = "campaign-controller-information-gain-atomic"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )

            with patch.object(
                OperationalCampaignLifecycle,
                "_advance_cycle_in_transaction",
                side_effect=RuntimeError("synthetic information-gain crash"),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "synthetic information-gain crash",
                ):
                    controller.record_information_gain(
                        execution=execution,
                        settlement_receipt=settled,
                    )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_INFORMATION_GAIN",
                    aggregate_id="cycle-001",
                ),
                (),
            )

            recovered = controller.record_information_gain(
                execution=execution,
                settlement_receipt=settled,
            )
            self.assertEqual(
                recovered.information_gain_status,
                "NO_MATERIAL_FINDING",
            )

    def test_replacement_lease_recovers_information_gain(self) -> None:
        campaign_id = "campaign-controller-information-gain-lease-recovery"
        recovered_owner = ProcessIdentity("host-controller", 149, 49_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )
            recovery_identity = _FakeProcessIdentityProvider(
                recovered_owner,
                process_starts={("host-controller", 144): None},
            )
            replacement = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 3_000_000,
            ).recover(
                cycle_id="cycle-001",
                acquisition_id="recover-information-gain",
                stale_after_ns=1,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=settled,
                )

            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 4_000_000,
            )
            information_gain = recovered.record_information_gain(
                execution=ExecutingOperationalCycle(
                    cycle=recovered.cycle_snapshot("cycle-001"),
                    lease=replacement,
                ),
            )

            self.assertEqual(
                information_gain.information_gain_status,
                "NO_MATERIAL_FINDING",
            )
            self.assertEqual(
                recovered.cycle_snapshot("cycle-001").status,
                CycleStatus.INFORMATION_GAIN_RECORDED,
            )

    def test_shadow_information_gain_stream_blocks_recording(self) -> None:
        campaign_id = "campaign-controller-information-gain-shadow"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )
            journal.append(
                event_id="shadow-information-gain-event",
                cycle_id="cycle-001",
                aggregate_type="OPERATIONAL_INFORMATION_GAIN",
                aggregate_id="shadow-cycle-001",
                event_type="OPERATIONAL_INFORMATION_GAIN_RECORDED",
                payload={"shadow": True},
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "information gain stream conflicts",
            ):
                controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=settled,
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )

    def test_cross_type_information_gain_event_blocks_recording(self) -> None:
        campaign_id = "campaign-controller-information-gain-cross-type-shadow"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, member, usage = (
                _completed_evidence_model_call(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            settled = controller.settle_cycle_without_learning(
                execution=execution,
                execution_usage=usage,
                evidence_receipt=evidence,
            )
            journal.append(
                event_id="cross-type-information-gain-event",
                cycle_id="cycle-001",
                aggregate_type="SHADOW_INFORMATION_GAIN",
                aggregate_id="cycle-001",
                event_type="OPERATIONAL_INFORMATION_GAIN_RECORDED",
                payload={"shadow": True},
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "information gain stream conflicts",
            ):
                controller.record_information_gain(
                    execution=execution,
                    settlement_receipt=settled,
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.SETTLED,
            )

    def test_eligible_information_gain_allows_one_budgeted_next_cycle(
        self,
    ) -> None:
        campaign_id = "campaign-controller-next-cycle-eligible"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                )
            )

            decision = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )
            replayed = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )

            self.assertEqual(replayed, decision)
            self.assertEqual(decision.decision, "CONTINUE")
            self.assertTrue(decision.continuation_allowed)
            self.assertEqual(decision.reason_code, "CONTINUATION_ELIGIBLE")
            self.assertEqual(decision.next_cycle_number, 2)
            self.assertEqual(
                decision.information_gain_manifest_sha256,
                information_gain.manifest_sha256,
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.COMPLETED,
            )

    def test_next_cycle_decision_recovers_durable_information_gain(
        self,
    ) -> None:
        campaign_id = "campaign-controller-next-cycle-recovery"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_no_material_information_gain(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )

            decision = controller.decide_next_cycle(execution=execution)
            replayed = controller.decide_next_cycle(
                execution=ExecutingOperationalCycle(
                    cycle=controller.cycle_snapshot("cycle-001"),
                    lease=execution.lease,
                ),
            )

            self.assertEqual(replayed, decision)
            self.assertEqual(decision.decision, "STOP")
            self.assertFalse(decision.continuation_allowed)
            self.assertEqual(
                decision.reason_code,
                "INFORMATION_GAIN_INELIGIBLE",
            )
            self.assertIsNone(decision.next_cycle_number)
            self.assertEqual(
                decision.information_gain_manifest_sha256,
                information_gain.manifest_sha256,
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.COMPLETED,
            )

    def test_next_cycle_decision_replay_survives_next_reservation(
        self,
    ) -> None:
        campaign_id = "campaign-controller-next-cycle-stable-replay"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                )
            )
            decision = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )
            prepared = _prepare_synthetic_cycle(
                controller,
                cycle_id="cycle-002",
                cycle_number=2,
            )

            replayed = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )

            self.assertEqual(replayed, decision)
            self.assertEqual(prepared.cycle_id, "cycle-002")
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.COMPLETED,
            )
            self.assertEqual(
                controller.cycle_snapshot("cycle-002").status,
                CycleStatus.FROZEN,
            )

    def test_stop_decision_blocks_next_cycle_before_budget_mutation(
        self,
    ) -> None:
        campaign_id = "campaign-controller-next-cycle-stop-boundary"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_no_material_information_gain(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            decision = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )
            cycle_budget_before = controller.cycle_budget_snapshot()
            resource_budget_before = controller.budget_snapshot()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "previous Cycle did not authorize continuation",
            ):
                _prepare_synthetic_cycle(
                    controller,
                    cycle_id="cycle-002",
                    cycle_number=2,
                )

            self.assertEqual(decision.decision, "STOP")
            self.assertEqual(
                controller.cycle_budget_snapshot(),
                cycle_budget_before,
            )
            self.assertEqual(
                controller.budget_snapshot(),
                resource_budget_before,
            )
            with self.assertRaises(CampaignLifecycleError):
                controller.cycle_snapshot("cycle-002")

    def test_cycle_budget_exhaustion_records_a_stop_decision(self) -> None:
        campaign_id = "campaign-controller-next-cycle-budget-stop"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                    max_cycles=1,
                )
            )

            decision = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )

            self.assertEqual(decision.decision, "STOP")
            self.assertFalse(decision.continuation_allowed)
            self.assertEqual(
                decision.reason_code,
                "CYCLE_BUDGET_EXHAUSTED",
            )
            self.assertIsNone(decision.next_cycle_number)
            self.assertEqual(decision.reserved_cycle_count, 1)
            self.assertEqual(decision.max_cycles, 1)
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.COMPLETED,
            )

    def test_missing_decision_blocks_successor_without_side_effects(
        self,
    ) -> None:
        campaign_id = "campaign-controller-next-cycle-missing-decision"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, _, _ = _completed_no_material_information_gain(
                root,
                journal,
                campaign_id=campaign_id,
            )
            cycle_budget_before = controller.cycle_budget_snapshot()
            resource_budget_before = controller.budget_snapshot()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "previous Cycle did not authorize continuation",
            ):
                _prepare_synthetic_cycle(
                    controller,
                    cycle_id="cycle-002",
                    cycle_number=2,
                )

            self.assertEqual(
                controller.cycle_budget_snapshot(),
                cycle_budget_before,
            )
            self.assertEqual(
                controller.budget_snapshot(),
                resource_budget_before,
            )
            with self.assertRaises(CampaignLifecycleError):
                controller.cycle_snapshot("cycle-002")

    def test_successor_admission_rejects_a_cycle_number_gap(self) -> None:
        campaign_id = "campaign-controller-next-cycle-gap"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                    max_cycles=3,
                )
            )
            controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )
            cycle_budget_before = controller.cycle_budget_snapshot()
            resource_budget_before = controller.budget_snapshot()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "previous Cycle did not authorize continuation",
            ):
                _prepare_synthetic_cycle(
                    controller,
                    cycle_id="cycle-003",
                    cycle_number=3,
                )

            self.assertEqual(
                controller.cycle_budget_snapshot(),
                cycle_budget_before,
            )
            self.assertEqual(
                controller.budget_snapshot(),
                resource_budget_before,
            )
            with self.assertRaises(CampaignLifecycleError):
                controller.cycle_snapshot("cycle-003")

    def test_first_cycle_cannot_start_at_ordinal_two(self) -> None:
        campaign_id = "campaign-controller-next-cycle-first-gap"
        owner = ProcessIdentity("host-controller", 151, 51_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=2,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 100,
            )
            cycle_budget_before = controller.cycle_budget_snapshot()
            resource_budget_before = controller.budget_snapshot()
            self.assertEqual(
                controller.campaign_snapshot().status,
                CampaignStatus.CREATED,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "previous Cycle did not authorize continuation",
            ):
                _prepare_synthetic_cycle(
                    controller,
                    cycle_id="cycle-002",
                    cycle_number=2,
                )

            self.assertEqual(
                controller.campaign_snapshot().status,
                CampaignStatus.CREATED,
            )
            self.assertEqual(
                controller.cycle_budget_snapshot(),
                cycle_budget_before,
            )
            self.assertEqual(
                controller.budget_snapshot(),
                resource_budget_before,
            )
            with self.assertRaises(CampaignLifecycleError):
                controller.cycle_snapshot("cycle-002")

    def test_first_cycle_rejects_an_orphan_budget_prefix_without_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-next-cycle-first-orphan-budget"
        owner = ProcessIdentity("host-controller", 152, 52_000)
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    currency="USD",
                    max_cycles=2,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
                    max_tool_attempts=2,
                ),
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 100,
            )
            controller._cycle_budget.reserve(cycle_id="rogue-orphan")
            campaign_before = controller.campaign_snapshot()
            cycle_budget_before = controller.cycle_budget_snapshot()
            resource_budget_before = controller.budget_snapshot()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "Cycle budget prefix conflicts",
            ):
                _prepare_synthetic_cycle(
                    controller,
                    cycle_id="cycle-001",
                    cycle_number=1,
                )

            self.assertEqual(controller.campaign_snapshot(), campaign_before)
            self.assertEqual(
                controller.cycle_budget_snapshot(),
                cycle_budget_before,
            )
            self.assertEqual(
                controller.budget_snapshot(),
                resource_budget_before,
            )
            with self.assertRaises(CampaignLifecycleError):
                controller.cycle_snapshot("cycle-001")

    def test_successor_rejects_an_orphan_budget_prefix_without_writes(
        self,
    ) -> None:
        campaign_id = "campaign-controller-next-cycle-successor-orphan-budget"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                    max_cycles=3,
                )
            )
            controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )
            controller._cycle_budget.reserve(cycle_id="rogue-orphan")
            campaign_before = controller.campaign_snapshot()
            cycle_budget_before = controller.cycle_budget_snapshot()
            resource_budget_before = controller.budget_snapshot()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "Cycle budget prefix conflicts",
            ):
                _prepare_synthetic_cycle(
                    controller,
                    cycle_id="cycle-002",
                    cycle_number=2,
                )

            self.assertEqual(controller.campaign_snapshot(), campaign_before)
            self.assertEqual(
                controller.cycle_budget_snapshot(),
                cycle_budget_before,
            )
            self.assertEqual(
                controller.budget_snapshot(),
                resource_budget_before,
            )
            with self.assertRaises(CampaignLifecycleError):
                controller.cycle_snapshot("cycle-002")

    def test_invalid_and_tainted_evidence_stop_next_cycle(self) -> None:
        cases = (
            (
                "invalid",
                _InvalidEvidenceArtifactBoundFakeProvider(),
                "EVIDENCE_INVALID",
            ),
            (
                "tainted",
                _TaintedEvidenceArtifactBoundFakeProvider(),
                "TAINTED_EVIDENCE",
            ),
        )
        for label, provider, expected_disposition in cases:
            campaign_id = f"campaign-controller-next-cycle-{label}-stop"
            with self.subTest(label=label):
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    controller, execution, member, usage = (
                        _completed_evidence_model_call(
                            root,
                            journal,
                            campaign_id=campaign_id,
                            provider=provider,
                            max_cycles=2,
                        )
                    )
                    evidence = controller.record_model_evidence(
                        execution=execution,
                        member_id=member.member_id,
                        evidence_adapter=EvidenceAdapter(
                            known_runners={"fixture-runner": "1.0.0"},
                            approved_protocol={"label": "synthetic-only"},
                        ),
                    )
                    settlement = controller.settle_cycle_without_learning(
                        execution=execution,
                        execution_usage=usage,
                        evidence_receipt=evidence,
                    )
                    information_gain = controller.record_information_gain(
                        execution=execution,
                        settlement_receipt=settlement,
                    )

                    decision = controller.decide_next_cycle(
                        execution=execution,
                        information_gain_receipt=information_gain,
                    )

                    self.assertEqual(
                        information_gain.disposition_reason,
                        expected_disposition,
                    )
                    self.assertEqual(decision.decision, "STOP")
                    self.assertFalse(decision.continuation_allowed)
                    self.assertEqual(
                        decision.reason_code,
                        "INFORMATION_GAIN_INELIGIBLE",
                    )

    def test_caller_cannot_self_report_next_cycle_eligibility(self) -> None:
        campaign_id = "campaign-controller-next-cycle-forged-eligibility"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_no_material_information_gain(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "information-gain receipt conflicts",
            ):
                controller.decide_next_cycle(
                    execution=execution,
                    information_gain_receipt=replace(
                        information_gain,
                        information_gain_status=(
                            "ELIGIBLE_LEARNING_COMMITTED"
                        ),
                        continuation_eligible=True,
                        disposition_reason=None,
                    ),
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.INFORMATION_GAIN_RECORDED,
            )

    def test_next_cycle_decision_and_completion_are_atomic(self) -> None:
        campaign_id = "campaign-controller-next-cycle-atomic"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                )
            )
            original_advance = (
                OperationalCampaignLifecycle._advance_cycle_in_transaction
            )

            def crash_before_completion(
                lifecycle,
                connection,
                *,
                cycle_id,
                expected_status,
                next_status,
            ):
                if next_status is CycleStatus.COMPLETED:
                    raise RuntimeError("synthetic next-Cycle completion crash")
                return original_advance(
                    lifecycle,
                    connection,
                    cycle_id=cycle_id,
                    expected_status=expected_status,
                    next_status=next_status,
                )

            with patch.object(
                OperationalCampaignLifecycle,
                "_advance_cycle_in_transaction",
                new=crash_before_completion,
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "synthetic next-Cycle completion crash",
                ):
                    controller.decide_next_cycle(
                        execution=execution,
                        information_gain_receipt=information_gain,
                    )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.INFORMATION_GAIN_RECORDED,
            )
            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_NEXT_CYCLE_DECISION",
                    aggregate_id="cycle-001",
                ),
                (),
            )

            recovered = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )
            self.assertEqual(recovered.decision, "CONTINUE")
            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.COMPLETED,
            )

    def test_replacement_lease_recovers_next_cycle_decision(self) -> None:
        campaign_id = "campaign-controller-next-cycle-lease-recovery"
        recovered_owner = ProcessIdentity("host-controller", 150, 50_000)
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=2,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                )
            )
            recovery_identity = _FakeProcessIdentityProvider(
                recovered_owner,
                process_starts={("host-controller", 144): None},
            )
            replacement = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=OperationalCampaignLifecycle(journal=journal),
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 3_000_000,
            ).recover(
                cycle_id="cycle-001",
                acquisition_id="recover-next-cycle-decision",
                stale_after_ns=1,
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                controller.decide_next_cycle(
                    execution=execution,
                    information_gain_receipt=information_gain,
                )

            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=recovery_identity,
                monotonic_ns=lambda: 4_000_000,
            )
            decision = recovered.decide_next_cycle(
                execution=ExecutingOperationalCycle(
                    cycle=recovered.cycle_snapshot("cycle-001"),
                    lease=replacement,
                ),
            )

            self.assertEqual(decision.decision, "CONTINUE")
            self.assertTrue(decision.continuation_allowed)
            self.assertEqual(
                recovered.cycle_snapshot("cycle-001").status,
                CycleStatus.COMPLETED,
            )

    def test_completed_decision_replays_read_only_after_process_death(
        self,
    ) -> None:
        campaign_id = "campaign-controller-next-cycle-completed-process-death"
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=2,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=_SPAWN_CAMPAIGN_WALL_TIME_MS,
            max_tool_attempts=2,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                )
            )
            decision = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )
            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-controller", 155, 55_000),
                    process_starts={
                        ("host-controller", execution.lease.owner.pid): None,
                    },
                ),
                monotonic_ns=lambda: 4_000_000,
            )
            events_before = (
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="OPERATIONAL_NEXT_CYCLE_DECISION",
                    aggregate_id="cycle-001",
                ),
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="CYCLE_STATE",
                    aggregate_id="cycle-001",
                ),
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                recovered.decide_next_cycle(execution=execution)

            replayed = recovered.replay_next_cycle_decision(
                cycle_id="cycle-001",
            )

            self.assertEqual(replayed, decision)
            self.assertEqual(
                (
                    journal.list_events(
                        cycle_id="cycle-001",
                        aggregate_type="OPERATIONAL_NEXT_CYCLE_DECISION",
                        aggregate_id="cycle-001",
                    ),
                    journal.list_events(
                        cycle_id="cycle-001",
                        aggregate_type="CYCLE_STATE",
                        aggregate_id="cycle-001",
                    ),
                ),
                events_before,
            )
            self.assertEqual(
                recovered.cycle_snapshot("cycle-001").status,
                CycleStatus.COMPLETED,
            )

    def test_shadow_next_cycle_decision_streams_fail_closed(self) -> None:
        cases = (
            (
                "shadow-aggregate",
                "OPERATIONAL_NEXT_CYCLE_DECISION",
                "shadow-cycle-001",
            ),
            (
                "cross-type",
                "SHADOW_NEXT_CYCLE_DECISION",
                "cycle-001",
            ),
        )
        for label, aggregate_type, aggregate_id in cases:
            campaign_id = f"campaign-controller-next-cycle-{label}"
            with self.subTest(label=label):
                with _authorized_campaign(campaign_id) as (root, _, journal):
                    controller, execution, information_gain = (
                        _completed_eligible_information_gain(
                            root,
                            journal,
                            test_case=self,
                            campaign_id=campaign_id,
                        )
                    )
                    journal.append(
                        event_id=f"{label}-next-cycle-event",
                        cycle_id="cycle-001",
                        aggregate_type=aggregate_type,
                        aggregate_id=aggregate_id,
                        event_type=(
                            "OPERATIONAL_NEXT_CYCLE_DECISION_RECORDED"
                        ),
                        payload={"shadow": True},
                    )

                    with self.assertRaisesRegex(
                        CampaignJournalError,
                        "next-Cycle decision stream conflicts",
                    ):
                        controller.decide_next_cycle(
                            execution=execution,
                            information_gain_receipt=information_gain,
                        )

                    self.assertEqual(
                        controller.cycle_snapshot("cycle-001").status,
                        CycleStatus.INFORMATION_GAIN_RECORDED,
                    )

    def test_next_cycle_decision_event_id_collision_fails_closed(self) -> None:
        campaign_id = "campaign-controller-next-cycle-event-id-collision"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                )
            )
            journal.append(
                event_id=_controller_event_id(
                    b"control_plane.controller_next_cycle_decision.v1",
                    journal.namespace,
                    campaign_id,
                    "cycle-001",
                ),
                cycle_id="cycle-001",
                aggregate_type="UNRELATED_COLLISION",
                aggregate_id="unrelated-collision",
                event_type="UNRELATED_COLLISION_RECORDED",
                payload={"collision": True},
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "operational next-Cycle decision conflicts",
            ):
                controller.decide_next_cycle(
                    execution=execution,
                    information_gain_receipt=information_gain,
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.INFORMATION_GAIN_RECORDED,
            )

    def test_next_cycle_decision_rejects_a_pre_reserved_successor(self) -> None:
        campaign_id = "campaign-controller-next-cycle-pre-reserved"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                    max_cycles=3,
                )
            )
            controller._cycle_budget.reserve(
                cycle_id="premature-cycle-002",
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "Cycle budget prefix conflicts",
            ):
                controller.decide_next_cycle(
                    execution=execution,
                    information_gain_receipt=information_gain,
                )

            self.assertEqual(
                controller.cycle_snapshot("cycle-001").status,
                CycleStatus.INFORMATION_GAIN_RECORDED,
            )

    def test_campaign_cannot_complete_before_continuation_is_consumed(
        self,
    ) -> None:
        campaign_id = "campaign-controller-next-cycle-pending-continuation"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_eligible_information_gain(
                    root,
                    journal,
                    test_case=self,
                    campaign_id=campaign_id,
                )
            )
            decision = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )

            with self.assertRaisesRegex(
                CampaignStateConflictError,
                "controller-managed Campaign requires controller completion",
            ):
                OperationalCampaignLifecycle(journal=journal).complete()
            with self.assertRaisesRegex(
                CampaignStateConflictError,
                "unconsumed continuation decision",
            ):
                controller.complete_campaign()

            self.assertEqual(decision.decision, "CONTINUE")
            self.assertEqual(
                controller.campaign_snapshot().status,
                CampaignStatus.ACTIVE,
            )
            prepared = _prepare_synthetic_cycle(
                controller,
                cycle_id="cycle-002",
                cycle_number=2,
            )
            self.assertEqual(prepared.cycle_id, "cycle-002")

    def test_campaign_completion_rejects_an_orphan_budget_prefix(
        self,
    ) -> None:
        campaign_id = "campaign-controller-next-cycle-stop-orphan-budget"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_no_material_information_gain(
                    root,
                    journal,
                    campaign_id=campaign_id,
                    max_cycles=2,
                )
            )
            controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )
            controller._cycle_budget.reserve(cycle_id="rogue-orphan")
            campaign_before = controller.campaign_snapshot()
            cycle_budget_before = controller.cycle_budget_snapshot()
            resource_budget_before = controller.budget_snapshot()

            with self.assertRaisesRegex(
                CampaignJournalError,
                "Cycle budget prefix conflicts",
            ):
                controller.complete_campaign()

            self.assertEqual(controller.campaign_snapshot(), campaign_before)
            self.assertEqual(
                controller.cycle_budget_snapshot(),
                cycle_budget_before,
            )
            self.assertEqual(
                controller.budget_snapshot(),
                resource_budget_before,
            )

    def test_stop_decision_replays_after_campaign_completion(self) -> None:
        campaign_id = "campaign-controller-next-cycle-stop-completed"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, information_gain = (
                _completed_no_material_information_gain(
                    root,
                    journal,
                    campaign_id=campaign_id,
                )
            )
            decision = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )
            completed = controller.complete_campaign()

            replayed = controller.decide_next_cycle(execution=execution)

            self.assertEqual(completed.status, CampaignStatus.COMPLETED)
            self.assertEqual(replayed, decision)
            self.assertEqual(replayed.decision, "STOP")

    def test_complete_model_execution_binds_resource_observation(self) -> None:
        campaign_id = "campaign-controller-resource-observation-bound"
        observation = ResourceObservation(
            tool_attempts=1,
            data_exposures=2,
            disk_growth_bytes=500,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, _member, usage = _completed_evidence_model_call(
                root,
                journal,
                campaign_id=campaign_id,
                resource_observation=observation,
                campaign_max_data_exposures=2,
                campaign_max_disk_growth_bytes=1000,
                reservation_max_data_exposures=2,
                reservation_max_disk_growth_bytes=1000,
            )
            self.assertEqual(usage.tool_attempts, 1)
            self.assertEqual(usage.data_exposures, 2)
            self.assertEqual(usage.disk_growth_bytes, 500)
            replayed = controller.complete_model_execution(
                execution=execution,
                resource_observation=observation,
            )
            self.assertEqual(replayed, usage)

    def test_default_resource_observation_is_zero(self) -> None:
        campaign_id = "campaign-controller-resource-observation-default-zero"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, _member, usage = _completed_evidence_model_call(
                root,
                journal,
                campaign_id=campaign_id,
            )
            self.assertEqual(usage.tool_attempts, 1)
            self.assertEqual(usage.data_exposures, 0)
            self.assertEqual(usage.disk_growth_bytes, 0)
            replayed = controller.complete_model_execution(execution=execution)
            self.assertEqual(replayed, usage)

    def test_resource_observation_tool_attempt_mismatch_fails_closed(self) -> None:
        campaign_id = "campaign-controller-resource-observation-attempt-drift"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, _member, _usage = _completed_evidence_model_call(
                root,
                journal,
                campaign_id=campaign_id,
            )
            with self.assertRaisesRegex(
                CampaignJournalError,
                "resource observation tool attempts conflict",
            ):
                controller.complete_model_execution(
                    execution=execution,
                    resource_observation=ResourceObservation(2, 0, 0),
                )

    def test_resource_observation_over_limit_fails_closed(self) -> None:
        campaign_id = "campaign-controller-resource-observation-over-limit"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller, execution, _member, _usage = _completed_evidence_model_call(
                root,
                journal,
                campaign_id=campaign_id,
                campaign_max_data_exposures=1,
                campaign_max_disk_growth_bytes=100,
                reservation_max_data_exposures=1,
                reservation_max_disk_growth_bytes=100,
            )
            with self.assertRaisesRegex(
                BudgetExceededError,
                "resource observation exceeds its reservation limits",
            ):
                controller.complete_model_execution(
                    execution=execution,
                    resource_observation=ResourceObservation(1, 2, 0),
                )



if __name__ == "__main__":
    unittest.main()
