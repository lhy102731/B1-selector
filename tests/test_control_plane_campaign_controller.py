from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import replace
from decimal import Context, Inexact, Rounded, localcontext
from threading import Barrier, Event
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
    OperationalCycleContextJournal,
)
from research_automation.control_plane.campaign_freeze import (
    CycleFreezeError,
    OperationalCycleFreezeJournal,
)
from research_automation.control_plane.budget import (
    BudgetConflictError,
    BudgetExceededError,
)
from research_automation.control_plane.campaign_lease import (
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
)
from research_automation.control_plane.campaign_roster import (
    OperationalRosterJournal,
    RosterDriftError,
)
from research_automation.control_plane.task_reports import build_task_report_v2
from research_automation.foundations.protocols import (
    MaterialProtocolChangeError,
    ProtocolDefinition,
    compile_execution_spec,
)
from research_automation.task_queue import ExperimentTask
from tests.test_control_plane_campaign_freeze import _protocol_member
from tests.test_control_plane_campaign_lease import _FakeProcessIdentityProvider
from tests.test_control_plane_campaign_preflight import _scope
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
                '"summary":"Synthetic eligible finding."},'
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
    usage = controller.complete_model_execution(execution=execution)
    return controller, execution, member, usage


def _completed_eligible_information_gain(
    root,
    journal,
    *,
    campaign_id: str,
    max_cycles: int = 2,
):
    claim = {
        "kind": "NEGATIVE",
        "summary": "Synthetic eligible finding.",
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
    information_gain = controller.record_information_gain(
        execution=execution,
        settlement_receipt=settlement,
    )
    return controller, execution, information_gain


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
                "OperationalCycleContextJournal.prepare",
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
            barrier = Barrier(2)

            def prepare(index: int) -> object:
                barrier.wait()
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

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = tuple(executor.map(prepare, range(2)))

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
                "OperationalCycleContextJournal.prepare",
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
                "hypothesis": "Preparation receipt recovers after freeze",
                "scope": _scope(generation="generation-1"),
            },
            source="synthetic-test",
        )
        owner = ProcessIdentity("host-controller", 116, 16_000)
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
                monotonic_ns=lambda: 1_600,
            )
            with patch.object(
                OperationalCampaignController,
                "_record_cycle_preparation",
                side_effect=RuntimeError("synthetic post-freeze crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "post-freeze crash"):
                    controller.prepare_cycle(
                        task=task,
                        cycle_number=1,
                        execution_spec=execution_spec,
                        roster_members=(_protocol_member(),),
                        reservation_limits=reservation,
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
            with self.assertRaisesRegex(
                CampaignJournalError,
                "receipt is missing",
            ):
                controller.start_execution(
                    cycle_id=task.task_id,
                    acquisition_id="pre-recovery-acquisition",
                )

            reopened = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=limits,
                identity_provider=_FakeProcessIdentityProvider(owner),
                monotonic_ns=lambda: 1_600,
            )
            prepared = reopened.prepare_cycle(
                task=task,
                cycle_number=1,
                execution_spec=execution_spec,
                roster_members=(_protocol_member(),),
                reservation_limits=reservation,
            )

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

    def test_learning_commit_rejects_missing_executed_protocol_before_intent(
        self,
    ) -> None:
        campaign_id = "campaign-controller-learning-protocol-missing"
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic eligible finding.",
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


if __name__ == "__main__":
    unittest.main()
