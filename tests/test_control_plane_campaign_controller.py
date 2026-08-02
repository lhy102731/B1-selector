from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier, Event
import hashlib
import json
import sqlite3
import unittest
from unittest.mock import patch

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
    InvocationOutcome,
    ProviderResponse,
    UsageEnvelope,
    UsageStatus,
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
    CycleStatus,
    OperationalCampaignLifecycle,
)
from research_automation.control_plane.campaign_store import (
    CampaignLearningCommitSink,
    CampaignJournalError,
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
    compile_execution_spec,
)
from research_automation.task_queue import ExperimentTask
from tests.test_control_plane_campaign_freeze import _protocol_member
from tests.test_control_plane_campaign_lease import _FakeProcessIdentityProvider
from tests.test_control_plane_campaign_preflight import _scope
from tests.test_control_plane_campaign_store import _authorized_campaign
from tests.test_control_plane_evidence_learning import (
    EvidenceLearningVerticalSliceTests,
)
from tests.test_foundations_protocols import _approval, _protocol


class _BoundFakeProvider:
    provider_name = "fake-provider"
    profile = "offline-local"
    model = "deterministic-reviewer"
    config_sha256 = "2" * 64
    capability_sha256 = "3" * 64

    def __init__(self, *, timeouts_before_success: int = 0) -> None:
        self.call_count = 0
        self.last_request: object | None = None
        self._timeouts_before_success = timeouts_before_success

    def invoke(self, request: object) -> ProviderResponse:
        self.call_count += 1
        self.last_request = request
        if self.call_count <= self._timeouts_before_success:
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
        self.call_count += 1
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
    def invoke(self, request: object) -> ProviderResponse:
        self.call_count += 1
        self.last_request = request
        return ProviderResponse(
            output_text=(
                '{"access_event_ids":["event:synthetic-eligible"],'
                '"artifact_refs":[{"ref":"fixtures/result.json",'
                '"sha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}],'
                '"claim":{"kind":"NEGATIVE",'
                '"summary":"Synthetic eligible finding."},'
                '"executed_protocol":{"label":"synthetic-only"},'
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


class _AuthorityEvidenceArtifactBoundFakeProvider(_BoundFakeProvider):
    def __init__(self, artifact: object) -> None:
        super().__init__()
        self._artifact = artifact

    def invoke(self, request: object) -> ProviderResponse:
        self.call_count += 1
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


_FAKE_CALL_LIMITS = OperationalModelCallLimits(
    max_input_tokens=20,
    max_output_tokens=10,
    max_cost="0.1",
    max_wall_time_ms=10,
    max_attempts=2,
)


def _completed_evidence_model_call(
    root,
    journal,
    *,
    campaign_id: str,
    provider=None,
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
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
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=10,
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
        limits=_FAKE_CALL_LIMITS,
    )
    usage = controller.complete_model_execution(execution=execution)
    return controller, execution, member, usage


def _controller_event_id(domain: bytes, *parts: str) -> str:
    return hashlib.sha256(
        domain + b"\0" + "\0".join(parts).encode("ascii")
    ).hexdigest()


class OperationalCampaignControllerTests(unittest.TestCase):
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
                        max_input_tokens=11,
                        max_output_tokens=1,
                        max_cost="0.1",
                    ),
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservations = (
            CycleReservationLimits(
                max_input_tokens=10,
                max_output_tokens=5,
                max_cost="0.1",
            ),
            CycleReservationLimits(
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
        )
        reservation = CycleReservationLimits(
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
            self.assertIsNone(execution_usage.currency)
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
            max_tool_attempts=2,
        )
        reservation = CycleReservationLimits(
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=10,
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

    def test_oversized_success_output_is_not_invoked_twice(self) -> None:
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
                    max_tool_attempts=2,
                ),
            )
            execution = controller.start_execution(
                cycle_id=task.task_id,
                acquisition_id="execute-oversized-output",
            )
            provider = _OversizedOutputBoundFakeProvider()
            with self.assertRaisesRegex(ValueError, "output exceeds"):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )

            self.assertEqual(provider.call_count, 1)
            replay_provider = _BoundFakeProvider()
            with self.assertRaisesRegex(
                CampaignJournalError,
                "incomplete and in doubt",
            ):
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=replay_provider,
                    prompt=prompt,
                    limits=_FAKE_CALL_LIMITS,
                )

            self.assertEqual(replay_provider.call_count, 0)
            self.assertEqual(
                controller.campaign_snapshot().status.value,
                "BLOCKED",
            )

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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=40,
                    max_output_tokens=20,
                    max_cost="0.2",
                    max_wall_time_ms=20,
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=40,
                    max_output_tokens=20,
                    max_cost="0.2",
                    max_wall_time_ms=20,
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
            max_tool_attempts=2,
        )
        reservation_limits = CycleReservationLimits(
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=10,
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
            max_tool_attempts=2,
        )
        reservation_limits = CycleReservationLimits(
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=10,
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=40,
                    max_output_tokens=20,
                    max_cost="0.2",
                    max_wall_time_ms=20,
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
                                max_input_tokens=21,
                                max_output_tokens=10,
                                max_cost="0.1",
                                max_wall_time_ms=10,
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
                        max_input_tokens=6,
                        max_output_tokens=10,
                        max_cost="0.1",
                        max_wall_time_ms=10,
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
            max_input_tokens=6,
            max_output_tokens=10,
            max_cost="0.1",
            max_wall_time_ms=10,
            max_attempts=2,
        )

        with _authorized_campaign(campaign_id) as (root, _, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=CampaignBudgetLimits(
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
                        max_input_tokens=6,
                        max_output_tokens=10,
                        max_cost="0.1",
                        max_wall_time_ms=10,
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

    def test_execution_usage_without_cost_currency_is_unknown(self) -> None:
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
            self.assertIsNone(usage.currency)

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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
                    max_input_tokens=20,
                    max_output_tokens=10,
                    max_cost="0.1",
                    max_wall_time_ms=10,
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
                    max_cycles=1,
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                    max_wall_time_ms=100,
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
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
                b"control_plane.operational_execution_usage.v1",
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
                    approved_protocol={"label": "synthetic-only"},
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
            report, binding, artifact, expected_evidence, _ = (
                EvidenceLearningVerticalSliceTests()._authority_fixture(root)
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
                    approved_protocol={"label": "synthetic-only"},
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
                    approved_protocol={"label": "synthetic-only"},
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
                    approved_protocol={"label": "synthetic-only"},
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
                    approved_protocol={"label": "synthetic-only"},
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
                    approved_protocol={"label": "synthetic-only"},
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
                    approved_protocol={"label": "synthetic-only"},
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
                EvidenceLearningVerticalSliceTests()._authority_fixture(root)
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
                EvidenceLearningVerticalSliceTests()._authority_fixture(root)
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
                    approved_protocol={"label": "synthetic-only"},
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
                    approved_protocol={"label": "synthetic-only"},
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
                    approved_protocol={"label": "synthetic-only"},
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
                EvidenceLearningVerticalSliceTests()._authority_fixture(root)
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
                EvidenceLearningVerticalSliceTests()._authority_fixture(root)
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
                    approved_protocol={"label": "synthetic-only"},
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
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
                    approved_protocol={"label": "synthetic-only"},
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
                    approved_protocol={"label": "synthetic-only"},
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
            self.assertEqual(snapshot.reserved_wall_time_ms, 10)
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
                    approved_protocol={"label": "synthetic-only"},
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
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
                EvidenceLearningVerticalSliceTests()._authority_fixture(root)
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
                EvidenceLearningVerticalSliceTests()._authority_fixture(root)
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
            self.assertEqual(snapshot.reserved_wall_time_ms, 10)
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
                    approved_protocol={"label": "synthetic-only"},
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
                    approved_protocol={"label": "synthetic-only"},
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
            max_cycles=1,
            max_input_tokens=100,
            max_output_tokens=50,
            max_cost="1",
            max_wall_time_ms=100,
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
                    approved_protocol={"label": "synthetic-only"},
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


if __name__ == "__main__":
    unittest.main()
