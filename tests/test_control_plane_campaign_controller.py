from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier
import unittest
from unittest.mock import patch

from research_automation.control_plane.campaign_controller import (
    CampaignBudgetLimits,
    CycleReservationLimits,
    OperationalCampaignController,
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
from research_automation.control_plane.campaign_lease import ProcessIdentity
from research_automation.control_plane.campaign_lifecycle import (
    CampaignLifecycleError,
    CycleStatus,
    OperationalCampaignLifecycle,
)
from research_automation.control_plane.campaign_store import CampaignJournalError
from research_automation.control_plane.campaign_roster import (
    OperationalRosterJournal,
)
from research_automation.foundations.protocols import (
    MaterialProtocolChangeError,
    compile_execution_spec,
)
from research_automation.task_queue import ExperimentTask
from tests.test_control_plane_campaign_freeze import _protocol_member
from tests.test_control_plane_campaign_lease import _FakeProcessIdentityProvider
from tests.test_control_plane_campaign_preflight import _scope
from tests.test_control_plane_campaign_store import _authorized_campaign
from tests.test_foundations_protocols import _approval, _protocol


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


if __name__ == "__main__":
    unittest.main()
