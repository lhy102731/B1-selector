"""Public P6 Campaign composition over the durable domain journals."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from research_automation.foundations.protocols import (
    ExecutionSpec,
    require_protocol_conformant,
)
from research_automation.task_queue import ExperimentTask

from . import stores
from .budget import BudgetLedger, BudgetReservation, BudgetSnapshot
from .campaign_context import (
    CycleContextReceipt,
    OperationalCycleContextJournal,
    canonical_campaign_proposal,
)
from .campaign_freeze import FrozenCycleInputs, OperationalCycleFreezeJournal
from .campaign_lease import (
    CycleLease,
    OperationalCycleLeaseJournal,
    ProcessIdentityProvider,
)
from .campaign_lifecycle import (
    CampaignSnapshot,
    CycleSnapshot,
    CycleStatus,
    OperationalCampaignLifecycle,
    _CYCLE_TRANSITIONED,
)
from .campaign_roster import (
    OperationalRosterJournal,
    RosterManifest,
    RosterMember,
    _roster_manifest,
)
from .campaign_store import (
    CampaignJournalError,
    CycleBudgetSnapshot,
    OperationalBudgetJournal,
    OperationalCampaignJournal,
    OperationalCycleBudgetJournal,
    _BUDGET_RESERVED,
    _BUDGET_SETTLED,
    _event_domain_payload,
    _identifier,
)
from .sqlite_uow import _SqliteUnitOfWork


_WORK_ITEM_AGGREGATE_TYPE = "CAMPAIGN_WORK_ITEM"
_WORK_ITEM_ADOPTED = "CAMPAIGN_WORK_ITEM_ADOPTED"
_PREPARATION_AGGREGATE_TYPE = "CAMPAIGN_CYCLE_PREPARATION"
_CYCLE_PREPARED = "CAMPAIGN_CYCLE_PREPARED"


def _bounded_limits(
    *,
    max_input_tokens: int,
    max_output_tokens: int,
    max_cost: str | int | Decimal,
    max_wall_time_ms: int,
    max_tool_attempts: int,
    max_data_exposures: int,
    max_disk_growth_bytes: int,
) -> None:
    BudgetLedger(
        max_input_tokens=max_input_tokens,
        max_output_tokens=max_output_tokens,
        max_cost=max_cost,
        max_wall_time_ms=max_wall_time_ms,
        max_tool_attempts=max_tool_attempts,
        max_data_exposures=max_data_exposures,
        max_disk_growth_bytes=max_disk_growth_bytes,
    )


@dataclass(frozen=True, slots=True)
class CampaignBudgetLimits:
    max_cycles: int
    max_input_tokens: int
    max_output_tokens: int
    max_cost: str | int | Decimal
    max_wall_time_ms: int = 0
    max_tool_attempts: int = 0
    max_data_exposures: int = 0
    max_disk_growth_bytes: int = 0

    def __post_init__(self) -> None:
        if type(self.max_cycles) is not int or self.max_cycles < 0:
            raise ValueError("max_cycles must be a non-negative integer")
        _bounded_limits(
            max_input_tokens=self.max_input_tokens,
            max_output_tokens=self.max_output_tokens,
            max_cost=self.max_cost,
            max_wall_time_ms=self.max_wall_time_ms,
            max_tool_attempts=self.max_tool_attempts,
            max_data_exposures=self.max_data_exposures,
            max_disk_growth_bytes=self.max_disk_growth_bytes,
        )


@dataclass(frozen=True, slots=True)
class CycleReservationLimits:
    max_input_tokens: int
    max_output_tokens: int
    max_cost: str | int | Decimal
    max_wall_time_ms: int = 0
    max_tool_attempts: int = 0
    max_data_exposures: int = 0
    max_disk_growth_bytes: int = 0

    def __post_init__(self) -> None:
        _bounded_limits(
            max_input_tokens=self.max_input_tokens,
            max_output_tokens=self.max_output_tokens,
            max_cost=self.max_cost,
            max_wall_time_ms=self.max_wall_time_ms,
            max_tool_attempts=self.max_tool_attempts,
            max_data_exposures=self.max_data_exposures,
            max_disk_growth_bytes=self.max_disk_growth_bytes,
        )


@dataclass(frozen=True, slots=True)
class PreparedOperationalCycle:
    cycle_id: str
    reservation: BudgetReservation
    context: CycleContextReceipt
    roster: RosterManifest
    frozen: FrozenCycleInputs
    preparation_manifest_sha256: str

    @property
    def context_manifest_sha256(self) -> str:
        return self.context.manifest_sha256

    @property
    def roster_manifest_sha256(self) -> str:
        return self.roster.manifest_sha256


@dataclass(frozen=True, slots=True)
class ExecutingOperationalCycle:
    cycle: CycleSnapshot
    lease: CycleLease


def _stable_id(domain: bytes, *parts: str) -> str:
    return hashlib.sha256(
        domain + b"\0" + "\0".join(parts).encode("ascii")
    ).hexdigest()


def _canonical_json_text(value: object, name: str) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, UnicodeError) as error:
        raise ValueError(f"{name} must be canonical JSON") from error


def _controller_sha256(domain: bytes, value: object, name: str) -> str:
    return hashlib.sha256(
        domain + b"\0" + _canonical_json_text(value, name).encode("utf-8")
    ).hexdigest()


def _canonical_task(
    task: ExperimentTask,
    *,
    cycle_number: int,
) -> tuple[str, dict[str, object]]:
    if not isinstance(task, ExperimentTask):
        raise TypeError("task must be an ExperimentTask input DTO")
    task_id = _identifier(task.task_id, "task.task_id")
    strategy = _identifier(task.strategy, "task.strategy")
    source = _identifier(task.source, "task.source")
    if type(task.priority) is not int or not 0 <= task.priority <= 1_000_000:
        raise ValueError("task.priority must be from 0 through 1000000")
    if type(cycle_number) is not int or not 1 <= cycle_number <= 1_000_000:
        raise ValueError("cycle_number must be from 1 through 1000000")
    proposal_json = _canonical_json_text(task.proposal, "task.proposal")
    try:
        proposal = json.loads(proposal_json)
    except json.JSONDecodeError as error:
        raise ValueError("task.proposal must be canonical JSON") from error
    if not isinstance(proposal, dict):
        raise ValueError("task.proposal must be a mapping")
    payload = {
        "schema_version": "control_plane.campaign_work_item.v1",
        "task_id": task_id,
        "cycle_number": cycle_number,
        "strategy": strategy,
        "proposal": proposal,
        "source": source,
        "priority": task.priority,
    }
    return task_id, payload


class OperationalCampaignController:
    """Compose one authoritative pre-execution path for Campaign Cycles."""

    __slots__ = (
        "_journal",
        "_lifecycle",
        "_cycle_budget",
        "_budget",
        "_context",
        "_roster",
        "_freeze",
        "_leases",
    )

    def __init__(
        self,
        *,
        journal: OperationalCampaignJournal,
        repository_root: str | Path,
        budget_limits: CampaignBudgetLimits,
        identity_provider: ProcessIdentityProvider,
        monotonic_ns: Callable[[], int],
        tokenizer_kind: str | None = None,
        tokenizer_name: str | None = None,
    ) -> None:
        if not isinstance(journal, OperationalCampaignJournal):
            raise TypeError("journal must be an OperationalCampaignJournal")
        if not isinstance(budget_limits, CampaignBudgetLimits):
            raise TypeError("budget_limits must be CampaignBudgetLimits")
        journal._authorize()
        lifecycle = OperationalCampaignLifecycle(journal=journal)
        budget_identity = (
            journal.namespace,
            journal.campaign_id,
        )
        cycle_budget = OperationalCycleBudgetJournal(
            journal=journal,
            budget_id=_stable_id(
                b"control_plane.controller_cycle_budget.v1",
                *budget_identity,
            ),
            max_cycles=budget_limits.max_cycles,
        )
        budget = OperationalBudgetJournal(
            journal=journal,
            budget_id=_stable_id(
                b"control_plane.controller_resource_budget.v1",
                *budget_identity,
            ),
            max_input_tokens=budget_limits.max_input_tokens,
            max_output_tokens=budget_limits.max_output_tokens,
            max_cost=budget_limits.max_cost,
            max_wall_time_ms=budget_limits.max_wall_time_ms,
            max_tool_attempts=budget_limits.max_tool_attempts,
            max_data_exposures=budget_limits.max_data_exposures,
            max_disk_growth_bytes=budget_limits.max_disk_growth_bytes,
        )
        context = OperationalCycleContextJournal(
            journal=journal,
            lifecycle=lifecycle,
            repository_root=repository_root,
            tokenizer_kind=tokenizer_kind,
            tokenizer_name=tokenizer_name,
        )
        roster = OperationalRosterJournal(
            journal=journal,
            lifecycle=lifecycle,
        )
        freeze = OperationalCycleFreezeJournal(
            journal=journal,
            lifecycle=lifecycle,
            roster=roster,
            context=context,
        )
        leases = OperationalCycleLeaseJournal(
            journal=journal,
            lifecycle=lifecycle,
            identity_provider=identity_provider,
            monotonic_ns=monotonic_ns,
        )
        self._journal = journal
        self._lifecycle = lifecycle
        self._cycle_budget = cycle_budget
        self._budget = budget
        self._context = context
        self._roster = roster
        self._freeze = freeze
        self._leases = leases

    def prepare_cycle(
        self,
        *,
        task: ExperimentTask,
        cycle_number: int,
        execution_spec: ExecutionSpec,
        roster_members: tuple[RosterMember, ...],
        reservation_limits: CycleReservationLimits,
    ) -> PreparedOperationalCycle:
        self._journal._authorize()
        if not isinstance(reservation_limits, CycleReservationLimits):
            raise TypeError("reservation_limits must be CycleReservationLimits")
        cycle_id, work_item = _canonical_task(
            task,
            cycle_number=cycle_number,
        )
        work_item["proposal"] = canonical_campaign_proposal(
            work_item["proposal"]
        )
        if not isinstance(execution_spec, ExecutionSpec):
            raise TypeError("execution_spec must be an ExecutionSpec")
        require_protocol_conformant(execution_spec)
        canonical_roster = _roster_manifest(cycle_id, roster_members)
        protocol_roster = tuple(
            sorted(
                (
                    member.role,
                    member.provider_profile_id,
                    member.model_id,
                )
                for member in execution_spec.protocol.roster
            )
        )
        operational_roster = tuple(
            sorted(
                (member.role, member.profile, member.model)
                for member in canonical_roster.members
            )
        )
        if operational_roster != protocol_roster:
            raise ValueError("ExecutionSpec roster conflicts with roster_members")
        self._lifecycle.activate()
        reservation_id = self._reservation_id(cycle_id)

        def reserve_and_open(connection):
            self._adopt_work_item_in_transaction(
                connection,
                cycle_id=cycle_id,
                payload=work_item,
            )
            self._cycle_budget._reserve_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            reservation = self._budget._reserve_in_transaction(
                connection,
                reservation_id=reservation_id,
                call_id=cycle_id,
                max_input_tokens=reservation_limits.max_input_tokens,
                max_output_tokens=reservation_limits.max_output_tokens,
                max_cost=reservation_limits.max_cost,
                max_wall_time_ms=reservation_limits.max_wall_time_ms,
                max_tool_attempts=reservation_limits.max_tool_attempts,
                max_data_exposures=reservation_limits.max_data_exposures,
                max_disk_growth_bytes=reservation_limits.max_disk_growth_bytes,
            )
            cycle = self._lifecycle._open_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                cycle_number=cycle_number,
            )
            return cycle, reservation

        cycle, reservation = _SqliteUnitOfWork(
            stores._operational_spec()
        )._write(reserve_and_open)
        if cycle.status is CycleStatus.CREATED:
            self._lifecycle.advance_cycle(
                cycle_id=cycle_id,
                expected_status=CycleStatus.CREATED,
                next_status=CycleStatus.BUDGET_RESERVED,
            )
        context = self._context.prepare(
            cycle_id=cycle_id,
            proposal=work_item["proposal"],
            roles=tuple(member.role for member in canonical_roster.members),
        )
        roster = self._roster.freeze(
            cycle_id=cycle_id,
            members=canonical_roster.members,
        )
        frozen = self._freeze.freeze(
            cycle_id=cycle_id,
            proposal=work_item["proposal"],
            execution_spec=execution_spec,
            expected_roster=roster,
        )
        preparation_manifest_sha256 = self._record_cycle_preparation(
            cycle_id=cycle_id,
            expected_work_item=work_item,
            expected_reservation=reservation,
            expected_context=context,
            expected_roster=roster,
            expected_frozen=frozen,
        )
        return PreparedOperationalCycle(
            cycle_id=cycle_id,
            reservation=reservation,
            context=context,
            roster=roster,
            frozen=frozen,
            preparation_manifest_sha256=preparation_manifest_sha256,
        )

    def campaign_snapshot(self) -> CampaignSnapshot:
        return self._lifecycle.snapshot()

    def start_execution(
        self,
        *,
        cycle_id: str,
        acquisition_id: str,
    ) -> ExecutingOperationalCycle:
        self._journal._authorize()
        frozen = self._freeze.snapshot(cycle_id=cycle_id)
        self._preparation_snapshot(cycle_id=cycle_id, frozen=frozen)
        lease = self._leases.acquire(
            cycle_id=cycle_id,
            acquisition_id=acquisition_id,
        )
        cycle = self._leases.advance_cycle(
            lease=lease,
            expected_status=CycleStatus.FROZEN,
            next_status=CycleStatus.EXECUTING,
        )
        return ExecutingOperationalCycle(cycle=cycle, lease=lease)

    def cycle_snapshot(self, cycle_id: str) -> CycleSnapshot:
        return self._lifecycle.cycle_snapshot(cycle_id)

    def cycle_budget_snapshot(self) -> CycleBudgetSnapshot:
        return self._cycle_budget.snapshot()

    def budget_snapshot(self) -> BudgetSnapshot:
        return self._budget.snapshot()

    @staticmethod
    def _reservation_payload(
        reservation: BudgetReservation,
    ) -> dict[str, object]:
        return {
            "reservation_id": reservation.reservation_id,
            "call_id": reservation.call_id,
            "max_input_tokens": reservation.max_input_tokens,
            "max_output_tokens": reservation.max_output_tokens,
            "max_cost": reservation.max_cost,
            "max_wall_time_ms": reservation.max_wall_time_ms,
            "max_tool_attempts": reservation.max_tool_attempts,
            "max_data_exposures": reservation.max_data_exposures,
            "max_disk_growth_bytes": reservation.max_disk_growth_bytes,
        }

    def _controller_artifact_identity_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        frozen: FrozenCycleInputs,
    ) -> tuple[
        dict[str, object],
        dict[str, object],
        dict[str, object],
        int,
    ]:
        work_event, work_item = self._replay_work_item(
            cycle_id=cycle_id,
            events=self._work_item_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            ),
        )
        cycle_budget = self._cycle_budget._snapshot_in_transaction(connection)
        if cycle_id not in cycle_budget.reserved_cycle_ids:
            raise CampaignJournalError(
                "controller preparation is incomplete: Cycle slot is missing"
            )
        budget_events = self._budget._events_in_transaction(connection)
        self._budget._replay(budget_events)
        reservation_id = self._reservation_id(cycle_id)
        reservation_events = tuple(
            event
            for event in budget_events
            if event.event_id
            == self._budget._event_id(
                "reserve",
                reservation_id=reservation_id,
            )
        )
        if len(reservation_events) != 1:
            raise CampaignJournalError(
                "controller preparation is incomplete: resource reservation is missing"
            )
        reservation_event = reservation_events[0]
        reservation_payload = _event_domain_payload(reservation_event)
        if (
            reservation_event.event_type != _BUDGET_RESERVED
            or reservation_payload.get("reservation_id") != reservation_id
            or reservation_payload.get("call_id") != cycle_id
            or any(
                event.event_type == _BUDGET_SETTLED
                and _event_domain_payload(event).get("reservation_id")
                == reservation_id
                for event in budget_events
            )
        ):
            raise CampaignJournalError(
                "controller preparation is incomplete: resource reservation is invalid"
            )
        cycle_events = self._lifecycle._cycle_events(connection, cycle_id)
        cycle = self._lifecycle._replay_cycle(cycle_events)
        opened = cycle_events[0]
        frozen_transitions = tuple(
            event
            for event in cycle_events
            if event.event_type == _CYCLE_TRANSITIONED
            and _event_domain_payload(event).get("to_status")
            == CycleStatus.FROZEN.value
        )
        if (
            work_item["cycle_number"] != cycle.cycle_number
            or work_event.sequence >= opened.sequence
            or reservation_event.sequence >= opened.sequence
            or frozen.cycle_id != cycle_id
            or len(frozen_transitions) != 1
            or frozen_transitions[0].sequence <= frozen.sequence
            or _controller_sha256(
                b"control_plane.campaign_proposal.v1",
                work_item["proposal"],
                "stored Campaign proposal",
            )
            != frozen.proposal_sha256
        ):
            raise CampaignJournalError(
                "controller preparation is incomplete: artifact binding is invalid"
            )
        identity = {
            "schema_version": "control_plane.campaign_cycle_preparation.v1",
            "cycle_id": cycle_id,
            "cycle_number": cycle.cycle_number,
            "cycle_budget_id": cycle_budget.budget_id,
            "resource_budget_id": self._budget._budget_id,
            "work_item_event_id": work_event.event_id,
            "work_item_sha256": _controller_sha256(
                b"control_plane.controller_work_item_payload.v1",
                work_item,
                "stored Campaign work item",
            ),
            "reservation_event_id": reservation_event.event_id,
            "reservation_id": reservation_id,
            "reservation_sha256": _controller_sha256(
                b"control_plane.controller_cycle_reservation_bounds.v1",
                reservation_payload,
                "stored Cycle reservation",
            ),
            "context_manifest_sha256": frozen.context_manifest_sha256,
            "roster_manifest_sha256": frozen.roster_manifest_sha256,
            "freeze_event_id": frozen.event_id,
            "freeze_manifest_sha256": frozen.manifest_sha256,
            "frozen_transition_event_id": frozen_transitions[0].event_id,
        }
        return (
            identity,
            work_item,
            reservation_payload,
            frozen_transitions[0].sequence,
        )

    def _record_cycle_preparation(
        self,
        *,
        cycle_id: str,
        expected_work_item: dict[str, object],
        expected_reservation: BudgetReservation,
        expected_context: CycleContextReceipt,
        expected_roster: RosterManifest,
        expected_frozen: FrozenCycleInputs,
    ) -> str:
        def record(connection) -> str:
            identity, work_item, reservation_payload, minimum_sequence = (
                self._controller_artifact_identity_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    frozen=expected_frozen,
                )
            )
            if (
                _canonical_json_text(work_item, "stored Campaign work item")
                != _canonical_json_text(
                    expected_work_item,
                    "expected Campaign work item",
                )
                or _canonical_json_text(
                    reservation_payload,
                    "stored Cycle reservation",
                )
                != _canonical_json_text(
                    self._reservation_payload(expected_reservation),
                    "expected Cycle reservation",
                )
                or identity["context_manifest_sha256"]
                != expected_context.manifest_sha256
                or identity["roster_manifest_sha256"]
                != expected_roster.manifest_sha256
                or identity["freeze_manifest_sha256"]
                != expected_frozen.manifest_sha256
            ):
                raise CampaignJournalError(
                    "controller preparation artifacts conflict"
                )
            manifest_sha256 = _controller_sha256(
                b"control_plane.campaign_cycle_preparation.v1",
                identity,
                "Cycle preparation identity",
            )
            payload = {**identity, "manifest_sha256": manifest_sha256}
            events = self._preparation_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            if events:
                self._replay_preparation(
                    cycle_id=cycle_id,
                    events=events,
                    expected_payload=payload,
                    minimum_sequence=minimum_sequence,
                )
                return manifest_sha256
            event = self._journal._append_in_transaction(
                connection,
                event_id=self._preparation_event_id(cycle_id),
                cycle_id=cycle_id,
                aggregate_type=_PREPARATION_AGGREGATE_TYPE,
                aggregate_id=cycle_id,
                event_type=_CYCLE_PREPARED,
                payload=payload,
            )
            if event.sequence <= minimum_sequence:
                raise CampaignJournalError(
                    "Cycle preparation must follow the frozen inputs"
                )
            return manifest_sha256

        return _SqliteUnitOfWork(stores._operational_spec())._write(record)

    def _preparation_snapshot(
        self,
        *,
        cycle_id: str,
        frozen: FrozenCycleInputs,
    ) -> str:
        def snapshot(connection) -> str:
            identity, _, _, minimum_sequence = (
                self._controller_artifact_identity_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    frozen=frozen,
                )
            )
            manifest_sha256 = _controller_sha256(
                b"control_plane.campaign_cycle_preparation.v1",
                identity,
                "Cycle preparation identity",
            )
            payload = {**identity, "manifest_sha256": manifest_sha256}
            self._replay_preparation(
                cycle_id=cycle_id,
                events=self._preparation_events_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                ),
                expected_payload=payload,
                minimum_sequence=minimum_sequence,
            )
            return manifest_sha256

        return _SqliteUnitOfWork(stores._operational_spec())._read(snapshot)

    def _preparation_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_cycle_preparation_event.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _preparation_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        related_streams = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _PREPARATION_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_id"] != cycle_id
            for row in related_streams
        ):
            raise CampaignJournalError(
                "Campaign Cycle preparation stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_PREPARATION_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _replay_preparation(
        self,
        *,
        cycle_id: str,
        events,
        expected_payload: dict[str, object],
        minimum_sequence: int,
    ) -> None:
        if not events:
            raise CampaignJournalError(
                "controller preparation is incomplete: receipt is missing"
            )
        if len(events) != 1:
            raise CampaignJournalError(
                "Campaign Cycle preparation receipt conflicts"
            )
        event = events[0]
        payload = _event_domain_payload(event)
        if (
            event.event_id != self._preparation_event_id(cycle_id)
            or event.event_type != _CYCLE_PREPARED
            or event.sequence <= minimum_sequence
            or _canonical_json_text(
                payload,
                "stored Cycle preparation receipt",
            )
            != _canonical_json_text(
                expected_payload,
                "expected Cycle preparation receipt",
            )
        ):
            raise CampaignJournalError(
                "Campaign Cycle preparation receipt conflicts"
            )

    def _reservation_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_cycle_reservation.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _work_item_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_work_item.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _work_item_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        related_streams = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _WORK_ITEM_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_id"] != cycle_id
            for row in related_streams
        ):
            raise CampaignJournalError("Campaign work item stream conflicts")
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_WORK_ITEM_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _replay_work_item(
        self,
        *,
        cycle_id: str,
        events,
    ):
        if not events:
            raise CampaignJournalError(
                "controller preparation is incomplete: work item is missing"
            )
        if (
            len(events) != 1
            or events[0].event_id != self._work_item_event_id(cycle_id)
            or events[0].event_type != _WORK_ITEM_ADOPTED
        ):
            raise CampaignJournalError("Campaign work item conflicts")
        payload = _event_domain_payload(events[0])
        if set(payload) != {
            "schema_version",
            "task_id",
            "cycle_number",
            "strategy",
            "proposal",
            "source",
            "priority",
        }:
            raise CampaignJournalError("Campaign work item conflicts")
        try:
            task = ExperimentTask(
                task_id=payload["task_id"],
                strategy=payload["strategy"],
                proposal=payload["proposal"],
                source=payload["source"],
                priority=payload["priority"],
            )
            replayed_cycle_id, canonical = _canonical_task(
                task,
                cycle_number=payload["cycle_number"],
            )
        except (TypeError, ValueError) as error:
            raise CampaignJournalError("Campaign work item conflicts") from error
        if (
            payload["schema_version"]
            != "control_plane.campaign_work_item.v1"
            or replayed_cycle_id != cycle_id
            or _canonical_json_text(payload, "stored Campaign work item")
            != _canonical_json_text(canonical, "canonical Campaign work item")
        ):
            raise CampaignJournalError("Campaign work item conflicts")
        return events[0], payload

    def _adopt_work_item_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        payload: dict[str, object],
    ) -> None:
        events = self._work_item_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        if events:
            _, stored_payload = self._replay_work_item(
                cycle_id=cycle_id,
                events=events,
            )
            if _canonical_json_text(
                stored_payload,
                "stored Campaign work item",
            ) != _canonical_json_text(payload, "requested Campaign work item"):
                raise CampaignJournalError("Campaign work item conflicts")
            return
        self._journal._append_in_transaction(
            connection,
            event_id=self._work_item_event_id(cycle_id),
            cycle_id=cycle_id,
            aggregate_type=_WORK_ITEM_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
            event_type=_WORK_ITEM_ADOPTED,
            payload=payload,
        )


__all__ = [
    "CampaignBudgetLimits",
    "CycleReservationLimits",
    "ExecutingOperationalCycle",
    "OperationalCampaignController",
    "PreparedOperationalCycle",
]
