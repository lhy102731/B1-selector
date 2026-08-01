"""OperationalJournal-backed Campaign and Cycle lifecycle state."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum

from . import stores
from .campaign_store import (
    CampaignEvent,
    CampaignJournalError,
    OperationalCampaignJournal,
    _event_domain_payload,
    _event_from_row,
    _identifier,
)
from .sqlite_uow import _SqliteUnitOfWork


class CampaignLifecycleError(RuntimeError):
    """Base error for persisted Campaign and Cycle lifecycle state."""


class CampaignStateConflictError(CampaignLifecycleError):
    """Raised when persisted state differs from the caller's expectation."""


class IllegalCycleTransitionError(CampaignLifecycleError):
    """Raised when a Cycle attempts to skip a required protocol state."""


class DuplicateCycleError(CampaignLifecycleError):
    """Raised when a Cycle identity or ordinal is reused inconsistently."""


class CampaignStatus(str, Enum):
    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    COMPLETED = "COMPLETED"


class CycleStatus(str, Enum):
    CREATED = "CREATED"
    BUDGET_RESERVED = "BUDGET_RESERVED"
    CONTEXT_READY = "CONTEXT_READY"
    FROZEN = "FROZEN"
    EXECUTING = "EXECUTING"
    EVIDENCE_READY = "EVIDENCE_READY"
    LEARNING_COMMITTED = "LEARNING_COMMITTED"
    SETTLED = "SETTLED"
    INFORMATION_GAIN_RECORDED = "INFORMATION_GAIN_RECORDED"
    NEXT_CYCLE_DECIDED = "NEXT_CYCLE_DECIDED"
    COMPLETED = "COMPLETED"


_CAMPAIGN_AGGREGATE_TYPE = "CAMPAIGN_STATE"
_CYCLE_AGGREGATE_TYPE = "CYCLE_STATE"
_CAMPAIGN_CREATED = "CAMPAIGN_CREATED"
_CAMPAIGN_TRANSITIONED = "CAMPAIGN_TRANSITIONED"
_CYCLE_OPENED = "CYCLE_OPENED"
_CYCLE_TRANSITIONED = "CYCLE_TRANSITIONED"

_CAMPAIGN_NEXT = {
    CampaignStatus.CREATED: CampaignStatus.ACTIVE,
    CampaignStatus.ACTIVE: CampaignStatus.COMPLETED,
}
_CYCLE_NEXT = {
    CycleStatus.CREATED: CycleStatus.BUDGET_RESERVED,
    CycleStatus.BUDGET_RESERVED: CycleStatus.CONTEXT_READY,
    CycleStatus.CONTEXT_READY: CycleStatus.FROZEN,
    CycleStatus.FROZEN: CycleStatus.EXECUTING,
    CycleStatus.EXECUTING: CycleStatus.EVIDENCE_READY,
    CycleStatus.EVIDENCE_READY: CycleStatus.LEARNING_COMMITTED,
    CycleStatus.LEARNING_COMMITTED: CycleStatus.SETTLED,
    CycleStatus.SETTLED: CycleStatus.INFORMATION_GAIN_RECORDED,
    CycleStatus.INFORMATION_GAIN_RECORDED: CycleStatus.NEXT_CYCLE_DECIDED,
    CycleStatus.NEXT_CYCLE_DECIDED: CycleStatus.COMPLETED,
}


@dataclass(frozen=True, slots=True)
class CampaignSnapshot:
    campaign_id: str
    status: CampaignStatus
    sequence: int


@dataclass(frozen=True, slots=True)
class CycleSnapshot:
    cycle_id: str
    cycle_number: int
    status: CycleStatus
    sequence: int


def _state_event_id(
    *,
    namespace: str,
    campaign_id: str,
    aggregate_type: str,
    aggregate_id: str,
    role: str,
) -> str:
    return hashlib.sha256(
        b"control_plane.campaign_lifecycle_event.v1\0"
        + "\0".join(
            (namespace, campaign_id, aggregate_type, aggregate_id, role)
        ).encode("ascii")
    ).hexdigest()


class OperationalCampaignLifecycle:
    """Persist and atomically order one Campaign and its Cycles.

    Lifecycle events are ordering/CAS facts, not evidence that the named protocol
    work occurred. The controller must verify the corresponding budget, freeze,
    evidence, learning, and decision receipts before requesting each transition.
    """

    __slots__ = ("_journal",)

    def __init__(self, *, journal: OperationalCampaignJournal) -> None:
        if not isinstance(journal, OperationalCampaignJournal):
            raise TypeError("journal must be an OperationalCampaignJournal")
        journal._authorize()
        self._journal = journal

        def open_campaign(connection) -> None:
            events = self._campaign_events(connection)
            if events:
                self._replay_campaign(events)
                return
            self._journal._append_in_transaction(
                connection,
                event_id=self._campaign_event_id(CampaignStatus.CREATED.value),
                cycle_id=None,
                aggregate_type=_CAMPAIGN_AGGREGATE_TYPE,
                aggregate_id=self._journal._campaign_id,
                event_type=_CAMPAIGN_CREATED,
                payload={
                    "campaign_id": self._journal._campaign_id,
                    "status": CampaignStatus.CREATED.value,
                },
            )

        _SqliteUnitOfWork(stores._operational_spec())._write(open_campaign)

    def snapshot(self) -> CampaignSnapshot:
        self._journal._authorize()
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._replay_campaign(
                self._campaign_events(connection)
            )
        )

    def activate(self) -> CampaignSnapshot:
        self._journal._authorize()

        def activate_campaign(connection) -> CampaignSnapshot:
            snapshot = self._replay_campaign(self._campaign_events(connection))
            if snapshot.status is CampaignStatus.ACTIVE:
                return snapshot
            if snapshot.status is not CampaignStatus.CREATED:
                raise CampaignStateConflictError(
                    "Campaign is not in the expected CREATED state"
                )
            event = self._journal._append_in_transaction(
                connection,
                event_id=self._campaign_event_id(CampaignStatus.ACTIVE.value),
                cycle_id=None,
                aggregate_type=_CAMPAIGN_AGGREGATE_TYPE,
                aggregate_id=self._journal._campaign_id,
                event_type=_CAMPAIGN_TRANSITIONED,
                payload={
                    "from_status": CampaignStatus.CREATED.value,
                    "to_status": CampaignStatus.ACTIVE.value,
                },
            )
            return CampaignSnapshot(
                self._journal._campaign_id,
                CampaignStatus.ACTIVE,
                event.sequence,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(
            activate_campaign
        )

    def open_cycle(self, *, cycle_id: str, cycle_number: int) -> CycleSnapshot:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")
        if type(cycle_number) is not int or not 1 <= cycle_number <= 1_000_000:
            raise ValueError("cycle_number must be from 1 through 1000000")

        def open_cycle_state(connection) -> CycleSnapshot:
            campaign = self._replay_campaign(self._campaign_events(connection))
            if campaign.status is not CampaignStatus.ACTIVE:
                raise CampaignStateConflictError("Campaign is not ACTIVE")
            existing: CycleSnapshot | None = None
            for opened in self._opened_cycles(connection):
                if opened.cycle_id == cycle_id:
                    if opened.cycle_number != cycle_number:
                        raise DuplicateCycleError(
                            "cycle_id has a different cycle_number"
                        )
                    existing = opened
                elif opened.cycle_number == cycle_number:
                    raise DuplicateCycleError("cycle_number is already assigned")
            if existing is not None:
                return self._replay_cycle(self._cycle_events(connection, cycle_id))
            event = self._journal._append_in_transaction(
                connection,
                event_id=self._cycle_event_id(
                    cycle_id,
                    CycleStatus.CREATED.value,
                ),
                cycle_id=cycle_id,
                aggregate_type=_CYCLE_AGGREGATE_TYPE,
                aggregate_id=cycle_id,
                event_type=_CYCLE_OPENED,
                payload={
                    "cycle_id": cycle_id,
                    "cycle_number": cycle_number,
                    "status": CycleStatus.CREATED.value,
                },
            )
            return CycleSnapshot(
                cycle_id,
                cycle_number,
                CycleStatus.CREATED,
                event.sequence,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(
            open_cycle_state
        )

    def complete(self) -> CampaignSnapshot:
        self._journal._authorize()

        def complete_campaign(connection) -> CampaignSnapshot:
            snapshot = self._replay_campaign(self._campaign_events(connection))
            if snapshot.status is CampaignStatus.COMPLETED:
                return snapshot
            if snapshot.status is not CampaignStatus.ACTIVE:
                raise CampaignStateConflictError("Campaign is not ACTIVE")
            opened = self._opened_cycles(connection)
            if not opened or any(
                self._replay_cycle(
                    self._cycle_events(connection, cycle.cycle_id)
                ).status
                is not CycleStatus.COMPLETED
                for cycle in opened
            ):
                raise CampaignStateConflictError(
                    "Campaign has an incomplete Cycle"
                )
            event = self._journal._append_in_transaction(
                connection,
                event_id=self._campaign_event_id(CampaignStatus.COMPLETED.value),
                cycle_id=None,
                aggregate_type=_CAMPAIGN_AGGREGATE_TYPE,
                aggregate_id=self._journal._campaign_id,
                event_type=_CAMPAIGN_TRANSITIONED,
                payload={
                    "from_status": CampaignStatus.ACTIVE.value,
                    "to_status": CampaignStatus.COMPLETED.value,
                },
            )
            return CampaignSnapshot(
                self._journal._campaign_id,
                CampaignStatus.COMPLETED,
                event.sequence,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(
            complete_campaign
        )

    def advance_cycle(
        self,
        *,
        cycle_id: str,
        expected_status: CycleStatus,
        next_status: CycleStatus,
    ) -> CycleSnapshot:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")
        if not isinstance(expected_status, CycleStatus) or not isinstance(
            next_status,
            CycleStatus,
        ):
            raise TypeError("Cycle transitions require CycleStatus values")
        if _CYCLE_NEXT.get(expected_status) is not next_status:
            raise IllegalCycleTransitionError(
                "Cycle transition skips a required protocol state"
            )

        def advance(connection) -> CycleSnapshot:
            campaign = self._replay_campaign(self._campaign_events(connection))
            if campaign.status is not CampaignStatus.ACTIVE:
                raise CampaignStateConflictError("Campaign is not ACTIVE")
            snapshot = self._replay_cycle(self._cycle_events(connection, cycle_id))
            if snapshot.status is next_status:
                return snapshot
            if snapshot.status is not expected_status:
                raise CampaignStateConflictError(
                    "Cycle is not in the expected state"
                )
            event = self._journal._append_in_transaction(
                connection,
                event_id=self._cycle_event_id(cycle_id, next_status.value),
                cycle_id=cycle_id,
                aggregate_type=_CYCLE_AGGREGATE_TYPE,
                aggregate_id=cycle_id,
                event_type=_CYCLE_TRANSITIONED,
                payload={
                    "cycle_id": cycle_id,
                    "cycle_number": snapshot.cycle_number,
                    "from_status": expected_status.value,
                    "to_status": next_status.value,
                },
            )
            return CycleSnapshot(
                cycle_id,
                snapshot.cycle_number,
                next_status,
                event.sequence,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(advance)

    def cycle_snapshot(self, cycle_id: str) -> CycleSnapshot:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._replay_cycle(
                self._cycle_events(connection, cycle_id)
            )
        )

    def _campaign_events(self, connection) -> tuple[CampaignEvent, ...]:
        return self._journal._list_in_transaction(
            connection,
            cycle_id=None,
            aggregate_type=_CAMPAIGN_AGGREGATE_TYPE,
            aggregate_id=self._journal._campaign_id,
        )

    def _cycle_events(
        self,
        connection,
        cycle_id: str,
    ) -> tuple[CampaignEvent, ...]:
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_CYCLE_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _opened_cycles(self, connection) -> tuple[CycleSnapshot, ...]:
        rows = connection.execute(
            """
            SELECT * FROM campaign_events
            WHERE namespace = ? AND campaign_id = ?
              AND cycle_id IS NOT NULL AND aggregate_type = ?
              AND event_type = ?
            ORDER BY sequence
            """,
            (
                self._journal._namespace,
                self._journal._campaign_id,
                _CYCLE_AGGREGATE_TYPE,
                _CYCLE_OPENED,
            ),
        ).fetchall()
        return tuple(
            self._replay_cycle((_event_from_row(row),)) for row in rows
        )

    def _campaign_event_id(self, role: str) -> str:
        return _state_event_id(
            namespace=self._journal._namespace,
            campaign_id=self._journal._campaign_id,
            aggregate_type=_CAMPAIGN_AGGREGATE_TYPE,
            aggregate_id=self._journal._campaign_id,
            role=role,
        )

    def _cycle_event_id(self, cycle_id: str, role: str) -> str:
        return _state_event_id(
            namespace=self._journal._namespace,
            campaign_id=self._journal._campaign_id,
            aggregate_type=_CYCLE_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
            role=role,
        )

    def _require_event_envelope(
        self,
        event: CampaignEvent,
        *,
        cycle_id: str | None,
        aggregate_type: str,
        aggregate_id: str,
    ) -> None:
        observed = (
            event.namespace,
            event.campaign_id,
            event.cycle_id,
            event.aggregate_type,
            event.aggregate_id,
        )
        expected = (
            self._journal._namespace,
            self._journal._campaign_id,
            cycle_id,
            aggregate_type,
            aggregate_id,
        )
        if observed != expected:
            raise CampaignLifecycleError("lifecycle event envelope is invalid")

    def _replay_campaign(
        self,
        events: tuple[CampaignEvent, ...],
    ) -> CampaignSnapshot:
        if not events:
            raise CampaignLifecycleError("Campaign lifecycle has not been created")
        created = events[0]
        self._require_event_envelope(
            created,
            cycle_id=None,
            aggregate_type=_CAMPAIGN_AGGREGATE_TYPE,
            aggregate_id=self._journal._campaign_id,
        )
        expected_created = {
            "campaign_id": self._journal._campaign_id,
            "status": CampaignStatus.CREATED.value,
        }
        if (
            created.event_id != self._campaign_event_id(CampaignStatus.CREATED.value)
            or created.event_type != _CAMPAIGN_CREATED
            or _event_domain_payload(created) != expected_created
        ):
            raise CampaignLifecycleError("Campaign CREATED event is invalid")
        status = CampaignStatus.CREATED
        sequence = created.sequence
        for event in events[1:]:
            self._require_event_envelope(
                event,
                cycle_id=None,
                aggregate_type=_CAMPAIGN_AGGREGATE_TYPE,
                aggregate_id=self._journal._campaign_id,
            )
            payload = _event_domain_payload(event)
            if (
                event.event_type != _CAMPAIGN_TRANSITIONED
                or set(payload) != {"from_status", "to_status"}
                or type(payload["from_status"]) is not str
                or type(payload["to_status"]) is not str
            ):
                raise CampaignLifecycleError("Campaign transition event is invalid")
            try:
                from_status = CampaignStatus(str(payload["from_status"]))
                to_status = CampaignStatus(str(payload["to_status"]))
            except ValueError as error:
                raise CampaignLifecycleError("Campaign status is invalid") from error
            if (
                from_status is not status
                or _CAMPAIGN_NEXT.get(from_status) is not to_status
                or event.event_id != self._campaign_event_id(to_status.value)
            ):
                raise CampaignLifecycleError("Campaign transition is invalid")
            status = to_status
            sequence = event.sequence
        return CampaignSnapshot(self._journal._campaign_id, status, sequence)

    def _replay_cycle(self, events: tuple[CampaignEvent, ...]) -> CycleSnapshot:
        if not events:
            raise CampaignLifecycleError("Cycle lifecycle has not been created")
        opened = events[0]
        payload = _event_domain_payload(opened)
        if set(payload) != {"cycle_id", "cycle_number", "status"}:
            raise CampaignLifecycleError("Cycle OPENED event is invalid")
        try:
            cycle_id = _identifier(payload["cycle_id"], "stored cycle_id")
        except ValueError as error:
            raise CampaignLifecycleError("Cycle identity is invalid") from error
        self._require_event_envelope(
            opened,
            cycle_id=cycle_id,
            aggregate_type=_CYCLE_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )
        cycle_number = payload["cycle_number"]
        if type(cycle_number) is not int or not 1 <= cycle_number <= 1_000_000:
            raise CampaignLifecycleError("Cycle number is invalid")
        if (
            opened.event_id != self._cycle_event_id(cycle_id, CycleStatus.CREATED.value)
            or opened.event_type != _CYCLE_OPENED
            or payload["status"] != CycleStatus.CREATED.value
        ):
            raise CampaignLifecycleError("Cycle OPENED event is invalid")
        status = CycleStatus.CREATED
        sequence = opened.sequence
        for event in events[1:]:
            self._require_event_envelope(
                event,
                cycle_id=cycle_id,
                aggregate_type=_CYCLE_AGGREGATE_TYPE,
                aggregate_id=cycle_id,
            )
            payload = _event_domain_payload(event)
            if (
                event.event_type != _CYCLE_TRANSITIONED
                or set(payload)
                != {"cycle_id", "cycle_number", "from_status", "to_status"}
                or type(payload["cycle_id"]) is not str
                or type(payload["cycle_number"]) is not int
                or type(payload["from_status"]) is not str
                or type(payload["to_status"]) is not str
                or payload["cycle_id"] != cycle_id
                or payload["cycle_number"] != cycle_number
            ):
                raise CampaignLifecycleError("Cycle transition event is invalid")
            try:
                from_status = CycleStatus(str(payload["from_status"]))
                to_status = CycleStatus(str(payload["to_status"]))
            except ValueError as error:
                raise CampaignLifecycleError("Cycle status is invalid") from error
            if (
                from_status is not status
                or _CYCLE_NEXT.get(from_status) is not to_status
                or event.event_id != self._cycle_event_id(cycle_id, to_status.value)
            ):
                raise CampaignLifecycleError("Cycle transition is invalid")
            status = to_status
            sequence = event.sequence
        return CycleSnapshot(cycle_id, cycle_number, status, sequence)


__all__ = [
    "CampaignLifecycleError",
    "CampaignSnapshot",
    "CampaignStateConflictError",
    "CampaignStatus",
    "CycleSnapshot",
    "CycleStatus",
    "DuplicateCycleError",
    "IllegalCycleTransitionError",
    "OperationalCampaignLifecycle",
]
