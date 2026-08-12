"""OperationalJournal-backed Campaign and Cycle lifecycle state."""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from enum import Enum

from . import stores
from .campaign_store import (
    CampaignEvent,
    CampaignJournalError,
    OperationalCampaignJournal,
    _CYCLE_BUDGET_AGGREGATE_TYPE,
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
    BLOCKED = "BLOCKED"
    COMPLETED = "COMPLETED"
    CLOSED = "CLOSED"


class CampaignPauseStatus(str, Enum):
    RUNNING = "RUNNING"
    PAUSE_REQUESTED = "PAUSE_REQUESTED"
    PAUSED = "PAUSED"


class CycleStatus(str, Enum):
    CREATED = "CREATED"
    BUDGET_RESERVED = "BUDGET_RESERVED"
    CONTEXT_READY = "CONTEXT_READY"
    FROZEN = "FROZEN"
    EXECUTING = "EXECUTING"
    EVIDENCE_READY = "EVIDENCE_READY"
    LEARNING_COMMITTED = "LEARNING_COMMITTED"
    LEARNING_SKIPPED = "LEARNING_SKIPPED"
    SETTLED = "SETTLED"
    INFORMATION_GAIN_RECORDED = "INFORMATION_GAIN_RECORDED"
    NEXT_CYCLE_DECIDED = "NEXT_CYCLE_DECIDED"
    COMPLETED = "COMPLETED"


_CAMPAIGN_AGGREGATE_TYPE = "CAMPAIGN_STATE"
_CAMPAIGN_PAUSE_AGGREGATE_TYPE = "CAMPAIGN_PAUSE"
_CYCLE_AGGREGATE_TYPE = "CYCLE_STATE"
_CYCLE_LEASE_AGGREGATE_TYPE = "CYCLE_LEASE"
_CYCLE_CONTEXT_POLICY_AGGREGATE_TYPE = "CAMPAIGN_CYCLE_CONTEXT_POLICY"
_CYCLE_FREEZE_POLICY_AGGREGATE_TYPE = "CAMPAIGN_CYCLE_FREEZE_POLICY"
_CAMPAIGN_CREATED = "CAMPAIGN_CREATED"
_CAMPAIGN_TRANSITIONED = "CAMPAIGN_TRANSITIONED"
_CAMPAIGN_BLOCKED = "CAMPAIGN_BLOCKED"
_CAMPAIGN_PAUSE_REQUESTED = "CAMPAIGN_PAUSE_REQUESTED"
_CAMPAIGN_PAUSED = "CAMPAIGN_PAUSED"
_CAMPAIGN_RESUMED = "CAMPAIGN_RESUMED"
_PRE_CYCLE_PAUSE_BOUNDARY = "PRE_CYCLE"
_CYCLE_OPENED = "CYCLE_OPENED"
_CYCLE_TRANSITIONED = "CYCLE_TRANSITIONED"

_CAMPAIGN_TRANSITIONS = frozenset(
    {
        (CampaignStatus.CREATED, CampaignStatus.ACTIVE),
        (CampaignStatus.ACTIVE, CampaignStatus.BLOCKED),
        (CampaignStatus.ACTIVE, CampaignStatus.COMPLETED),
        (CampaignStatus.COMPLETED, CampaignStatus.CLOSED),
    }
)
_CYCLE_TRANSITIONS = frozenset(
    {
        (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
        (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
        (CycleStatus.CONTEXT_READY, CycleStatus.FROZEN),
        (CycleStatus.FROZEN, CycleStatus.EXECUTING),
        (CycleStatus.EXECUTING, CycleStatus.EVIDENCE_READY),
        (CycleStatus.EVIDENCE_READY, CycleStatus.LEARNING_COMMITTED),
        (CycleStatus.EVIDENCE_READY, CycleStatus.LEARNING_SKIPPED),
        (CycleStatus.LEARNING_COMMITTED, CycleStatus.SETTLED),
        (CycleStatus.LEARNING_SKIPPED, CycleStatus.SETTLED),
        (CycleStatus.SETTLED, CycleStatus.INFORMATION_GAIN_RECORDED),
        (
            CycleStatus.INFORMATION_GAIN_RECORDED,
            CycleStatus.NEXT_CYCLE_DECIDED,
        ),
        (CycleStatus.NEXT_CYCLE_DECIDED, CycleStatus.COMPLETED),
    }
)


@dataclass(frozen=True, slots=True)
class CampaignSnapshot:
    campaign_id: str
    status: CampaignStatus
    sequence: int
    block_reason_code: str | None = None
    block_source_ref: str | None = None


@dataclass(frozen=True, slots=True)
class CampaignPauseSnapshot:
    status: CampaignPauseStatus
    active_pause_id: str | None
    boundary_cycle_id: str | None
    sequence: int
    last_pause_id: str | None = None
    last_resume_id: str | None = None


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


def _stored_sha256(value: object, name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


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

    def pause_snapshot(self) -> CampaignPauseSnapshot:
        self._journal._authorize()
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._replay_pause(
                connection,
                self._pause_events(connection),
            )
        )

    def request_pause(self, *, pause_id: str) -> CampaignPauseSnapshot:
        self._journal._authorize()
        pause_id = _identifier(pause_id, "pause_id")

        def request(connection) -> CampaignPauseSnapshot:
            campaign = self._replay_campaign(self._campaign_events(connection))
            if campaign.status is not CampaignStatus.ACTIVE:
                raise CampaignStateConflictError("Campaign is not ACTIVE")
            events = self._pause_events(connection)
            snapshot = self._replay_pause(connection, events)
            if snapshot.status is not CampaignPauseStatus.RUNNING:
                if snapshot.active_pause_id != pause_id:
                    raise CampaignStateConflictError(
                        "Campaign already has another pause request"
                    )
                return snapshot
            if any(
                event.event_type == _CAMPAIGN_PAUSE_REQUESTED
                and _event_domain_payload(event)["pause_id"] == pause_id
                for event in events
            ):
                raise CampaignStateConflictError(
                    "pause_id was already used by this Campaign"
                )
            event = self._journal._append_in_transaction(
                connection,
                event_id=self._pause_event_id(
                    CampaignPauseStatus.PAUSE_REQUESTED.value,
                    pause_id,
                ),
                cycle_id=None,
                aggregate_type=_CAMPAIGN_PAUSE_AGGREGATE_TYPE,
                aggregate_id=self._journal._campaign_id,
                event_type=_CAMPAIGN_PAUSE_REQUESTED,
                payload={"pause_id": pause_id},
            )
            return CampaignPauseSnapshot(
                CampaignPauseStatus.PAUSE_REQUESTED,
                pause_id,
                None,
                event.sequence,
                pause_id,
                snapshot.last_resume_id,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(request)

    def pause_at_safe_boundary(
        self,
        *,
        pause_id: str,
        boundary_cycle_id: str | None,
    ) -> CampaignPauseSnapshot:
        self._journal._authorize()
        pause_id = _identifier(pause_id, "pause_id")
        if boundary_cycle_id is not None:
            boundary_cycle_id = _identifier(
                boundary_cycle_id,
                "boundary_cycle_id",
            )

        def pause(connection) -> CampaignPauseSnapshot:
            campaign = self._replay_campaign(self._campaign_events(connection))
            if campaign.status is not CampaignStatus.ACTIVE:
                raise CampaignStateConflictError("Campaign is not ACTIVE")
            snapshot = self._replay_pause(
                connection,
                self._pause_events(connection),
            )
            if snapshot.status is CampaignPauseStatus.PAUSED:
                if (
                    snapshot.active_pause_id != pause_id
                    or snapshot.boundary_cycle_id != boundary_cycle_id
                ):
                    raise CampaignStateConflictError(
                        "Campaign is PAUSED at another boundary"
                    )
                return snapshot
            if (
                snapshot.status is not CampaignPauseStatus.PAUSE_REQUESTED
                or snapshot.active_pause_id != pause_id
            ):
                raise CampaignStateConflictError(
                    "Campaign does not have the expected pause request"
                )
            opened = self._opened_cycles(connection)
            boundary: CycleSnapshot | None = None
            if opened:
                if boundary_cycle_id is not None:
                    boundary = next(
                        (
                            cycle
                            for cycle in opened
                            if cycle.cycle_id == boundary_cycle_id
                        ),
                        None,
                    )
                if (
                    boundary is None
                    or boundary.cycle_number
                    != max(cycle.cycle_number for cycle in opened)
                    or any(
                        self._replay_cycle(
                            self._cycle_events(connection, cycle.cycle_id)
                        ).status
                        is not CycleStatus.COMPLETED
                        for cycle in opened
                    )
                ):
                    raise CampaignStateConflictError(
                        "Campaign pause requires the latest completed Cycle boundary"
                    )
            elif boundary_cycle_id is not None:
                raise CampaignStateConflictError(
                    "pre-Cycle pause cannot name a Cycle boundary"
                )
            boundary_role = (
                _PRE_CYCLE_PAUSE_BOUNDARY
                if boundary_cycle_id is None
                else boundary_cycle_id
            )
            event = self._journal._append_in_transaction(
                connection,
                event_id=self._pause_event_id(
                    CampaignPauseStatus.PAUSED.value,
                    pause_id,
                    boundary_role,
                ),
                cycle_id=None,
                aggregate_type=_CAMPAIGN_PAUSE_AGGREGATE_TYPE,
                aggregate_id=self._journal._campaign_id,
                event_type=_CAMPAIGN_PAUSED,
                payload={
                    "pause_id": pause_id,
                    "boundary_cycle_id": boundary_cycle_id,
                    "boundary_cycle_number": (
                        None if boundary is None else boundary.cycle_number
                    ),
                },
            )
            return CampaignPauseSnapshot(
                CampaignPauseStatus.PAUSED,
                pause_id,
                boundary_cycle_id,
                event.sequence,
                pause_id,
                snapshot.last_resume_id,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(pause)

    def resume_pause(
        self,
        *,
        pause_id: str,
        resume_id: str,
    ) -> CampaignPauseSnapshot:
        self._journal._authorize()
        pause_id = _identifier(pause_id, "pause_id")
        resume_id = _identifier(resume_id, "resume_id")

        def resume(connection) -> CampaignPauseSnapshot:
            campaign = self._replay_campaign(self._campaign_events(connection))
            if campaign.status is not CampaignStatus.ACTIVE:
                raise CampaignStateConflictError("Campaign is not ACTIVE")
            events = self._pause_events(connection)
            snapshot = self._replay_pause(connection, events)
            if snapshot.status is CampaignPauseStatus.RUNNING:
                if (
                    snapshot.last_pause_id == pause_id
                    and snapshot.last_resume_id == resume_id
                ):
                    return snapshot
                raise CampaignStateConflictError("Campaign is not paused")
            if snapshot.active_pause_id != pause_id:
                raise CampaignStateConflictError(
                    "Campaign has another active pause request"
                )
            if any(
                event.event_type == _CAMPAIGN_RESUMED
                and _event_domain_payload(event)["resume_id"] == resume_id
                for event in events
            ):
                raise CampaignStateConflictError(
                    "resume_id was already used by this Campaign"
                )
            event = self._journal._append_in_transaction(
                connection,
                event_id=self._pause_event_id(
                    CampaignPauseStatus.RUNNING.value,
                    pause_id,
                    resume_id,
                ),
                cycle_id=None,
                aggregate_type=_CAMPAIGN_PAUSE_AGGREGATE_TYPE,
                aggregate_id=self._journal._campaign_id,
                event_type=_CAMPAIGN_RESUMED,
                payload={
                    "pause_id": pause_id,
                    "resume_id": resume_id,
                },
            )
            return CampaignPauseSnapshot(
                CampaignPauseStatus.RUNNING,
                None,
                None,
                event.sequence,
                pause_id,
                resume_id,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(resume)

    def activate(self) -> CampaignSnapshot:
        self._journal._authorize()

        return _SqliteUnitOfWork(stores._operational_spec())._write(
            self._activate_in_transaction
        )

    def _activate_in_transaction(self, connection) -> CampaignSnapshot:
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

    def open_cycle(self, *, cycle_id: str, cycle_number: int) -> CycleSnapshot:
        self._journal._authorize()

        def open_cycle_state(connection) -> CycleSnapshot:
            if self._cycle_budget_configured(connection):
                raise CampaignStateConflictError(
                    "configured Cycle budget requires budgeted Cycle open"
                )
            return self._open_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                cycle_number=cycle_number,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(
            open_cycle_state
        )

    def _open_cycle_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        cycle_number: int,
    ) -> CycleSnapshot:
        cycle_id = _identifier(cycle_id, "cycle_id")
        if type(cycle_number) is not int or not 1 <= cycle_number <= 1_000_000:
            raise ValueError("cycle_number must be from 1 through 1000000")
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
        pause = self._replay_pause(
            connection,
            self._pause_events(connection),
        )
        if pause.status is not CampaignPauseStatus.RUNNING:
            raise CampaignStateConflictError(
                "Campaign pause prevents opening a new Cycle"
            )
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

    def _cycle_budget_configured(self, connection) -> bool:
        return connection.execute(
            "SELECT 1 FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? LIMIT 1",
            (
                self._journal._namespace,
                self._journal._campaign_id,
                _CYCLE_BUDGET_AGGREGATE_TYPE,
            ),
        ).fetchone() is not None

    def complete(self) -> CampaignSnapshot:
        self._journal._authorize()

        def complete_unmanaged_campaign(connection) -> CampaignSnapshot:
            if self._cycle_context_policy_configured(connection):
                raise CampaignStateConflictError(
                    "controller-managed Campaign requires controller completion"
                )
            return self._complete_in_transaction(connection)

        return _SqliteUnitOfWork(stores._operational_spec())._write(
            complete_unmanaged_campaign
        )

    def _complete_in_transaction(
        self,
        connection,
    ) -> CampaignSnapshot:
        snapshot = self._replay_campaign(self._campaign_events(connection))
        if snapshot.status is CampaignStatus.COMPLETED:
            return snapshot
        if snapshot.status is not CampaignStatus.ACTIVE:
            raise CampaignStateConflictError("Campaign is not ACTIVE")
        pause = self._replay_pause(
            connection,
            self._pause_events(connection),
        )
        if pause.status is not CampaignPauseStatus.RUNNING:
            raise CampaignStateConflictError(
                "Campaign must resume before completion"
            )
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

    def block(self, *, reason_code: str, source_ref: str) -> CampaignSnapshot:
        self._journal._authorize()
        reason_code = _identifier(reason_code, "reason_code")
        source_ref = _identifier(source_ref, "source_ref")
        return _SqliteUnitOfWork(stores._operational_spec())._write(
            lambda connection: self._block_in_transaction(
                connection,
                reason_code=reason_code,
                source_ref=source_ref,
            )
        )

    def _block_in_transaction(
        self,
        connection,
        *,
        reason_code: str,
        source_ref: str,
    ) -> CampaignSnapshot:
        snapshot = self._replay_campaign(self._campaign_events(connection))
        if snapshot.status is CampaignStatus.BLOCKED:
            if (
                snapshot.block_reason_code != reason_code
                or snapshot.block_source_ref != source_ref
            ):
                raise CampaignStateConflictError(
                    "Campaign is already BLOCKED by different provenance"
                )
            return snapshot
        if snapshot.status is not CampaignStatus.ACTIVE:
            raise CampaignStateConflictError("Campaign is not ACTIVE")
        primary_event_id = self._campaign_event_id(CampaignStatus.BLOCKED.value)
        event_id = primary_event_id
        occupied = self._event_collision_binding_in_transaction(
            connection,
            primary_event_id,
        )
        payload: dict[str, object] = {
            "from_status": CampaignStatus.ACTIVE.value,
            "to_status": CampaignStatus.BLOCKED.value,
            "reason_code": reason_code,
            "source_ref": source_ref,
        }
        if occupied is not None:
            collision_event_id, collision_integrity_sha256 = occupied
            while True:
                recovery_nonce = secrets.token_hex(32)
                event_id = self._campaign_block_recovery_event_id(
                    reason_code=reason_code,
                    source_ref=source_ref,
                    recovery_nonce=recovery_nonce,
                    collision_event_id=collision_event_id,
                    collision_integrity_sha256=collision_integrity_sha256,
                )
                if (
                    self._event_collision_binding_in_transaction(
                        connection,
                        event_id,
                    )
                    is None
                ):
                    break
            payload["event_id_recovery"] = {
                "nonce": recovery_nonce,
                "collision_event_id": collision_event_id,
                "collision_integrity_sha256": collision_integrity_sha256,
            }
        event = self._journal._append_in_transaction(
            connection,
            event_id=event_id,
            cycle_id=None,
            aggregate_type=_CAMPAIGN_AGGREGATE_TYPE,
            aggregate_id=self._journal._campaign_id,
            event_type=_CAMPAIGN_BLOCKED,
            payload=payload,
        )
        return CampaignSnapshot(
            self._journal._campaign_id,
            CampaignStatus.BLOCKED,
            event.sequence,
            reason_code,
            source_ref,
        )

    @staticmethod
    def _event_collision_binding_in_transaction(
        connection,
        event_id: str,
    ) -> tuple[str, str] | None:
        try:
            event = OperationalCampaignJournal._event_in_transaction(
                connection,
                event_id,
            )
        except (CampaignJournalError, KeyError, TypeError, ValueError):
            row = connection.execute(
                "SELECT * FROM campaign_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if row is None:
                return None
            digest = hashlib.sha256(
                b"control_plane.corrupt_campaign_event_collision.v1\0"
            )
            for field in (
                "sequence",
                "event_id",
                "namespace",
                "campaign_id",
                "cycle_id",
                "aggregate_type",
                "aggregate_id",
                "event_type",
                "payload_json",
                "payload_sha256",
                "occurred_at",
            ):
                value = row[field]
                if value is None:
                    encoded = b"none:"
                elif isinstance(value, bytes):
                    encoded = b"bytes:" + value
                else:
                    encoded = (
                        f"{type(value).__name__}:{value}"
                    ).encode("utf-8", errors="surrogatepass")
                digest.update(field.encode("ascii"))
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
            return event_id, digest.hexdigest()
        if event is None:
            return None
        return event.event_id, event.payload_sha256

    def advance_cycle(
        self,
        *,
        cycle_id: str,
        expected_status: CycleStatus,
        next_status: CycleStatus,
    ) -> CycleSnapshot:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")
        self._validate_cycle_transition(expected_status, next_status)
        if CycleStatus.LEARNING_SKIPPED in {
            expected_status,
            next_status,
        }:
            raise CampaignStateConflictError(
                "LEARNING_SKIPPED transitions are controller-owned"
            )

        def advance_unleased(connection) -> CycleSnapshot:
            if (
                next_status is CycleStatus.CONTEXT_READY
                and self._cycle_context_policy_configured(connection)
            ):
                raise CampaignStateConflictError(
                    "configured Cycle context policy requires a safe context receipt"
                )
            if (
                next_status is CycleStatus.FROZEN
                and self._cycle_freeze_policy_configured(connection)
            ):
                raise CampaignStateConflictError(
                    "configured Cycle freeze policy requires a frozen input manifest"
                )
            occupied = connection.execute(
                """
                SELECT 1 FROM campaign_events
                WHERE namespace = ? AND campaign_id = ? AND cycle_id = ?
                  AND aggregate_type = ? AND aggregate_id = ?
                LIMIT 1
                """,
                (
                    self._journal._namespace,
                    self._journal._campaign_id,
                    cycle_id,
                    _CYCLE_LEASE_AGGREGATE_TYPE,
                    cycle_id,
                ),
            ).fetchone()
            if occupied is not None:
                raise CampaignStateConflictError(
                    "Cycle has an execution lease and requires fenced mutation"
                )
            return self._advance_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                expected_status=expected_status,
                next_status=next_status,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(
            advance_unleased
        )

    def _cycle_context_policy_configured(self, connection) -> bool:
        return connection.execute(
            "SELECT 1 FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? LIMIT 1",
            (
                self._journal._namespace,
                self._journal._campaign_id,
                _CYCLE_CONTEXT_POLICY_AGGREGATE_TYPE,
            ),
        ).fetchone() is not None

    def _cycle_freeze_policy_configured(self, connection) -> bool:
        return connection.execute(
            "SELECT 1 FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? LIMIT 1",
            (
                self._journal._namespace,
                self._journal._campaign_id,
                _CYCLE_FREEZE_POLICY_AGGREGATE_TYPE,
            ),
        ).fetchone() is not None

    @staticmethod
    def _validate_cycle_transition(
        expected_status: CycleStatus,
        next_status: CycleStatus,
    ) -> None:
        if not isinstance(expected_status, CycleStatus) or not isinstance(
            next_status,
            CycleStatus,
        ):
            raise TypeError("Cycle transitions require CycleStatus values")
        if (expected_status, next_status) not in _CYCLE_TRANSITIONS:
            raise IllegalCycleTransitionError(
                "Cycle transition skips a required protocol state"
            )

    def _advance_cycle_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        expected_status: CycleStatus,
        next_status: CycleStatus,
    ) -> CycleSnapshot:
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

    def _pause_events(self, connection) -> tuple[CampaignEvent, ...]:
        return self._journal._list_in_transaction(
            connection,
            cycle_id=None,
            aggregate_type=_CAMPAIGN_PAUSE_AGGREGATE_TYPE,
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

    def _opened_cycles(
        self,
        connection,
        *,
        sequence_cutoff: int | None = None,
    ) -> tuple[CycleSnapshot, ...]:
        """Enumerate opened Cycles and enforce unique Cycle numbers.

        Without a cutoff this reads every CYCLE_OPENED row in sequence
        order and replays only that event. With a cutoff, the sequence
        bound is applied in the SQLite query (rows at or past the cutoff
        are never loaded) and each Cycle is replayed from its stream as it
        existed strictly before the cutoff, so later transitions cannot
        retroactively alter the result.

        Duplicate Cycle numbers raise DuplicateCycleError("Cycle numbers
        must be unique") and fail fast: the error propagates through every
        write and snapshot path here (open, complete, pause boundary) and
        through the campaign_context, campaign_controller, and
        campaign_freeze operations that enumerate opened Cycles inside
        their own transactions, rolling those transactions back. Callers
        must not swallow it.
        """
        query = """
            SELECT * FROM campaign_events
            WHERE namespace = ? AND campaign_id = ?
              AND cycle_id IS NOT NULL AND aggregate_type = ?
              AND event_type = ?
        """
        query_values = [
            self._journal._namespace,
            self._journal._campaign_id,
            _CYCLE_AGGREGATE_TYPE,
            _CYCLE_OPENED,
        ]
        if sequence_cutoff is not None:
            query += " AND sequence < ?"
            query_values.append(sequence_cutoff)
        query += " ORDER BY sequence"
        rows = connection.execute(query, query_values).fetchall()
        opened: list[CycleSnapshot] = []
        opened_numbers: dict[int, str] = {}
        for row in rows:
            cycle_event = _event_from_row(row)
            if sequence_cutoff is None:
                snapshot = self._replay_cycle((cycle_event,))
            else:
                snapshot = self._replay_cycle(
                    self._cycle_events_before(
                        connection,
                        cycle_event.aggregate_id,
                        sequence_cutoff,
                    )
                )
            existing_cycle_id = opened_numbers.get(snapshot.cycle_number)
            if (
                existing_cycle_id is not None
                and existing_cycle_id != snapshot.cycle_id
            ):
                raise DuplicateCycleError(
                    "Cycle numbers must be unique"
                )
            opened_numbers[snapshot.cycle_number] = snapshot.cycle_id
            opened.append(snapshot)
        return tuple(opened)

    def _campaign_event_id(self, role: str) -> str:
        return _state_event_id(
            namespace=self._journal._namespace,
            campaign_id=self._journal._campaign_id,
            aggregate_type=_CAMPAIGN_AGGREGATE_TYPE,
            aggregate_id=self._journal._campaign_id,
            role=role,
        )

    def _pause_event_id(self, *role_parts: str) -> str:
        if not role_parts:
            raise ValueError("pause event identity requires role components")
        components = tuple(
            _identifier(value, "pause event role component")
            for value in role_parts
        )
        return hashlib.sha256(
            b"control_plane.campaign_pause_event.v2\0"
            + "\0".join(
                (
                    self._journal._namespace,
                    self._journal._campaign_id,
                    *components,
                )
            ).encode("ascii")
        ).hexdigest()

    def _campaign_block_recovery_event_id(
        self,
        *,
        reason_code: str,
        source_ref: str,
        recovery_nonce: str,
        collision_event_id: str,
        collision_integrity_sha256: str,
    ) -> str:
        recovery_binding = hashlib.sha256(
            b"control_plane.campaign_block_recovery.v2\0"
            + "\0".join(
                (
                    reason_code,
                    source_ref,
                    recovery_nonce,
                    collision_event_id,
                    collision_integrity_sha256,
                )
            ).encode("ascii")
        ).hexdigest()
        return self._campaign_event_id(
            f"{CampaignStatus.BLOCKED.value}_RECOVERY:{recovery_binding}"
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
        block_reason_code: str | None = None
        block_source_ref: str | None = None
        for event in events[1:]:
            self._require_event_envelope(
                event,
                cycle_id=None,
                aggregate_type=_CAMPAIGN_AGGREGATE_TYPE,
                aggregate_id=self._journal._campaign_id,
            )
            payload = _event_domain_payload(event)
            expected_event_id: str | None = None
            if event.event_type == _CAMPAIGN_TRANSITIONED:
                if set(payload) != {"from_status", "to_status"}:
                    raise CampaignLifecycleError(
                        "Campaign transition event is invalid"
                    )
                allowed = {
                    (CampaignStatus.CREATED, CampaignStatus.ACTIVE),
                    (CampaignStatus.ACTIVE, CampaignStatus.COMPLETED),
                }
            elif event.event_type == _CAMPAIGN_BLOCKED:
                block_fields = {
                    "from_status",
                    "to_status",
                    "reason_code",
                    "source_ref",
                }
                if frozenset(payload) not in {
                    frozenset(block_fields),
                    frozenset((*block_fields, "event_id_recovery")),
                }:
                    raise CampaignLifecycleError(
                        "Campaign BLOCKED event is invalid"
                    )
                try:
                    reason_code = _identifier(
                        payload["reason_code"],
                        "stored reason_code",
                    )
                    source_ref = _identifier(
                        payload["source_ref"],
                        "stored source_ref",
                    )
                except ValueError as error:
                    raise CampaignLifecycleError(
                        "Campaign BLOCKED binding is invalid"
                    ) from error
                expected_event_id = self._campaign_event_id(
                    CampaignStatus.BLOCKED.value
                )
                if "event_id_recovery" in payload:
                    recovery = payload["event_id_recovery"]
                    if (
                        not isinstance(recovery, dict)
                        or set(recovery)
                        != {
                            "nonce",
                            "collision_event_id",
                            "collision_integrity_sha256",
                        }
                    ):
                        raise CampaignLifecycleError(
                            "Campaign BLOCKED recovery is invalid"
                        )
                    try:
                        recovery_nonce = _stored_sha256(
                            recovery["nonce"],
                            "stored recovery nonce",
                        )
                        collision_event_id = _identifier(
                            recovery["collision_event_id"],
                            "stored collision event_id",
                        )
                        collision_integrity_sha256 = _stored_sha256(
                            recovery["collision_integrity_sha256"],
                            "stored collision integrity_sha256",
                        )
                    except ValueError as error:
                        raise CampaignLifecycleError(
                            "Campaign BLOCKED recovery binding is invalid"
                        ) from error
                    if collision_event_id != expected_event_id:
                        raise CampaignLifecycleError(
                            "Campaign BLOCKED recovery collision is invalid"
                        )
                    expected_event_id = self._campaign_block_recovery_event_id(
                        reason_code=reason_code,
                        source_ref=source_ref,
                        recovery_nonce=recovery_nonce,
                        collision_event_id=collision_event_id,
                        collision_integrity_sha256=(
                            collision_integrity_sha256
                        ),
                    )
                allowed = {
                    (CampaignStatus.ACTIVE, CampaignStatus.BLOCKED),
                }
                block_reason_code = reason_code
                block_source_ref = source_ref
            else:
                raise CampaignLifecycleError("Campaign transition event is invalid")
            if (
                type(payload["from_status"]) is not str
                or type(payload["to_status"]) is not str
            ):
                raise CampaignLifecycleError("Campaign transition event is invalid")
            try:
                from_status = CampaignStatus(str(payload["from_status"]))
                to_status = CampaignStatus(str(payload["to_status"]))
            except ValueError as error:
                raise CampaignLifecycleError("Campaign status is invalid") from error
            if expected_event_id is None:
                expected_event_id = self._campaign_event_id(to_status.value)
            if (
                from_status is not status
                or (from_status, to_status) not in _CAMPAIGN_TRANSITIONS
                or (from_status, to_status) not in allowed
                or event.event_id != expected_event_id
            ):
                raise CampaignLifecycleError("Campaign transition is invalid")
            status = to_status
            sequence = event.sequence
        return CampaignSnapshot(
            self._journal._campaign_id,
            status,
            sequence,
            block_reason_code,
            block_source_ref,
        )

    def _replay_pause(
        self,
        connection,
        events: tuple[CampaignEvent, ...],
    ) -> CampaignPauseSnapshot:
        if not events:
            return CampaignPauseSnapshot(
                CampaignPauseStatus.RUNNING,
                None,
                None,
                0,
                None,
                None,
            )
        status = CampaignPauseStatus.RUNNING
        active_pause_id: str | None = None
        boundary_cycle_id: str | None = None
        last_pause_id: str | None = None
        last_resume_id: str | None = None
        seen_pause_ids: set[str] = set()
        seen_resume_ids: set[str] = set()
        sequence = 0
        for event in events:
            self._require_event_envelope(
                event,
                cycle_id=None,
                aggregate_type=_CAMPAIGN_PAUSE_AGGREGATE_TYPE,
                aggregate_id=self._journal._campaign_id,
            )
            payload = _event_domain_payload(event)
            if event.event_type == _CAMPAIGN_PAUSE_REQUESTED:
                if set(payload) != {"pause_id"}:
                    raise CampaignLifecycleError(
                        "Campaign pause request is invalid"
                    )
                try:
                    pause_id = _identifier(
                        payload["pause_id"],
                        "stored pause_id",
                    )
                except ValueError as error:
                    raise CampaignLifecycleError(
                        "Campaign pause binding is invalid"
                    ) from error
                if (
                    status is not CampaignPauseStatus.RUNNING
                    or pause_id in seen_pause_ids
                    or event.event_id
                    != self._pause_event_id(
                        CampaignPauseStatus.PAUSE_REQUESTED.value,
                        pause_id,
                    )
                ):
                    raise CampaignLifecycleError(
                        "Campaign pause transition is invalid"
                    )
                status = CampaignPauseStatus.PAUSE_REQUESTED
                active_pause_id = pause_id
                boundary_cycle_id = None
                last_pause_id = pause_id
                seen_pause_ids.add(pause_id)
            elif event.event_type == _CAMPAIGN_PAUSED:
                if set(payload) != {
                    "pause_id",
                    "boundary_cycle_id",
                    "boundary_cycle_number",
                }:
                    raise CampaignLifecycleError(
                        "Campaign PAUSED event is invalid"
                    )
                try:
                    pause_id = _identifier(
                        payload["pause_id"],
                        "stored pause_id",
                    )
                    stored_boundary_cycle_id = payload["boundary_cycle_id"]
                    if stored_boundary_cycle_id is not None:
                        stored_boundary_cycle_id = _identifier(
                            stored_boundary_cycle_id,
                            "stored boundary_cycle_id",
                        )
                except ValueError as error:
                    raise CampaignLifecycleError(
                        "Campaign PAUSED binding is invalid"
                    ) from error
                boundary_cycle_number = payload["boundary_cycle_number"]
                boundary_binding_valid = (
                    stored_boundary_cycle_id is None
                    and boundary_cycle_number is None
                ) or (
                    stored_boundary_cycle_id is not None
                    and type(boundary_cycle_number) is int
                    and 1 <= boundary_cycle_number <= 1_000_000
                )
                boundary_role = (
                    _PRE_CYCLE_PAUSE_BOUNDARY
                    if stored_boundary_cycle_id is None
                    else stored_boundary_cycle_id
                )
                if (
                    not boundary_binding_valid
                    or status is not CampaignPauseStatus.PAUSE_REQUESTED
                    or active_pause_id != pause_id
                    or event.event_id
                    != self._pause_event_id(
                        CampaignPauseStatus.PAUSED.value,
                        pause_id,
                        boundary_role,
                    )
                ):
                    raise CampaignLifecycleError(
                        "Campaign PAUSED transition is invalid"
                    )
                self._validate_pause_boundary_in_transaction(
                    connection,
                    pause_event=event,
                    boundary_cycle_id=stored_boundary_cycle_id,
                    boundary_cycle_number=boundary_cycle_number,
                )
                status = CampaignPauseStatus.PAUSED
                boundary_cycle_id = stored_boundary_cycle_id
            elif event.event_type == _CAMPAIGN_RESUMED:
                if set(payload) != {"pause_id", "resume_id"}:
                    raise CampaignLifecycleError(
                        "Campaign RESUMED event is invalid"
                    )
                try:
                    pause_id = _identifier(
                        payload["pause_id"],
                        "stored pause_id",
                    )
                    resume_id = _identifier(
                        payload["resume_id"],
                        "stored resume_id",
                    )
                except ValueError as error:
                    raise CampaignLifecycleError(
                        "Campaign RESUMED binding is invalid"
                    ) from error
                if (
                    status
                    not in {
                        CampaignPauseStatus.PAUSE_REQUESTED,
                        CampaignPauseStatus.PAUSED,
                    }
                    or active_pause_id != pause_id
                    or resume_id in seen_resume_ids
                    or event.event_id
                    != self._pause_event_id(
                        CampaignPauseStatus.RUNNING.value,
                        pause_id,
                        resume_id,
                    )
                ):
                    raise CampaignLifecycleError(
                        "Campaign RESUMED transition is invalid"
                    )
                status = CampaignPauseStatus.RUNNING
                active_pause_id = None
                boundary_cycle_id = None
                last_pause_id = pause_id
                last_resume_id = resume_id
                seen_resume_ids.add(resume_id)
            else:
                raise CampaignLifecycleError("Campaign pause event is invalid")
            sequence = event.sequence
        return CampaignPauseSnapshot(
            status,
            active_pause_id,
            boundary_cycle_id,
            sequence,
            last_pause_id,
            last_resume_id,
        )

    def _cycle_events_before(
        self,
        connection,
        cycle_id: str,
        sequence: int,
    ) -> tuple[CampaignEvent, ...]:
        """Return one Cycle stream as it existed before an event sequence.

        The cutoff is applied in the SQLite query so rows at or past the
        cutoff are never loaded or replayed in Python.
        """
        rows = connection.execute(
            """
            SELECT * FROM campaign_events
            WHERE namespace = ? AND campaign_id = ? AND cycle_id = ?
              AND aggregate_type = ? AND aggregate_id = ?
              AND sequence < ?
            ORDER BY sequence
            """,
            (
                self._journal._namespace,
                self._journal._campaign_id,
                cycle_id,
                _CYCLE_AGGREGATE_TYPE,
                cycle_id,
                sequence,
            ),
        ).fetchall()
        return tuple(_event_from_row(row) for row in rows)

    def _validate_pause_boundary_in_transaction(
        self,
        connection,
        *,
        pause_event: CampaignEvent,
        boundary_cycle_id: str | None,
        boundary_cycle_number: int | None,
    ) -> None:
        """Cross-check a persisted pause boundary against the Cycle stream.

        The write path admits a pause only at the latest completed Cycle
        boundary, so a replay that accepts anything else would hide a
        corrupted pause stream. Completion and prior-completion are
        evaluated from each Cycle stream as it existed at the pause event's
        sequence; later Cycle transitions cannot retroactively legitimize a
        forged pause. Cycles opened after the pause event cannot have been
        part of its boundary and are intentionally ignored.

        Replay cost is bounded and non-material: each PAUSED event triggers
        one shared opened-Cycle enumeration plus one stream replay per
        opened Cycle (O(G * C * E) event replays across G pause
        generations). No caches or additional persistence are used;
        duplicate Cycle numbers still surface as DuplicateCycleError from
        the shared enumeration before any boundary check.
        """
        opened_before = {
            snapshot.cycle_id: snapshot
            for snapshot in self._opened_cycles(
                connection,
                sequence_cutoff=pause_event.sequence,
            )
        }
        if boundary_cycle_id is None:
            if opened_before:
                raise CampaignLifecycleError(
                    "pre-Cycle pause boundary conflicts with opened Cycles"
                )
            return
        boundary_snapshot = opened_before.get(boundary_cycle_id)
        if boundary_snapshot is None:
            raise CampaignLifecycleError(
                "pause boundary Cycle does not exist"
            )
        if boundary_snapshot.cycle_number != boundary_cycle_number:
            raise CampaignLifecycleError(
                "pause boundary Cycle number is invalid"
            )
        if boundary_snapshot.status is not CycleStatus.COMPLETED:
            raise CampaignLifecycleError(
                "pause boundary Cycle is not completed"
            )
        if boundary_snapshot.cycle_number != max(
            snapshot.cycle_number for snapshot in opened_before.values()
        ):
            raise CampaignLifecycleError(
                "pause boundary is not the latest completed Cycle"
            )
        if any(
            snapshot.status is not CycleStatus.COMPLETED
            for snapshot in opened_before.values()
        ):
            raise CampaignLifecycleError(
                "pause boundary requires every prior Cycle completed"
            )

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
                or (from_status, to_status) not in _CYCLE_TRANSITIONS
                or event.event_id != self._cycle_event_id(cycle_id, to_status.value)
            ):
                raise CampaignLifecycleError("Cycle transition is invalid")
            status = to_status
            sequence = event.sequence
        return CycleSnapshot(cycle_id, cycle_number, status, sequence)


__all__ = [
    "CampaignLifecycleError",
    "CampaignPauseSnapshot",
    "CampaignPauseStatus",
    "CampaignSnapshot",
    "CampaignStateConflictError",
    "CampaignStatus",
    "CycleSnapshot",
    "CycleStatus",
    "DuplicateCycleError",
    "IllegalCycleTransitionError",
    "OperationalCampaignLifecycle",
]
