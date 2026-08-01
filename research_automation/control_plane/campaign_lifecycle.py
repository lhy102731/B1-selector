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
_CYCLE_LEASE_AGGREGATE_TYPE = "CYCLE_LEASE"
_CAMPAIGN_CREATED = "CAMPAIGN_CREATED"
_CAMPAIGN_TRANSITIONED = "CAMPAIGN_TRANSITIONED"
_CAMPAIGN_BLOCKED = "CAMPAIGN_BLOCKED"
_CYCLE_OPENED = "CYCLE_OPENED"
_CYCLE_TRANSITIONED = "CYCLE_TRANSITIONED"

_CAMPAIGN_TRANSITIONS = frozenset(
    {
        (CampaignStatus.CREATED, CampaignStatus.ACTIVE),
        (CampaignStatus.ACTIVE, CampaignStatus.BLOCKED),
        (CampaignStatus.ACTIVE, CampaignStatus.COMPLETED),
    }
)
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
    block_reason_code: str | None = None
    block_source_ref: str | None = None


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

        def advance_unleased(connection) -> CycleSnapshot:
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
        if _CYCLE_NEXT.get(expected_status) is not next_status:
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
