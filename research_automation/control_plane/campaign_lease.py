"""Cycle-scoped P6 execution leases with persisted fencing identity."""

from __future__ import annotations

import hashlib
import math
import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol, runtime_checkable

import psutil

from . import stores
from .campaign_lifecycle import (
    CampaignStatus,
    CycleSnapshot,
    CycleStatus,
    OperationalCampaignLifecycle,
)
from .campaign_store import (
    CampaignEvent,
    CampaignJournalError,
    OperationalCampaignJournal,
    _event_domain_payload,
    _identifier,
)
from .sqlite_uow import _SqliteUnitOfWork


_LEASE_AGGREGATE_TYPE = "CYCLE_LEASE"
_LEASE_ACQUIRED = "CYCLE_LEASE_ACQUIRED"
_LEASE_HEARTBEAT = "CYCLE_LEASE_HEARTBEAT"
_LEASE_REPLACED = "CYCLE_LEASE_REPLACED"
_MAX_PID = (1 << 31) - 1
_MAX_CLOCK_VALUE = (1 << 63) - 1
_LEASE_OWNED_CYCLE_STATUSES = frozenset(
    {
        CycleStatus.FROZEN,
        CycleStatus.EXECUTING,
        CycleStatus.EVIDENCE_READY,
        CycleStatus.LEARNING_COMMITTED,
        CycleStatus.LEARNING_SKIPPED,
        CycleStatus.SETTLED,
        CycleStatus.INFORMATION_GAIN_RECORDED,
        CycleStatus.NEXT_CYCLE_DECIDED,
    }
)


class CycleLeaseError(RuntimeError):
    """Base error for persisted P6 Cycle leases."""


class CycleLeaseConflictError(CycleLeaseError):
    """Raised when another process identity owns a Cycle lease."""


class CycleLeaseIntegrityError(CycleLeaseError):
    """Raised when persisted Cycle lease events are not canonical."""


class StaleFencingTokenError(CycleLeaseConflictError):
    """Raised when a stale lease snapshot attempts a fenced write."""


def _bounded_int(
    value: int,
    name: str,
    *,
    minimum: int,
    maximum: int = _MAX_CLOCK_VALUE,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside the supported range")
    return value


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    host_id: str
    pid: int
    process_started_at_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "host_id",
            _identifier(self.host_id, "host_id"),
        )
        object.__setattr__(
            self,
            "pid",
            _bounded_int(
                self.pid,
                "pid",
                minimum=1,
                maximum=_MAX_PID,
            ),
        )
        object.__setattr__(
            self,
            "process_started_at_ns",
            _bounded_int(
                self.process_started_at_ns,
                "process_started_at_ns",
                minimum=1,
            ),
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "host_id": self.host_id,
            "pid": self.pid,
            "process_started_at_ns": self.process_started_at_ns,
        }


def _process_started_at_ns(process: psutil.Process) -> int:
    started_at = process.create_time()
    if (
        isinstance(started_at, bool)
        or not isinstance(started_at, (int, float))
        or not math.isfinite(started_at)
        or started_at <= 0
    ):
        raise ValueError("process create time is invalid")
    return _bounded_int(
        int(round(started_at * 1_000_000_000)),
        "process_started_at_ns",
        minimum=1,
    )


def _local_host_id() -> str:
    if os.name == "nt":
        import winreg

        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Cryptography",
        ) as key:
            material, _ = winreg.QueryValueEx(key, "MachineGuid")
    else:
        material = None
        for path in (
            Path("/etc/machine-id"),
            Path("/var/lib/dbus/machine-id"),
        ):
            try:
                material = path.read_text(encoding="ascii")
                break
            except FileNotFoundError:
                continue
    if (
        type(material) is not str
        or not material.strip()
        or len(material) > 4_096
    ):
        raise RuntimeError("stable local machine identity is unavailable")
    digest = hashlib.sha256(
        b"control_plane.local_host_identity.v1\0"
        + material.strip().lower().encode("utf-8")
    ).hexdigest()
    return f"host_{digest}"


@runtime_checkable
class ProcessIdentityProvider(Protocol):
    def current(self) -> ProcessIdentity: ...

    def probe(self, host_id: str, pid: int) -> int | None: ...


class LocalProcessIdentityProvider:
    """Read the current host/process identity from the local process table."""

    __slots__ = ("_host_id",)

    def __init__(self) -> None:
        self._host_id = _identifier(_local_host_id(), "local host_id")

    def current(self) -> ProcessIdentity:
        process = psutil.Process(os.getpid())
        return ProcessIdentity(
            host_id=self._host_id,
            pid=process.pid,
            process_started_at_ns=_process_started_at_ns(process),
        )

    def probe(self, host_id: str, pid: int) -> int | None:
        host_id = _identifier(host_id, "host_id")
        pid = _bounded_int(pid, "pid", minimum=1, maximum=_MAX_PID)
        if host_id != self._host_id:
            raise ValueError("remote process identity cannot be probed locally")
        try:
            return _process_started_at_ns(psutil.Process(pid))
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            return None


def _verified_current_owner(
    identity_provider: ProcessIdentityProvider,
) -> ProcessIdentity:
    if not isinstance(identity_provider, ProcessIdentityProvider):
        raise TypeError("identity_provider must provide current and probe methods")
    try:
        owner = identity_provider.current()
        if not isinstance(owner, ProcessIdentity):
            raise TypeError("current process identity is invalid")
        observed_start = identity_provider.probe(owner.host_id, owner.pid)
        observed_start = _bounded_int(
            observed_start,
            "observed current process_started_at_ns",
            minimum=1,
        )
    except Exception as error:
        raise CycleLeaseConflictError(
            "current process identity could not be verified"
        ) from error
    if observed_start != owner.process_started_at_ns:
        raise CycleLeaseConflictError(
            "current process identity does not match the process table"
        )
    return owner


@dataclass(frozen=True, slots=True)
class CycleLease:
    cycle_id: str
    acquisition_id: str
    lease_id: str
    fencing_token: int
    owner: ProcessIdentity
    heartbeat_sequence: int
    heartbeat_monotonic_ns: int
    event_sequence: int


@dataclass(frozen=True, slots=True)
class _LeaseHistory:
    active: CycleLease
    acquisition_ids: frozenset[str]
    heartbeat_ids: frozenset[str]
    last_heartbeat_id: str | None
    previous: CycleLease | None


def _sha256(value: object, name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _lease_event_id(
    *,
    namespace: str,
    campaign_id: str,
    cycle_id: str,
    lease_id: str,
    role: str,
) -> str:
    return hashlib.sha256(
        b"control_plane.campaign_lease_event.v1\0"
        + "\0".join(
            (namespace, campaign_id, cycle_id, lease_id, role)
        ).encode("ascii")
    ).hexdigest()


class OperationalCycleLeaseJournal:
    """Acquire one independently fenced execution lease per Campaign Cycle."""

    __slots__ = (
        "_journal",
        "_lifecycle",
        "_owner",
        "_monotonic_ns",
        "_identity_provider",
    )

    def __init__(
        self,
        *,
        journal: OperationalCampaignJournal,
        lifecycle: OperationalCampaignLifecycle,
        identity_provider: ProcessIdentityProvider,
        monotonic_ns: Callable[[], int],
    ) -> None:
        if not isinstance(journal, OperationalCampaignJournal) or not isinstance(
            lifecycle,
            OperationalCampaignLifecycle,
        ):
            raise TypeError("journal and lifecycle must be operational P6 objects")
        journal._authorize()
        if lifecycle._journal is not journal:
            raise ValueError("lifecycle must use the same Campaign journal")
        if not callable(monotonic_ns):
            raise TypeError("monotonic_ns must be callable")
        owner = _verified_current_owner(identity_provider)
        self._journal = journal
        self._lifecycle = lifecycle
        self._owner = owner
        self._monotonic_ns = monotonic_ns
        self._identity_provider = identity_provider

    def acquire(self, *, cycle_id: str, acquisition_id: str) -> CycleLease:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")
        acquisition_id = _identifier(acquisition_id, "acquisition_id")
        self._require_active_campaign()
        if _verified_current_owner(self._identity_provider) != self._owner:
            raise CycleLeaseConflictError(
                "current process identity changed after journal construction"
            )

        def acquire_cycle(connection) -> CycleLease | None:
            return self._acquire_in_transaction(
                connection,
                cycle_id=cycle_id,
                acquisition_id=acquisition_id,
            )

        lease = _SqliteUnitOfWork(stores._operational_spec())._write(
            acquire_cycle
        )
        if lease is None:
            raise CycleLeaseIntegrityError(
                "invalid Cycle lease journal blocked Campaign"
            )
        return lease

    def _acquire_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        acquisition_id: str,
    ) -> CycleLease | None:
        campaign = self._lifecycle._replay_campaign(
            self._lifecycle._campaign_events(connection)
        )
        if campaign.status is not CampaignStatus.ACTIVE:
            raise CycleLeaseConflictError("Campaign is not ACTIVE")
        events = self._events_or_block(connection, cycle_id)
        if events is None:
            return None
        if events:
            history = self._replay_or_block(connection, events)
            if history is None:
                return None
            active = history.active
            if (
                active.acquisition_id == acquisition_id
                and active.owner == self._owner
            ):
                return active
            raise CycleLeaseConflictError(
                "Cycle already has an active execution lease"
            )
        cycle = self._lifecycle._replay_cycle(
            self._lifecycle._cycle_events(connection, cycle_id)
        )
        if cycle.status is not CycleStatus.FROZEN:
            raise CycleLeaseConflictError(
                "Cycle lease requires the FROZEN boundary"
            )
        heartbeat_monotonic_ns = _bounded_int(
            self._monotonic_ns(),
            "heartbeat_monotonic_ns",
            minimum=0,
        )
        while True:
            lease_id = f"cyclelease_{secrets.token_hex(16)}"
            event_id = _lease_event_id(
                namespace=self._journal._namespace,
                campaign_id=self._journal._campaign_id,
                cycle_id=cycle_id,
                lease_id=lease_id,
                role="acquired",
            )
            if self._journal._event_in_transaction(connection, event_id) is None:
                break
        event = self._journal._append_in_transaction(
            connection,
            event_id=event_id,
            cycle_id=cycle_id,
            aggregate_type=_LEASE_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
            event_type=_LEASE_ACQUIRED,
            payload={
                "cycle_id": cycle_id,
                "acquisition_id": acquisition_id,
                "lease_id": lease_id,
                "fencing_token": 1,
                "owner": self._owner.to_payload(),
                "owner_observed_process_started_at_ns": (
                    self._owner.process_started_at_ns
                ),
                "heartbeat_sequence": 0,
                "heartbeat_monotonic_ns": heartbeat_monotonic_ns,
            },
        )
        return CycleLease(
            cycle_id,
            acquisition_id,
            lease_id,
            1,
            self._owner,
            0,
            heartbeat_monotonic_ns,
            event.sequence,
        )

    def advance_cycle(
        self,
        *,
        lease: CycleLease,
        expected_status: CycleStatus,
        next_status: CycleStatus,
    ) -> CycleSnapshot:
        self._journal._authorize()
        if not isinstance(lease, CycleLease):
            raise TypeError("lease must be a CycleLease")
        cycle_id = _identifier(lease.cycle_id, "cycle_id")
        self._lifecycle._validate_cycle_transition(expected_status, next_status)
        self._require_active_campaign()
        if _verified_current_owner(self._identity_provider) != self._owner:
            raise CycleLeaseConflictError(
                "current process identity changed after journal construction"
            )

        def advance(connection) -> CycleSnapshot | None:
            return self._advance_cycle_in_transaction(
                connection,
                lease=lease,
                expected_status=expected_status,
                next_status=next_status,
            )

        snapshot = _SqliteUnitOfWork(stores._operational_spec())._write(
            advance
        )
        if snapshot is None:
            raise CycleLeaseIntegrityError(
                "invalid Cycle lease journal blocked Campaign"
            )
        return snapshot

    def _advance_cycle_in_transaction(
        self,
        connection,
        *,
        lease: CycleLease,
        expected_status: CycleStatus,
        next_status: CycleStatus,
    ) -> CycleSnapshot | None:
        campaign = self._lifecycle._replay_campaign(
            self._lifecycle._campaign_events(connection)
        )
        if campaign.status is not CampaignStatus.ACTIVE:
            raise CycleLeaseConflictError("Campaign is not ACTIVE")
        events = self._events_or_block(connection, lease.cycle_id)
        if events is None:
            return None
        history = self._replay_or_block(connection, events)
        if history is None:
            return None
        active = history.active
        if active != lease or active.owner != self._owner:
            raise StaleFencingTokenError(
                "Cycle lease snapshot is stale or owned by another process"
            )
        return self._lifecycle._advance_cycle_in_transaction(
            connection,
            cycle_id=lease.cycle_id,
            expected_status=expected_status,
            next_status=next_status,
        )

    def _start_execution_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        acquisition_id: str,
    ) -> tuple[CycleLease, CycleSnapshot] | None:
        if _verified_current_owner(self._identity_provider) != self._owner:
            raise CycleLeaseConflictError(
                "current process identity changed after journal construction"
            )
        self._lifecycle._validate_cycle_transition(
            CycleStatus.FROZEN,
            CycleStatus.EXECUTING,
        )
        lease = self._acquire_in_transaction(
            connection,
            cycle_id=cycle_id,
            acquisition_id=acquisition_id,
        )
        if lease is None:
            return None
        cycle = self._advance_cycle_in_transaction(
            connection,
            lease=lease,
            expected_status=CycleStatus.FROZEN,
            next_status=CycleStatus.EXECUTING,
        )
        if cycle is None:
            return None
        return lease, cycle

    def heartbeat(
        self,
        *,
        lease: CycleLease,
        heartbeat_id: str,
    ) -> CycleLease:
        self._journal._authorize()
        if not isinstance(lease, CycleLease):
            raise TypeError("lease must be a CycleLease")
        heartbeat_id = _identifier(heartbeat_id, "heartbeat_id")
        self._require_active_campaign()
        if _verified_current_owner(self._identity_provider) != self._owner:
            raise CycleLeaseConflictError(
                "current process identity changed after journal construction"
            )

        def load_candidate_events(connection) -> tuple[CampaignEvent, ...]:
            campaign = self._lifecycle._replay_campaign(
                self._lifecycle._campaign_events(connection)
            )
            if campaign.status is not CampaignStatus.ACTIVE:
                raise CycleLeaseConflictError("Campaign is not ACTIVE")
            cycle = self._lifecycle._replay_cycle(
                self._lifecycle._cycle_events(connection, lease.cycle_id)
            )
            if cycle.status is not CycleStatus.EXECUTING:
                raise CycleLeaseConflictError(
                    "Cycle heartbeat requires the EXECUTING state"
                )
            try:
                return self._events(connection, lease.cycle_id)
            except (KeyError, TypeError, ValueError) as error:
                raise CampaignJournalError(
                    "Cycle lease journal storage is invalid"
                ) from error

        def block_invalid_history(connection) -> _LeaseHistory | None:
            events = self._events_or_block(connection, lease.cycle_id)
            if events is None:
                return None
            return self._replay_or_block(connection, events)

        retry_write = object()
        for _ in range(2):
            try:
                events = _SqliteUnitOfWork(
                    stores._operational_spec()
                )._read(load_candidate_events)
                try:
                    history = self._replay(events)
                except (
                    CampaignJournalError,
                    CycleLeaseIntegrityError,
                    KeyError,
                    TypeError,
                    ValueError,
                ) as error:
                    raise CycleLeaseIntegrityError(
                        "Cycle lease journal is invalid"
                    ) from error
            except (CampaignJournalError, CycleLeaseIntegrityError):
                history = _SqliteUnitOfWork(
                    stores._operational_spec()
                )._write(block_invalid_history)
                if history is None:
                    raise CycleLeaseIntegrityError(
                        "invalid Cycle lease journal blocked Campaign"
                    )
            active = history.active
            if (
                history.last_heartbeat_id == heartbeat_id
                and history.previous == lease
            ):
                return active
            if heartbeat_id in history.heartbeat_ids:
                raise CycleLeaseConflictError(
                    "heartbeat_id was already used by this lease"
                )
            if active != lease or active.owner != self._owner:
                raise StaleFencingTokenError(
                    "Cycle lease snapshot is stale or owned by another process"
                )

            def record_heartbeat(connection):
                campaign = self._lifecycle._replay_campaign(
                    self._lifecycle._campaign_events(connection)
                )
                if campaign.status is not CampaignStatus.ACTIVE:
                    raise CycleLeaseConflictError("Campaign is not ACTIVE")
                cycle = self._lifecycle._replay_cycle(
                    self._lifecycle._cycle_events(connection, lease.cycle_id)
                )
                if cycle.status is not CycleStatus.EXECUTING:
                    raise CycleLeaseConflictError(
                        "Cycle heartbeat requires the EXECUTING state"
                    )
                latest_sequence = self._latest_event_sequence(
                    connection,
                    active.cycle_id,
                )
                if latest_sequence != active.event_sequence:
                    return retry_write
                heartbeat_monotonic_ns = _bounded_int(
                    self._monotonic_ns(),
                    "heartbeat_monotonic_ns",
                    minimum=0,
                )
                if heartbeat_monotonic_ns <= active.heartbeat_monotonic_ns:
                    raise ValueError("heartbeat monotonic time did not advance")
                while True:
                    event_nonce = secrets.token_hex(32)
                    event_id = _lease_event_id(
                        namespace=self._journal._namespace,
                        campaign_id=self._journal._campaign_id,
                        cycle_id=active.cycle_id,
                        lease_id=active.lease_id,
                        role=f"heartbeat:{event_nonce}",
                    )
                    if (
                        self._journal._event_in_transaction(
                            connection,
                            event_id,
                        )
                        is None
                    ):
                        break
                event = self._journal._append_in_transaction(
                    connection,
                    event_id=event_id,
                    cycle_id=active.cycle_id,
                    aggregate_type=_LEASE_AGGREGATE_TYPE,
                    aggregate_id=active.cycle_id,
                    event_type=_LEASE_HEARTBEAT,
                    payload={
                        "cycle_id": active.cycle_id,
                        "lease_id": active.lease_id,
                        "fencing_token": active.fencing_token,
                        "owner": active.owner.to_payload(),
                        "heartbeat_id": heartbeat_id,
                        "from_heartbeat_sequence": active.heartbeat_sequence,
                        "to_heartbeat_sequence": active.heartbeat_sequence + 1,
                        "previous_monotonic_ns": active.heartbeat_monotonic_ns,
                        "heartbeat_monotonic_ns": heartbeat_monotonic_ns,
                        "event_nonce": event_nonce,
                    },
                )
                return CycleLease(
                    active.cycle_id,
                    active.acquisition_id,
                    active.lease_id,
                    active.fencing_token,
                    active.owner,
                    active.heartbeat_sequence + 1,
                    heartbeat_monotonic_ns,
                    event.sequence,
                )

            heartbeat = _SqliteUnitOfWork(
                stores._operational_spec()
            )._write(record_heartbeat)
            if heartbeat is retry_write:
                continue
            return heartbeat
        raise StaleFencingTokenError(
            "Cycle lease changed while heartbeat was being recorded"
        )

    def recover(
        self,
        *,
        cycle_id: str,
        acquisition_id: str,
        stale_after_ns: int,
    ) -> CycleLease:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")
        acquisition_id = _identifier(acquisition_id, "acquisition_id")
        stale_after_ns = _bounded_int(
            stale_after_ns,
            "stale_after_ns",
            minimum=1,
        )

        def load_candidate(connection) -> _LeaseHistory | None:
            campaign = self._lifecycle._replay_campaign(
                self._lifecycle._campaign_events(connection)
            )
            if campaign.status is not CampaignStatus.ACTIVE:
                raise CycleLeaseConflictError("Campaign is not ACTIVE")
            cycle = self._lifecycle._replay_cycle(
                self._lifecycle._cycle_events(connection, cycle_id)
            )
            if cycle.status not in _LEASE_OWNED_CYCLE_STATUSES:
                raise CycleLeaseConflictError(
                    "Cycle recovery requires an incomplete leased state"
                )
            events = self._events_or_block(connection, cycle_id)
            if events is None:
                return None
            return self._replay_or_block(connection, events)

        candidate = _SqliteUnitOfWork(stores._operational_spec())._write(
            load_candidate
        )
        if candidate is None:
            raise CycleLeaseIntegrityError(
                "invalid Cycle lease journal blocked Campaign"
            )
        active = candidate.active
        owner = _verified_current_owner(self._identity_provider)
        if owner != self._owner:
            raise CycleLeaseConflictError(
                "current process identity changed after journal construction"
            )
        if (
            active.acquisition_id == acquisition_id
            and active.owner == owner
        ):
            return active
        if active.owner == owner:
            raise CycleLeaseConflictError(
                "lease owner cannot replace its own process identity"
            )
        if acquisition_id in candidate.acquisition_ids:
            raise CycleLeaseConflictError(
                "acquisition_id was already bound to another lease generation"
            )
        if active.owner.host_id != owner.host_id:
            raise CycleLeaseConflictError(
                "remote process identity cannot be disproven locally"
            )
        probe_started_monotonic_ns = _bounded_int(
            self._monotonic_ns(),
            "recovery_probe_started_monotonic_ns",
            minimum=0,
        )
        if (
            probe_started_monotonic_ns - active.heartbeat_monotonic_ns
            < stale_after_ns
        ):
            raise CycleLeaseConflictError("Cycle lease heartbeat is not stale")
        try:
            observed_process_start = self._identity_provider.probe(
                active.owner.host_id,
                active.owner.pid,
            )
            if observed_process_start is not None:
                observed_process_start = _bounded_int(
                    observed_process_start,
                    "observed_process_started_at_ns",
                    minimum=1,
                )
        except Exception as error:
            raise CycleLeaseConflictError(
                "process identity probe failed closed"
            ) from error
        if observed_process_start == active.owner.process_started_at_ns:
            raise CycleLeaseConflictError(
                "the existing lease owner process is still live"
            )
        probe_result = (
            "PROCESS_ABSENT"
            if observed_process_start is None
            else "PID_REUSED"
        )
        recovery_monotonic_ns = _bounded_int(
            self._monotonic_ns(),
            "recovery_monotonic_ns",
            minimum=0,
        )
        if (
            recovery_monotonic_ns - active.heartbeat_monotonic_ns
            < stale_after_ns
        ):
            raise CycleLeaseConflictError("Cycle lease heartbeat is not stale")
        next_fencing_token = _bounded_int(
            active.fencing_token + 1,
            "next_fencing_token",
            minimum=1,
        )

        def replace_stale_lease(connection) -> CycleLease | None:
            campaign = self._lifecycle._replay_campaign(
                self._lifecycle._campaign_events(connection)
            )
            if campaign.status is not CampaignStatus.ACTIVE:
                raise CycleLeaseConflictError("Campaign is not ACTIVE")
            cycle = self._lifecycle._replay_cycle(
                self._lifecycle._cycle_events(connection, cycle_id)
            )
            if cycle.status not in _LEASE_OWNED_CYCLE_STATUSES:
                raise CycleLeaseConflictError(
                    "Cycle recovery requires an incomplete leased state"
                )
            events = self._events_or_block(connection, cycle_id)
            if events is None:
                return None
            history = self._replay_or_block(connection, events)
            if history is None:
                return None
            current = history.active
            if (
                current.acquisition_id == acquisition_id
                and current.owner == owner
            ):
                return current
            if current != active:
                raise StaleFencingTokenError(
                    "Cycle lease changed while recovery proof was collected"
                )
            while True:
                new_lease_id = f"cyclelease_{secrets.token_hex(16)}"
                event_nonce = secrets.token_hex(32)
                event_id = _lease_event_id(
                    namespace=self._journal._namespace,
                    campaign_id=self._journal._campaign_id,
                    cycle_id=cycle_id,
                    lease_id=active.lease_id,
                    role=f"replaced:{new_lease_id}:{event_nonce}",
                )
                if self._journal._event_in_transaction(connection, event_id) is None:
                    break
            event = self._journal._append_in_transaction(
                connection,
                event_id=event_id,
                cycle_id=cycle_id,
                aggregate_type=_LEASE_AGGREGATE_TYPE,
                aggregate_id=cycle_id,
                event_type=_LEASE_REPLACED,
                payload={
                    "cycle_id": cycle_id,
                    "old_lease_id": active.lease_id,
                    "old_fencing_token": active.fencing_token,
                    "old_owner": active.owner.to_payload(),
                    "old_heartbeat_sequence": active.heartbeat_sequence,
                    "old_heartbeat_monotonic_ns": (
                        active.heartbeat_monotonic_ns
                    ),
                    "stale_after_ns": stale_after_ns,
                    "recovery_monotonic_ns": recovery_monotonic_ns,
                    "process_probe_result": probe_result,
                    "observed_process_started_at_ns": (
                        observed_process_start
                    ),
                    "acquisition_id": acquisition_id,
                    "new_lease_id": new_lease_id,
                    "new_fencing_token": next_fencing_token,
                    "new_owner": owner.to_payload(),
                    "new_owner_observed_process_started_at_ns": (
                        owner.process_started_at_ns
                    ),
                    "new_heartbeat_sequence": 0,
                    "new_heartbeat_monotonic_ns": recovery_monotonic_ns,
                    "event_nonce": event_nonce,
                },
            )
            return CycleLease(
                cycle_id,
                acquisition_id,
                new_lease_id,
                next_fencing_token,
                owner,
                0,
                recovery_monotonic_ns,
                event.sequence,
            )

        lease = _SqliteUnitOfWork(stores._operational_spec())._write(
            replace_stale_lease
        )
        if lease is None:
            raise CycleLeaseIntegrityError(
                "invalid Cycle lease journal blocked Campaign"
            )
        return lease

    def snapshot(self, *, cycle_id: str) -> CycleLease:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")

        def load_snapshot(connection) -> CycleLease | None:
            events = self._events_or_block(connection, cycle_id)
            if events is None:
                return None
            history = self._replay_or_block(connection, events)
            if history is None:
                return None
            return history.active

        snapshot = _SqliteUnitOfWork(stores._operational_spec())._write(
            load_snapshot
        )
        if snapshot is None:
            raise CycleLeaseIntegrityError(
                "invalid Cycle lease journal blocked Campaign"
            )
        return snapshot

    def _require_active_campaign(self) -> None:
        campaign = _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._lifecycle._replay_campaign(
                self._lifecycle._campaign_events(connection)
            )
        )
        if campaign.status is not CampaignStatus.ACTIVE:
            raise CycleLeaseConflictError("Campaign is not ACTIVE")

    def _events(
        self,
        connection,
        cycle_id: str,
    ) -> tuple[CampaignEvent, ...]:
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_LEASE_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _latest_event_sequence(
        self,
        connection,
        cycle_id: str,
    ) -> int | None:
        row = connection.execute(
            """
            SELECT MAX(sequence) AS latest_sequence
            FROM campaign_events
            WHERE namespace = ? AND campaign_id = ? AND cycle_id = ?
              AND aggregate_type = ? AND aggregate_id = ?
            """,
            (
                self._journal._namespace,
                self._journal._campaign_id,
                cycle_id,
                _LEASE_AGGREGATE_TYPE,
                cycle_id,
            ),
        ).fetchone()
        if row is None or row["latest_sequence"] is None:
            return None
        return _bounded_int(
            row["latest_sequence"],
            "stored latest lease event sequence",
            minimum=1,
        )

    def _events_or_block(
        self,
        connection,
        cycle_id: str,
    ) -> tuple[CampaignEvent, ...] | None:
        try:
            return self._events(connection, cycle_id)
        except (CampaignJournalError, KeyError, TypeError, ValueError):
            source_ref = hashlib.sha256(
                b"control_plane.cycle_lease_storage_corruption.v1\0"
                + "\0".join(
                    (
                        self._journal._namespace,
                        self._journal._campaign_id,
                        cycle_id,
                    )
                ).encode("ascii")
            ).hexdigest()
            self._lifecycle._block_in_transaction(
                connection,
                reason_code="CYCLE_LEASE_JOURNAL_INVALID",
                source_ref=source_ref,
            )
            return None

    def _block_invalid_history(
        self,
        connection,
        events: tuple[CampaignEvent, ...],
    ) -> None:
        if not events:
            raise CycleLeaseIntegrityError("Cycle lease journal is empty")
        source_event = events[-1]
        try:
            source_ref = _identifier(
                source_event.event_id,
                "invalid lease event source_ref",
            )
        except (TypeError, ValueError):
            digest = hashlib.sha256(
                b"control_plane.cycle_lease_invalid_source_ref.v1\0"
            )
            for value in (
                self._journal._namespace,
                self._journal._campaign_id,
                source_event.cycle_id,
                source_event.sequence,
                source_event.event_id,
            ):
                encoded = (
                    "<none>" if value is None else str(value)
                ).encode("utf-8", errors="surrogatepass")
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
            source_ref = digest.hexdigest()
        self._lifecycle._block_in_transaction(
            connection,
            reason_code="CYCLE_LEASE_JOURNAL_INVALID",
            source_ref=source_ref,
        )

    def _replay_or_block(
        self,
        connection,
        events: tuple[CampaignEvent, ...],
    ) -> _LeaseHistory | None:
        try:
            return self._replay(events)
        except (
            CampaignJournalError,
            CycleLeaseIntegrityError,
            KeyError,
            TypeError,
            ValueError,
        ):
            self._block_invalid_history(connection, events)
            return None

    def _replay(self, events: tuple[CampaignEvent, ...]) -> _LeaseHistory:
        if not events:
            raise CycleLeaseIntegrityError("Cycle lease journal is invalid")
        event = events[0]
        payload = _event_domain_payload(event)
        if set(payload) != {
            "cycle_id",
            "acquisition_id",
            "lease_id",
            "fencing_token",
            "owner",
            "owner_observed_process_started_at_ns",
            "heartbeat_sequence",
            "heartbeat_monotonic_ns",
        }:
            raise CycleLeaseIntegrityError("Cycle lease payload is invalid")
        try:
            cycle_id = _identifier(payload["cycle_id"], "stored cycle_id")
            acquisition_id = _identifier(
                payload["acquisition_id"],
                "stored acquisition_id",
            )
            lease_id = _identifier(payload["lease_id"], "stored lease_id")
            fencing_token = _bounded_int(
                payload["fencing_token"],
                "stored fencing_token",
                minimum=1,
            )
            heartbeat_sequence = _bounded_int(
                payload["heartbeat_sequence"],
                "stored heartbeat_sequence",
                minimum=0,
            )
            heartbeat_monotonic_ns = _bounded_int(
                payload["heartbeat_monotonic_ns"],
                "stored heartbeat_monotonic_ns",
                minimum=0,
            )
            raw_owner = payload["owner"]
            if not isinstance(raw_owner, dict) or set(raw_owner) != {
                "host_id",
                "pid",
                "process_started_at_ns",
            }:
                raise ValueError("stored owner is invalid")
            owner = ProcessIdentity(**raw_owner)
            owner_observed_process_start = _bounded_int(
                payload["owner_observed_process_started_at_ns"],
                "stored owner_observed_process_started_at_ns",
                minimum=1,
            )
        except (TypeError, ValueError) as error:
            raise CycleLeaseIntegrityError(
                "Cycle lease binding is invalid"
            ) from error
        expected_event_id = _lease_event_id(
            namespace=self._journal._namespace,
            campaign_id=self._journal._campaign_id,
            cycle_id=cycle_id,
            lease_id=lease_id,
            role="acquired",
        )
        observed_envelope = (
            event.namespace,
            event.campaign_id,
            event.cycle_id,
            event.aggregate_type,
            event.aggregate_id,
            event.event_type,
            event.event_id,
        )
        expected_envelope = (
            self._journal._namespace,
            self._journal._campaign_id,
            cycle_id,
            _LEASE_AGGREGATE_TYPE,
            cycle_id,
            _LEASE_ACQUIRED,
            expected_event_id,
        )
        expected_payload = {
            "cycle_id": cycle_id,
            "acquisition_id": acquisition_id,
            "lease_id": lease_id,
            "fencing_token": fencing_token,
            "owner": owner.to_payload(),
            "owner_observed_process_started_at_ns": (
                owner_observed_process_start
            ),
            "heartbeat_sequence": heartbeat_sequence,
            "heartbeat_monotonic_ns": heartbeat_monotonic_ns,
        }
        if (
            observed_envelope != expected_envelope
            or payload != expected_payload
            or fencing_token != 1
            or owner_observed_process_start != owner.process_started_at_ns
            or heartbeat_sequence != 0
        ):
            raise CycleLeaseIntegrityError("Cycle lease event is invalid")
        active = CycleLease(
            cycle_id,
            acquisition_id,
            lease_id,
            fencing_token,
            owner,
            heartbeat_sequence,
            heartbeat_monotonic_ns,
            event.sequence,
        )
        acquisition_ids = {acquisition_id}
        heartbeat_ids: set[str] = set()
        last_heartbeat_id: str | None = None
        previous: CycleLease | None = None
        for heartbeat_event in events[1:]:
            if heartbeat_event.event_type == _LEASE_REPLACED:
                replacement = self._replay_replacement_event(
                    heartbeat_event,
                    active,
                )
                if replacement.acquisition_id in acquisition_ids:
                    raise CycleLeaseIntegrityError(
                        "Cycle lease acquisition_id was reused"
                    )
                acquisition_ids.add(replacement.acquisition_id)
                active = replacement
                heartbeat_ids.clear()
                last_heartbeat_id = None
                previous = None
                continue
            if heartbeat_event.event_type != _LEASE_HEARTBEAT:
                raise CycleLeaseIntegrityError(
                    "Cycle lease journal event type is invalid"
                )
            heartbeat_payload = _event_domain_payload(heartbeat_event)
            if set(heartbeat_payload) != {
                "cycle_id",
                "lease_id",
                "fencing_token",
                "owner",
                "heartbeat_id",
                "from_heartbeat_sequence",
                "to_heartbeat_sequence",
                "previous_monotonic_ns",
                "heartbeat_monotonic_ns",
                "event_nonce",
            }:
                raise CycleLeaseIntegrityError(
                    "Cycle heartbeat payload is invalid"
                )
            try:
                stored_cycle_id = _identifier(
                    heartbeat_payload["cycle_id"],
                    "stored cycle_id",
                )
                stored_lease_id = _identifier(
                    heartbeat_payload["lease_id"],
                    "stored lease_id",
                )
                stored_fencing_token = _bounded_int(
                    heartbeat_payload["fencing_token"],
                    "stored fencing_token",
                    minimum=1,
                )
                heartbeat_id = _identifier(
                    heartbeat_payload["heartbeat_id"],
                    "stored heartbeat_id",
                )
                from_sequence = _bounded_int(
                    heartbeat_payload["from_heartbeat_sequence"],
                    "stored from_heartbeat_sequence",
                    minimum=0,
                )
                to_sequence = _bounded_int(
                    heartbeat_payload["to_heartbeat_sequence"],
                    "stored to_heartbeat_sequence",
                    minimum=1,
                )
                previous_monotonic_ns = _bounded_int(
                    heartbeat_payload["previous_monotonic_ns"],
                    "stored previous_monotonic_ns",
                    minimum=0,
                )
                next_monotonic_ns = _bounded_int(
                    heartbeat_payload["heartbeat_monotonic_ns"],
                    "stored heartbeat_monotonic_ns",
                    minimum=0,
                )
                event_nonce = _sha256(
                    heartbeat_payload["event_nonce"],
                    "stored event_nonce",
                )
                raw_owner = heartbeat_payload["owner"]
                if not isinstance(raw_owner, dict) or set(raw_owner) != {
                    "host_id",
                    "pid",
                    "process_started_at_ns",
                }:
                    raise ValueError("stored heartbeat owner is invalid")
                heartbeat_owner = ProcessIdentity(**raw_owner)
            except (TypeError, ValueError) as error:
                raise CycleLeaseIntegrityError(
                    "Cycle heartbeat binding is invalid"
                ) from error
            expected_event_id = _lease_event_id(
                namespace=self._journal._namespace,
                campaign_id=self._journal._campaign_id,
                cycle_id=active.cycle_id,
                lease_id=active.lease_id,
                role=f"heartbeat:{event_nonce}",
            )
            expected_payload = {
                "cycle_id": active.cycle_id,
                "lease_id": active.lease_id,
                "fencing_token": active.fencing_token,
                "owner": active.owner.to_payload(),
                "heartbeat_id": heartbeat_id,
                "from_heartbeat_sequence": active.heartbeat_sequence,
                "to_heartbeat_sequence": active.heartbeat_sequence + 1,
                "previous_monotonic_ns": active.heartbeat_monotonic_ns,
                "heartbeat_monotonic_ns": next_monotonic_ns,
                "event_nonce": event_nonce,
            }
            observed_envelope = (
                heartbeat_event.namespace,
                heartbeat_event.campaign_id,
                heartbeat_event.cycle_id,
                heartbeat_event.aggregate_type,
                heartbeat_event.aggregate_id,
                heartbeat_event.event_type,
                heartbeat_event.event_id,
            )
            expected_envelope = (
                self._journal._namespace,
                self._journal._campaign_id,
                active.cycle_id,
                _LEASE_AGGREGATE_TYPE,
                active.cycle_id,
                _LEASE_HEARTBEAT,
                expected_event_id,
            )
            if (
                stored_cycle_id != active.cycle_id
                or stored_lease_id != active.lease_id
                or stored_fencing_token != active.fencing_token
                or heartbeat_owner != active.owner
                or from_sequence != active.heartbeat_sequence
                or to_sequence != active.heartbeat_sequence + 1
                or previous_monotonic_ns != active.heartbeat_monotonic_ns
                or next_monotonic_ns <= active.heartbeat_monotonic_ns
                or heartbeat_id in heartbeat_ids
                or heartbeat_payload != expected_payload
                or observed_envelope != expected_envelope
            ):
                raise CycleLeaseIntegrityError("Cycle heartbeat event is invalid")
            previous = active
            active = CycleLease(
                active.cycle_id,
                active.acquisition_id,
                active.lease_id,
                active.fencing_token,
                active.owner,
                to_sequence,
                next_monotonic_ns,
                heartbeat_event.sequence,
            )
            heartbeat_ids.add(heartbeat_id)
            last_heartbeat_id = heartbeat_id
        return _LeaseHistory(
            active,
            frozenset(acquisition_ids),
            frozenset(heartbeat_ids),
            last_heartbeat_id,
            previous,
        )

    def _replay_replacement_event(
        self,
        event: CampaignEvent,
        active: CycleLease,
    ) -> CycleLease:
        payload = _event_domain_payload(event)
        if set(payload) != {
            "cycle_id",
            "old_lease_id",
            "old_fencing_token",
            "old_owner",
            "old_heartbeat_sequence",
            "old_heartbeat_monotonic_ns",
            "stale_after_ns",
            "recovery_monotonic_ns",
            "process_probe_result",
            "observed_process_started_at_ns",
            "acquisition_id",
            "new_lease_id",
            "new_fencing_token",
            "new_owner",
            "new_owner_observed_process_started_at_ns",
            "new_heartbeat_sequence",
            "new_heartbeat_monotonic_ns",
            "event_nonce",
        }:
            raise CycleLeaseIntegrityError(
                "Cycle lease replacement payload is invalid"
            )
        try:
            cycle_id = _identifier(payload["cycle_id"], "stored cycle_id")
            old_lease_id = _identifier(
                payload["old_lease_id"],
                "stored old_lease_id",
            )
            old_fencing_token = _bounded_int(
                payload["old_fencing_token"],
                "stored old_fencing_token",
                minimum=1,
            )
            old_heartbeat_sequence = _bounded_int(
                payload["old_heartbeat_sequence"],
                "stored old_heartbeat_sequence",
                minimum=0,
            )
            old_heartbeat_monotonic_ns = _bounded_int(
                payload["old_heartbeat_monotonic_ns"],
                "stored old_heartbeat_monotonic_ns",
                minimum=0,
            )
            stale_after_ns = _bounded_int(
                payload["stale_after_ns"],
                "stored stale_after_ns",
                minimum=1,
            )
            recovery_monotonic_ns = _bounded_int(
                payload["recovery_monotonic_ns"],
                "stored recovery_monotonic_ns",
                minimum=0,
            )
            probe_result = _identifier(
                payload["process_probe_result"],
                "stored process_probe_result",
            )
            observed_process_start = payload["observed_process_started_at_ns"]
            if observed_process_start is not None:
                observed_process_start = _bounded_int(
                    observed_process_start,
                    "stored observed_process_started_at_ns",
                    minimum=1,
                )
            acquisition_id = _identifier(
                payload["acquisition_id"],
                "stored acquisition_id",
            )
            new_lease_id = _identifier(
                payload["new_lease_id"],
                "stored new_lease_id",
            )
            new_fencing_token = _bounded_int(
                payload["new_fencing_token"],
                "stored new_fencing_token",
                minimum=1,
            )
            new_heartbeat_sequence = _bounded_int(
                payload["new_heartbeat_sequence"],
                "stored new_heartbeat_sequence",
                minimum=0,
            )
            new_heartbeat_monotonic_ns = _bounded_int(
                payload["new_heartbeat_monotonic_ns"],
                "stored new_heartbeat_monotonic_ns",
                minimum=0,
            )
            event_nonce = _sha256(
                payload["event_nonce"],
                "stored event_nonce",
            )
            raw_old_owner = payload["old_owner"]
            raw_new_owner = payload["new_owner"]
            owner_fields = {
                "host_id",
                "pid",
                "process_started_at_ns",
            }
            if (
                not isinstance(raw_old_owner, dict)
                or set(raw_old_owner) != owner_fields
                or not isinstance(raw_new_owner, dict)
                or set(raw_new_owner) != owner_fields
            ):
                raise ValueError("stored replacement owner is invalid")
            old_owner = ProcessIdentity(**raw_old_owner)
            new_owner = ProcessIdentity(**raw_new_owner)
            new_owner_observed_process_start = _bounded_int(
                payload["new_owner_observed_process_started_at_ns"],
                "stored new_owner_observed_process_started_at_ns",
                minimum=1,
            )
        except (TypeError, ValueError) as error:
            raise CycleLeaseIntegrityError(
                "Cycle lease replacement binding is invalid"
            ) from error
        is_stale = (
            recovery_monotonic_ns - active.heartbeat_monotonic_ns
            >= stale_after_ns
        )
        process_disproven = (
            probe_result == "PROCESS_ABSENT"
            and observed_process_start is None
        ) or (
            probe_result == "PID_REUSED"
            and observed_process_start is not None
            and observed_process_start != active.owner.process_started_at_ns
        )
        expected_payload = {
            "cycle_id": active.cycle_id,
            "old_lease_id": active.lease_id,
            "old_fencing_token": active.fencing_token,
            "old_owner": active.owner.to_payload(),
            "old_heartbeat_sequence": active.heartbeat_sequence,
            "old_heartbeat_monotonic_ns": active.heartbeat_monotonic_ns,
            "stale_after_ns": stale_after_ns,
            "recovery_monotonic_ns": recovery_monotonic_ns,
            "process_probe_result": probe_result,
            "observed_process_started_at_ns": observed_process_start,
            "acquisition_id": acquisition_id,
            "new_lease_id": new_lease_id,
            "new_fencing_token": active.fencing_token + 1,
            "new_owner": new_owner.to_payload(),
            "new_owner_observed_process_started_at_ns": (
                new_owner_observed_process_start
            ),
            "new_heartbeat_sequence": 0,
            "new_heartbeat_monotonic_ns": recovery_monotonic_ns,
            "event_nonce": event_nonce,
        }
        expected_event_id = _lease_event_id(
            namespace=self._journal._namespace,
            campaign_id=self._journal._campaign_id,
            cycle_id=active.cycle_id,
            lease_id=active.lease_id,
            role=f"replaced:{new_lease_id}:{event_nonce}",
        )
        observed_envelope = (
            event.namespace,
            event.campaign_id,
            event.cycle_id,
            event.aggregate_type,
            event.aggregate_id,
            event.event_type,
            event.event_id,
        )
        expected_envelope = (
            self._journal._namespace,
            self._journal._campaign_id,
            active.cycle_id,
            _LEASE_AGGREGATE_TYPE,
            active.cycle_id,
            _LEASE_REPLACED,
            expected_event_id,
        )
        if (
            cycle_id != active.cycle_id
            or old_lease_id != active.lease_id
            or old_fencing_token != active.fencing_token
            or old_owner != active.owner
            or new_owner.host_id != active.owner.host_id
            or new_owner == active.owner
            or new_owner_observed_process_start
            != new_owner.process_started_at_ns
            or old_heartbeat_sequence != active.heartbeat_sequence
            or old_heartbeat_monotonic_ns != active.heartbeat_monotonic_ns
            or not is_stale
            or not process_disproven
            or new_fencing_token != active.fencing_token + 1
            or new_heartbeat_sequence != 0
            or new_heartbeat_monotonic_ns != recovery_monotonic_ns
            or payload != expected_payload
            or observed_envelope != expected_envelope
        ):
            raise CycleLeaseIntegrityError(
                "Cycle lease replacement event is invalid"
            )
        return CycleLease(
            active.cycle_id,
            acquisition_id,
            new_lease_id,
            new_fencing_token,
            new_owner,
            0,
            recovery_monotonic_ns,
            event.sequence,
        )


__all__ = [
    "CycleLease",
    "CycleLeaseConflictError",
    "CycleLeaseError",
    "CycleLeaseIntegrityError",
    "LocalProcessIdentityProvider",
    "OperationalCycleLeaseJournal",
    "ProcessIdentity",
    "ProcessIdentityProvider",
    "StaleFencingTokenError",
]
