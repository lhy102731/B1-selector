from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Barrier, Event
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.campaign_lease import (
    CycleLeaseConflictError,
    CycleLeaseIntegrityError,
    LocalProcessIdentityProvider,
    OperationalCycleLeaseJournal as _OperationalCycleLeaseJournal,
    ProcessIdentity,
    StaleFencingTokenError,
)
from research_automation.control_plane.campaign_lifecycle import (
    CampaignStatus,
    CycleStatus,
    OperationalCampaignLifecycle,
)
from research_automation.control_plane.campaign_store import (
    OperationalCampaignJournal,
    campaign_scope_sha256,
    _event_integrity_sha256,
)
from research_automation.control_plane.contracts import Actor, Phase, SideEffect


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"
NOW = datetime(2026, 8, 1, 2, 3, 4, tzinfo=timezone.utc)


class _FakeProcessIdentityProvider:
    def __init__(
        self,
        current: ProcessIdentity,
        *,
        process_starts: dict[tuple[str, int], int | None] | None = None,
        probe=None,
    ) -> None:
        self._current = current
        self.current_calls = 0
        self.probe_calls: list[tuple[str, int]] = []
        self._process_starts = {
            (current.host_id, current.pid): current.process_started_at_ns,
            **(process_starts or {}),
        }
        self._probe = probe

    def current(self) -> ProcessIdentity:
        self.current_calls += 1
        return self._current

    def set_current(self, current: ProcessIdentity) -> None:
        self._current = current
        self._process_starts[(current.host_id, current.pid)] = (
            current.process_started_at_ns
        )

    def probe(self, host_id: str, pid: int) -> int | None:
        self.probe_calls.append((host_id, pid))
        if self._probe is not None:
            return self._probe(host_id, pid)
        return self._process_starts.get((host_id, pid))


def OperationalCycleLeaseJournal(
    *,
    journal: OperationalCampaignJournal,
    lifecycle: OperationalCampaignLifecycle,
    monotonic_ns,
    owner: ProcessIdentity | None = None,
    identity_provider=None,
    process_start_probe=None,
):
    if identity_provider is not None:
        return _OperationalCycleLeaseJournal(
            journal=journal,
            lifecycle=lifecycle,
            identity_provider=identity_provider,
            monotonic_ns=monotonic_ns,
        )
    if not isinstance(owner, ProcessIdentity):
        raise TypeError("test lease journal requires an owner")

    def verified_probe(host_id: str, pid: int) -> int | None:
        if (host_id, pid) == (owner.host_id, owner.pid):
            return owner.process_started_at_ns
        if process_start_probe is None:
            return None
        return process_start_probe(host_id, pid)

    return _OperationalCycleLeaseJournal(
        journal=journal,
        lifecycle=lifecycle,
        identity_provider=_FakeProcessIdentityProvider(
            owner,
            probe=verified_probe,
        ),
        monotonic_ns=monotonic_ns,
    )


@contextmanager
def _authorized_campaign(campaign_id: str):
    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        with patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
            _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
        ):
            stores_module._expected_schema_sha256.cache_clear()
            stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
            actor = Actor("p6-runner", "automation", f"{campaign_id}-lease")
            identity = stores_module.AuthorityIdentity(
                "a" * 64,
                campaign_scope_sha256(
                    namespace="formal",
                    campaign_id=campaign_id,
                ),
                "c" * 64,
            )
            authority = stores_module._AuthorityStore(
                root_secret=ROOT_SECRET,
                clock=lambda: NOW,
            )
            authorization = authority._provision_authorization(
                phase=Phase.P6,
                attempt_id=f"{campaign_id}-attempt",
                actor=actor,
                identity=identity,
                expires_at=NOW.replace(year=2027),
                allowed_side_effects=(
                    SideEffect.READ,
                    SideEffect.WRITE_CONTROL_PLANE,
                ),
            )
            grant = authority.claim_authorization(
                authorization,
                expected_phase=Phase.P6,
                expected_attempt_id=f"{campaign_id}-attempt",
                actor=actor,
                identity=identity,
            )
            try:
                yield grant, OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id=campaign_id,
                    clock=lambda: NOW,
                )
            finally:
                stores_module._expected_schema_sha256.cache_clear()


def _freeze_cycle(
    lifecycle: OperationalCampaignLifecycle,
    *,
    cycle_id: str,
    cycle_number: int,
) -> None:
    lifecycle.open_cycle(cycle_id=cycle_id, cycle_number=cycle_number)
    for expected, next_status in (
        (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
        (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
        (CycleStatus.CONTEXT_READY, CycleStatus.FROZEN),
    ):
        lifecycle.advance_cycle(
            cycle_id=cycle_id,
            expected_status=expected,
            next_status=next_status,
        )


def _cycle_lease_event_id(
    *,
    campaign_id: str,
    cycle_id: str,
    lease_id: str,
    role: str,
) -> str:
    return hashlib.sha256(
        b"control_plane.campaign_lease_event.v1\0"
        + "\0".join(
            ("formal", campaign_id, cycle_id, lease_id, role)
        ).encode("ascii")
    ).hexdigest()


def _append_replacement_event(
    *,
    journal: OperationalCampaignJournal,
    campaign_id: str,
    acquired,
    old_owner: ProcessIdentity,
    new_owner: ProcessIdentity,
    acquisition_id: str,
) -> str:
    new_lease_id = f"cyclelease_{'c' * 32}"
    event_nonce = "b" * 64
    event_id = _cycle_lease_event_id(
        campaign_id=campaign_id,
        cycle_id=acquired.cycle_id,
        lease_id=acquired.lease_id,
        role=f"replaced:{new_lease_id}:{event_nonce}",
    )
    journal.append(
        event_id=event_id,
        cycle_id=acquired.cycle_id,
        aggregate_type="CYCLE_LEASE",
        aggregate_id=acquired.cycle_id,
        event_type="CYCLE_LEASE_REPLACED",
        payload={
            "cycle_id": acquired.cycle_id,
            "old_lease_id": acquired.lease_id,
            "old_fencing_token": acquired.fencing_token,
            "old_owner": old_owner.to_payload(),
            "old_heartbeat_sequence": acquired.heartbeat_sequence,
            "old_heartbeat_monotonic_ns": acquired.heartbeat_monotonic_ns,
            "stale_after_ns": 50,
            "recovery_monotonic_ns": 1_000,
            "process_probe_result": "PROCESS_ABSENT",
            "observed_process_started_at_ns": None,
            "acquisition_id": acquisition_id,
            "new_lease_id": new_lease_id,
            "new_fencing_token": acquired.fencing_token + 1,
            "new_owner": new_owner.to_payload(),
            "new_owner_observed_process_started_at_ns": (
                new_owner.process_started_at_ns
            ),
            "new_heartbeat_sequence": 0,
            "new_heartbeat_monotonic_ns": 1_000,
            "event_nonce": event_nonce,
        },
    )
    return event_id


class OperationalCycleLeaseJournalTests(unittest.TestCase):
    def test_local_process_identity_provider_verifies_the_current_process(self) -> None:
        provider = LocalProcessIdentityProvider()

        current = provider.current()

        self.assertEqual(current.pid, os.getpid())
        self.assertRegex(current.host_id, r"host_[0-9a-f]{64}")
        self.assertEqual(
            provider.probe(current.host_id, current.pid),
            current.process_started_at_ns,
        )

    def test_journal_rejects_an_owner_not_verified_by_the_process_provider(self) -> None:
        campaign_id = "campaign-lease-026"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            claimed_owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=9_999,
            )
            provider = _FakeProcessIdentityProvider(
                claimed_owner,
                process_starts={("host-a", 101): 2_000},
            )

            with self.assertRaises(CycleLeaseConflictError):
                _OperationalCycleLeaseJournal(
                    journal=journal,
                    lifecycle=lifecycle,
                    identity_provider=provider,
                    monotonic_ns=lambda: 100,
                )

            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(events, ())

    def test_journal_does_not_accept_an_unverified_owner_dto(self) -> None:
        campaign_id = "campaign-lease-027"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()

            with self.assertRaises(TypeError):
                _OperationalCycleLeaseJournal(
                    journal=journal,
                    lifecycle=lifecycle,
                    owner=ProcessIdentity(
                        host_id="host-a",
                        pid=101,
                        process_started_at_ns=1_000,
                    ),
                    monotonic_ns=lambda: 100,
                )

    def test_acquire_persists_the_verified_owner_process_start(self) -> None:
        campaign_id = "campaign-lease-028"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )

            event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )[0]
            self.assertEqual(
                event.payload()["owner_observed_process_started_at_ns"],
                owner.process_started_at_ns,
            )

    def test_acquire_rejects_a_provider_identity_changed_after_construction(self) -> None:
        campaign_id = "campaign-lease-031"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            provider = _FakeProcessIdentityProvider(
                ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                )
            )
            leases = _OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                identity_provider=provider,
                monotonic_ns=lambda: 100,
            )
            provider.set_current(
                ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                )
            )

            with self.assertRaises(CycleLeaseConflictError):
                leases.acquire(
                    cycle_id="cycle-001",
                    acquisition_id="acquire-cycle-001",
                )

            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(events, ())

    def test_heartbeat_rejects_a_provider_identity_changed_after_acquire(self) -> None:
        campaign_id = "campaign-lease-032"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            provider = _FakeProcessIdentityProvider(
                ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                )
            )
            leases = _OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                identity_provider=provider,
                monotonic_ns=lambda: 100,
            )
            acquired = leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            provider.set_current(
                ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                )
            )

            with self.assertRaises(CycleLeaseConflictError):
                leases.heartbeat(
                    lease=acquired,
                    heartbeat_id="must-not-write",
                )

            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_recovery_rejects_a_provider_identity_changed_after_construction(self) -> None:
        campaign_id = "campaign-lease-033"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            old_leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            )
            old_leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            provider = _FakeProcessIdentityProvider(
                ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                process_starts={("host-a", 101): None},
            )
            recovery = _OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                identity_provider=provider,
                monotonic_ns=lambda: self.fail(
                    "identity-switched recovery read the clock"
                ),
            )
            provider.set_current(
                ProcessIdentity(
                    host_id="host-a",
                    pid=303,
                    process_started_at_ns=3_000,
                )
            )

            with self.assertRaises(CycleLeaseConflictError):
                recovery.recover(
                    cycle_id="cycle-001",
                    acquisition_id="must-not-write",
                    stale_after_ns=50,
                )

            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_concurrent_acquire_on_one_cycle_has_exactly_one_winner(self) -> None:
        campaign_id = "campaign-lease-019"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            contenders = tuple(
                OperationalCycleLeaseJournal(
                    journal=journal,
                    lifecycle=lifecycle,
                    owner=ProcessIdentity(
                        host_id="host-a",
                        pid=pid,
                        process_started_at_ns=pid * 10,
                    ),
                    monotonic_ns=lambda value=clock: value,
                )
                for pid, clock in ((101, 100), (202, 200))
            )
            barrier = Barrier(2)

            def acquire(index: int):
                barrier.wait()
                try:
                    return contenders[index].acquire(
                        cycle_id="cycle-001",
                        acquisition_id=f"acquire-cycle-001-{index}",
                    )
                except CycleLeaseConflictError:
                    return None

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = tuple(executor.map(acquire, range(2)))

            winners = tuple(result for result in results if result is not None)
            self.assertEqual(len(winners), 1)
            self.assertEqual(
                contenders[0].snapshot(cycle_id="cycle-001"),
                winners[0],
            )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_concurrent_acquire_on_distinct_cycles_allows_both_owners(self) -> None:
        campaign_id = "campaign-lease-020"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            _freeze_cycle(lifecycle, cycle_id="cycle-002", cycle_number=2)
            contenders = tuple(
                OperationalCycleLeaseJournal(
                    journal=journal,
                    lifecycle=lifecycle,
                    owner=ProcessIdentity(
                        host_id="host-a",
                        pid=pid,
                        process_started_at_ns=pid * 10,
                    ),
                    monotonic_ns=lambda value=clock: value,
                )
                for pid, clock in ((101, 100), (202, 200))
            )
            barrier = Barrier(2)

            def acquire(index: int):
                cycle_id = f"cycle-00{index + 1}"
                barrier.wait()
                return contenders[index].acquire(
                    cycle_id=cycle_id,
                    acquisition_id=f"acquire-{cycle_id}",
                )

            with ThreadPoolExecutor(max_workers=2) as executor:
                leases = tuple(executor.map(acquire, range(2)))

            self.assertEqual(
                {lease.cycle_id for lease in leases},
                {"cycle-001", "cycle-002"},
            )
            for lease in leases:
                self.assertEqual(
                    contenders[0].snapshot(cycle_id=lease.cycle_id),
                    lease,
                )

    def test_concurrent_recovery_creates_one_new_fencing_generation(self) -> None:
        campaign_id = "campaign-lease-025"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            contenders = tuple(
                OperationalCycleLeaseJournal(
                    journal=journal,
                    lifecycle=lifecycle,
                    owner=ProcessIdentity(
                        host_id="host-a",
                        pid=pid,
                        process_started_at_ns=pid * 10,
                    ),
                    monotonic_ns=lambda: 1_000,
                    process_start_probe=lambda host_id, pid: None,
                )
                for pid in (202, 303)
            )
            barrier = Barrier(2)

            def recover(index: int):
                barrier.wait()
                try:
                    return contenders[index].recover(
                        cycle_id="cycle-001",
                        acquisition_id=f"recover-cycle-001-{index}",
                        stale_after_ns=50,
                    )
                except CycleLeaseConflictError:
                    return None

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = tuple(executor.map(recover, range(2)))

            winners = tuple(result for result in results if result is not None)
            self.assertEqual(len(winners), 1)
            self.assertEqual(winners[0].fencing_token, 2)
            self.assertEqual(
                contenders[0].snapshot(cycle_id="cycle-001"),
                winners[0],
            )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 2)

    def test_slow_recovery_probe_does_not_block_an_unrelated_heartbeat(self) -> None:
        campaign_id = "campaign-lease-029"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            old_owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            heartbeat_owner = ProcessIdentity(
                host_id="host-a",
                pid=202,
                process_started_at_ns=2_000,
            )
            leases = []
            for cycle_number, owner in enumerate(
                (old_owner, heartbeat_owner),
                start=1,
            ):
                cycle_id = f"cycle-00{cycle_number}"
                _freeze_cycle(
                    lifecycle,
                    cycle_id=cycle_id,
                    cycle_number=cycle_number,
                )
                leases.append(
                    OperationalCycleLeaseJournal(
                        journal=journal,
                        lifecycle=lifecycle,
                        owner=owner,
                        monotonic_ns=lambda value=cycle_number * 100: value,
                    ).acquire(
                        cycle_id=cycle_id,
                        acquisition_id=f"acquire-{cycle_id}",
                    )
                )
                lifecycle.advance_cycle(
                    cycle_id=cycle_id,
                    expected_status=CycleStatus.FROZEN,
                    next_status=CycleStatus.EXECUTING,
                )
            probe_entered = Event()
            release_probe = Event()

            def slow_probe(host_id: str, pid: int) -> None:
                probe_entered.set()
                if not release_probe.wait(timeout=5):
                    raise TimeoutError("test did not release process probe")
                return None

            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=303,
                    process_started_at_ns=3_000,
                ),
                monotonic_ns=lambda: 1_000,
                process_start_probe=slow_probe,
            )
            heartbeat = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=heartbeat_owner,
                monotonic_ns=lambda: 300,
            )

            with ThreadPoolExecutor(max_workers=2) as executor:
                recovery_future = executor.submit(
                    recovery.recover,
                    cycle_id="cycle-001",
                    acquisition_id="recover-cycle-001",
                    stale_after_ns=50,
                )
                self.assertTrue(probe_entered.wait(timeout=1))
                heartbeat_future = executor.submit(
                    heartbeat.heartbeat,
                    lease=leases[1],
                    heartbeat_id="heartbeat-cycle-002",
                )
                try:
                    recorded_heartbeat = heartbeat_future.result(timeout=0.5)
                finally:
                    release_probe.set()
                recovered = recovery_future.result(timeout=3)

            self.assertEqual(recorded_heartbeat.heartbeat_sequence, 1)
            self.assertEqual(recovered.fencing_token, 2)

    def test_slow_heartbeat_replay_does_not_hold_the_global_write_lock(self) -> None:
        campaign_id = "campaign-lease-037"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            owners = (
                ProcessIdentity("host-a", 101, 1_000),
                ProcessIdentity("host-a", 202, 2_000),
            )
            journals = []
            leases = []
            for cycle_number, owner in enumerate(owners, start=1):
                cycle_id = f"cycle-00{cycle_number}"
                _freeze_cycle(
                    lifecycle,
                    cycle_id=cycle_id,
                    cycle_number=cycle_number,
                )
                lease_journal = OperationalCycleLeaseJournal(
                    journal=journal,
                    lifecycle=lifecycle,
                    owner=owner,
                    monotonic_ns=lambda value=cycle_number * 100: value,
                )
                journals.append(lease_journal)
                leases.append(
                    lease_journal.acquire(
                        cycle_id=cycle_id,
                        acquisition_id=f"acquire-{cycle_id}",
                    )
                )
                lifecycle.advance_cycle(
                    cycle_id=cycle_id,
                    expected_status=CycleStatus.FROZEN,
                    next_status=CycleStatus.EXECUTING,
                )
            heartbeat_journals = tuple(
                OperationalCycleLeaseJournal(
                    journal=journal,
                    lifecycle=lifecycle,
                    owner=owner,
                    monotonic_ns=lambda value=(index + 3) * 100: value,
                )
                for index, owner in enumerate(owners)
            )
            replay_entered = Event()
            release_replay = Event()
            unrelated_finished = Event()
            original_replay = _OperationalCycleLeaseJournal._replay

            def slow_replay(instance, events):
                if events and events[0].cycle_id == "cycle-001":
                    replay_entered.set()
                    if not release_replay.wait(timeout=5):
                        raise TimeoutError("test did not release lease replay")
                return original_replay(instance, events)

            def unrelated_heartbeat():
                try:
                    return heartbeat_journals[1].heartbeat(
                        lease=leases[1],
                        heartbeat_id="heartbeat-cycle-002",
                    )
                finally:
                    unrelated_finished.set()

            with patch.object(
                _OperationalCycleLeaseJournal,
                "_replay",
                slow_replay,
            ), ThreadPoolExecutor(max_workers=2) as executor:
                slow_future = executor.submit(
                    heartbeat_journals[0].heartbeat,
                    lease=leases[0],
                    heartbeat_id="heartbeat-cycle-001",
                )
                self.assertTrue(replay_entered.wait(timeout=1))
                unrelated_future = executor.submit(unrelated_heartbeat)
                completed_without_write_lock = unrelated_finished.wait(timeout=0.5)
                release_replay.set()
                slow_heartbeat = slow_future.result(timeout=3)
                fast_heartbeat = unrelated_future.result(timeout=3)

            self.assertTrue(completed_without_write_lock)
            self.assertEqual(slow_heartbeat.heartbeat_sequence, 1)
            self.assertEqual(fast_heartbeat.heartbeat_sequence, 1)

    def test_concurrent_identical_heartbeats_replay_one_exact_result(self) -> None:
        campaign_id = "campaign-lease-039"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            owner = ProcessIdentity("host-a", 101, 1_000)
            acquired = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            heartbeats = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 200,
            )
            barrier = Barrier(2)

            def heartbeat():
                barrier.wait()
                return heartbeats.heartbeat(
                    lease=acquired,
                    heartbeat_id="shared-heartbeat",
                )

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = tuple(executor.map(lambda _: heartbeat(), range(2)))

            self.assertEqual(results[0], results[1])
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(
                tuple(event.event_type for event in events),
                ("CYCLE_LEASE_ACQUIRED", "CYCLE_LEASE_HEARTBEAT"),
            )

    def test_concurrent_distinct_heartbeats_have_one_stale_writer(self) -> None:
        campaign_id = "campaign-lease-040"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            owner = ProcessIdentity("host-a", 101, 1_000)
            acquired = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            heartbeats = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 200,
            )
            barrier = Barrier(2)

            def heartbeat(index: int):
                barrier.wait()
                try:
                    return heartbeats.heartbeat(
                        lease=acquired,
                        heartbeat_id=f"heartbeat-{index}",
                    )
                except StaleFencingTokenError:
                    return None

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = tuple(executor.map(heartbeat, range(2)))

            winners = tuple(result for result in results if result is not None)
            self.assertEqual(len(winners), 1)
            self.assertEqual(winners[0].heartbeat_sequence, 1)
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 2)
            self.assertIn(
                events[-1].payload()["heartbeat_id"],
                {"heartbeat-0", "heartbeat-1"},
            )

    def test_recovery_proof_is_fenced_when_same_cycle_heartbeat_advances(self) -> None:
        campaign_id = "campaign-lease-030"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            old_owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            old_leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=old_owner,
                monotonic_ns=lambda: 100,
            )
            acquired = old_leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            probe_entered = Event()
            release_probe = Event()

            def slow_probe(host_id: str, pid: int) -> None:
                probe_entered.set()
                if not release_probe.wait(timeout=5):
                    raise TimeoutError("test did not release process probe")
                return None

            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                monotonic_ns=lambda: 1_000,
                process_start_probe=slow_probe,
            )
            heartbeat = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=old_owner,
                monotonic_ns=lambda: 200,
            )

            with ThreadPoolExecutor(max_workers=1) as executor:
                recovery_future = executor.submit(
                    recovery.recover,
                    cycle_id="cycle-001",
                    acquisition_id="recover-cycle-001",
                    stale_after_ns=50,
                )
                self.assertTrue(probe_entered.wait(timeout=1))
                recorded_heartbeat = heartbeat.heartbeat(
                    lease=acquired,
                    heartbeat_id="heartbeat-during-recovery",
                )
                release_probe.set()
                with self.assertRaises(StaleFencingTokenError):
                    recovery_future.result(timeout=3)

            self.assertEqual(
                heartbeat.snapshot(cycle_id="cycle-001"),
                recorded_heartbeat,
            )

    def test_acquire_redraws_when_first_event_id_is_occupied_off_aggregate(self) -> None:
        campaign_id = "campaign-lease-021"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            poisoned_lease_id = f"cyclelease_{'a' * 32}"
            poisoned_event_id = _cycle_lease_event_id(
                campaign_id=campaign_id,
                cycle_id="cycle-001",
                lease_id=poisoned_lease_id,
                role="acquired",
            )
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="POISONED_EVENT",
                aggregate_id="poisoned-acquire-event",
                event_type="POISONED_EVENT",
                payload={"candidate": poisoned_lease_id},
            )
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            )

            with patch(
                "research_automation.control_plane.campaign_lease.secrets.token_hex",
                side_effect=("a" * 32, "b" * 32),
            ):
                acquired = leases.acquire(
                    cycle_id="cycle-001",
                    acquisition_id="acquire-cycle-001",
                )

            self.assertEqual(acquired.lease_id, f"cyclelease_{'b' * 32}")
            self.assertEqual(lifecycle.snapshot().status, CampaignStatus.ACTIVE)
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_heartbeat_redraws_when_first_event_id_is_occupied_off_aggregate(self) -> None:
        campaign_id = "campaign-lease-022"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            acquired = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            first_nonce = "a" * 64
            poisoned_event_id = _cycle_lease_event_id(
                campaign_id=campaign_id,
                cycle_id="cycle-001",
                lease_id=acquired.lease_id,
                role=f"heartbeat:{first_nonce}",
            )
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="POISONED_EVENT",
                aggregate_id="poisoned-heartbeat-event",
                event_type="POISONED_EVENT",
                payload={"candidate": first_nonce},
            )
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 200,
            )

            with patch(
                "research_automation.control_plane.campaign_lease.secrets.token_hex",
                side_effect=(first_nonce, "b" * 64),
            ):
                heartbeat = leases.heartbeat(
                    lease=acquired,
                    heartbeat_id="heartbeat-001",
                )

            self.assertEqual(heartbeat.heartbeat_sequence, 1)
            self.assertEqual(lifecycle.snapshot().status, CampaignStatus.ACTIVE)
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 2)

    def test_recovery_redraws_when_first_event_id_is_occupied_off_aggregate(self) -> None:
        campaign_id = "campaign-lease-023"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            acquired = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            first_new_lease_id = f"cyclelease_{'a' * 32}"
            first_nonce = "b" * 64
            poisoned_event_id = _cycle_lease_event_id(
                campaign_id=campaign_id,
                cycle_id="cycle-001",
                lease_id=acquired.lease_id,
                role=f"replaced:{first_new_lease_id}:{first_nonce}",
            )
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="POISONED_EVENT",
                aggregate_id="poisoned-recovery-event",
                event_type="POISONED_EVENT",
                payload={"candidate": first_new_lease_id},
            )
            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                monotonic_ns=lambda: 1_000,
                process_start_probe=lambda host_id, pid: None,
            )

            with patch(
                "research_automation.control_plane.campaign_lease.secrets.token_hex",
                side_effect=(
                    "a" * 32,
                    first_nonce,
                    "c" * 32,
                    "d" * 64,
                ),
            ):
                recovered = recovery.recover(
                    cycle_id="cycle-001",
                    acquisition_id="recover-cycle-001",
                    stale_after_ns=50,
                )

            self.assertEqual(recovered.lease_id, f"cyclelease_{'c' * 32}")
            self.assertEqual(recovered.fencing_token, 2)
            self.assertEqual(lifecycle.snapshot().status, CampaignStatus.ACTIVE)
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 2)

    def test_cycle_scoped_acquire_is_exclusive_and_reopens_idempotently(self) -> None:
        campaign_id = "campaign-lease-001"
        with _authorized_campaign(campaign_id) as (grant, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            _freeze_cycle(lifecycle, cycle_id="cycle-002", cycle_number=2)
            owner_a = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            owner_b = ProcessIdentity(
                host_id="host-a",
                pid=202,
                process_started_at_ns=2_000,
            )
            leases_a = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner_a,
                monotonic_ns=lambda: 10_000,
            )
            first = leases_a.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )

            reopened_journal = OperationalCampaignJournal(
                root_secret=ROOT_SECRET,
                grant=grant,
                namespace="formal",
                campaign_id=campaign_id,
                clock=lambda: NOW,
            )
            reopened = OperationalCycleLeaseJournal(
                journal=reopened_journal,
                lifecycle=OperationalCampaignLifecycle(
                    journal=reopened_journal,
                ),
                owner=owner_a,
                monotonic_ns=lambda: 20_000,
            )
            replay = reopened.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )

            self.assertEqual(replay, first)
            self.assertEqual(first.fencing_token, 1)
            self.assertEqual(first.owner, owner_a)
            self.assertEqual(first.heartbeat_sequence, 0)
            self.assertEqual(first.heartbeat_monotonic_ns, 10_000)

            leases_b = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner_b,
                monotonic_ns=lambda: 30_000,
            )
            with self.assertRaises(CycleLeaseConflictError):
                leases_b.acquire(
                    cycle_id="cycle-001",
                    acquisition_id="other-owner-cycle-001",
                )

            unrelated = leases_b.acquire(
                cycle_id="cycle-002",
                acquisition_id="acquire-cycle-002",
            )
            self.assertEqual(unrelated.fencing_token, 1)
            self.assertEqual(unrelated.owner, owner_b)

    def test_heartbeat_is_monotonic_idempotent_and_fences_stale_snapshot(self) -> None:
        campaign_id = "campaign-lease-002"
        with _authorized_campaign(campaign_id) as (grant, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            clock_values = iter((10_000, 11_000))
            owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: next(clock_values),
            )
            acquired = leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )

            heartbeat = leases.heartbeat(
                lease=acquired,
                heartbeat_id="heartbeat-001",
            )
            replay = leases.heartbeat(
                lease=acquired,
                heartbeat_id="heartbeat-001",
            )

            self.assertEqual(replay, heartbeat)
            self.assertEqual(heartbeat.fencing_token, acquired.fencing_token)
            self.assertEqual(heartbeat.heartbeat_sequence, 1)
            self.assertEqual(heartbeat.heartbeat_monotonic_ns, 11_000)
            with self.assertRaises(CycleLeaseConflictError):
                leases.heartbeat(
                    lease=heartbeat,
                    heartbeat_id="heartbeat-001",
                )
            with self.assertRaises(StaleFencingTokenError):
                leases.heartbeat(
                    lease=acquired,
                    heartbeat_id="heartbeat-002",
                )

            reopened_journal = OperationalCampaignJournal(
                root_secret=ROOT_SECRET,
                grant=grant,
                namespace="formal",
                campaign_id=campaign_id,
                clock=lambda: NOW,
            )
            reopened = OperationalCycleLeaseJournal(
                journal=reopened_journal,
                lifecycle=OperationalCampaignLifecycle(
                    journal=reopened_journal,
                ),
                owner=owner,
                monotonic_ns=lambda: 12_000,
            )
            self.assertEqual(
                reopened.snapshot(cycle_id="cycle-001"),
                heartbeat,
            )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(
                tuple(event.event_type for event in events),
                ("CYCLE_LEASE_ACQUIRED", "CYCLE_LEASE_HEARTBEAT"),
            )

    def test_non_advancing_heartbeat_clock_writes_no_event(self) -> None:
        campaign_id = "campaign-lease-024"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 100,
            )
            acquired = leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )

            with self.assertRaises(ValueError):
                leases.heartbeat(
                    lease=acquired,
                    heartbeat_id="heartbeat-001",
                )

            self.assertEqual(
                leases.snapshot(cycle_id="cycle-001"),
                acquired,
            )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_pid_reuse_recovery_increments_fence_and_rejects_old_snapshot(self) -> None:
        campaign_id = "campaign-lease-003"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            old_owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            old_leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=old_owner,
                monotonic_ns=lambda: 100,
            )
            acquired = old_leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            replacement_owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=2_000,
            )
            replacement_leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=replacement_owner,
                monotonic_ns=lambda: 200,
                process_start_probe=lambda host_id, pid: (
                    2_000 if (host_id, pid) == ("host-a", 101) else None
                ),
            )

            recovered = replacement_leases.recover(
                cycle_id="cycle-001",
                acquisition_id="recover-cycle-001",
                stale_after_ns=50,
            )

            self.assertEqual(recovered.fencing_token, 2)
            self.assertEqual(recovered.owner, replacement_owner)
            self.assertEqual(recovered.heartbeat_sequence, 0)
            self.assertEqual(recovered.heartbeat_monotonic_ns, 200)
            self.assertEqual(
                replacement_leases.snapshot(cycle_id="cycle-001"),
                recovered,
            )
            with self.assertRaises(StaleFencingTokenError):
                old_leases.heartbeat(
                    lease=acquired,
                    heartbeat_id="stale-owner-heartbeat",
                )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(
                tuple(event.event_type for event in events),
                ("CYCLE_LEASE_ACQUIRED", "CYCLE_LEASE_REPLACED"),
            )
            self.assertEqual(
                events[1].payload()[
                    "new_owner_observed_process_started_at_ns"
                ],
                replacement_owner.process_started_at_ns,
            )

    def test_recovery_uses_post_probe_clock_for_the_new_heartbeat(self) -> None:
        campaign_id = "campaign-lease-035"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            clock_values = iter((1_000, 10_000))
            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                monotonic_ns=lambda: next(clock_values),
                process_start_probe=lambda host_id, pid: None,
            )

            recovered = recovery.recover(
                cycle_id="cycle-001",
                acquisition_id="recover-cycle-001",
                stale_after_ns=50,
            )

            self.assertEqual(recovered.heartbeat_monotonic_ns, 10_000)
            event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )[-1]
            self.assertEqual(event.payload()["recovery_monotonic_ns"], 10_000)
            self.assertEqual(
                event.payload()["new_heartbeat_monotonic_ns"],
                10_000,
            )

    def test_committed_recovery_replays_without_probe_or_clock(self) -> None:
        campaign_id = "campaign-lease-014"
        with _authorized_campaign(campaign_id) as (grant, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            replacement_owner = ProcessIdentity(
                host_id="host-a",
                pid=202,
                process_started_at_ns=2_000,
            )
            recovered = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=replacement_owner,
                monotonic_ns=lambda: 1_000,
                process_start_probe=lambda host_id, pid: None,
            ).recover(
                cycle_id="cycle-001",
                acquisition_id="recover-cycle-001",
                stale_after_ns=50,
            )
            reopened_journal = OperationalCampaignJournal(
                root_secret=ROOT_SECRET,
                grant=grant,
                namespace="formal",
                campaign_id=campaign_id,
                clock=lambda: NOW,
            )

            replay = OperationalCycleLeaseJournal(
                journal=reopened_journal,
                lifecycle=OperationalCampaignLifecycle(
                    journal=reopened_journal,
                ),
                owner=replacement_owner,
                monotonic_ns=lambda: self.fail("replay read the clock"),
            ).recover(
                cycle_id="cycle-001",
                acquisition_id="recover-cycle-001",
                stale_after_ns=50,
            )

            self.assertEqual(replay, recovered)
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 2)

    def test_recovery_cannot_rebind_an_acquisition_id_from_an_older_generation(self) -> None:
        campaign_id = "campaign-lease-005"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            original = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            replacement = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                monotonic_ns=lambda: 1_000,
                process_start_probe=lambda host_id, pid: None,
            )

            with self.assertRaises(CycleLeaseConflictError):
                replacement.recover(
                    cycle_id="cycle-001",
                    acquisition_id=original.acquisition_id,
                    stale_after_ns=50,
                )

            self.assertEqual(
                replacement.snapshot(cycle_id="cycle-001"),
                original,
            )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_unknown_lease_history_atomically_blocks_the_campaign(self) -> None:
        campaign_id = "campaign-lease-006"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            )
            leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            poisoned_event_id = "poisoned-cycle-lease-tail"
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
                event_type="UNKNOWN_CYCLE_LEASE_EVENT",
                payload={"cycle_id": "cycle-001"},
            )

            with self.assertRaises(CycleLeaseIntegrityError):
                leases.snapshot(cycle_id="cycle-001")

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(
                blocked.block_reason_code,
                "CYCLE_LEASE_JOURNAL_INVALID",
            )
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)

    def test_storage_corruption_atomically_blocks_the_campaign(self) -> None:
        campaign_id = "campaign-lease-034"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            )
            leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )[0]
            with closing(
                sqlite3.connect(stores_module._OPERATIONAL_STORE_PATH)
            ) as connection:
                with connection:
                    connection.execute(
                        "UPDATE campaign_events SET payload_json = ? WHERE event_id = ?",
                        ('{"corrupt":true}', event.event_id),
                    )

            with self.assertRaises(CycleLeaseIntegrityError):
                leases.snapshot(cycle_id="cycle-001")

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(
                blocked.block_reason_code,
                "CYCLE_LEASE_JOURNAL_INVALID",
            )

    def test_corrupt_lease_cannot_poison_the_campaign_block_event_id(self) -> None:
        campaign_id = "campaign-lease-036"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            )
            leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )[0]
            block_event_id = hashlib.sha256(
                b"control_plane.campaign_lifecycle_event.v1\0"
                + (
                    f"formal\0{campaign_id}\0CAMPAIGN_STATE\0"
                    f"{campaign_id}\0BLOCKED"
                ).encode("ascii")
            ).hexdigest()
            with closing(
                sqlite3.connect(stores_module._OPERATIONAL_STORE_PATH)
            ) as connection:
                with connection:
                    connection.execute(
                        """
                        UPDATE campaign_events
                        SET event_id = ?, payload_json = ?
                        WHERE event_id = ?
                        """,
                        (block_event_id, '{"corrupt":true}', event.event_id),
                    )

            with self.assertRaises(CycleLeaseIntegrityError):
                leases.snapshot(cycle_id="cycle-001")

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(
                blocked.block_reason_code,
                "CYCLE_LEASE_JOURNAL_INVALID",
            )

    def test_invalid_event_id_blocks_with_replayable_provenance(self) -> None:
        campaign_id = "campaign-lease-038"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            )
            leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            event = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )[0]
            invalid_event_id = ""
            integrity_sha256 = _event_integrity_sha256(
                event_id=invalid_event_id,
                namespace=event.namespace,
                campaign_id=event.campaign_id,
                cycle_id=event.cycle_id,
                aggregate_type=event.aggregate_type,
                aggregate_id=event.aggregate_id,
                event_type=event.event_type,
                payload_json=event.payload_json,
                occurred_at=event.occurred_at.isoformat(),
                sequence=event.sequence,
            )
            with closing(
                sqlite3.connect(stores_module._OPERATIONAL_STORE_PATH)
            ) as connection:
                with connection:
                    connection.execute(
                        """
                        UPDATE campaign_events
                        SET event_id = ?, payload_sha256 = ?
                        WHERE event_id = ?
                        """,
                        (invalid_event_id, integrity_sha256, event.event_id),
                    )

            with self.assertRaises(CycleLeaseIntegrityError):
                leases.snapshot(cycle_id="cycle-001")

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(
                blocked.block_reason_code,
                "CYCLE_LEASE_JOURNAL_INVALID",
            )
            self.assertEqual(len(blocked.block_source_ref), 64)

    def test_cross_host_replacement_history_is_rejected_and_blocks_campaign(self) -> None:
        campaign_id = "campaign-lease-010"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            old_owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=old_owner,
                monotonic_ns=lambda: 100,
            )
            acquired = leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            new_owner = ProcessIdentity(
                host_id="host-b",
                pid=202,
                process_started_at_ns=2_000,
            )
            event_id = _append_replacement_event(
                journal=journal,
                campaign_id=campaign_id,
                acquired=acquired,
                old_owner=old_owner,
                new_owner=new_owner,
                acquisition_id="recover-cycle-001",
            )

            with self.assertRaises(CycleLeaseIntegrityError):
                leases.snapshot(cycle_id="cycle-001")

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_source_ref, event_id)

    def test_same_owner_replacement_history_is_rejected_and_blocks_campaign(self) -> None:
        campaign_id = "campaign-lease-013"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 100,
            )
            acquired = leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            event_id = _append_replacement_event(
                journal=journal,
                campaign_id=campaign_id,
                acquired=acquired,
                old_owner=owner,
                new_owner=owner,
                acquisition_id="recover-cycle-001",
            )

            with self.assertRaises(CycleLeaseIntegrityError):
                leases.snapshot(cycle_id="cycle-001")

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_source_ref, event_id)

    def test_heartbeat_blocks_before_writing_through_invalid_lease_history(self) -> None:
        campaign_id = "campaign-lease-007"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            )
            acquired = leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            poisoned_event_id = "poisoned-heartbeat-lease-tail"
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
                event_type="UNKNOWN_CYCLE_LEASE_EVENT",
                payload={"cycle_id": "cycle-001"},
            )

            with self.assertRaises(CycleLeaseIntegrityError):
                leases.heartbeat(
                    lease=acquired,
                    heartbeat_id="must-not-write",
                )

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 2)

    def test_blocked_campaign_rejects_heartbeat_before_clock_or_write(self) -> None:
        campaign_id = "campaign-lease-017"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            provider = _FakeProcessIdentityProvider(owner)
            blocked_leases = _OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                identity_provider=provider,
                monotonic_ns=lambda: 100,
            )
            acquired = blocked_leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            provider.current_calls = 0
            provider.probe_calls.clear()
            lifecycle.block(
                reason_code="TEST_BLOCK",
                source_ref="test-block-source",
            )

            with self.assertRaises(CycleLeaseConflictError):
                blocked_leases.heartbeat(
                    lease=acquired,
                    heartbeat_id="must-not-write",
                )

            self.assertEqual(provider.current_calls, 0)
            self.assertEqual(provider.probe_calls, [])
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_blocked_campaign_rejects_recovery_before_clock_or_probe(self) -> None:
        campaign_id = "campaign-lease-018"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            lifecycle.block(
                reason_code="TEST_BLOCK",
                source_ref="test-block-source",
            )
            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                monotonic_ns=lambda: self.fail(
                    "blocked Campaign recovery read the clock"
                ),
                process_start_probe=lambda host_id, pid: self.fail(
                    "blocked Campaign recovery probed process identity"
                ),
            )

            with self.assertRaises(CycleLeaseConflictError):
                recovery.recover(
                    cycle_id="cycle-001",
                    acquisition_id="recover-cycle-001",
                    stale_after_ns=50,
                )

            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_acquire_replay_atomically_blocks_invalid_lease_history(self) -> None:
        campaign_id = "campaign-lease-008"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            )
            leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            poisoned_event_id = "poisoned-acquire-lease-tail"
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
                event_type="UNKNOWN_CYCLE_LEASE_EVENT",
                payload={"cycle_id": "cycle-001"},
            )

            with self.assertRaises(CycleLeaseIntegrityError):
                leases.acquire(
                    cycle_id="cycle-001",
                    acquisition_id="acquire-cycle-001",
                )

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)

    def test_recovery_blocks_invalid_history_before_process_probe(self) -> None:
        campaign_id = "campaign-lease-009"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            poisoned_event_id = "poisoned-recovery-lease-tail"
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
                event_type="UNKNOWN_CYCLE_LEASE_EVENT",
                payload={"cycle_id": "cycle-001"},
            )
            probe_calls: list[tuple[str, int]] = []

            def process_probe(host_id: str, pid: int) -> None:
                probe_calls.append((host_id, pid))
                return None

            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                monotonic_ns=lambda: 1_000,
                process_start_probe=process_probe,
            )

            with self.assertRaises(CycleLeaseIntegrityError):
                recovery.recover(
                    cycle_id="cycle-001",
                    acquisition_id="recover-cycle-001",
                    stale_after_ns=50,
                )

            self.assertEqual(probe_calls, [])
            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)

    def test_invalid_or_failed_process_probe_fails_closed_without_replacement(self) -> None:
        campaign_id = "campaign-lease-011"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            original = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                monotonic_ns=lambda: 1_000,
                process_start_probe=lambda host_id, pid: 0,
            )

            with self.assertRaises(CycleLeaseConflictError):
                recovery.recover(
                    cycle_id="cycle-001",
                    acquisition_id="recover-cycle-001",
                    stale_after_ns=50,
                )

            def failed_probe(host_id: str, pid: int) -> None:
                raise PermissionError("process table is unavailable")

            failed_recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=303,
                    process_started_at_ns=3_000,
                ),
                monotonic_ns=lambda: 1_000,
                process_start_probe=failed_probe,
            )
            with self.assertRaises(CycleLeaseConflictError):
                failed_recovery.recover(
                    cycle_id="cycle-001",
                    acquisition_id="recover-cycle-002",
                    stale_after_ns=50,
                )

            self.assertEqual(
                recovery.snapshot(cycle_id="cycle-001"),
                original,
            )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_non_stale_recovery_does_not_probe_process_identity(self) -> None:
        campaign_id = "campaign-lease-015"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            original = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                monotonic_ns=lambda: 120,
                process_start_probe=lambda host_id, pid: self.fail(
                    "non-stale recovery probed process identity"
                ),
            )

            with self.assertRaises(CycleLeaseConflictError):
                recovery.recover(
                    cycle_id="cycle-001",
                    acquisition_id="recover-cycle-001",
                    stale_after_ns=50,
                )

            self.assertEqual(
                recovery.snapshot(cycle_id="cycle-001"),
                original,
            )

    def test_remote_host_lease_cannot_be_reaped_by_local_probe(self) -> None:
        campaign_id = "campaign-lease-016"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            original = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=101,
                    process_started_at_ns=1_000,
                ),
                monotonic_ns=lambda: 100,
            ).acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-b",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                monotonic_ns=lambda: self.fail(
                    "remote-host recovery read the local monotonic clock"
                ),
                process_start_probe=lambda host_id, pid: self.fail(
                    "remote-host recovery invoked a local probe"
                ),
            )

            with self.assertRaises(CycleLeaseConflictError):
                recovery.recover(
                    cycle_id="cycle-001",
                    acquisition_id="recover-cycle-001",
                    stale_after_ns=50,
                )

            self.assertEqual(
                recovery.snapshot(cycle_id="cycle-001"),
                original,
            )

    def test_same_process_identity_cannot_replace_its_own_lease(self) -> None:
        campaign_id = "campaign-lease-012"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            probe_calls: list[tuple[str, int]] = []
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 100,
            )
            original = leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )

            def process_probe(host_id: str, pid: int) -> None:
                probe_calls.append((host_id, pid))
                return None

            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 1_000,
                process_start_probe=process_probe,
            )

            with self.assertRaises(CycleLeaseConflictError):
                recovery.recover(
                    cycle_id="cycle-001",
                    acquisition_id="different-acquisition",
                    stale_after_ns=50,
                )

            self.assertEqual(probe_calls, [])
            self.assertEqual(
                recovery.snapshot(cycle_id="cycle-001"),
                original,
            )

    def test_stale_heartbeat_cannot_reap_same_live_process_identity(self) -> None:
        campaign_id = "campaign-lease-004"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            _freeze_cycle(lifecycle, cycle_id="cycle-001", cycle_number=1)
            owner = ProcessIdentity(
                host_id="host-a",
                pid=101,
                process_started_at_ns=1_000,
            )
            leases = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=owner,
                monotonic_ns=lambda: 100,
            )
            acquired = leases.acquire(
                cycle_id="cycle-001",
                acquisition_id="acquire-cycle-001",
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            recovery = OperationalCycleLeaseJournal(
                journal=journal,
                lifecycle=lifecycle,
                owner=ProcessIdentity(
                    host_id="host-a",
                    pid=202,
                    process_started_at_ns=2_000,
                ),
                monotonic_ns=lambda: 1_000,
                process_start_probe=lambda host_id, pid: (
                    1_000 if (host_id, pid) == ("host-a", 101) else None
                ),
            )

            with self.assertRaises(CycleLeaseConflictError):
                recovery.recover(
                    cycle_id="cycle-001",
                    acquisition_id="unsafe-recovery",
                    stale_after_ns=50,
                )

            self.assertEqual(
                recovery.snapshot(cycle_id="cycle-001"),
                acquired,
            )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_LEASE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)


if __name__ == "__main__":
    unittest.main()
