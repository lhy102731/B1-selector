from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Barrier
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.campaign import (
    InvalidModelResponseError,
    InvocationOutcome,
    ModelInvocation,
    ProviderResponse,
    UsageStatus,
)
from research_automation.control_plane.campaign_store import (
    CampaignJournalError,
    OperationalBudgetJournal,
    OperationalCampaignJournal,
    OperationalUsageJournal,
    campaign_scope_sha256,
)
from research_automation.control_plane.budget import (
    BudgetConflictError,
    BudgetExceededError,
)
from research_automation.control_plane.contracts import Actor, Phase, SideEffect
from research_automation.control_plane.campaign_lifecycle import (
    CampaignLifecycleError,
    CampaignPauseStatus,
    CampaignStateConflictError,
    CampaignStatus,
    CycleStatus,
    DuplicateCycleError,
    IllegalCycleTransitionError,
    OperationalCampaignLifecycle,
)


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"
NOW = datetime(2026, 8, 1, 1, 2, 3, tzinfo=timezone.utc)
_COMPLETE_CYCLE_TRANSITIONS = (
    (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
    (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
    (CycleStatus.CONTEXT_READY, CycleStatus.FROZEN),
    (CycleStatus.FROZEN, CycleStatus.EXECUTING),
    (CycleStatus.EXECUTING, CycleStatus.EVIDENCE_READY),
    (CycleStatus.EVIDENCE_READY, CycleStatus.LEARNING_COMMITTED),
    (CycleStatus.LEARNING_COMMITTED, CycleStatus.SETTLED),
    (CycleStatus.SETTLED, CycleStatus.INFORMATION_GAIN_RECORDED),
    (
        CycleStatus.INFORMATION_GAIN_RECORDED,
        CycleStatus.NEXT_CYCLE_DECIDED,
    ),
    (CycleStatus.NEXT_CYCLE_DECIDED, CycleStatus.COMPLETED),
)


class _InvalidJsonProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text="{invalid-json",
            request_model="fake-model",
            response_model="fake-model",
            raw_usage={"input_tokens": 7, "output_tokens": 2, "total_tokens": 9},
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
            actor = Actor("p6-runner", "automation", f"{campaign_id}-test")
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
                yield root, grant, OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id=campaign_id,
                    clock=lambda: NOW,
                )
            finally:
                stores_module._expected_schema_sha256.cache_clear()


def _complete_cycle(
    lifecycle: OperationalCampaignLifecycle,
    *,
    cycle_id: str,
) -> None:
    for expected, next_status in _COMPLETE_CYCLE_TRANSITIONS:
        lifecycle.advance_cycle(
            cycle_id=cycle_id,
            expected_status=expected,
            next_status=next_status,
        )


class OperationalCampaignMigrationTests(unittest.TestCase):
    def test_v2_migration_adds_campaign_events_without_touching_authority(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                original_schema = stores_module._OPERATIONAL_SCHEMA
                original_version = stores_module._OPERATIONAL_SCHEMA_VERSION
                try:
                    stores_module._OPERATIONAL_SCHEMA = (
                        stores_module._OPERATIONAL_SCHEMA_V2
                    )
                    stores_module._OPERATIONAL_SCHEMA_VERSION = 2
                    stores_module._expected_schema_sha256.cache_clear()
                    stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                finally:
                    stores_module._OPERATIONAL_SCHEMA = original_schema
                    stores_module._OPERATIONAL_SCHEMA_VERSION = original_version
                    stores_module._expected_schema_sha256.cache_clear()

                actor = Actor("p6-runner", "automation", "p6-migration-test")
                identity = stores_module.AuthorityIdentity(
                    "a" * 64,
                    campaign_scope_sha256(
                        namespace="formal",
                        campaign_id="campaign-authorized",
                    ),
                    "c" * 64,
                )
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: NOW,
                )
                authorization = authority._provision_authorization(
                    phase=Phase.P6,
                    attempt_id="p6-migration-attempt",
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
                    expected_attempt_id="p6-migration-attempt",
                    actor=actor,
                    identity=identity,
                )
                with self.assertRaises(PermissionError):
                    OperationalCampaignJournal(
                        root_secret=ROOT_SECRET,
                        grant=grant,
                        namespace="formal",
                        campaign_id="campaign-not-authorized",
                    )
                connection = sqlite3.connect(operational_path)
                try:
                    self.assertEqual(
                        connection.execute("PRAGMA user_version").fetchone()[0],
                        2,
                    )
                finally:
                    connection.close()

                authority_before = hashlib.sha256(
                    authority_path.read_bytes()
                ).hexdigest()
                self.assertTrue(
                    stores_module._migrate_operational_journal_v3(
                        root_secret=ROOT_SECRET
                    )
                )
                self.assertFalse(
                    stores_module._migrate_operational_journal_v3(
                        root_secret=ROOT_SECRET
                    )
                )
                self.assertEqual(
                    hashlib.sha256(authority_path.read_bytes()).hexdigest(),
                    authority_before,
                )
                connection = sqlite3.connect(operational_path)
                try:
                    table = connection.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE type = 'table' AND name = 'campaign_events'"
                    ).fetchone()
                    version = connection.execute(
                        "PRAGMA user_version"
                    ).fetchone()[0]
                finally:
                    connection.close()
                self.assertIsNotNone(table)
                self.assertEqual(version, 3)


class OperationalBudgetJournalTests(unittest.TestCase):
    def test_concurrent_reservation_is_atomic_and_survives_reopen(self) -> None:
        with _authorized_campaign("campaign-budget-001") as (_, grant, journal):
            budgets = (
                OperationalBudgetJournal(
                    journal=journal,
                    budget_id="campaign-budget",
                    max_input_tokens=100,
                    max_output_tokens=100,
                    max_cost="1.00",
                ),
                OperationalBudgetJournal(
                    journal=OperationalCampaignJournal(
                        root_secret=ROOT_SECRET,
                        grant=grant,
                        namespace="formal",
                        campaign_id="campaign-budget-001",
                        clock=lambda: NOW,
                    ),
                    budget_id="campaign-budget",
                    max_input_tokens=100,
                    max_output_tokens=100,
                    max_cost="1.00",
                ),
            )

            def reserve(index: int) -> bool:
                try:
                    budgets[index].reserve(
                        reservation_id=f"reservation-{index}",
                        call_id=f"call-{index}",
                        max_input_tokens=60,
                        max_output_tokens=60,
                        max_cost="0.60",
                    )
                except BudgetExceededError:
                    return False
                return True

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = tuple(executor.map(reserve, range(2)))

            self.assertEqual(sum(outcomes), 1)
            reopened = OperationalBudgetJournal(
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id="campaign-budget-001",
                    clock=lambda: NOW,
                ),
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            snapshot = reopened.snapshot()
            self.assertEqual(snapshot.reserved_input_tokens, 60)
            self.assertEqual(snapshot.reserved_output_tokens, 60)
            self.assertEqual(snapshot.reserved_cost, "0.6")

    def test_known_settlement_survives_reopen_and_is_idempotent(self) -> None:
        with _authorized_campaign("campaign-budget-002") as (_, grant, journal):
            budget = OperationalBudgetJournal(
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            budget.reserve(
                reservation_id="reservation-known",
                call_id="call-known",
                max_input_tokens=60,
                max_output_tokens=60,
                max_cost="0.60",
            )

            settlement = budget.settle(
                "reservation-known",
                input_tokens=20,
                output_tokens=10,
                cost="0.20",
            )
            reopened = OperationalBudgetJournal(
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id="campaign-budget-002",
                    clock=lambda: NOW,
                ),
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.0",
            )
            replay = reopened.settle(
                "reservation-known",
                input_tokens=20,
                output_tokens=10,
                cost="0.2",
            )

            self.assertEqual(settlement.state, "SETTLED")
            self.assertEqual(replay.state, "SETTLED")
            snapshot = reopened.snapshot()
            self.assertEqual(snapshot.reserved_input_tokens, 0)
            self.assertEqual(snapshot.reserved_output_tokens, 0)
            self.assertEqual(snapshot.reserved_cost, "0")
            self.assertEqual(snapshot.spent_input_tokens, 20)
            self.assertEqual(snapshot.spent_output_tokens, 10)
            self.assertEqual(snapshot.spent_cost, "0.2")

    def test_unknown_settlement_keeps_full_persistent_reservation(self) -> None:
        with _authorized_campaign("campaign-budget-003") as (_, grant, journal):
            budget = OperationalBudgetJournal(
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            budget.reserve(
                reservation_id="reservation-unknown",
                call_id="call-unknown",
                max_input_tokens=60,
                max_output_tokens=60,
                max_cost="0.60",
            )

            settlement = budget.settle(
                "reservation-unknown",
                input_tokens=None,
                output_tokens=None,
                cost=None,
            )
            reopened = OperationalBudgetJournal(
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id="campaign-budget-003",
                    clock=lambda: NOW,
                ),
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )

            self.assertEqual(settlement.state, "SETTLED_UNKNOWN")
            snapshot = reopened.snapshot()
            self.assertEqual(snapshot.reserved_input_tokens, 60)
            self.assertEqual(snapshot.reserved_output_tokens, 60)
            self.assertEqual(snapshot.reserved_cost, "0.6")
            with self.assertRaises(BudgetExceededError):
                reopened.reserve(
                    reservation_id="reservation-next",
                    call_id="call-next",
                    max_input_tokens=50,
                    max_output_tokens=50,
                    max_cost="0.50",
                )

    def test_reopen_rejects_budget_configuration_drift(self) -> None:
        with _authorized_campaign("campaign-budget-004") as (_, grant, journal):
            OperationalBudgetJournal(
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )

            with self.assertRaises(BudgetConflictError):
                OperationalBudgetJournal(
                    journal=OperationalCampaignJournal(
                        root_secret=ROOT_SECRET,
                        grant=grant,
                        namespace="formal",
                        campaign_id="campaign-budget-004",
                        clock=lambda: NOW,
                    ),
                    budget_id="campaign-budget",
                    max_input_tokens=101,
                    max_output_tokens=100,
                    max_cost="1.00",
                )
            events = journal.list_events(
                cycle_id=None,
                aggregate_type="CAMPAIGN_BUDGET",
                aggregate_id="campaign-budget",
            )
            self.assertEqual(len(events), 1)

    def test_replay_rejects_malformed_budget_identifiers_fail_closed(self) -> None:
        with _authorized_campaign("campaign-budget-005") as (_, _, journal):
            budget = OperationalBudgetJournal(
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            journal.append(
                event_id="malformed-budget-reservation",
                cycle_id=None,
                aggregate_type="CAMPAIGN_BUDGET",
                aggregate_id="campaign-budget",
                event_type="BUDGET_RESERVED",
                payload={
                    "reservation_id": "réservation-invalid",
                    "call_id": " ",
                    "max_input_tokens": 1,
                    "max_output_tokens": 1,
                    "max_cost": "0.1",
                },
            )

            with self.assertRaises(CampaignJournalError):
                budget.snapshot()

    def test_replay_rejects_noncanonical_settlement_payload(self) -> None:
        campaign_id = "campaign-budget-006"
        reservation_id = "reservation-noncanonical"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            budget = OperationalBudgetJournal(
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            budget.reserve(
                reservation_id=reservation_id,
                call_id="call-noncanonical",
                max_input_tokens=60,
                max_output_tokens=60,
                max_cost="0.60",
            )
            event_id = hashlib.sha256(
                b"control_plane.campaign_budget_event.v1\0"
                + (
                    f"formal\0{campaign_id}\0campaign-budget\0settle\0"
                    f"{reservation_id}"
                ).encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=event_id,
                cycle_id=None,
                aggregate_type="CAMPAIGN_BUDGET",
                aggregate_id="campaign-budget",
                event_type="BUDGET_SETTLED",
                payload={
                    "reservation_id": reservation_id,
                    "input_tokens": 20,
                    "output_tokens": 10,
                    "cost": "0.20",
                    "state": "SETTLED",
                },
            )

            with self.assertRaises(CampaignJournalError):
                budget.snapshot()

    def test_revocation_precedes_budget_input_validation(self) -> None:
        with _authorized_campaign("campaign-budget-007") as (root, grant, journal):
            budget = OperationalBudgetJournal(
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            connection = sqlite3.connect(root / "authority.sqlite3")
            try:
                connection.execute(
                    "UPDATE phase_grants_v2 SET state = 'REVOKED' "
                    "WHERE grant_id = ?",
                    (grant.grant_id,),
                )
                connection.commit()
            finally:
                connection.close()

            with self.assertRaises(PermissionError):
                budget.reserve(
                    reservation_id="invalid identifier",
                    call_id="",
                    max_input_tokens=1,
                    max_output_tokens=1,
                    max_cost="0.1",
                )


class OperationalCampaignLifecycleTests(unittest.TestCase):
    def test_pause_request_keeps_current_cycle_live_and_blocks_new_cycle(self) -> None:
        campaign_id = "campaign-lifecycle-pause-001"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            opened = lifecycle.open_cycle(
                cycle_id="cycle-001",
                cycle_number=1,
            )

            requested = lifecycle.request_pause(pause_id="pause-001")

            self.assertEqual(requested.status, CampaignPauseStatus.PAUSE_REQUESTED)
            self.assertEqual(requested.active_pause_id, "pause-001")
            self.assertEqual(lifecycle.snapshot().status, CampaignStatus.ACTIVE)
            self.assertEqual(
                lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1),
                opened,
            )
            advanced = lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CREATED,
                next_status=CycleStatus.BUDGET_RESERVED,
            )
            self.assertEqual(advanced.status, CycleStatus.BUDGET_RESERVED)
            with self.assertRaises(CampaignStateConflictError):
                lifecycle.open_cycle(cycle_id="cycle-002", cycle_number=2)

            reopened = OperationalCampaignLifecycle(journal=journal)
            self.assertEqual(reopened.pause_snapshot(), requested)

    def test_pause_is_acknowledged_only_at_a_completed_cycle_boundary(self) -> None:
        campaign_id = "campaign-lifecycle-pause-002"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            requested = lifecycle.request_pause(pause_id="pause-001")

            with self.assertRaises(CampaignStateConflictError):
                lifecycle.pause_at_safe_boundary(
                    pause_id="pause-001",
                    boundary_cycle_id="cycle-001",
                )
            self.assertEqual(lifecycle.pause_snapshot(), requested)

            _complete_cycle(lifecycle, cycle_id="cycle-001")

            paused = lifecycle.pause_at_safe_boundary(
                pause_id="pause-001",
                boundary_cycle_id="cycle-001",
            )

            self.assertEqual(paused.status, CampaignPauseStatus.PAUSED)
            self.assertEqual(paused.active_pause_id, "pause-001")
            self.assertEqual(paused.boundary_cycle_id, "cycle-001")
            self.assertEqual(
                lifecycle.pause_at_safe_boundary(
                    pause_id="pause-001",
                    boundary_cycle_id="cycle-001",
                ),
                paused,
            )
            with self.assertRaises(CampaignStateConflictError):
                lifecycle.open_cycle(cycle_id="cycle-002", cycle_number=2)
            reopened = OperationalCampaignLifecycle(journal=journal)
            self.assertEqual(reopened.pause_snapshot(), paused)

    def test_resume_persists_and_allows_the_next_cycle(self) -> None:
        campaign_id = "campaign-lifecycle-pause-003"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            lifecycle.request_pause(pause_id="pause-001")
            _complete_cycle(lifecycle, cycle_id="cycle-001")
            lifecycle.pause_at_safe_boundary(
                pause_id="pause-001",
                boundary_cycle_id="cycle-001",
            )

            resumed = lifecycle.resume_pause(
                pause_id="pause-001",
                resume_id="resume-001",
            )

            self.assertEqual(resumed.status, CampaignPauseStatus.RUNNING)
            self.assertIsNone(resumed.active_pause_id)
            self.assertIsNone(resumed.boundary_cycle_id)
            self.assertEqual(resumed.last_pause_id, "pause-001")
            self.assertEqual(resumed.last_resume_id, "resume-001")
            self.assertEqual(
                lifecycle.resume_pause(
                    pause_id="pause-001",
                    resume_id="resume-001",
                ),
                resumed,
            )
            reopened = OperationalCampaignLifecycle(journal=journal)
            self.assertEqual(reopened.pause_snapshot(), resumed)
            opened = reopened.open_cycle(
                cycle_id="cycle-002",
                cycle_number=2,
            )
            self.assertEqual(opened.status, CycleStatus.CREATED)

    def test_pause_request_and_cycle_open_are_serialized_at_the_boundary(self) -> None:
        campaign_id = "campaign-lifecycle-pause-004"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            first = OperationalCampaignLifecycle(journal=journal)
            second = OperationalCampaignLifecycle(journal=journal)
            first.activate()
            barrier = Barrier(2)

            def request_pause():
                barrier.wait(timeout=5)
                return first.request_pause(pause_id="pause-001")

            def open_cycle():
                barrier.wait(timeout=5)
                return second.open_cycle(
                    cycle_id="cycle-001",
                    cycle_number=1,
                )

            with ThreadPoolExecutor(max_workers=2) as pool:
                pause_future = pool.submit(request_pause)
                cycle_future = pool.submit(open_cycle)
                requested = pause_future.result(timeout=5)
                try:
                    opened = cycle_future.result(timeout=5)
                except CampaignStateConflictError:
                    opened = None

            self.assertEqual(requested.status, CampaignPauseStatus.PAUSE_REQUESTED)
            if opened is not None:
                self.assertLess(opened.sequence, requested.sequence)
            with self.assertRaises(CampaignStateConflictError):
                first.open_cycle(cycle_id="cycle-002", cycle_number=2)

    def test_paused_campaign_requires_resume_before_completion(self) -> None:
        campaign_id = "campaign-lifecycle-pause-005"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            lifecycle.request_pause(pause_id="pause-001")
            _complete_cycle(lifecycle, cycle_id="cycle-001")
            paused = lifecycle.pause_at_safe_boundary(
                pause_id="pause-001",
                boundary_cycle_id="cycle-001",
            )

            with self.assertRaises(CampaignStateConflictError):
                lifecycle.complete()

            self.assertEqual(lifecycle.pause_snapshot(), paused)
            lifecycle.resume_pause(
                pause_id="pause-001",
                resume_id="resume-001",
            )
            completed = lifecycle.complete()
            self.assertEqual(completed.status, CampaignStatus.COMPLETED)

    def test_resume_can_cancel_a_pending_pause_without_restarting_the_cycle(self) -> None:
        campaign_id = "campaign-lifecycle-pause-006"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            opened = lifecycle.open_cycle(
                cycle_id="cycle-001",
                cycle_number=1,
            )
            lifecycle.request_pause(pause_id="pause-001")

            resumed = lifecycle.resume_pause(
                pause_id="pause-001",
                resume_id="resume-001",
            )

            self.assertEqual(resumed.status, CampaignPauseStatus.RUNNING)
            self.assertEqual(
                lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1),
                opened,
            )
            advanced = lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CREATED,
                next_status=CycleStatus.BUDGET_RESERVED,
            )
            self.assertEqual(advanced.status, CycleStatus.BUDGET_RESERVED)

    def test_pause_and_resume_ids_cannot_rebind_across_generations(self) -> None:
        campaign_id = "campaign-lifecycle-pause-007"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            requested = lifecycle.request_pause(pause_id="pause-001")

            self.assertEqual(
                lifecycle.request_pause(pause_id="pause-001"),
                requested,
            )
            with self.assertRaises(CampaignStateConflictError):
                lifecycle.request_pause(pause_id="pause-002")
            lifecycle.resume_pause(
                pause_id="pause-001",
                resume_id="resume-001",
            )
            with self.assertRaises(CampaignStateConflictError):
                lifecycle.request_pause(pause_id="pause-001")

            lifecycle.request_pause(pause_id="pause-002")
            with self.assertRaises(CampaignStateConflictError):
                lifecycle.resume_pause(
                    pause_id="pause-002",
                    resume_id="resume-001",
                )

    def test_pause_event_identity_is_unambiguous_for_colon_identifiers(self) -> None:
        campaign_id = "campaign-lifecycle-pause-colon-ids"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()

            lifecycle.request_pause(pause_id="a:b")
            lifecycle.resume_pause(pause_id="a:b", resume_id="c")
            lifecycle.request_pause(pause_id="a")
            resumed = lifecycle.resume_pause(pause_id="a", resume_id="b:c")

            self.assertEqual(resumed.status, CampaignPauseStatus.RUNNING)
            self.assertEqual(resumed.last_pause_id, "a")
            self.assertEqual(resumed.last_resume_id, "b:c")
            self.assertEqual(
                OperationalCampaignLifecycle(journal=journal).pause_snapshot(),
                resumed,
            )

    def test_pause_replay_rejects_an_alias_event_identity(self) -> None:
        campaign_id = "campaign-lifecycle-pause-008"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            journal.append(
                event_id="alias-pause-request",
                cycle_id=None,
                aggregate_type="CAMPAIGN_PAUSE",
                aggregate_id=campaign_id,
                event_type="CAMPAIGN_PAUSE_REQUESTED",
                payload={"pause_id": "pause-001"},
            )

            with self.assertRaises(CampaignLifecycleError):
                lifecycle.pause_snapshot()
            with self.assertRaises(CampaignLifecycleError):
                lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)

    def test_campaign_can_pause_at_the_boundary_before_its_first_cycle(self) -> None:
        campaign_id = "campaign-lifecycle-pause-009"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.request_pause(pause_id="pause-001")

            paused = lifecycle.pause_at_safe_boundary(
                pause_id="pause-001",
                boundary_cycle_id=None,
            )

            self.assertEqual(paused.status, CampaignPauseStatus.PAUSED)
            self.assertIsNone(paused.boundary_cycle_id)
            with self.assertRaises(CampaignStateConflictError):
                lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            lifecycle.resume_pause(
                pause_id="pause-001",
                resume_id="resume-001",
            )
            opened = lifecycle.open_cycle(
                cycle_id="cycle-001",
                cycle_number=1,
            )
            self.assertEqual(opened.status, CycleStatus.CREATED)

    def test_cycle_cannot_skip_required_protocol_states(self) -> None:
        with _authorized_campaign("campaign-lifecycle-001") as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            self.assertEqual(lifecycle.snapshot().status, CampaignStatus.CREATED)
            lifecycle.activate()
            opened = lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            self.assertEqual(opened.status, CycleStatus.CREATED)

            with self.assertRaises(IllegalCycleTransitionError):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=CycleStatus.CREATED,
                    next_status=CycleStatus.EXECUTING,
                )

            unchanged = lifecycle.cycle_snapshot("cycle-001")
            self.assertEqual(unchanged.status, CycleStatus.CREATED)
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_STATE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_cycle_id_replay_is_idempotent_but_cycle_number_is_unique(self) -> None:
        with _authorized_campaign("campaign-lifecycle-002") as (_, grant, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            reopened = OperationalCampaignLifecycle(
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id="campaign-lifecycle-002",
                    clock=lambda: NOW,
                )
            )

            replay = reopened.open_cycle(cycle_id="cycle-001", cycle_number=1)
            self.assertEqual(replay.status, CycleStatus.CREATED)
            with self.assertRaises(DuplicateCycleError):
                reopened.open_cycle(cycle_id="cycle-002", cycle_number=1)

            first_events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_STATE",
                aggregate_id="cycle-001",
            )
            duplicate_events = journal.list_events(
                cycle_id="cycle-002",
                aggregate_type="CYCLE_STATE",
                aggregate_id="cycle-002",
            )
            self.assertEqual(len(first_events), 1)
            self.assertEqual(duplicate_events, ())

    def test_complete_cycle_protocol_survives_reopen(self) -> None:
        with _authorized_campaign("campaign-lifecycle-003") as (_, grant, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            transitions = (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
                (CycleStatus.CONTEXT_READY, CycleStatus.FROZEN),
                (CycleStatus.FROZEN, CycleStatus.EXECUTING),
                (CycleStatus.EXECUTING, CycleStatus.EVIDENCE_READY),
                (CycleStatus.EVIDENCE_READY, CycleStatus.LEARNING_COMMITTED),
                (CycleStatus.LEARNING_COMMITTED, CycleStatus.SETTLED),
                (
                    CycleStatus.SETTLED,
                    CycleStatus.INFORMATION_GAIN_RECORDED,
                ),
                (
                    CycleStatus.INFORMATION_GAIN_RECORDED,
                    CycleStatus.NEXT_CYCLE_DECIDED,
                ),
                (CycleStatus.NEXT_CYCLE_DECIDED, CycleStatus.COMPLETED),
            )
            for expected, next_status in transitions:
                advanced = lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
                self.assertEqual(advanced.status, next_status)

            reopened = OperationalCampaignLifecycle(
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id="campaign-lifecycle-003",
                    clock=lambda: NOW,
                )
            )
            completed = reopened.cycle_snapshot("cycle-001")
            replay = reopened.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.NEXT_CYCLE_DECIDED,
                next_status=CycleStatus.COMPLETED,
            )

            self.assertEqual(completed.status, CycleStatus.COMPLETED)
            self.assertEqual(replay, completed)
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_STATE",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 11)

    def test_campaign_completion_requires_every_cycle_completed(self) -> None:
        with _authorized_campaign("campaign-lifecycle-004") as (_, grant, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)

            with self.assertRaises(CampaignStateConflictError):
                lifecycle.complete()
            transitions = (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
                (CycleStatus.CONTEXT_READY, CycleStatus.FROZEN),
                (CycleStatus.FROZEN, CycleStatus.EXECUTING),
                (CycleStatus.EXECUTING, CycleStatus.EVIDENCE_READY),
                (CycleStatus.EVIDENCE_READY, CycleStatus.LEARNING_COMMITTED),
                (CycleStatus.LEARNING_COMMITTED, CycleStatus.SETTLED),
                (
                    CycleStatus.SETTLED,
                    CycleStatus.INFORMATION_GAIN_RECORDED,
                ),
                (
                    CycleStatus.INFORMATION_GAIN_RECORDED,
                    CycleStatus.NEXT_CYCLE_DECIDED,
                ),
                (CycleStatus.NEXT_CYCLE_DECIDED, CycleStatus.COMPLETED),
            )
            for expected, next_status in transitions:
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )

            completed = lifecycle.complete()
            reopened = OperationalCampaignLifecycle(
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id="campaign-lifecycle-004",
                    clock=lambda: NOW,
                )
            )
            self.assertEqual(completed.status, CampaignStatus.COMPLETED)
            self.assertEqual(reopened.snapshot().status, CampaignStatus.COMPLETED)
            with self.assertRaises(CampaignStateConflictError):
                reopened.open_cycle(cycle_id="cycle-002", cycle_number=2)

    def test_cycle_replay_rejects_alias_event_envelope(self) -> None:
        campaign_id = "campaign-lifecycle-005"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            event_id = hashlib.sha256(
                b"control_plane.campaign_lifecycle_event.v1\0"
                + (
                    f"formal\0{campaign_id}\0CYCLE_STATE\0cycle-001\0CREATED"
                ).encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=event_id,
                cycle_id="cycle-alias",
                aggregate_type="CYCLE_STATE",
                aggregate_id="cycle-alias",
                event_type="CYCLE_OPENED",
                payload={
                    "cycle_id": "cycle-001",
                    "cycle_number": 1,
                    "status": "CREATED",
                },
            )

            with self.assertRaisesRegex(CampaignLifecycleError, "envelope"):
                lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)


class OperationalUsageJournalTests(unittest.TestCase):
    def test_response_received_cannot_be_recorded_as_final_outcome(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
                _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
            ):
                stores_module._expected_schema_sha256.cache_clear()
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                actor = Actor("p6-runner", "automation", "p6-finish-test")
                identity = stores_module.AuthorityIdentity(
                    "a" * 64,
                    campaign_scope_sha256(
                        namespace="formal",
                        campaign_id="campaign-finish-001",
                    ),
                    "c" * 64,
                )
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: NOW,
                )
                authorization = authority._provision_authorization(
                    phase=Phase.P6,
                    attempt_id="p6-finish-attempt",
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
                    expected_attempt_id="p6-finish-attempt",
                    actor=actor,
                    identity=identity,
                )
                usage = OperationalUsageJournal(
                    journal=OperationalCampaignJournal(
                        root_secret=ROOT_SECRET,
                        grant=grant,
                        namespace="formal",
                        campaign_id="campaign-finish-001",
                        clock=lambda: NOW,
                    ),
                    cycle_id="cycle-001",
                )

                with self.assertRaisesRegex(ValueError, "final outcome"):
                    usage.finish(
                        call_id="call-not-started",
                        attempt_id="attempt-not-started",
                        outcome=InvocationOutcome.RESPONSE_RECEIVED,
                    )
                stores_module._expected_schema_sha256.cache_clear()

    def test_invalid_json_usage_survives_journal_reopen(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
                _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
            ):
                stores_module._expected_schema_sha256.cache_clear()
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                actor = Actor("p6-runner", "automation", "p6-test-invocation")
                identity = stores_module.AuthorityIdentity(
                    "a" * 64,
                    campaign_scope_sha256(
                        namespace="formal",
                        campaign_id="campaign-offline-001",
                    ),
                    "c" * 64,
                )
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: NOW,
                )
                authorization = authority._provision_authorization(
                    phase=Phase.P6,
                    attempt_id="p6-test-attempt",
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
                    expected_attempt_id="p6-test-attempt",
                    actor=actor,
                    identity=identity,
                )
                journal = OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id="campaign-offline-001",
                    clock=lambda: NOW,
                )
                usage = OperationalUsageJournal(
                    journal=journal,
                    cycle_id="cycle-001",
                )
                invocation = ModelInvocation(
                    provider=_InvalidJsonProvider(),
                    usage_journal=usage,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-model",
                )

                with self.assertRaises(InvalidModelResponseError):
                    invocation.invoke_json(
                        {"prompt": "offline-only"},
                        call_id="call-persisted",
                        attempt_id="attempt-001",
                    )

                reopened = OperationalUsageJournal(
                    journal=OperationalCampaignJournal(
                        root_secret=ROOT_SECRET,
                        grant=grant,
                        namespace="formal",
                        campaign_id="campaign-offline-001",
                        clock=lambda: NOW,
                    ),
                    cycle_id="cycle-001",
                )
                recorded = reopened.read_attempt(
                    call_id="call-persisted",
                    attempt_id="attempt-001",
                )
                self.assertEqual(recorded.envelope.usage_status, UsageStatus.REPORTED)
                self.assertEqual(recorded.envelope.total_tokens, 9)
                self.assertEqual(
                    recorded.final_outcome,
                    InvocationOutcome.INVALID_JSON,
                )
                connection = sqlite3.connect(root / "operational.sqlite3")
                try:
                    connection.execute(
                        "UPDATE campaign_events SET payload_json = ? "
                        "WHERE event_type = 'MODEL_USAGE_RECORDED'",
                        ('{"tampered":true}',),
                    )
                    connection.commit()
                finally:
                    connection.close()
                with self.assertRaises(CampaignJournalError):
                    reopened.read_attempt(
                        call_id="call-persisted",
                        attempt_id="attempt-001",
                    )
                stores_module._expected_schema_sha256.cache_clear()


if __name__ == "__main__":
    unittest.main()
