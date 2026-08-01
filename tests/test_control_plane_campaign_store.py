from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
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


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"
NOW = datetime(2026, 8, 1, 1, 2, 3, tzinfo=timezone.utc)


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
