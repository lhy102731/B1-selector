from __future__ import annotations

import hashlib
import json
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
    RetryingModelInvocation,
    UsageEnvelope,
    UsageStatus,
)
from research_automation.control_plane.campaign_store import (
    _event_integrity_sha256,
    CampaignExecutionMode,
    CampaignLearningCommitSink,
    DryRunIsolationError,
    CampaignJournalError,
    OperationalBudgetJournal,
    OperationalCampaignJournal,
    OperationalCycleBudgetJournal,
    OperationalUsageJournal,
    campaign_execution_mode,
    campaign_scope_sha256,
    dry_run_namespace,
)
from research_automation.control_plane.evidence_learning import (
    EvidenceAdapter,
    LearningCommitService,
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
from research_automation.control_plane.runner_control import P4RunController


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


class _TimeoutThenSuccessProvider:
    def __init__(self) -> None:
        self.call_count = 0

    def invoke(self, request: object) -> ProviderResponse:
        self.call_count += 1
        if self.call_count == 1:
            raise TimeoutError("synthetic first-attempt timeout")
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-model",
            response_model="fake-model",
            raw_usage={
                "input_tokens": 3,
                "output_tokens": 2,
                "total_tokens": 5,
                "reported_cost": "0.01",
            },
        )


def _claim_campaign_grant(
    *,
    campaign_id: str,
    namespace: str,
    actor_id: str,
    invocation_id: str,
    attempt_id: str,
    plan_sha256: str,
    instruction_sha256: str,
) -> stores_module.AuthorityGrant:
    actor = Actor(actor_id, "automation", invocation_id)
    identity = stores_module.AuthorityIdentity(
        plan_sha256,
        campaign_scope_sha256(
            namespace=namespace,
            campaign_id=campaign_id,
        ),
        instruction_sha256,
    )
    authority = stores_module._AuthorityStore(
        root_secret=ROOT_SECRET,
        clock=lambda: NOW,
    )
    authorization = authority._provision_authorization(
        phase=Phase.P6,
        attempt_id=attempt_id,
        actor=actor,
        identity=identity,
        expires_at=NOW.replace(year=2027),
        allowed_side_effects=(
            SideEffect.READ,
            SideEffect.WRITE_CONTROL_PLANE,
        ),
    )
    return authority.claim_authorization(
        authorization,
        expected_phase=Phase.P6,
        expected_attempt_id=attempt_id,
        actor=actor,
        identity=identity,
    )


@contextmanager
def _authorized_campaign(campaign_id: str, *, namespace: str = "formal"):
    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        with patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
            _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
        ):
            stores_module._expected_schema_sha256.cache_clear()
            stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
            grant = _claim_campaign_grant(
                campaign_id=campaign_id,
                namespace=namespace,
                actor_id="p6-runner",
                invocation_id=f"{campaign_id}-test",
                attempt_id=f"{campaign_id}-attempt",
                plan_sha256="a" * 64,
                instruction_sha256="c" * 64,
            )
            try:
                yield root, grant, OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace=namespace,
                    campaign_id=campaign_id,
                    clock=lambda: NOW,
                )
            finally:
                stores_module._expected_schema_sha256.cache_clear()


def _campaign_full_rows(
    root: Path,
    *,
    campaign_id: str,
    namespace: str = "formal",
) -> tuple[tuple[object, ...], ...]:
    connection = sqlite3.connect(root / "operational.sqlite3")
    try:
        return tuple(
            connection.execute(
                "SELECT event_id, namespace, campaign_id, cycle_id, "
                "aggregate_type, aggregate_id, event_type, payload_json, "
                "payload_sha256, occurred_at, sequence "
                "FROM campaign_events WHERE namespace = ? AND campaign_id = ? "
                "ORDER BY sequence",
                (namespace, campaign_id),
            ).fetchall()
        )
    finally:
        connection.close()


def _rewrite_campaign_event_payload(
    root: Path,
    *,
    event_id: str,
    payload: dict[str, object],
) -> None:
    connection = sqlite3.connect(root / "operational.sqlite3")
    try:
        row = connection.execute(
            "SELECT event_id, namespace, campaign_id, cycle_id, aggregate_type, "
            "aggregate_id, event_type, occurred_at, sequence "
            "FROM campaign_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if row is None:
            raise AssertionError("campaign event does not exist")
        payload_json = json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        payload_sha256 = _event_integrity_sha256(
            event_id=row[0],
            namespace=row[1],
            campaign_id=row[2],
            cycle_id=row[3],
            aggregate_type=row[4],
            aggregate_id=row[5],
            event_type=row[6],
            payload_json=payload_json,
            occurred_at=row[7],
            sequence=row[8],
        )
        connection.execute(
            "UPDATE campaign_events SET payload_json = ?, payload_sha256 = ? "
            "WHERE event_id = ?",
            (payload_json, payload_sha256, event_id),
        )
        connection.commit()
    finally:
        connection.close()


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


class CampaignNamespaceTests(unittest.TestCase):
    def test_campaign_namespace_contract_is_closed_and_bounded(self) -> None:
        longest_dry_run_id = "x" * 120

        self.assertEqual(
            campaign_execution_mode("formal"),
            CampaignExecutionMode.FORMAL,
        )
        self.assertEqual(
            campaign_execution_mode(dry_run_namespace("preview:colon")),
            CampaignExecutionMode.DRY_RUN,
        )
        self.assertEqual(len(dry_run_namespace(longest_dry_run_id)), 128)
        with self.assertRaises(ValueError):
            dry_run_namespace(longest_dry_run_id + "x")
        with self.assertRaises(ValueError):
            campaign_execution_mode("preview")


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
    def test_currency_survives_budget_journal_replay_and_conflicting_reopen(self) -> None:
        campaign_id = "campaign-budget-currency-001"
        with _authorized_campaign(campaign_id) as (root, grant, journal):
            budget = OperationalBudgetJournal(
                journal=journal,
                budget_id="campaign-budget",
                currency="USD",
                max_input_tokens=100,
                max_output_tokens=50,
                max_cost="1",
            )
            reservation = budget.reserve(
                reservation_id="reservation-currency",
                call_id="call-currency",
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.2",
            )
            settlement = budget.settle(
                reservation.reservation_id,
                currency="USD",
                input_tokens=None,
                output_tokens=None,
                cost=None,
            )
            reopened = OperationalBudgetJournal(
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id=campaign_id,
                    clock=lambda: NOW,
                ),
                budget_id="campaign-budget",
                currency="USD",
                max_input_tokens=100,
                max_output_tokens=50,
                max_cost="1.0",
            )

            self.assertEqual(reservation.currency, "USD")
            self.assertEqual(settlement.currency, "USD")
            self.assertEqual(reopened.snapshot().currency, "USD")
            events = journal.list_events(
                cycle_id=None,
                aggregate_type="CAMPAIGN_BUDGET",
                aggregate_id="campaign-budget",
            )
            domain_payloads = []
            for event in events:
                payload = event.payload()
                payload.pop("_authority_grant_id")
                domain_payloads.append(payload)
            self.assertEqual(
                domain_payloads,
                [
                    {
                        "budget_id": "campaign-budget",
                        "currency": "USD",
                        "max_input_tokens": 100,
                        "max_output_tokens": 50,
                        "max_cost": "1",
                        "max_wall_time_ms": 0,
                        "max_tool_attempts": 0,
                        "max_data_exposures": 0,
                        "max_disk_growth_bytes": 0,
                    },
                    {
                        "reservation_id": "reservation-currency",
                        "call_id": "call-currency",
                        "currency": "USD",
                        "max_input_tokens": 20,
                        "max_output_tokens": 10,
                        "max_cost": "0.2",
                        "max_wall_time_ms": 0,
                        "max_tool_attempts": 0,
                        "max_data_exposures": 0,
                        "max_disk_growth_bytes": 0,
                    },
                    {
                        "reservation_id": "reservation-currency",
                        "currency": "USD",
                        "input_tokens": None,
                        "output_tokens": None,
                        "cost": None,
                        "wall_time_ms": None,
                        "tool_attempts": None,
                        "data_exposures": None,
                        "disk_growth_bytes": None,
                        "state": "SETTLED_UNKNOWN",
                    },
                ],
            )
            connection = sqlite3.connect(root / "operational.sqlite3")
            try:
                rows_before = connection.execute(
                    "SELECT event_id, event_type, payload_json, sequence "
                    "FROM campaign_events WHERE namespace = ? "
                    "AND campaign_id = ? AND aggregate_type = ? "
                    "AND aggregate_id = ? ORDER BY sequence",
                    (
                        "formal",
                        campaign_id,
                        "CAMPAIGN_BUDGET",
                        "campaign-budget",
                    ),
                ).fetchall()
            finally:
                connection.close()

            with self.assertRaises(BudgetConflictError):
                OperationalBudgetJournal(
                    journal=journal,
                    budget_id="campaign-budget",
                    currency="EUR",
                    max_input_tokens=100,
                    max_output_tokens=50,
                    max_cost="1",
                )

            connection = sqlite3.connect(root / "operational.sqlite3")
            try:
                rows_after = connection.execute(
                    "SELECT event_id, event_type, payload_json, sequence "
                    "FROM campaign_events WHERE namespace = ? "
                    "AND campaign_id = ? AND aggregate_type = ? "
                    "AND aggregate_id = ? ORDER BY sequence",
                    (
                        "formal",
                        campaign_id,
                        "CAMPAIGN_BUDGET",
                        "campaign-budget",
                    ),
                ).fetchall()
            finally:
                connection.close()
            self.assertEqual(rows_after, rows_before)
            self.assertEqual(reopened.snapshot().currency, "USD")

    def test_resource_budget_settlement_survives_reopen(self) -> None:
        campaign_id = "campaign-resource-budget-001"
        with _authorized_campaign(campaign_id) as (_, grant, journal):
            budget = OperationalBudgetJournal(
                currency="USD",
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=10,
                max_output_tokens=10,
                max_cost="1",
                max_wall_time_ms=1_000,
                max_tool_attempts=10,
                max_data_exposures=10,
                max_disk_growth_bytes=1_000,
            )
            budget.reserve(
                currency="USD",
                reservation_id="resource-reservation",
                call_id="resource-call",
                max_input_tokens=0,
                max_output_tokens=0,
                max_cost="0",
                max_wall_time_ms=600,
                max_tool_attempts=6,
                max_data_exposures=5,
                max_disk_growth_bytes=700,
            )
            budget.settle(
                "resource-reservation",
                currency="USD",
                input_tokens=0,
                output_tokens=0,
                cost="0",
                wall_time_ms=400,
                tool_attempts=4,
                data_exposures=3,
                disk_growth_bytes=500,
            )

            reopened = OperationalBudgetJournal(
                currency="USD",
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id=campaign_id,
                    clock=lambda: NOW,
                ),
                budget_id="campaign-budget",
                max_input_tokens=10,
                max_output_tokens=10,
                max_cost="1",
                max_wall_time_ms=1_000,
                max_tool_attempts=10,
                max_data_exposures=10,
                max_disk_growth_bytes=1_000,
            )
            snapshot = reopened.snapshot()
            self.assertEqual(snapshot.reserved_wall_time_ms, 0)
            self.assertEqual(snapshot.reserved_tool_attempts, 0)
            self.assertEqual(snapshot.reserved_data_exposures, 0)
            self.assertEqual(snapshot.reserved_disk_growth_bytes, 0)
            self.assertEqual(snapshot.spent_wall_time_ms, 400)
            self.assertEqual(snapshot.spent_tool_attempts, 4)
            self.assertEqual(snapshot.spent_data_exposures, 3)
            self.assertEqual(snapshot.spent_disk_growth_bytes, 500)

    def test_unknown_resource_usage_keeps_persistent_reservations(self) -> None:
        campaign_id = "campaign-resource-budget-002"
        with _authorized_campaign(campaign_id) as (_, grant, journal):
            budget = OperationalBudgetJournal(
                currency="USD",
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=0,
                max_output_tokens=0,
                max_cost="0",
                max_wall_time_ms=1_000,
                max_tool_attempts=10,
                max_data_exposures=10,
                max_disk_growth_bytes=1_000,
            )
            budget.reserve(
                currency="USD",
                reservation_id="resource-reservation-unknown",
                call_id="resource-call-unknown",
                max_input_tokens=0,
                max_output_tokens=0,
                max_cost="0",
                max_wall_time_ms=600,
                max_tool_attempts=6,
                max_data_exposures=5,
                max_disk_growth_bytes=700,
            )

            settlement = budget.settle(
                "resource-reservation-unknown",
                currency="USD",
                input_tokens=0,
                output_tokens=0,
                cost="0",
            )
            reopened = OperationalBudgetJournal(
                currency="USD",
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id=campaign_id,
                    clock=lambda: NOW,
                ),
                budget_id="campaign-budget",
                max_input_tokens=0,
                max_output_tokens=0,
                max_cost="0",
                max_wall_time_ms=1_000,
                max_tool_attempts=10,
                max_data_exposures=10,
                max_disk_growth_bytes=1_000,
            )

            self.assertEqual(settlement.state, "SETTLED_UNKNOWN")
            snapshot = reopened.snapshot()
            self.assertEqual(snapshot.reserved_wall_time_ms, 600)
            self.assertEqual(snapshot.reserved_tool_attempts, 6)
            self.assertEqual(snapshot.reserved_data_exposures, 5)
            self.assertEqual(snapshot.reserved_disk_growth_bytes, 700)

    def test_concurrent_resource_reservations_are_atomic(self) -> None:
        campaign_id = "campaign-resource-budget-003"
        with _authorized_campaign(campaign_id) as (_, grant, journal):
            budgets = (
                OperationalBudgetJournal(
                    currency="USD",
                    journal=journal,
                    budget_id="campaign-budget",
                    max_input_tokens=0,
                    max_output_tokens=0,
                    max_cost="0",
                    max_wall_time_ms=100,
                ),
                OperationalBudgetJournal(
                    currency="USD",
                    journal=OperationalCampaignJournal(
                        root_secret=ROOT_SECRET,
                        grant=grant,
                        namespace="formal",
                        campaign_id=campaign_id,
                        clock=lambda: NOW,
                    ),
                    budget_id="campaign-budget",
                    max_input_tokens=0,
                    max_output_tokens=0,
                    max_cost="0",
                    max_wall_time_ms=100,
                ),
            )

            def reserve(index: int) -> bool:
                try:
                    budgets[index].reserve(
                        currency="USD",
                        reservation_id=f"resource-reservation-{index}",
                        call_id=f"resource-call-{index}",
                        max_input_tokens=0,
                        max_output_tokens=0,
                        max_cost="0",
                        max_wall_time_ms=60,
                    )
                except BudgetExceededError:
                    return False
                return True

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = tuple(executor.map(reserve, range(2)))

            self.assertEqual(sum(outcomes), 1)
            self.assertEqual(
                budgets[0].snapshot().reserved_wall_time_ms,
                60,
            )

    def test_dry_run_budget_cannot_reach_the_formal_learning_sink(self) -> None:
        namespace = dry_run_namespace("preview-001")
        with _authorized_campaign(
            "campaign-dry-run-001",
            namespace=namespace,
        ) as (root, grant, journal):
            packet_directory = (
                root / "research_state/control_plane/learning_packets"
            )
            packet_directory.mkdir(parents=True)
            formal_packet = packet_directory / f"{'a' * 64}.json"
            formal_ledger = (
                root / "research_state/control_plane/learning_commit.sqlite3"
            )
            formal_packet.write_bytes(b"formal-packet-sentinel")
            formal_ledger.write_bytes(b"formal-ledger-sentinel")
            formal_root = root / "research_state/control_plane"
            formal_before = {
                path.relative_to(formal_root).as_posix(): path.read_bytes()
                for path in formal_root.rglob("*")
                if path.is_file()
            }
            budget = OperationalBudgetJournal(
                currency="USD",
                journal=journal,
                budget_id="dry-run-budget",
                max_input_tokens=100,
                max_output_tokens=50,
                max_cost="1.00",
            )
            budget.reserve(
                currency="USD",
                reservation_id="preview-reservation-001",
                call_id="preview-call-001",
                max_input_tokens=10,
                max_output_tokens=5,
                max_cost="0.10",
            )

            sink = CampaignLearningCommitSink(
                journal=journal,
                service=LearningCommitService(repository_root=root),
            )
            with self.assertRaises(DryRunIsolationError):
                sink.commit({})

            self.assertEqual(budget.snapshot().reserved_input_tokens, 10)
            self.assertEqual(
                {
                    path.relative_to(formal_root).as_posix(): path.read_bytes()
                    for path in formal_root.rglob("*")
                    if path.is_file()
                },
                formal_before,
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
                sink.commit({})

    def test_formal_and_dry_run_budgets_do_not_alias_with_identical_ids(self) -> None:
        campaign_id = "campaign-budget-namespace-isolation"
        with _authorized_campaign(campaign_id) as (
            root,
            formal_grant,
            formal_journal,
        ):
            dry_namespace = dry_run_namespace("preview:colon")
            dry_grant = _claim_campaign_grant(
                campaign_id=campaign_id,
                namespace=dry_namespace,
                actor_id="p6-dry-runner",
                invocation_id="campaign-budget-namespace-isolation-dry-test",
                attempt_id="campaign-budget-namespace-isolation-dry-attempt",
                plan_sha256="d" * 64,
                instruction_sha256="e" * 64,
            )
            dry_journal = OperationalCampaignJournal(
                root_secret=ROOT_SECRET,
                grant=dry_grant,
                namespace=dry_namespace,
                campaign_id=campaign_id,
                clock=lambda: NOW,
            )
            formal_budget = OperationalBudgetJournal(
                currency="USD",
                journal=formal_journal,
                budget_id="shared-budget-id",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            dry_budget = OperationalBudgetJournal(
                currency="USD",
                journal=dry_journal,
                budget_id="shared-budget-id",
                max_input_tokens=20,
                max_output_tokens=20,
                max_cost="0.20",
            )

            formal_budget.reserve(
                currency="USD",
                reservation_id="shared-reservation-id",
                call_id="shared-call-id",
                max_input_tokens=11,
                max_output_tokens=5,
                max_cost="0.11",
            )
            dry_budget.reserve(
                currency="USD",
                reservation_id="shared-reservation-id",
                call_id="shared-call-id",
                max_input_tokens=7,
                max_output_tokens=3,
                max_cost="0.07",
            )

            reopened_formal = OperationalBudgetJournal(
                currency="USD",
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=formal_grant,
                    namespace="formal",
                    campaign_id=campaign_id,
                    clock=lambda: NOW,
                ),
                budget_id="shared-budget-id",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            reopened_dry = OperationalBudgetJournal(
                currency="USD",
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=dry_grant,
                    namespace=dry_namespace,
                    campaign_id=campaign_id,
                    clock=lambda: NOW,
                ),
                budget_id="shared-budget-id",
                max_input_tokens=20,
                max_output_tokens=20,
                max_cost="0.20",
            )
            self.assertEqual(reopened_formal.snapshot().reserved_input_tokens, 11)
            self.assertEqual(reopened_dry.snapshot().reserved_input_tokens, 7)

    def test_concurrent_reservation_is_atomic_and_survives_reopen(self) -> None:
        with _authorized_campaign("campaign-budget-001") as (_, grant, journal):
            budgets = (
                OperationalBudgetJournal(
                    currency="USD",
                    journal=journal,
                    budget_id="campaign-budget",
                    max_input_tokens=100,
                    max_output_tokens=100,
                    max_cost="1.00",
                ),
                OperationalBudgetJournal(
                    currency="USD",
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
                        currency="USD",
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
                currency="USD",
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
                currency="USD",
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            budget.reserve(
                currency="USD",
                reservation_id="reservation-known",
                call_id="call-known",
                max_input_tokens=60,
                max_output_tokens=60,
                max_cost="0.60",
            )

            settlement = budget.settle(
                "reservation-known",
                currency="USD",
                input_tokens=20,
                output_tokens=10,
                cost="0.20",
            )
            reopened = OperationalBudgetJournal(
                currency="USD",
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
                currency="USD",
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
                currency="USD",
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            budget.reserve(
                currency="USD",
                reservation_id="reservation-unknown",
                call_id="call-unknown",
                max_input_tokens=60,
                max_output_tokens=60,
                max_cost="0.60",
            )

            settlement = budget.settle(
                "reservation-unknown",
                currency="USD",
                input_tokens=None,
                output_tokens=None,
                cost=None,
            )
            reopened = OperationalBudgetJournal(
                currency="USD",
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
                    currency="USD",
                    reservation_id="reservation-next",
                    call_id="call-next",
                    max_input_tokens=50,
                    max_output_tokens=50,
                    max_cost="0.50",
                )

    def test_reopen_rejects_budget_configuration_drift(self) -> None:
        with _authorized_campaign("campaign-budget-004") as (_, grant, journal):
            OperationalBudgetJournal(
                currency="USD",
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )

            with self.assertRaises(BudgetConflictError):
                OperationalBudgetJournal(
                    currency="USD",
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
            with self.assertRaises(BudgetConflictError):
                OperationalBudgetJournal(
                    currency="USD",
                    journal=OperationalCampaignJournal(
                        root_secret=ROOT_SECRET,
                        grant=grant,
                        namespace="formal",
                        campaign_id="campaign-budget-004",
                        clock=lambda: NOW,
                    ),
                    budget_id="campaign-budget",
                    max_input_tokens=100,
                    max_output_tokens=100,
                    max_cost="1.00",
                    max_wall_time_ms=1,
                )
            events = journal.list_events(
                cycle_id=None,
                aggregate_type="CAMPAIGN_BUDGET",
                aggregate_id="campaign-budget",
            )
            self.assertEqual(len(events), 1)

    def test_budget_replay_rejects_non_integer_token_limits(self) -> None:
        stored_limits = (
            ("input-bool", True, 1),
            ("output-float", 1, 1.0),
        )
        for label, stored_input, stored_output in stored_limits:
            with self.subTest(label=label):
                campaign_id = f"campaign-budget-config-{label}"
                budget_id = "campaign-budget"
                with _authorized_campaign(campaign_id) as (_, _, journal):
                    event_id = hashlib.sha256(
                        b"control_plane.campaign_budget_event.v1\0"
                        + (
                            f"formal\0{campaign_id}\0{budget_id}\0open"
                        ).encode("ascii")
                    ).hexdigest()
                    journal.append(
                        event_id=event_id,
                        cycle_id=None,
                        aggregate_type="CAMPAIGN_BUDGET",
                        aggregate_id=budget_id,
                        event_type="BUDGET_OPENED",
                        payload={
                            "budget_id": budget_id,
                            "currency": "USD",
                            "max_input_tokens": stored_input,
                            "max_output_tokens": stored_output,
                            "max_cost": "1",
                            "max_wall_time_ms": 0,
                            "max_tool_attempts": 0,
                            "max_data_exposures": 0,
                            "max_disk_growth_bytes": 0,
                        },
                    )

                    with self.assertRaises(BudgetConflictError):
                        OperationalBudgetJournal(
                            currency="USD",
                            journal=journal,
                            budget_id=budget_id,
                            max_input_tokens=1,
                            max_output_tokens=1,
                            max_cost="1",
                        )

    def test_replay_rejects_malformed_budget_identifiers_fail_closed(self) -> None:
        with _authorized_campaign("campaign-budget-005") as (_, _, journal):
            budget = OperationalBudgetJournal(
                currency="USD",
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
                    "currency": "USD",
                    "max_input_tokens": 1,
                    "max_output_tokens": 1,
                    "max_cost": "0.1",
                    "max_wall_time_ms": 0,
                    "max_tool_attempts": 0,
                    "max_data_exposures": 0,
                    "max_disk_growth_bytes": 0,
                },
            )

            with self.assertRaises(CampaignJournalError):
                budget.snapshot()

    def test_replay_rejects_noncanonical_settlement_payload(self) -> None:
        campaign_id = "campaign-budget-006"
        reservation_id = "reservation-noncanonical"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            budget = OperationalBudgetJournal(
                currency="USD",
                journal=journal,
                budget_id="campaign-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )
            budget.reserve(
                currency="USD",
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
                    "currency": "USD",
                    "input_tokens": 20,
                    "output_tokens": 10,
                    "cost": "0.20",
                    "wall_time_ms": None,
                    "tool_attempts": None,
                    "data_exposures": None,
                    "disk_growth_bytes": None,
                    "state": "SETTLED",
                },
            )

            with self.assertRaises(CampaignJournalError):
                budget.snapshot()

    def test_revocation_precedes_budget_input_validation(self) -> None:
        with _authorized_campaign("campaign-budget-007") as (root, grant, journal):
            budget = OperationalBudgetJournal(
                currency="USD",
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
                    currency="USD",
                    reservation_id="invalid identifier",
                    call_id="",
                    max_input_tokens=1,
                    max_output_tokens=1,
                    max_cost="0.1",
                )

    def test_campaign_cannot_split_usage_across_budget_ids(self) -> None:
        with _authorized_campaign("campaign-budget-008") as (_, _, journal):
            OperationalBudgetJournal(
                currency="USD",
                journal=journal,
                budget_id="primary-budget",
                max_input_tokens=100,
                max_output_tokens=100,
                max_cost="1.00",
            )

            with self.assertRaises(BudgetConflictError):
                OperationalBudgetJournal(
                    currency="USD",
                    journal=journal,
                    budget_id="second-budget",
                    max_input_tokens=100,
                    max_output_tokens=100,
                    max_cost="1.00",
                )


class OperationalCycleBudgetJournalTests(unittest.TestCase):
    def test_concurrent_cycle_reservation_is_atomic_and_survives_reopen(self) -> None:
        campaign_id = "campaign-cycle-budget-001"
        with _authorized_campaign(campaign_id) as (_, grant, journal):
            budgets = (
                OperationalCycleBudgetJournal(
                    journal=journal,
                    budget_id="cycle-budget",
                    max_cycles=1,
                ),
                OperationalCycleBudgetJournal(
                    journal=OperationalCampaignJournal(
                        root_secret=ROOT_SECRET,
                        grant=grant,
                        namespace="formal",
                        campaign_id=campaign_id,
                        clock=lambda: NOW,
                    ),
                    budget_id="cycle-budget",
                    max_cycles=1,
                ),
            )

            def reserve(index: int) -> str | None:
                cycle_id = f"cycle-{index + 1:03d}"
                try:
                    budgets[index].reserve(cycle_id=cycle_id)
                except BudgetExceededError:
                    return None
                return cycle_id

            with ThreadPoolExecutor(max_workers=2) as executor:
                winners = tuple(executor.map(reserve, range(2)))

            reserved_cycle_ids = tuple(
                cycle_id for cycle_id in winners if cycle_id is not None
            )
            self.assertEqual(len(reserved_cycle_ids), 1)
            reopened = OperationalCycleBudgetJournal(
                journal=OperationalCampaignJournal(
                    root_secret=ROOT_SECRET,
                    grant=grant,
                    namespace="formal",
                    campaign_id=campaign_id,
                    clock=lambda: NOW,
                ),
                budget_id="cycle-budget",
                max_cycles=1,
            )
            self.assertEqual(
                reopened.snapshot().reserved_cycle_ids,
                reserved_cycle_ids,
            )

    def test_cycle_reservation_replay_is_idempotent_and_config_is_immutable(self) -> None:
        campaign_id = "campaign-cycle-budget-002"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            budget = OperationalCycleBudgetJournal(
                journal=journal,
                budget_id="cycle-budget",
                max_cycles=2,
            )

            first = budget.reserve(cycle_id="cycle-001")
            replay = budget.reserve(cycle_id="cycle-001")

            self.assertEqual(replay, first)
            events = journal.list_events(
                cycle_id=None,
                aggregate_type="CAMPAIGN_CYCLE_BUDGET",
                aggregate_id="cycle-budget",
            )
            self.assertEqual(len(events), 2)
            with self.assertRaises(BudgetConflictError):
                OperationalCycleBudgetJournal(
                    journal=journal,
                    budget_id="cycle-budget",
                    max_cycles=3,
                )

    def test_configured_cycle_budget_is_the_only_cycle_open_path(self) -> None:
        campaign_id = "campaign-cycle-budget-003"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            budget = OperationalCycleBudgetJournal(
                journal=journal,
                budget_id="cycle-budget",
                max_cycles=1,
            )

            with self.assertRaises(CampaignStateConflictError):
                lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)

            opened = budget.open_cycle(
                lifecycle=lifecycle,
                cycle_id="cycle-001",
                cycle_number=1,
            )
            self.assertEqual(opened.status, CycleStatus.CREATED)
            self.assertEqual(
                budget.snapshot().reserved_cycle_ids,
                ("cycle-001",),
            )
            with self.assertRaises(BudgetExceededError):
                budget.open_cycle(
                    lifecycle=lifecycle,
                    cycle_id="cycle-002",
                    cycle_number=2,
                )
            with self.assertRaises(CampaignLifecycleError):
                lifecycle.cycle_snapshot("cycle-002")

    def test_cycle_budget_cannot_be_added_after_an_unbudgeted_cycle(self) -> None:
        campaign_id = "campaign-cycle-budget-004"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)

            with self.assertRaises(BudgetConflictError):
                OperationalCycleBudgetJournal(
                    journal=journal,
                    budget_id="late-cycle-budget",
                    max_cycles=2,
                )

            self.assertEqual(
                journal.list_events(
                    cycle_id=None,
                    aggregate_type="CAMPAIGN_CYCLE_BUDGET",
                    aggregate_id="late-cycle-budget",
                ),
                (),
            )

    def test_campaign_cannot_split_cycle_slots_across_budget_ids(self) -> None:
        campaign_id = "campaign-cycle-budget-005"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            OperationalCycleBudgetJournal(
                journal=journal,
                budget_id="primary-cycle-budget",
                max_cycles=1,
            )

            with self.assertRaises(BudgetConflictError):
                OperationalCycleBudgetJournal(
                    journal=journal,
                    budget_id="second-cycle-budget",
                    max_cycles=1,
                )

    def test_campaign_rejects_a_preexisting_second_cycle_budget_stream(self) -> None:
        campaign_id = "campaign-cycle-budget-006"
        second_budget_id = "second-cycle-budget"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            primary = OperationalCycleBudgetJournal(
                journal=journal,
                budget_id="primary-cycle-budget",
                max_cycles=1,
            )
            second_event_id = hashlib.sha256(
                b"control_plane.campaign_cycle_budget_event.v1\0"
                + (
                    f"formal\0{campaign_id}\0{second_budget_id}\0open"
                ).encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=second_event_id,
                cycle_id=None,
                aggregate_type="CAMPAIGN_CYCLE_BUDGET",
                aggregate_id=second_budget_id,
                event_type="CYCLE_BUDGET_OPENED",
                payload={"budget_id": second_budget_id, "max_cycles": 1},
            )

            with self.assertRaises(BudgetConflictError):
                OperationalCycleBudgetJournal(
                    journal=journal,
                    budget_id=second_budget_id,
                    max_cycles=1,
                )
            with self.assertRaises(BudgetConflictError):
                primary.snapshot()

    def test_cycle_budget_cannot_retroactively_adopt_an_open_cycle(self) -> None:
        campaign_id = "campaign-cycle-budget-007"
        cycle_id = "cycle-001"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            budget = OperationalCycleBudgetJournal(
                journal=journal,
                budget_id="cycle-budget",
                max_cycles=1,
            )
            cycle_event_id = hashlib.sha256(
                b"control_plane.campaign_lifecycle_event.v1\0"
                + (
                    f"formal\0{campaign_id}\0CYCLE_STATE\0{cycle_id}\0CREATED"
                ).encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=cycle_event_id,
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

            with self.assertRaises(BudgetConflictError):
                budget.open_cycle(
                    lifecycle=lifecycle,
                    cycle_id=cycle_id,
                    cycle_number=1,
                )
            with self.assertRaises(BudgetConflictError):
                budget.snapshot()

    def test_cycle_budget_replay_rejects_non_integer_cycle_limits(self) -> None:
        for label, stored_limit in (("bool", True), ("float", 1.0)):
            with self.subTest(stored_limit=stored_limit):
                campaign_id = f"campaign-cycle-budget-008-{label}"
                budget_id = "cycle-budget"
                with _authorized_campaign(campaign_id) as (_, _, journal):
                    event_id = hashlib.sha256(
                        b"control_plane.campaign_cycle_budget_event.v1\0"
                        + (
                            f"formal\0{campaign_id}\0{budget_id}\0open"
                        ).encode("ascii")
                    ).hexdigest()
                    journal.append(
                        event_id=event_id,
                        cycle_id=None,
                        aggregate_type="CAMPAIGN_CYCLE_BUDGET",
                        aggregate_id=budget_id,
                        event_type="CYCLE_BUDGET_OPENED",
                        payload={
                            "budget_id": budget_id,
                            "max_cycles": stored_limit,
                        },
                    )

                    with self.assertRaises(BudgetConflictError):
                        OperationalCycleBudgetJournal(
                            journal=journal,
                            budget_id=budget_id,
                            max_cycles=1,
                        )


class CampaignLearningCommitSinkTests(unittest.TestCase):
    def test_formal_learning_sink_binding_is_exact_and_immutable(self) -> None:
        class UnboundLearningCommitService(LearningCommitService):
            def commit(self, *_args, **_kwargs) -> str:
                return "f" * 64

        with _authorized_campaign(
            "campaign-formal-learning-sink-boundary"
        ) as (root, _, journal):
            with self.assertRaisesRegex(
                TypeError,
                "service must be a LearningCommitService",
            ):
                CampaignLearningCommitSink(
                    journal=journal,
                    service=UnboundLearningCommitService(
                        repository_root=root
                    ),
                )

            service = LearningCommitService(repository_root=root)
            sink = CampaignLearningCommitSink(
                journal=journal,
                service=service,
            )
            with self.assertRaisesRegex(
                AttributeError,
                "CampaignLearningCommitSink is immutable",
            ):
                sink._service = service
            with self.assertRaisesRegex(
                AttributeError,
                "CampaignLearningCommitSink is immutable",
            ):
                sink._journal = journal
            with self.assertRaisesRegex(
                AttributeError,
                "CampaignLearningCommitSink is immutable",
            ):
                del sink._service

    def test_valid_p4_evidence_is_blocked_by_a_dry_run_sink_before_write(self) -> None:
        claim = {"kind": "NEGATIVE", "summary": "Preview-only finding."}
        protocol = {"label": "signal-day", "embargo_days": 5}
        artifact = {
            "schema_version": "runner.artifact.v1",
            "runner": "fake-preview-runner",
            "runner_version": "1.0.0",
            "status": "COMPLETED",
            "claim": claim,
            "protocol_conformance": "CONFORMING",
            "executed_protocol": protocol,
            "artifact_refs": [
                {"ref": "preview/result.json", "sha256": "b" * 64}
            ],
            "access_event_ids": ["preview-access-001"],
            "taint_refs": [],
        }
        with _authorized_campaign(
            "campaign-dry-run-p4-controller",
            namespace=dry_run_namespace("preview-p4-controller"),
        ) as (root, _, journal):
            controller = P4RunController(
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fake-preview-runner": "1.0.0"},
                    approved_protocol=protocol,
                    approved_claim=claim,
                ),
                learning_commit_service=CampaignLearningCommitSink(
                    journal=journal,
                    service=LearningCommitService(repository_root=root),
                ),
            )

            with self.assertRaises(DryRunIsolationError):
                controller.finalize(
                    artifact=artifact,
                    authority_task_report={"would_be": "formal-authority"},
                )

            self.assertFalse((root / "research_state").exists())


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

    def test_generic_lifecycle_cannot_record_learning_skipped(self) -> None:
        campaign_id = "campaign-lifecycle-no-learning-controller-only"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
                (CycleStatus.CONTEXT_READY, CycleStatus.FROZEN),
                (CycleStatus.FROZEN, CycleStatus.EXECUTING),
                (CycleStatus.EXECUTING, CycleStatus.EVIDENCE_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )

            with self.assertRaisesRegex(
                CampaignStateConflictError,
                "controller-owned",
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=CycleStatus.EVIDENCE_READY,
                    next_status=CycleStatus.LEARNING_SKIPPED,
                )

            self.assertEqual(
                lifecycle.cycle_snapshot("cycle-001").status,
                CycleStatus.EVIDENCE_READY,
            )

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
    def test_finish_rejects_noncanonical_usage_payload_without_writing(self) -> None:
        campaign_id = "campaign-usage-noncanonical-finish"
        cycle_id = "cycle-001"
        call_id = "call-noncanonical-finish"
        attempt_id = "call-noncanonical-finish-attempt-001"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id=cycle_id,
            )
            usage.begin(
                UsageEnvelope(
                    provider="fake-provider",
                    profile="offline",
                    request_model="fake-model",
                    response_model="fake-model",
                    call_id=call_id,
                    attempt_id=attempt_id,
                    usage_status=UsageStatus.REPORTED,
                    input_tokens=None,
                    output_tokens=None,
                    total_tokens=None,
                    cache_read_tokens=None,
                    cache_write_tokens=None,
                    reasoning_tokens=None,
                    reported_cost="0.01",
                    currency="USD",
                    fallback=False,
                    streamed=False,
                    outcome=InvocationOutcome.RESPONSE_RECEIVED,
                    raw_usage_sha256="4" * 64,
                )
            )
            usage_row = next(
                row
                for row in _campaign_full_rows(root, campaign_id=campaign_id)
                if row[6] == "MODEL_USAGE_RECORDED"
            )
            payload = json.loads(usage_row[7])
            self.assertIn("_authority_grant_id", payload)
            payload["legacy_extra_field"] = "unexpected"
            _rewrite_campaign_event_payload(
                root,
                event_id=usage_row[0],
                payload=payload,
            )

            rows_before = _campaign_full_rows(root, campaign_id=campaign_id)
            count_before = len(rows_before)
            hashes_before = tuple((row[0], row[8]) for row in rows_before)
            rewritten_usage_row = next(
                row for row in rows_before if row[6] == "MODEL_USAGE_RECORDED"
            )
            self.assertEqual(
                rewritten_usage_row[8],
                _event_integrity_sha256(
                    event_id=rewritten_usage_row[0],
                    namespace=rewritten_usage_row[1],
                    campaign_id=rewritten_usage_row[2],
                    cycle_id=rewritten_usage_row[3],
                    aggregate_type=rewritten_usage_row[4],
                    aggregate_id=rewritten_usage_row[5],
                    event_type=rewritten_usage_row[6],
                    payload_json=rewritten_usage_row[7],
                    occurred_at=rewritten_usage_row[9],
                    sequence=rewritten_usage_row[10],
                ),
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "^model usage payload is not canonical$",
            ):
                usage.finish(
                    call_id=call_id,
                    attempt_id=attempt_id,
                    outcome=InvocationOutcome.SUCCESS,
                )

            rows_after = _campaign_full_rows(root, campaign_id=campaign_id)
            self.assertEqual(rows_after, rows_before)
            self.assertEqual(len(rows_after), count_before)
            self.assertEqual(
                tuple((row[0], row[8]) for row in rows_after),
                hashes_before,
            )
            self.assertNotIn(
                "MODEL_USAGE_FINISHED",
                {row[6] for row in rows_after},
            )

    def test_finish_normalizes_currencyless_usage_recovery_error(self) -> None:
        campaign_id = "campaign-usage-currencyless-finish"
        cycle_id = "cycle-001"
        call_id = "call-currencyless-finish"
        attempt_id = "call-currencyless-finish-attempt-001"
        with _authorized_campaign(campaign_id) as (root, _, journal):
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id=cycle_id,
            )
            usage.begin(
                UsageEnvelope(
                    provider="fake-provider",
                    profile="offline",
                    request_model="fake-model",
                    response_model="fake-model",
                    call_id=call_id,
                    attempt_id=attempt_id,
                    usage_status=UsageStatus.REPORTED,
                    input_tokens=None,
                    output_tokens=None,
                    total_tokens=None,
                    cache_read_tokens=None,
                    cache_write_tokens=None,
                    reasoning_tokens=None,
                    reported_cost="0.01",
                    currency="USD",
                    fallback=False,
                    streamed=False,
                    outcome=InvocationOutcome.RESPONSE_RECEIVED,
                    raw_usage_sha256="4" * 64,
                )
            )
            usage_row = next(
                row
                for row in _campaign_full_rows(root, campaign_id=campaign_id)
                if row[6] == "MODEL_USAGE_RECORDED"
            )
            payload = json.loads(usage_row[7])
            self.assertEqual(payload.pop("currency"), "USD")
            _rewrite_campaign_event_payload(
                root,
                event_id=usage_row[0],
                payload=payload,
            )

            rows_before = _campaign_full_rows(root, campaign_id=campaign_id)
            count_before = len(rows_before)
            hashes_before = tuple((row[0], row[8]) for row in rows_before)
            rewritten_usage_row = next(
                row for row in rows_before if row[6] == "MODEL_USAGE_RECORDED"
            )
            self.assertEqual(
                rewritten_usage_row[8],
                _event_integrity_sha256(
                    event_id=rewritten_usage_row[0],
                    namespace=rewritten_usage_row[1],
                    campaign_id=rewritten_usage_row[2],
                    cycle_id=rewritten_usage_row[3],
                    aggregate_type=rewritten_usage_row[4],
                    aggregate_id=rewritten_usage_row[5],
                    event_type=rewritten_usage_row[6],
                    payload_json=rewritten_usage_row[7],
                    occurred_at=rewritten_usage_row[9],
                    sequence=rewritten_usage_row[10],
                ),
            )

            with self.assertRaisesRegex(
                CampaignJournalError,
                "^model usage payload is invalid$",
            ):
                usage.finish(
                    call_id=call_id,
                    attempt_id=attempt_id,
                    outcome=InvocationOutcome.SUCCESS,
                )

            rows_after = _campaign_full_rows(root, campaign_id=campaign_id)
            self.assertEqual(rows_after, rows_before)
            self.assertEqual(len(rows_after), count_before)
            self.assertEqual(
                tuple((row[0], row[8]) for row in rows_after),
                hashes_before,
            )
            self.assertNotIn(
                "MODEL_USAGE_FINISHED",
                {row[6] for row in rows_after},
            )

    def test_call_attempts_replay_in_persisted_order(self) -> None:
        with _authorized_campaign("campaign-usage-list-001") as (_, _, journal):
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            )
            invocation = RetryingModelInvocation(
                attempt=ModelInvocation(
                    provider=_TimeoutThenSuccessProvider(),
                    usage_journal=usage,
                    provider_name="fake-provider",
                    profile="offline",
                    request_model="fake-model",
                ),
                max_attempts=2,
            )

            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-persisted-order",
            )

            attempts = usage.list_attempts(call_id="call-persisted-order")
            self.assertEqual(
                tuple(attempt.envelope.attempt_id for attempt in attempts),
                (
                    "call-persisted-order-attempt-001",
                    "call-persisted-order-attempt-002",
                ),
            )
            self.assertEqual(
                tuple(attempt.final_outcome for attempt in attempts),
                (InvocationOutcome.TIMEOUT, InvocationOutcome.SUCCESS),
            )

    def test_usage_replay_rejects_bool_token_counters(self) -> None:
        campaign_id = "campaign-usage-bool-counter"
        cycle_id = "cycle-001"
        call_id = "call-bool-counter"
        attempt_id = "call-bool-counter-attempt-001"
        aggregate_id = hashlib.sha256(
            f"{cycle_id}\0{call_id}\0{attempt_id}".encode("ascii")
        ).hexdigest()
        event_id = hashlib.sha256(
            (
                f"formal\0{campaign_id}\0{cycle_id}\0{aggregate_id}\0usage"
            ).encode("ascii")
        ).hexdigest()

        with _authorized_campaign(campaign_id) as (_, _, journal):
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id=cycle_id,
            )
            journal.append(
                event_id=event_id,
                cycle_id=cycle_id,
                aggregate_type="MODEL_ATTEMPT",
                aggregate_id=aggregate_id,
                event_type="MODEL_USAGE_RECORDED",
                payload={
                    "provider": "fake-provider",
                    "profile": "offline",
                    "request_model": "fake-model",
                    "response_model": None,
                    "call_id": call_id,
                    "attempt_id": attempt_id,
                    "usage_status": UsageStatus.UNKNOWN.value,
                    "input_tokens": True,
                    "output_tokens": None,
                    "total_tokens": None,
                    "cache_read_tokens": None,
                    "cache_write_tokens": None,
                    "reasoning_tokens": None,
                    "reported_cost": None,
                    "currency": None,
                    "fallback": False,
                    "streamed": False,
                    "outcome": InvocationOutcome.TIMEOUT.value,
                    "raw_usage_sha256": "4" * 64,
                },
            )

            with self.assertRaisesRegex(CampaignJournalError, "payload"):
                usage.list_attempts()

    def test_usage_writer_rejects_bool_token_counters_before_persistence(
        self,
    ) -> None:
        campaign_id = "campaign-usage-writer-bool-counter"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            )
            envelope = UsageEnvelope(
                provider="fake-provider",
                profile="offline",
                request_model="fake-model",
                response_model=None,
                call_id="call-bool-counter",
                attempt_id="call-bool-counter-attempt-001",
                usage_status=UsageStatus.UNKNOWN,
                input_tokens=True,
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

            with self.assertRaisesRegex(ValueError, "envelope"):
                usage.begin(envelope)

            self.assertEqual(
                journal.list_events(
                    cycle_id="cycle-001",
                    aggregate_type="MODEL_ATTEMPT",
                    aggregate_id=hashlib.sha256(
                        b"cycle-001\0call-bool-counter\0"
                        b"call-bool-counter-attempt-001"
                    ).hexdigest(),
                ),
                (),
            )

    def test_usage_writer_rejects_total_below_known_token_components(self) -> None:
        campaign_id = "campaign-usage-inconsistent-total"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            )
            envelope = UsageEnvelope(
                provider="fake-provider",
                profile="offline",
                request_model="fake-model",
                response_model="fake-model",
                call_id="call-inconsistent-total",
                attempt_id="call-inconsistent-total-attempt-001",
                usage_status=UsageStatus.REPORTED,
                input_tokens=3,
                output_tokens=2,
                total_tokens=4,
                cache_read_tokens=None,
                cache_write_tokens=None,
                reasoning_tokens=None,
                reported_cost=None,
                currency=None,
                fallback=False,
                streamed=False,
                outcome=InvocationOutcome.RESPONSE_RECEIVED,
                raw_usage_sha256="4" * 64,
            )

            with self.assertRaisesRegex(ValueError, "envelope"):
                usage.begin(envelope)

            self.assertEqual(
                usage.list_attempts(call_id="call-inconsistent-total"),
                (),
            )

    def test_terminal_usage_cannot_accept_a_later_finish_event(self) -> None:
        with _authorized_campaign("campaign-terminal-usage-finish") as (
            _,
            _,
            journal,
        ):
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            )
            call_id = "call-terminal-timeout"
            attempt_id = "call-terminal-timeout-attempt-001"
            usage.begin(
                UsageEnvelope(
                    provider="fake-provider",
                    profile="offline",
                    request_model="fake-model",
                    response_model=None,
                    call_id=call_id,
                    attempt_id=attempt_id,
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
                "terminal usage",
            ):
                usage.finish(
                    call_id=call_id,
                    attempt_id=attempt_id,
                    outcome=InvocationOutcome.SUCCESS,
                )

            recorded = usage.read_attempt(
                call_id=call_id,
                attempt_id=attempt_id,
            )
            self.assertEqual(
                recorded.final_outcome,
                InvocationOutcome.TIMEOUT,
            )

    def test_success_cannot_be_persisted_as_an_initial_usage_outcome(self) -> None:
        campaign_id = "campaign-usage-initial-success"
        cycle_id = "cycle-001"
        call_id = "call-initial-success"
        attempt_id = "call-initial-success-attempt-001"
        with _authorized_campaign(campaign_id) as (_, _, journal):
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id=cycle_id,
            )
            envelope = UsageEnvelope(
                provider="fake-provider",
                profile="offline",
                request_model="fake-model",
                response_model="fake-model",
                call_id=call_id,
                attempt_id=attempt_id,
                usage_status=UsageStatus.REPORTED,
                input_tokens=1,
                output_tokens=1,
                total_tokens=2,
                cache_read_tokens=None,
                cache_write_tokens=None,
                reasoning_tokens=None,
                reported_cost=None,
                currency=None,
                fallback=False,
                streamed=False,
                outcome=InvocationOutcome.SUCCESS,
                raw_usage_sha256="4" * 64,
            )

            with self.assertRaisesRegex(ValueError, "initial outcome"):
                usage.begin(envelope)

            self.assertEqual(usage.list_attempts(call_id=call_id), ())

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
