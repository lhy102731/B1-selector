from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.campaign import (
    InvocationOutcome,
    ModelInvocation,
    ProviderResponse,
    UsageEnvelope,
    UsageStatus,
)
from research_automation.control_plane.campaign_lifecycle import (
    CampaignStateConflictError,
    CampaignStatus,
    CycleStatus,
    OperationalCampaignLifecycle,
)
from research_automation.control_plane.campaign_roster import (
    OperationalRosterJournal,
    RosterConflictError,
    RosterDriftError,
    RosterMember,
)
from research_automation.control_plane.campaign_store import (
    OperationalCampaignJournal,
    OperationalUsageJournal,
    campaign_scope_sha256,
)
from research_automation.control_plane.contracts import Actor, Phase, SideEffect


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"
NOW = datetime(2026, 8, 1, 2, 3, 4, tzinfo=timezone.utc)


class _FakeProvider:
    def __init__(self, response_model: str) -> None:
        self._response_model = response_model

    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="ignored-provider-attribution",
            response_model=self._response_model,
            raw_usage={
                "input_tokens": 3,
                "output_tokens": 2,
                "total_tokens": 5,
                "reported_cost": "0.01",
                "currency": "USD",
            },
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
            actor = Actor("p6-runner", "automation", f"{campaign_id}-roster")
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


def _member(member_id: str, *, model: str) -> RosterMember:
    return RosterMember(
        member_id=member_id,
        provider="fake-provider",
        profile="offline",
        model=model,
        role=f"role-{member_id}",
        prompt_sha256="1" * 64,
        config_sha256="2" * 64,
        capability_sha256="3" * 64,
    )


def _record_response(
    *,
    journal: OperationalCampaignJournal,
    member: RosterMember,
    response_model: str,
    call_id: str,
) -> OperationalUsageJournal:
    usage = OperationalUsageJournal(journal=journal, cycle_id="cycle-001")
    invocation = ModelInvocation(
        provider=_FakeProvider(response_model),
        usage_journal=usage,
        provider_name=member.provider,
        profile=member.profile,
        request_model=member.model,
    )
    invocation.invoke_json(
        {"prompt": "offline-only"},
        call_id=call_id,
        attempt_id=f"{call_id}-attempt-001",
    )
    return usage


class OperationalRosterJournalTests(unittest.TestCase):
    def test_roster_freeze_is_order_independent_and_drift_conflicts(self) -> None:
        campaign_id = "campaign-roster-001"
        with _authorized_campaign(campaign_id) as (grant, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CREATED,
                next_status=CycleStatus.BUDGET_RESERVED,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.BUDGET_RESERVED,
                next_status=CycleStatus.CONTEXT_READY,
            )
            alpha = _member("alpha", model="fake-model-a")
            beta = _member("beta", model="fake-model-b")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )

            frozen = roster.freeze(
                cycle_id="cycle-001",
                members=(beta, alpha),
            )
            reopened_journal = OperationalCampaignJournal(
                root_secret=ROOT_SECRET,
                grant=grant,
                namespace="formal",
                campaign_id=campaign_id,
                clock=lambda: NOW,
            )
            reopened = OperationalRosterJournal(
                journal=reopened_journal,
                lifecycle=OperationalCampaignLifecycle(
                    journal=reopened_journal,
                ),
            )
            replay = reopened.freeze(
                cycle_id="cycle-001",
                members=(alpha, beta),
            )

            self.assertEqual(replay, frozen)
            self.assertEqual(
                tuple(member.member_id for member in frozen.members),
                ("alpha", "beta"),
            )
            with self.assertRaises(RosterConflictError):
                reopened.freeze(
                    cycle_id="cycle-001",
                    members=(alpha, _member("beta", model="drifted-model")),
                )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_ROSTER",
                aggregate_id="cycle-001",
            )
            self.assertEqual(len(events), 1)

    def test_response_model_drift_atomically_blocks_campaign(self) -> None:
        campaign_id = "campaign-roster-002"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            transitions = (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            )
            for expected, next_status in transitions:
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(member,))
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            usage = _record_response(
                journal=journal,
                member=member,
                response_model="drifted-model",
                call_id="call-alpha",
            )

            with self.assertRaises(RosterDriftError):
                roster.verify_response(
                    cycle_id="cycle-001",
                    member_id="alpha",
                    usage_journal=usage,
                    call_id="call-alpha",
                    attempt_id="call-alpha-attempt-001",
                )

            blocked = lifecycle.snapshot()
            roster_snapshot = roster.snapshot(cycle_id="cycle-001")
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(
                roster_snapshot.terminal_event_type,
                "ROSTER_DRIFT_DETECTED",
            )
            self.assertEqual(
                roster_snapshot.terminal_event_id,
                blocked.block_source_ref,
            )
            self.assertEqual(
                blocked.block_reason_code,
                "RESPONSE_MODEL_DRIFT",
            )
            with self.assertRaises(CampaignStateConflictError):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=CycleStatus.EXECUTING,
                    next_status=CycleStatus.EVIDENCE_READY,
                )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_ROSTER",
                aggregate_id="cycle-001",
            )
            self.assertEqual(
                tuple(event.event_type for event in events),
                ("ROSTER_FROZEN", "ROSTER_DRIFT_DETECTED"),
            )

    def test_missing_required_member_blocks_campaign(self) -> None:
        campaign_id = "campaign-roster-003"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            alpha = _member("alpha", model="fake-model-a")
            beta = _member("beta", model="fake-model-b")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(alpha, beta))
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            usage = _record_response(
                journal=journal,
                member=alpha,
                response_model=alpha.model,
                call_id="call-alpha",
            )
            roster.verify_response(
                cycle_id="cycle-001",
                member_id="alpha",
                usage_journal=usage,
                call_id="call-alpha",
                attempt_id="call-alpha-attempt-001",
            )

            with self.assertRaises(RosterDriftError):
                roster.complete_responses(cycle_id="cycle-001")

            blocked = lifecycle.snapshot()
            roster_snapshot = roster.snapshot(cycle_id="cycle-001")
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(
                blocked.block_reason_code,
                "REQUIRED_MEMBER_MISSING",
            )
            self.assertEqual(
                roster_snapshot.terminal_event_type,
                "ROSTER_DRIFT_DETECTED",
            )
            self.assertEqual(
                roster_snapshot.terminal_event_id,
                blocked.block_source_ref,
            )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_ROSTER",
                aggregate_id="cycle-001",
            )
            self.assertEqual(
                tuple(event.event_type for event in events),
                (
                    "ROSTER_FROZEN",
                    "ROSTER_RESPONSE_VERIFIED",
                    "ROSTER_DRIFT_DETECTED",
                ),
            )
            self.assertEqual(
                events[-1].payload()["missing_member_ids"],
                ["beta"],
            )

    def test_complete_roster_responses_reopen_idempotently(self) -> None:
        campaign_id = "campaign-roster-004"
        with _authorized_campaign(campaign_id) as (grant, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            members = (
                _member("alpha", model="fake-model-a"),
                _member("beta", model="fake-model-b"),
            )
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=members)
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            for member in reversed(members):
                call_id = f"call-{member.member_id}"
                usage = _record_response(
                    journal=journal,
                    member=member,
                    response_model=member.model,
                    call_id=call_id,
                )
                roster.verify_response(
                    cycle_id="cycle-001",
                    member_id=member.member_id,
                    usage_journal=usage,
                    call_id=call_id,
                    attempt_id=f"{call_id}-attempt-001",
                )
            completed = roster.complete_responses(cycle_id="cycle-001")

            reopened_journal = OperationalCampaignJournal(
                root_secret=ROOT_SECRET,
                grant=grant,
                namespace="formal",
                campaign_id=campaign_id,
                clock=lambda: NOW,
            )
            reopened = OperationalRosterJournal(
                journal=reopened_journal,
                lifecycle=OperationalCampaignLifecycle(
                    journal=reopened_journal,
                ),
            )
            replay = reopened.complete_responses(cycle_id="cycle-001")
            roster_snapshot = reopened.snapshot(cycle_id="cycle-001")

            self.assertEqual(replay, completed)
            self.assertEqual(completed.member_ids, ("alpha", "beta"))
            self.assertEqual(
                roster_snapshot.verified_member_ids,
                ("alpha", "beta"),
            )
            self.assertEqual(
                roster_snapshot.terminal_event_type,
                "ROSTER_RESPONSES_COMPLETED",
            )
            self.assertEqual(
                roster_snapshot.terminal_event_id,
                completed.event_id,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.EXECUTING,
                next_status=CycleStatus.EVIDENCE_READY,
            )
            events = journal.list_events(
                cycle_id="cycle-001",
                aggregate_type="CYCLE_ROSTER",
                aggregate_id="cycle-001",
            )
            self.assertEqual(
                tuple(event.event_type for event in events),
                (
                    "ROSTER_FROZEN",
                    "ROSTER_RESPONSE_VERIFIED",
                    "ROSTER_RESPONSE_VERIFIED",
                    "ROSTER_RESPONSES_COMPLETED",
                ),
            )

    def test_malformed_drift_event_cannot_prevent_campaign_block(self) -> None:
        campaign_id = "campaign-roster-005"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(member,))
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            poisoned_event_id = hashlib.sha256(
                b"control_plane.campaign_roster_event.v1\0"
                + f"formal\0{campaign_id}\0cycle-001\0drift".encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="CYCLE_ROSTER",
                aggregate_id="cycle-001",
                event_type="ROSTER_DRIFT_DETECTED",
                payload={"malformed": True},
            )
            usage = _record_response(
                journal=journal,
                member=member,
                response_model="drifted-model",
                call_id="call-alpha",
            )

            with self.assertRaises(RosterDriftError):
                roster.verify_response(
                    cycle_id="cycle-001",
                    member_id="alpha",
                    usage_journal=usage,
                    call_id="call-alpha",
                    attempt_id="call-alpha-attempt-001",
                )

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)

    def test_snapshot_blocks_campaign_before_reporting_invalid_tail(self) -> None:
        campaign_id = "campaign-roster-006"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(
                cycle_id="cycle-001",
                members=(_member("alpha", model="fake-model-a"),),
            )
            poisoned_event_id = "poisoned-roster-tail"
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="CYCLE_ROSTER",
                aggregate_id="cycle-001",
                event_type="UNKNOWN_ROSTER_EVENT",
                payload={"malformed": True},
            )

            with self.assertRaises(RosterDriftError):
                roster.snapshot(cycle_id="cycle-001")

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)

    def test_freeze_replay_does_not_ignore_invalid_tail(self) -> None:
        campaign_id = "campaign-roster-007"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(member,))
            poisoned_event_id = "poisoned-freeze-replay-tail"
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="CYCLE_ROSTER",
                aggregate_id="cycle-001",
                event_type="UNKNOWN_ROSTER_EVENT",
                payload={"malformed": True},
            )

            with self.assertRaises(RosterDriftError):
                roster.freeze(cycle_id="cycle-001", members=(member,))

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)

    def test_missing_persisted_attempt_in_verified_event_blocks_campaign(self) -> None:
        campaign_id = "campaign-roster-008"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            manifest = roster.freeze(
                cycle_id="cycle-001",
                members=(member,),
            )
            poisoned_event_id = hashlib.sha256(
                b"control_plane.campaign_roster_event.v1\0"
                + f"formal\0{campaign_id}\0cycle-001\0verified:alpha".encode(
                    "ascii"
                )
            ).hexdigest()
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="CYCLE_ROSTER",
                aggregate_id="cycle-001",
                event_type="ROSTER_RESPONSE_VERIFIED",
                payload={
                    "cycle_id": "cycle-001",
                    "manifest_sha256": manifest.manifest_sha256,
                    "member_id": "alpha",
                    "attempt": {
                        "call_id": "missing-call",
                        "attempt_id": "missing-attempt",
                    },
                },
            )

            with self.assertRaises(RosterDriftError):
                roster.snapshot(cycle_id="cycle-001")

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)

    def test_verified_member_idempotency_is_bound_to_exact_attempt(self) -> None:
        campaign_id = "campaign-roster-009"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(member,))
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            first_usage = _record_response(
                journal=journal,
                member=member,
                response_model=member.model,
                call_id="call-alpha-first",
            )
            first = roster.verify_response(
                cycle_id="cycle-001",
                member_id="alpha",
                usage_journal=first_usage,
                call_id="call-alpha-first",
                attempt_id="call-alpha-first-attempt-001",
            )

            replay = roster.verify_response(
                cycle_id="cycle-001",
                member_id="alpha",
                usage_journal=first_usage,
                call_id="call-alpha-first",
                attempt_id="call-alpha-first-attempt-001",
            )
            self.assertEqual(replay, first)

            second_usage = _record_response(
                journal=journal,
                member=member,
                response_model=member.model,
                call_id="call-alpha-second",
            )
            with self.assertRaises(RosterConflictError):
                roster.verify_response(
                    cycle_id="cycle-001",
                    member_id="alpha",
                    usage_journal=second_usage,
                    call_id="call-alpha-second",
                    attempt_id="call-alpha-second-attempt-001",
                )

    def test_campaign_block_replay_requires_exact_provenance(self) -> None:
        campaign_id = "campaign-roster-010"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()

            blocked = lifecycle.block(
                reason_code="ROSTER_JOURNAL_INVALID",
                source_ref="poisoned-roster-event",
            )
            replay = lifecycle.block(
                reason_code="ROSTER_JOURNAL_INVALID",
                source_ref="poisoned-roster-event",
            )

            self.assertEqual(replay, blocked)
            with self.assertRaises(CampaignStateConflictError):
                lifecycle.block(
                    reason_code="RESPONSE_MODEL_DRIFT",
                    source_ref="different-roster-event",
                )
            self.assertEqual(lifecycle.snapshot(), blocked)

    def test_post_terminal_roster_event_blocks_campaign(self) -> None:
        campaign_id = "campaign-roster-011"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            manifest = roster.freeze(
                cycle_id="cycle-001",
                members=(member,),
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            usage = _record_response(
                journal=journal,
                member=member,
                response_model=member.model,
                call_id="call-alpha",
            )
            roster.verify_response(
                cycle_id="cycle-001",
                member_id="alpha",
                usage_journal=usage,
                call_id="call-alpha",
                attempt_id="call-alpha-attempt-001",
            )
            roster.complete_responses(cycle_id="cycle-001")
            poisoned_event_id = "aliased-second-roster-terminal"
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="CYCLE_ROSTER",
                aggregate_id="cycle-001",
                event_type="ROSTER_RESPONSES_COMPLETED",
                payload={
                    "cycle_id": "cycle-001",
                    "manifest_sha256": manifest.manifest_sha256,
                    "member_ids": ["alpha"],
                },
            )

            with self.assertRaises(RosterDriftError):
                roster.snapshot(cycle_id="cycle-001")

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)

    def test_unknown_model_attempt_tail_blocks_before_roster_verification(self) -> None:
        campaign_id = "campaign-roster-012"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(member,))
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            usage = _record_response(
                journal=journal,
                member=member,
                response_model=member.model,
                call_id="call-alpha",
            )
            aggregate_id = hashlib.sha256(
                b"cycle-001\0call-alpha\0call-alpha-attempt-001"
            ).hexdigest()
            poisoned_event_id = "poisoned-model-attempt-tail"
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="MODEL_ATTEMPT",
                aggregate_id=aggregate_id,
                event_type="UNKNOWN_MODEL_ATTEMPT_EVENT",
                payload={"malformed": True},
            )

            with self.assertRaises(RosterDriftError):
                roster.verify_response(
                    cycle_id="cycle-001",
                    member_id="alpha",
                    usage_journal=usage,
                    call_id="call-alpha",
                    attempt_id="call-alpha-attempt-001",
                )

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)
            self.assertEqual(
                roster.snapshot(cycle_id="cycle-001").verified_member_ids,
                (),
            )

    def test_off_aggregate_drift_id_poison_blocks_campaign(self) -> None:
        campaign_id = "campaign-roster-013"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(member,))
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            poisoned_event_id = hashlib.sha256(
                b"control_plane.campaign_roster_event.v1\0"
                + f"formal\0{campaign_id}\0cycle-001\0drift".encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="POISONED_RESERVED_ID",
                aggregate_id="off-roster-aggregate",
                event_type="POISONED_RESERVED_ID",
                payload={"malformed": True},
            )
            usage = _record_response(
                journal=journal,
                member=member,
                response_model="drifted-model",
                call_id="call-alpha",
            )

            with self.assertRaises(RosterDriftError):
                roster.verify_response(
                    cycle_id="cycle-001",
                    member_id="alpha",
                    usage_journal=usage,
                    call_id="call-alpha",
                    attempt_id="call-alpha-attempt-001",
                )

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)

    def test_off_aggregate_campaign_block_id_poison_cannot_leave_active(self) -> None:
        campaign_id = "campaign-roster-014"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            poisoned_event_id = hashlib.sha256(
                b"control_plane.campaign_lifecycle_event.v1\0"
                + (
                    f"formal\0{campaign_id}\0CAMPAIGN_STATE\0"
                    f"{campaign_id}\0BLOCKED"
                ).encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=poisoned_event_id,
                cycle_id=None,
                aggregate_type="POISONED_RESERVED_ID",
                aggregate_id="off-campaign-state-aggregate",
                event_type="POISONED_RESERVED_ID",
                payload={"malformed": True},
            )

            blocked = lifecycle.block(
                reason_code="ROSTER_JOURNAL_INVALID",
                source_ref="poisoned-roster-event",
            )

            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(blocked.block_source_ref, "poisoned-roster-event")
            self.assertEqual(lifecycle.snapshot(), blocked)

    def test_off_aggregate_verified_id_poison_uses_shared_seam(self) -> None:
        campaign_id = "campaign-roster-015"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(member,))
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            poisoned_event_id = hashlib.sha256(
                b"control_plane.campaign_roster_event.v1\0"
                + (
                    f"formal\0{campaign_id}\0cycle-001\0verified:alpha"
                ).encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=poisoned_event_id,
                cycle_id="cycle-001",
                aggregate_type="POISONED_RESERVED_ID",
                aggregate_id="off-roster-aggregate",
                event_type="POISONED_RESERVED_ID",
                payload={"malformed": True},
            )
            usage = _record_response(
                journal=journal,
                member=member,
                response_model=member.model,
                call_id="call-alpha",
            )

            with self.assertRaises(RosterDriftError):
                roster.verify_response(
                    cycle_id="cycle-001",
                    member_id="alpha",
                    usage_journal=usage,
                    call_id="call-alpha",
                    attempt_id="call-alpha-attempt-001",
                )

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(blocked.block_source_ref, poisoned_event_id)

    def test_one_event_success_attempt_cannot_verify_roster_member(self) -> None:
        campaign_id = "campaign-roster-016"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(member,))
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            )
            aggregate_id = hashlib.sha256(
                b"cycle-001\0call-alpha\0call-alpha-attempt-001"
            ).hexdigest()
            usage_event_id = hashlib.sha256(
                (
                    f"formal\0{campaign_id}\0cycle-001\0{aggregate_id}\0usage"
                ).encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=usage_event_id,
                cycle_id="cycle-001",
                aggregate_type="MODEL_ATTEMPT",
                aggregate_id=aggregate_id,
                event_type="MODEL_USAGE_RECORDED",
                payload={
                    "provider": member.provider,
                    "profile": member.profile,
                    "request_model": member.model,
                    "response_model": member.model,
                    "call_id": "call-alpha",
                    "attempt_id": "call-alpha-attempt-001",
                    "usage_status": UsageStatus.REPORTED.value,
                    "input_tokens": 3,
                    "output_tokens": 2,
                    "total_tokens": 5,
                    "cache_read_tokens": None,
                    "cache_write_tokens": None,
                    "reasoning_tokens": None,
                    "reported_cost": "0.01",
                    "currency": "USD",
                    "fallback": False,
                    "streamed": False,
                    "outcome": InvocationOutcome.SUCCESS.value,
                    "raw_usage_sha256": "4" * 64,
                },
            )

            with self.assertRaises(RosterDriftError):
                roster.verify_response(
                    cycle_id="cycle-001",
                    member_id="alpha",
                    usage_journal=usage,
                    call_id="call-alpha",
                    attempt_id="call-alpha-attempt-001",
                )

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(
                roster.snapshot(cycle_id="cycle-001").verified_member_ids,
                (),
            )

    def test_predictable_block_recovery_chain_cannot_be_exhausted(self) -> None:
        campaign_id = "campaign-roster-017"
        reason_code = "ROSTER_JOURNAL_INVALID"
        source_ref = "poisoned-roster-event"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()

            def state_event_id(role: str) -> str:
                return hashlib.sha256(
                    b"control_plane.campaign_lifecycle_event.v1\0"
                    + (
                        f"formal\0{campaign_id}\0CAMPAIGN_STATE\0"
                        f"{campaign_id}\0{role}"
                    ).encode("ascii")
                ).hexdigest()

            event_id = state_event_id("BLOCKED")
            collisions: list[tuple[str, str]] = []
            for index in range(65):
                poisoned = journal.append(
                    event_id=event_id,
                    cycle_id=None,
                    aggregate_type="POISONED_RESERVED_ID",
                    aggregate_id=f"off-campaign-state-{index}",
                    event_type="POISONED_RESERVED_ID",
                    payload={"collision_index": index},
                )
                collisions.append(
                    (poisoned.event_id, poisoned.payload_sha256)
                )
                binding = hashlib.sha256(
                    b"control_plane.campaign_block_collision.v1\0"
                    + "\0".join(
                        (
                            reason_code,
                            source_ref,
                            *(
                                value
                                for collision in collisions
                                for value in collision
                            ),
                        )
                    ).encode("ascii")
                ).hexdigest()
                event_id = state_event_id(f"BLOCKED_RECOVERY:{binding}")

            blocked = lifecycle.block(
                reason_code=reason_code,
                source_ref=source_ref,
            )

            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, reason_code)
            self.assertEqual(blocked.block_source_ref, source_ref)
            self.assertEqual(lifecycle.snapshot(), blocked)

    def test_terminal_begin_cannot_be_rewritten_by_success_finish(self) -> None:
        campaign_id = "campaign-roster-018"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(member,))
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            )
            usage.begin(
                UsageEnvelope(
                    provider=member.provider,
                    profile=member.profile,
                    request_model=member.model,
                    response_model=member.model,
                    call_id="call-alpha",
                    attempt_id="call-alpha-attempt-001",
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
            aggregate_id = hashlib.sha256(
                b"cycle-001\0call-alpha\0call-alpha-attempt-001"
            ).hexdigest()
            finish_event_id = hashlib.sha256(
                (
                    f"formal\0{campaign_id}\0cycle-001\0{aggregate_id}\0finish"
                ).encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=finish_event_id,
                cycle_id="cycle-001",
                aggregate_type="MODEL_ATTEMPT",
                aggregate_id=aggregate_id,
                event_type="MODEL_USAGE_FINISHED",
                payload={
                    "call_id": "call-alpha",
                    "attempt_id": "call-alpha-attempt-001",
                    "outcome": InvocationOutcome.SUCCESS.value,
                },
            )

            with self.assertRaises(RosterDriftError):
                roster.verify_response(
                    cycle_id="cycle-001",
                    member_id="alpha",
                    usage_journal=usage,
                    call_id="call-alpha",
                    attempt_id="call-alpha-attempt-001",
                )

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(
                roster.snapshot(cycle_id="cycle-001").verified_member_ids,
                (),
            )

    def test_streamed_attempt_cannot_be_finished_as_success(self) -> None:
        campaign_id = "campaign-roster-019"
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle = OperationalCampaignLifecycle(journal=journal)
            lifecycle.activate()
            lifecycle.open_cycle(cycle_id="cycle-001", cycle_number=1)
            for expected, next_status in (
                (CycleStatus.CREATED, CycleStatus.BUDGET_RESERVED),
                (CycleStatus.BUDGET_RESERVED, CycleStatus.CONTEXT_READY),
            ):
                lifecycle.advance_cycle(
                    cycle_id="cycle-001",
                    expected_status=expected,
                    next_status=next_status,
                )
            member = _member("alpha", model="fake-model-a")
            roster = OperationalRosterJournal(
                journal=journal,
                lifecycle=lifecycle,
            )
            roster.freeze(cycle_id="cycle-001", members=(member,))
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            lifecycle.advance_cycle(
                cycle_id="cycle-001",
                expected_status=CycleStatus.FROZEN,
                next_status=CycleStatus.EXECUTING,
            )
            usage = OperationalUsageJournal(
                journal=journal,
                cycle_id="cycle-001",
            )
            usage.begin(
                UsageEnvelope(
                    provider=member.provider,
                    profile=member.profile,
                    request_model=member.model,
                    response_model=member.model,
                    call_id="call-alpha",
                    attempt_id="call-alpha-attempt-001",
                    usage_status=UsageStatus.REPORTED,
                    input_tokens=3,
                    output_tokens=2,
                    total_tokens=5,
                    cache_read_tokens=None,
                    cache_write_tokens=None,
                    reasoning_tokens=None,
                    reported_cost="0.01",
                    currency="USD",
                    fallback=False,
                    streamed=True,
                    outcome=InvocationOutcome.RESPONSE_RECEIVED,
                    raw_usage_sha256="4" * 64,
                )
            )
            aggregate_id = hashlib.sha256(
                b"cycle-001\0call-alpha\0call-alpha-attempt-001"
            ).hexdigest()
            finish_event_id = hashlib.sha256(
                (
                    f"formal\0{campaign_id}\0cycle-001\0{aggregate_id}\0finish"
                ).encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=finish_event_id,
                cycle_id="cycle-001",
                aggregate_type="MODEL_ATTEMPT",
                aggregate_id=aggregate_id,
                event_type="MODEL_USAGE_FINISHED",
                payload={
                    "call_id": "call-alpha",
                    "attempt_id": "call-alpha-attempt-001",
                    "outcome": InvocationOutcome.SUCCESS.value,
                },
            )

            with self.assertRaises(RosterDriftError):
                roster.verify_response(
                    cycle_id="cycle-001",
                    member_id="alpha",
                    usage_journal=usage,
                    call_id="call-alpha",
                    attempt_id="call-alpha-attempt-001",
                )

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(blocked.block_reason_code, "ROSTER_JOURNAL_INVALID")
            self.assertEqual(
                roster.snapshot(cycle_id="cycle-001").verified_member_ids,
                (),
            )


if __name__ == "__main__":
    unittest.main()
