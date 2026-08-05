from __future__ import annotations

import hashlib
import json
import sqlite3
import unittest
from dataclasses import replace

from research_automation.control_plane import campaign_context as campaign_context_module
from research_automation.control_plane import campaign_freeze as campaign_freeze_module
from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.campaign_freeze import (
    CycleFreezeConflictError,
    CycleFreezeIntegrityError,
    OperationalCycleFreezeJournal,
)
from research_automation.control_plane.campaign_context import (
    OperationalCycleContextJournal,
)
from research_automation.control_plane.campaign_lifecycle import (
    CampaignStateConflictError,
    CampaignStatus,
    CycleStatus,
    OperationalCampaignLifecycle,
)
from research_automation.control_plane.campaign_roster import (
    OperationalRosterJournal,
    RosterMember,
)
from research_automation.control_plane.campaign_store import _event_integrity_sha256
from research_automation.foundations.protocols import compile_execution_spec
from tests.test_control_plane_campaign_preflight import _scope
from tests.test_control_plane_campaign_roster import (
    _authorized_campaign,
)
from tests.test_foundations_protocols import _approval, _protocol


class _MutatingProposal(dict[str, object]):
    def get(self, key: str, default: object = None) -> object:
        value = super().get(key, default)
        if key == "scope":
            self["hypothesis"] = "Mutated after preflight read"
        return value


def _protocol_member() -> RosterMember:
    return RosterMember(
        member_id="factor-engineer",
        provider="fake-provider",
        profile="offline-local",
        model="deterministic-reviewer",
        role="factor_engineer",
        prompt_sha256="1" * 64,
        config_sha256="2" * 64,
        capability_sha256="3" * 64,
    )


def _prepare_context_ready_cycle(
    journal,
    *,
    cycle_id: str,
    proposal: dict[str, object],
    members: tuple[RosterMember, ...] | None = None,
):
    lifecycle = OperationalCampaignLifecycle(journal=journal)
    lifecycle.activate()
    lifecycle.open_cycle(cycle_id=cycle_id, cycle_number=1)
    lifecycle.advance_cycle(
        cycle_id=cycle_id,
        expected_status=CycleStatus.CREATED,
        next_status=CycleStatus.BUDGET_RESERVED,
    )
    selected_members = (
        (_protocol_member(),) if members is None else members
    )
    context = OperationalCycleContextJournal(
        journal=journal,
        lifecycle=lifecycle,
        repository_root=stores_module._OPERATIONAL_STORE_PATH.parent,
    )
    context.prepare(
        cycle_id=cycle_id,
        proposal=proposal,
        roles=tuple(sorted({member.role for member in selected_members})),
    )
    roster = OperationalRosterJournal(
        journal=journal,
        lifecycle=lifecycle,
    )
    roster_manifest = roster.freeze(
        cycle_id=cycle_id,
        members=selected_members,
    )
    return lifecycle, context, roster, roster_manifest


def _all_operational_table_bytes() -> tuple[bytes, ...]:
    connection = sqlite3.connect(stores_module._OPERATIONAL_STORE_PATH)
    try:
        return tuple(line.encode("utf-8") for line in connection.iterdump())
    finally:
        connection.close()


def _rewrite_event_payload(event, payload: dict[str, object]) -> None:
    payload_json = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    integrity_sha256 = _event_integrity_sha256(
        event_id=event.event_id,
        namespace=event.namespace,
        campaign_id=event.campaign_id,
        cycle_id=event.cycle_id,
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        event_type=event.event_type,
        payload_json=payload_json,
        occurred_at=event.occurred_at.isoformat(),
        sequence=event.sequence,
    )
    connection = sqlite3.connect(stores_module._OPERATIONAL_STORE_PATH)
    try:
        cursor = connection.execute(
            "UPDATE campaign_events SET payload_json = ?, payload_sha256 = ? "
            "WHERE event_id = ?",
            (payload_json, integrity_sha256, event.event_id),
        )
        if cursor.rowcount != 1:
            raise AssertionError("target Campaign event was not rewritten")
        connection.commit()
    finally:
        connection.close()


def _swap_and_resign_event_sequences(first, second) -> None:
    first_integrity = _event_integrity_sha256(
        event_id=first.event_id,
        namespace=first.namespace,
        campaign_id=first.campaign_id,
        cycle_id=first.cycle_id,
        aggregate_type=first.aggregate_type,
        aggregate_id=first.aggregate_id,
        event_type=first.event_type,
        payload_json=first.payload_json,
        occurred_at=first.occurred_at.isoformat(),
        sequence=second.sequence,
    )
    second_integrity = _event_integrity_sha256(
        event_id=second.event_id,
        namespace=second.namespace,
        campaign_id=second.campaign_id,
        cycle_id=second.cycle_id,
        aggregate_type=second.aggregate_type,
        aggregate_id=second.aggregate_id,
        event_type=second.event_type,
        payload_json=second.payload_json,
        occurred_at=second.occurred_at.isoformat(),
        sequence=first.sequence,
    )
    connection = sqlite3.connect(stores_module._OPERATIONAL_STORE_PATH)
    try:
        connection.execute(
            "UPDATE campaign_events SET sequence = -1 WHERE event_id = ?",
            (first.event_id,),
        )
        connection.execute(
            "UPDATE campaign_events SET sequence = ?, payload_sha256 = ? "
            "WHERE event_id = ?",
            (first.sequence, second_integrity, second.event_id),
        )
        connection.execute(
            "UPDATE campaign_events SET sequence = ?, payload_sha256 = ? "
            "WHERE event_id = ?",
            (second.sequence, first_integrity, first.event_id),
        )
        connection.commit()
    finally:
        connection.close()


def _graft_context_proposal_and_resign_freeze_binding(
    journal,
    *,
    cycle_id: str,
    proposal: dict[str, object],
) -> None:
    context_event = journal.list_events(
        cycle_id=cycle_id,
        aggregate_type="CYCLE_SAFE_CONTEXT",
        aggregate_id=cycle_id,
    )[0]
    context_payload = context_event.payload()
    context_payload["proposal"] = proposal
    proposal_text, _ = campaign_context_module._canonical_snapshot(
        proposal,
        "grafted context proposal",
        maximum_bytes=16 * 1024 * 1024,
    )
    context_payload["proposal_sha256"] = (
        campaign_context_module._content_sha256(
            b"control_plane.cycle_context_proposal.v2",
            proposal_text,
        )
    )
    context_identity = {
        key: value
        for key, value in context_payload.items()
        if key
        not in {
            "_authority_grant_id",
            "manifest_sha256",
            "safe_context",
            "projection_input",
            "proposal",
            "untrusted_sources",
        }
    }
    context_payload["manifest_sha256"] = (
        campaign_context_module._content_sha256(
            b"control_plane.cycle_context_receipt.v2",
            campaign_context_module._canonical_snapshot(
                context_identity,
                "grafted context identity",
                maximum_bytes=48 * 1024,
            )[0],
        )
    )
    _rewrite_event_payload(context_event, context_payload)

    freeze_event = journal.list_events(
        cycle_id=cycle_id,
        aggregate_type="CYCLE_INPUT_FREEZE",
        aggregate_id=cycle_id,
    )[0]
    freeze_payload = freeze_event.payload()
    freeze_payload["context_manifest_sha256"] = context_payload[
        "manifest_sha256"
    ]
    freeze_identity = {
        key: value
        for key, value in freeze_payload.items()
        if key not in {"_authority_grant_id", "manifest_sha256"}
    }
    freeze_payload["manifest_sha256"] = campaign_freeze_module._content_sha256(
        campaign_freeze_module._CYCLE_FREEZE_MANIFEST_DOMAIN,
        freeze_identity,
        "grafted Cycle freeze identity",
    )
    _rewrite_event_payload(freeze_event, freeze_payload)


class OperationalCycleFreezeJournalTests(unittest.TestCase):
    def test_first_public_freeze_rejects_full_context_proposal_drift_without_writes(
        self,
    ) -> None:
        campaign_id = "campaign-freeze-context-proposal-drift"
        cycle_id = "cycle-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        context_proposal = {
            "hypothesis": "The context-bound mechanism",
            "scope": _scope(generation="generation-1"),
            "research_note": "original",
        }
        drifted_proposal = {
            **context_proposal,
            "hypothesis": "A different mechanism in the same scope",
            "research_note": "drifted",
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = (
                _prepare_context_ready_cycle(
                    journal,
                    cycle_id=cycle_id,
                    proposal=context_proposal,
                )
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )
            before = _all_operational_table_bytes()

            with self.assertRaisesRegex(
                CycleFreezeConflictError,
                "^safe context proposal conflicts with the proposal$",
            ):
                freeze.freeze(
                    cycle_id=cycle_id,
                    proposal=drifted_proposal,
                    execution_spec=execution_spec,
                    expected_roster=roster_manifest,
                )

            self.assertEqual(_all_operational_table_bytes(), before)
            self.assertEqual(
                lifecycle.cycle_snapshot(cycle_id).status,
                CycleStatus.CONTEXT_READY,
            )

    def test_only_a_complete_frozen_input_manifest_can_freeze_a_cycle(self) -> None:
        campaign_id = "campaign-freeze-001"
        cycle_id = "cycle-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        proposal = {
            "hypothesis": "A bounded offline mechanism",
            "scope": _scope(generation="generation-1"),
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = _prepare_context_ready_cycle(
                journal,
                cycle_id=cycle_id,
                proposal=proposal,
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )

            with self.assertRaises(CampaignStateConflictError):
                lifecycle.advance_cycle(
                    cycle_id=cycle_id,
                    expected_status=CycleStatus.CONTEXT_READY,
                    next_status=CycleStatus.FROZEN,
                )

            frozen = freeze.freeze(
                cycle_id=cycle_id,
                proposal=proposal,
                execution_spec=execution_spec,
                expected_roster=roster_manifest,
            )
            replay = freeze.freeze(
                cycle_id=cycle_id,
                proposal=proposal,
                execution_spec=execution_spec,
                expected_roster=roster_manifest,
            )

            self.assertEqual(replay, frozen)
            self.assertEqual(frozen.generation_id, "generation-1")
            self.assertEqual(
                frozen.generation_manifest_artifact_id,
                protocol.generation_manifest_artifact_id,
            )
            self.assertEqual(
                frozen.roster_manifest_sha256,
                roster_manifest.manifest_sha256,
            )
            self.assertEqual(
                lifecycle.cycle_snapshot(cycle_id).status,
                CycleStatus.FROZEN,
            )
            self.assertEqual(freeze.snapshot(cycle_id=cycle_id), frozen)

            changed = dict(proposal)
            changed["hypothesis"] = "A changed mechanism"
            with self.assertRaises(CycleFreezeConflictError):
                freeze.freeze(
                    cycle_id=cycle_id,
                    proposal=changed,
                    execution_spec=execution_spec,
                    expected_roster=roster_manifest,
                )

            connection = sqlite3.connect(stores_module._OPERATIONAL_STORE_PATH)
            try:
                cursor = connection.execute(
                    "DELETE FROM campaign_events "
                    "WHERE namespace = ? AND campaign_id = ? "
                    "AND cycle_id = ? AND aggregate_type = ? "
                    "AND event_type = ? AND payload_json LIKE ?",
                    (
                        "formal",
                        campaign_id,
                        cycle_id,
                        "CYCLE_STATE",
                        "CYCLE_TRANSITIONED",
                        '%"to_status":"FROZEN"%',
                    ),
                )
                self.assertEqual(cursor.rowcount, 1)
                connection.commit()
            finally:
                connection.close()

            with self.assertRaises(CycleFreezeIntegrityError):
                freeze.snapshot(cycle_id=cycle_id)

    def test_snapshot_rejects_roster_before_context_ready_transition(self) -> None:
        campaign_id = "campaign-freeze-roster-before-context-ready"
        cycle_id = "cycle-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        proposal = {
            "hypothesis": "Roster freeze follows durable context readiness",
            "scope": _scope(generation="generation-1"),
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = (
                _prepare_context_ready_cycle(
                    journal,
                    cycle_id=cycle_id,
                    proposal=proposal,
                )
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )
            freeze.freeze(
                cycle_id=cycle_id,
                proposal=proposal,
                execution_spec=execution_spec,
                expected_roster=roster_manifest,
            )
            context_ready = next(
                event
                for event in journal.list_events(
                    cycle_id=cycle_id,
                    aggregate_type="CYCLE_STATE",
                    aggregate_id=cycle_id,
                )
                if event.payload().get("to_status") == CycleStatus.CONTEXT_READY.value
            )
            roster_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="CYCLE_ROSTER",
                aggregate_id=cycle_id,
            )[0]
            self.assertLess(context_ready.sequence, roster_event.sequence)
            _swap_and_resign_event_sequences(context_ready, roster_event)
            before = _all_operational_table_bytes()

            with self.assertRaisesRegex(
                CycleFreezeIntegrityError,
                "ordering",
            ):
                freeze.snapshot(cycle_id=cycle_id)

            self.assertEqual(_all_operational_table_bytes(), before)

    def test_snapshot_rejects_resigned_arbitrary_preflight_digest(self) -> None:
        campaign_id = "campaign-freeze-arbitrary-preflight-digest"
        cycle_id = "cycle-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        proposal = {
            "hypothesis": "Frozen preflight identities remain reconstructible",
            "scope": _scope(generation="generation-1"),
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = (
                _prepare_context_ready_cycle(
                    journal,
                    cycle_id=cycle_id,
                    proposal=proposal,
                )
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )
            freeze.freeze(
                cycle_id=cycle_id,
                proposal=proposal,
                execution_spec=execution_spec,
                expected_roster=roster_manifest,
            )
            freeze_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="CYCLE_INPUT_FREEZE",
                aggregate_id=cycle_id,
            )[0]
            payload = freeze_event.payload()
            payload["preflight_sha256"] = "0" * 64
            identity = {
                key: value
                for key, value in payload.items()
                if key not in {"_authority_grant_id", "manifest_sha256"}
            }
            payload["manifest_sha256"] = campaign_freeze_module._content_sha256(
                campaign_freeze_module._CYCLE_FREEZE_MANIFEST_DOMAIN,
                identity,
                "resigned Cycle freeze identity",
            )
            _rewrite_event_payload(freeze_event, payload)
            before = _all_operational_table_bytes()

            with self.assertRaisesRegex(
                CycleFreezeIntegrityError,
                "preflight",
            ):
                freeze.snapshot(cycle_id=cycle_id)

            self.assertEqual(_all_operational_table_bytes(), before)

    def test_snapshot_rejects_resigned_preflight_semantic_graft(self) -> None:
        campaign_id = "campaign-freeze-preflight-semantic-graft"
        cycle_id = "cycle-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        proposal = {
            "hypothesis": "Frozen preflight semantics bind durable context",
            "scope": _scope(generation="generation-1"),
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = (
                _prepare_context_ready_cycle(
                    journal,
                    cycle_id=cycle_id,
                    proposal=proposal,
                )
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )
            freeze.freeze(
                cycle_id=cycle_id,
                proposal=proposal,
                execution_spec=execution_spec,
                expected_roster=roster_manifest,
            )
            freeze_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="CYCLE_INPUT_FREEZE",
                aggregate_id=cycle_id,
            )[0]
            payload = freeze_event.payload()
            forged_preflight = json.loads(payload["preflight_json"])
            forged_preflight["learning_verdict"]["warning_codes"] = [
                "FORGED_WARNING"
            ]
            payload["preflight_json"] = json.dumps(
                forged_preflight,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            payload["preflight_sha256"] = campaign_freeze_module._content_sha256(
                b"control_plane.campaign_preflight.v1",
                forged_preflight,
                "forged preflight",
            )
            identity = {
                key: value
                for key, value in payload.items()
                if key not in {"_authority_grant_id", "manifest_sha256"}
            }
            payload["manifest_sha256"] = campaign_freeze_module._content_sha256(
                campaign_freeze_module._CYCLE_FREEZE_MANIFEST_DOMAIN,
                identity,
                "forged Cycle freeze identity",
            )
            _rewrite_event_payload(freeze_event, payload)
            before = _all_operational_table_bytes()

            with self.assertRaisesRegex(
                CycleFreezeIntegrityError,
                "preflight",
            ):
                freeze.snapshot(cycle_id=cycle_id)

            self.assertEqual(_all_operational_table_bytes(), before)

    def test_snapshot_rejects_legacy_v1_freeze_payload(self) -> None:
        campaign_id = "campaign-freeze-legacy-v1"
        cycle_id = "cycle-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        proposal = {
            "hypothesis": "Legacy opaque preflight freezes fail closed",
            "scope": _scope(generation="generation-1"),
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = (
                _prepare_context_ready_cycle(
                    journal,
                    cycle_id=cycle_id,
                    proposal=proposal,
                )
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )
            freeze.freeze(
                cycle_id=cycle_id,
                proposal=proposal,
                execution_spec=execution_spec,
                expected_roster=roster_manifest,
            )
            freeze_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="CYCLE_INPUT_FREEZE",
                aggregate_id=cycle_id,
            )[0]
            payload = freeze_event.payload()
            payload.pop("schema_version")
            payload.pop("execution_spec_json")
            payload.pop("preflight_json")
            identity = {
                key: value
                for key, value in payload.items()
                if key not in {"_authority_grant_id", "manifest_sha256"}
            }
            payload["manifest_sha256"] = campaign_freeze_module._content_sha256(
                b"control_plane.campaign_cycle_freeze.v1",
                identity,
                "legacy Cycle freeze identity",
            )
            _rewrite_event_payload(freeze_event, payload)
            before = _all_operational_table_bytes()

            with self.assertRaises(CycleFreezeIntegrityError):
                freeze.snapshot(cycle_id=cycle_id)

            self.assertEqual(_all_operational_table_bytes(), before)

    def test_snapshot_rejects_a_resigned_context_only_proposal_graft(
        self,
    ) -> None:
        campaign_id = "campaign-freeze-context-only-proposal-graft"
        cycle_id = "cycle-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        proposal_a = {
            "hypothesis": "The originally frozen mechanism",
            "scope": _scope(generation="generation-1"),
        }
        proposal_b = {
            **proposal_a,
            "context_only_graft": "proposal-b",
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = (
                _prepare_context_ready_cycle(
                    journal,
                    cycle_id=cycle_id,
                    proposal=proposal_a,
                )
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )
            frozen = freeze.freeze(
                cycle_id=cycle_id,
                proposal=proposal_a,
                execution_spec=execution_spec,
                expected_roster=roster_manifest,
            )
            _graft_context_proposal_and_resign_freeze_binding(
                journal,
                cycle_id=cycle_id,
                proposal=proposal_b,
            )
            resigned_context = context.snapshot(cycle_id=cycle_id)
            self.assertNotEqual(
                resigned_context.manifest_sha256,
                frozen.context_manifest_sha256,
            )
            freeze_event = journal.list_events(
                cycle_id=cycle_id,
                aggregate_type="CYCLE_INPUT_FREEZE",
                aggregate_id=cycle_id,
            )[0]
            self.assertEqual(
                freeze_event.payload()["proposal_sha256"],
                frozen.proposal_sha256,
            )
            before = _all_operational_table_bytes()

            with self.assertRaisesRegex(
                CycleFreezeIntegrityError,
                "proposal binding",
            ):
                freeze.snapshot(cycle_id=cycle_id)

            self.assertEqual(_all_operational_table_bytes(), before)

    def test_rejected_preflight_cannot_write_or_advance_the_freeze(self) -> None:
        campaign_id = "campaign-freeze-002"
        cycle_id = "cycle-001"
        protocol = _protocol()
        unapproved = compile_execution_spec(
            protocol,
            approved_protocol=None,
            approval=None,
            amendment=None,
        )
        proposal = {
            "hypothesis": "An unapproved protocol cannot run",
            "scope": _scope(generation="generation-1"),
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = _prepare_context_ready_cycle(
                journal,
                cycle_id=cycle_id,
                proposal=proposal,
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )

            with self.assertRaises(CycleFreezeConflictError):
                freeze.freeze(
                    cycle_id=cycle_id,
                    proposal=proposal,
                    execution_spec=unapproved,
                    expected_roster=roster_manifest,
                )

            self.assertEqual(
                journal.list_events(
                    cycle_id=cycle_id,
                    aggregate_type="CYCLE_INPUT_FREEZE",
                    aggregate_id=cycle_id,
                ),
                (),
            )
            self.assertEqual(
                lifecycle.cycle_snapshot(cycle_id).status,
                CycleStatus.CONTEXT_READY,
            )

    def test_freeze_policy_cannot_adopt_a_cycle_already_frozen_by_legacy_path(
        self,
    ) -> None:
        campaign_id = "campaign-freeze-003"
        cycle_id = "cycle-001"
        proposal = {
            "hypothesis": "Legacy freeze history cannot be adopted",
            "scope": _scope(generation="generation-1"),
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, _ = _prepare_context_ready_cycle(
                journal,
                cycle_id=cycle_id,
                proposal=proposal,
            )
            lifecycle.advance_cycle(
                cycle_id=cycle_id,
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )

            with self.assertRaises(CycleFreezeConflictError):
                OperationalCycleFreezeJournal(
                    journal=journal,
                    lifecycle=lifecycle,
                    roster=roster,
                    context=context,
                )

    def test_protocol_and_operational_roster_drift_cannot_freeze(self) -> None:
        campaign_id = "campaign-freeze-004"
        cycle_id = "cycle-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        proposal = {
            "hypothesis": "Roster drift must fail closed",
            "scope": _scope(generation="generation-1"),
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = _prepare_context_ready_cycle(
                journal,
                cycle_id=cycle_id,
                proposal=proposal,
                members=(replace(_protocol_member(), model="wrong-model"),),
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )

            with self.assertRaises(CycleFreezeConflictError):
                freeze.freeze(
                    cycle_id=cycle_id,
                    proposal=proposal,
                    execution_spec=execution_spec,
                    expected_roster=roster_manifest,
                )

            self.assertEqual(
                journal.list_events(
                    cycle_id=cycle_id,
                    aggregate_type="CYCLE_INPUT_FREEZE",
                    aggregate_id=cycle_id,
                ),
                (),
            )
            self.assertEqual(
                lifecycle.cycle_snapshot(cycle_id).status,
                CycleStatus.CONTEXT_READY,
            )

    def test_proposal_is_snapshotted_once_before_preflight_and_hashing(self) -> None:
        campaign_id = "campaign-freeze-005"
        cycle_id = "cycle-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        original = {
            "hypothesis": "Original bounded mechanism",
            "scope": _scope(generation="generation-1"),
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = _prepare_context_ready_cycle(
                journal,
                cycle_id=cycle_id,
                proposal=original,
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )

            frozen = freeze.freeze(
                cycle_id=cycle_id,
                proposal=_MutatingProposal(original),
                execution_spec=execution_spec,
                expected_roster=roster_manifest,
            )
            replay = freeze.freeze(
                cycle_id=cycle_id,
                proposal=original,
                execution_spec=execution_spec,
                expected_roster=roster_manifest,
            )

            self.assertEqual(replay, frozen)

    def test_freeze_event_identity_collision_blocks_the_campaign(self) -> None:
        campaign_id = "campaign-freeze-006"
        cycle_id = "cycle-001"
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        proposal = {
            "hypothesis": "Collision must fail closed",
            "scope": _scope(generation="generation-1"),
        }
        with _authorized_campaign(campaign_id) as (_, journal):
            lifecycle, context, roster, roster_manifest = _prepare_context_ready_cycle(
                journal,
                cycle_id=cycle_id,
                proposal=proposal,
            )
            freeze = OperationalCycleFreezeJournal(
                journal=journal,
                lifecycle=lifecycle,
                roster=roster,
                context=context,
            )
            freeze_event_id = hashlib.sha256(
                b"control_plane.campaign_cycle_freeze_event.v1\0"
                + (
                    f"formal\0{campaign_id}\0CYCLE_INPUT_FREEZE\0"
                    f"{cycle_id}\0freeze"
                ).encode("ascii")
            ).hexdigest()
            journal.append(
                event_id=freeze_event_id,
                cycle_id=cycle_id,
                aggregate_type="FOREIGN_FREEZE_EVENT",
                aggregate_id=cycle_id,
                event_type="FOREIGN_FREEZE_EVENT",
                payload={"collision": True},
            )

            with self.assertRaises(CycleFreezeConflictError):
                freeze.freeze(
                    cycle_id=cycle_id,
                    proposal=proposal,
                    execution_spec=execution_spec,
                    expected_roster=roster_manifest,
                )

            blocked = lifecycle.snapshot()
            self.assertEqual(blocked.status, CampaignStatus.BLOCKED)
            self.assertEqual(
                blocked.block_reason_code,
                "CYCLE_FREEZE_JOURNAL_INVALID",
            )
            self.assertEqual(blocked.block_source_ref, freeze_event_id)


if __name__ == "__main__":
    unittest.main()
