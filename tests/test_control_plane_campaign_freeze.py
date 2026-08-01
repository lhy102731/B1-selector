from __future__ import annotations

import hashlib
import sqlite3
import unittest
from dataclasses import replace

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


class OperationalCycleFreezeJournalTests(unittest.TestCase):
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
