from __future__ import annotations

from dataclasses import replace
import json
import unittest
from unittest.mock import patch

from research_automation.control_plane.campaign_controller import (
    CampaignBudgetLimits,
    CampaignJournalError,
    CycleReservationLimits,
    OperationalCampaignController,
    operational_prompt_sha256,
)
from research_automation.control_plane.campaign_lifecycle import (
    CampaignStatus,
    CycleStatus,
)
from research_automation.control_plane.campaign_store import (
    CampaignLearningCommitSink,
)
from research_automation.control_plane.evidence_learning import (
    EvidenceAdapter,
    LearningCommitService,
)
from research_automation.control_plane.memory import (
    CommittedLearningLedgerReader,
    ContextProjection,
)
from research_automation.control_plane.campaign_lease import ProcessIdentity
from research_automation.foundations.protocols import compile_execution_spec
from research_automation.task_queue import ExperimentTask
from tests.test_control_plane_campaign_controller import (
    _AuthorityEvidenceArtifactBoundFakeProvider,
    _EvidenceArtifactBoundFakeProvider,
    _FAKE_CALL_LIMITS,
    _FakeMonotonicClock,
)
from tests.test_control_plane_campaign_freeze import _protocol_member
from tests.test_control_plane_campaign_lease import _FakeProcessIdentityProvider
from tests.test_control_plane_campaign_preflight import _scope
from tests.test_control_plane_campaign_store import _authorized_campaign
from tests import test_control_plane_evidence_learning as evidence_fixtures
from tests.test_foundations_protocols import _approval, _protocol


_BUDGET_LIMITS = CampaignBudgetLimits(
    max_cycles=2,
    max_input_tokens=200,
    max_output_tokens=100,
    max_cost="2",
    max_wall_time_ms=200,
    max_tool_attempts=4,
)
_RESERVATION_LIMITS = CycleReservationLimits(
    max_input_tokens=20,
    max_output_tokens=10,
    max_cost="0.1",
    max_wall_time_ms=10,
    max_tool_attempts=2,
)


def _execution_spec_and_member(prompt: object):
    protocol = _protocol()
    execution_spec = compile_execution_spec(
        protocol,
        approved_protocol=protocol,
        approval=_approval(protocol),
        amendment=None,
    )
    member = replace(
        _protocol_member(),
        prompt_sha256=operational_prompt_sha256(prompt),
    )
    return execution_spec, member


class OfflineTwoCycleProofTests(unittest.TestCase):
    def test_committed_cycle_one_learning_enters_recovered_cycle_two_context(
        self,
    ) -> None:
        campaign_id = "campaign-controller-offline-two-cycle-proof"
        first_owner = ProcessIdentity("host-two-cycle", 201, 201_000)
        first_identity_provider = _FakeProcessIdentityProvider(first_owner)
        claim_scope = _scope(generation="generation-1")
        claim_summary = "Synthetic scoped finding from cycle one"
        claim = {
            "kind": "NEGATIVE",
            "summary": claim_summary,
            "scope": json.dumps(
                claim_scope,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "parent_lineage": [],
            "reopen_predicate": "[]",
            "future_usage_guidance": (
                '{"conclusion":"AVOID","directional_status":"avoid"}'
            ),
        }
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, binding, artifact, expected_evidence, _ = (
                evidence_fixtures.EvidenceLearningVerticalSliceTests()
                ._authority_fixture(root, claim=claim)
            )
            authority_reader = patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            )
            authority_reader.start()
            self.addCleanup(authority_reader.stop)
            first_prompt = {
                "instruction": "Return the authority-bound synthetic artifact"
            }
            first_spec, first_member = _execution_spec_and_member(first_prompt)
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=_BUDGET_LIMITS,
                identity_provider=first_identity_provider,
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            first_prepared = controller.prepare_cycle(
                task=ExperimentTask(
                    task_id="cycle-001",
                    strategy="b1",
                    proposal={
                        "hypothesis": claim_summary,
                        "scope": claim_scope,
                    },
                    source="synthetic-test",
                ),
                cycle_number=1,
                execution_spec=first_spec,
                roster_members=(first_member,),
                reservation_limits=_RESERVATION_LIMITS,
            )
            first_execution = controller.start_execution(
                cycle_id=first_prepared.cycle_id,
                acquisition_id="execute-cycle-001",
            )
            controller.invoke_member_json(
                execution=first_execution,
                member_id=first_member.member_id,
                provider=_AuthorityEvidenceArtifactBoundFakeProvider(artifact),
                prompt=first_prompt,
                limits=_FAKE_CALL_LIMITS,
            )
            first_usage = controller.complete_model_execution(
                execution=first_execution
            )
            first_evidence = controller.record_model_evidence(
                execution=first_execution,
                member_id=first_member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol=artifact["executed_protocol"],
                    approved_claim=artifact["claim"],
                ),
            )
            service = LearningCommitService(repository_root=root)
            first_learning = controller.commit_learning(
                execution=first_execution,
                evidence_receipt=first_evidence,
                authority_task_report=report,
                learning_commit_sink=CampaignLearningCommitSink(
                    journal=journal,
                    service=service,
                ),
            )
            first_settlement = controller.settle_cycle(
                execution=first_execution,
                execution_usage=first_usage,
                learning_commit_receipt=first_learning,
            )
            first_information_gain = controller.record_information_gain(
                execution=first_execution,
                settlement_receipt=first_settlement,
            )
            first_decision = controller.decide_next_cycle(
                execution=first_execution,
                information_gain_receipt=first_information_gain,
            )

            self.assertEqual(first_evidence.evidence, expected_evidence)
            self.assertEqual(first_decision.decision, "CONTINUE")
            ledger_claims = CommittedLearningLedgerReader(root).read_claims()
            self.assertEqual(
                [committed_claim["claim_id"] for committed_claim in ledger_claims],
                [first_learning.packet_hash],
            )

            second_owner = ProcessIdentity("host-two-cycle", 202, 202_000)
            second_identity_provider = _FakeProcessIdentityProvider(
                second_owner,
                process_starts={
                    (first_owner.host_id, first_owner.pid): None,
                },
            )
            recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=_BUDGET_LIMITS,
                identity_provider=second_identity_provider,
                monotonic_ns=_FakeMonotonicClock(
                    3_000_000,
                    4_000_000,
                    5_000_000,
                ),
            )
            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                recovered.decide_next_cycle(execution=first_execution)
            replayed_first_decision = recovered.replay_next_cycle_decision(
                cycle_id="cycle-001",
            )
            second_prompt = {
                "instruction": "Return a no-material synthetic artifact"
            }
            second_spec, second_member = _execution_spec_and_member(
                second_prompt
            )
            second_prepared = recovered.prepare_cycle(
                task=ExperimentTask(
                    task_id="cycle-002",
                    strategy="b1",
                    proposal={
                        "hypothesis": "Alternative bounded synthetic mechanism",
                        "scope": claim_scope,
                    },
                    source="synthetic-test",
                ),
                cycle_number=2,
                execution_spec=second_spec,
                roster_members=(second_member,),
                reservation_limits=_RESERVATION_LIMITS,
            )
            second_messages = second_prepared.context.messages_for(
                second_member.role
            )
            trusted_context = json.loads(
                second_messages["system_message"]["content"]
            )
            projected_claims = trusted_context["learning_memory"]["claims"]

            self.assertEqual(replayed_first_decision, first_decision)
            self.assertEqual(
                projected_claims,
                ContextProjection().project(ledger_claims)["claims"],
            )
            self.assertNotIn(
                first_learning.packet_hash,
                json.dumps(second_messages["untrusted_messages"]),
            )

            second_execution = recovered.start_execution(
                cycle_id=second_prepared.cycle_id,
                acquisition_id="execute-cycle-002",
            )
            recovered.invoke_member_json(
                execution=second_execution,
                member_id=second_member.member_id,
                provider=_EvidenceArtifactBoundFakeProvider(),
                prompt=second_prompt,
                limits=_FAKE_CALL_LIMITS,
            )
            second_usage = recovered.complete_model_execution(
                execution=second_execution
            )
            second_evidence = recovered.record_model_evidence(
                execution=second_execution,
                member_id=second_member.member_id,
                evidence_adapter=EvidenceAdapter(
                    known_runners={"fixture-runner": "1.0.0"},
                    approved_protocol={"label": "synthetic-only"},
                ),
            )
            second_settlement = recovered.settle_cycle_without_learning(
                execution=second_execution,
                execution_usage=second_usage,
                evidence_receipt=second_evidence,
            )
            second_information_gain = recovered.record_information_gain(
                execution=second_execution,
                settlement_receipt=second_settlement,
            )
            second_decision = recovered.decide_next_cycle(
                execution=second_execution,
                information_gain_receipt=second_information_gain,
            )
            completed = recovered.complete_campaign()

            final_owner = ProcessIdentity("host-two-cycle", 203, 203_000)
            final_recovered = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=_BUDGET_LIMITS,
                identity_provider=_FakeProcessIdentityProvider(
                    final_owner,
                    process_starts={
                        (second_owner.host_id, second_owner.pid): None,
                    },
                ),
                monotonic_ns=lambda: 6_000_000,
            )
            with self.assertRaisesRegex(
                CampaignJournalError,
                "execution receipt is stale",
            ):
                final_recovered.decide_next_cycle(execution=second_execution)
            replayed_second_decision = (
                final_recovered.replay_next_cycle_decision(
                    cycle_id="cycle-002",
                )
            )

            self.assertEqual(second_decision.decision, "STOP")
            self.assertEqual(replayed_second_decision, second_decision)
            self.assertEqual(completed.status, CampaignStatus.COMPLETED)
            self.assertEqual(
                final_recovered.cycle_snapshot("cycle-002").status,
                CycleStatus.COMPLETED,
            )


if __name__ == "__main__":
    unittest.main()
