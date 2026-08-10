from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from research_automation.control_plane.access import (
    AccessEvent,
    AccessOperation,
    DatasetRole,
    FinalHoldoutUnavailable,
    Taint,
)
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
from tests import test_control_plane_campaign_controller as controller_fixtures
from tests.test_control_plane_campaign_freeze import _protocol_member
from tests.test_control_plane_campaign_lease import _FakeProcessIdentityProvider
from tests.test_control_plane_campaign_preflight import _scope
from tests.test_control_plane_campaign_store import _authorized_campaign
from tests import test_control_plane_evidence_learning as evidence_fixtures
from tests.test_foundations_protocols import _approval, _protocol


_BUDGET_LIMITS = CampaignBudgetLimits(
    currency="USD",
    max_cycles=2,
    max_input_tokens=200,
    max_output_tokens=100,
    max_cost="2",
    max_wall_time_ms=_FAKE_CALL_LIMITS.max_wall_time_ms * 2,
    max_tool_attempts=4,
)
_RESERVATION_LIMITS = CycleReservationLimits(
    currency="USD",
    max_input_tokens=20,
    max_output_tokens=10,
    max_cost="0.1",
    max_wall_time_ms=_FAKE_CALL_LIMITS.max_wall_time_ms,
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
    def setUp(self) -> None:
        self._owned_provider_call_counter_paths: set[Path] = set()
        self.addCleanup(self._cleanup_owned_provider_call_counters)

    def _new_owned_fake_provider(self, provider_class, *args, **kwargs):
        provider = provider_class(*args, **kwargs)
        self._owned_provider_call_counter_paths.add(
            Path(provider._call_count_path)
        )
        return provider

    def _cleanup_owned_provider_call_counters(self) -> None:
        for path in self._owned_provider_call_counter_paths:
            path.unlink(missing_ok=True)
            controller_fixtures._PROVIDER_CALL_COUNTER_PATHS.discard(path)
        self._owned_provider_call_counter_paths.clear()

    def _complete_authority_learning_cycle(
        self,
        *,
        controller: OperationalCampaignController,
        journal,
        root: Path,
        cycle_id: str,
        cycle_number: int,
        claim_scope: dict[str, object],
        report: dict[str, object],
        binding: object,
        artifact: dict[str, object],
    ):
        prompt = {
            "instruction": f"Return Authority-bound artifact for {cycle_id}"
        }
        execution_spec, member = _execution_spec_and_member(prompt)
        prepared = controller.prepare_cycle(
            task=ExperimentTask(
                task_id=cycle_id,
                strategy="b1",
                proposal={
                    "hypothesis": f"Synthetic finding for Cycle {cycle_number}",
                    "scope": claim_scope,
                },
                source="synthetic-test",
            ),
            cycle_number=cycle_number,
            execution_spec=execution_spec,
            roster_members=(member,),
            reservation_limits=_RESERVATION_LIMITS,
        )
        execution = controller.start_execution(
            cycle_id=prepared.cycle_id,
            acquisition_id=f"execute-{cycle_id}",
        )
        controller.invoke_member_json(
            execution=execution,
            member_id=member.member_id,
            provider=self._new_owned_fake_provider(
                _AuthorityEvidenceArtifactBoundFakeProvider,
                artifact,
            ),
            prompt=prompt,
            limits=_FAKE_CALL_LIMITS,
        )
        usage = controller.complete_model_execution(execution=execution)
        evidence = controller.record_model_evidence(
            execution=execution,
            member_id=member.member_id,
            evidence_adapter=EvidenceAdapter(
                known_runners={"fixture-runner": "1.0.0"},
                approved_protocol=artifact["executed_protocol"],
                approved_claim=artifact["claim"],
            ),
        )
        with patch(
            "research_automation.control_plane.evidence_learning."
            "AuthorityReader.verify_task_report_binding",
            return_value=binding,
        ):
            learning = controller.commit_learning(
                execution=execution,
                evidence_receipt=evidence,
                authority_task_report=report,
                learning_commit_sink=CampaignLearningCommitSink(
                    journal=journal,
                    service=LearningCommitService(repository_root=root),
                ),
            )
            settlement = controller.settle_cycle(
                execution=execution,
                execution_usage=usage,
                learning_commit_receipt=learning,
            )
            information_gain = controller.record_information_gain(
                execution=execution,
                settlement_receipt=settlement,
            )
        return prepared, execution, learning, information_gain

    def test_provider_call_counters_are_owned_by_test_instance(self) -> None:
        paths_before = set(controller_fixtures._PROVIDER_CALL_COUNTER_PATHS)
        provider = self._new_owned_fake_provider(
            _EvidenceArtifactBoundFakeProvider
        )
        owned_paths = {Path(provider._call_count_path)}

        self.assertEqual(len(owned_paths), 1)
        self.doCleanups()

        self.assertEqual(
            controller_fixtures._PROVIDER_CALL_COUNTER_PATHS,
            paths_before,
        )
        self.assertTrue(all(not path.exists() for path in owned_paths))

    def test_cleanup_preserves_interleaved_provider_counter_owner(self) -> None:
        owned_provider = self._new_owned_fake_provider(
            _EvidenceArtifactBoundFakeProvider
        )
        owned_path = Path(owned_provider._call_count_path)
        foreign_owner = OfflineTwoCycleProofTests(
            "test_provider_call_counters_are_owned_by_test_instance"
        )
        foreign_owner.setUp()
        foreign_provider = foreign_owner._new_owned_fake_provider(
            _EvidenceArtifactBoundFakeProvider
        )
        foreign_path = Path(foreign_provider._call_count_path)

        try:
            self.doCleanups()

            self.assertFalse(owned_path.exists())
            self.assertNotIn(
                owned_path,
                controller_fixtures._PROVIDER_CALL_COUNTER_PATHS,
            )
            self.assertTrue(foreign_path.exists())
            self.assertIn(
                foreign_path,
                controller_fixtures._PROVIDER_CALL_COUNTER_PATHS,
            )
        finally:
            foreign_owner.doCleanups()

    def test_unprojectable_learning_packet_cannot_authorize_next_cycle(
        self,
    ) -> None:
        campaign_id = "campaign-controller-unprojectable-learning"
        claim_scope = _scope(generation="generation-1")
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, binding, artifact, _, _ = (
                evidence_fixtures.EvidenceLearningVerticalSliceTests()
                ._authority_fixture(
                    root,
                    claim={
                        "kind": "NEGATIVE",
                        "scope": json.dumps(
                            claim_scope,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    },
                    protocol=_protocol().model_dump(mode="json"),
                )
            )
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=_BUDGET_LIMITS,
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-unprojectable", 211, 211_000)
                ),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )

            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                _, execution, learning, information_gain = (
                    self._complete_authority_learning_cycle(
                        controller=controller,
                        journal=journal,
                        root=root,
                        cycle_id="cycle-001",
                        cycle_number=1,
                        claim_scope=claim_scope,
                        report=report,
                        binding=binding,
                        artifact=artifact,
                    )
                )
                projection_input = CommittedLearningLedgerReader(
                    root
                ).read_projection_input()
                decision = controller.decide_next_cycle(
                    execution=execution,
                    information_gain_receipt=information_gain,
                )

            self.assertEqual(projection_input["claims"], [])
            self.assertEqual(
                projection_input["excluded_claims"],
                [
                    {
                        "claim_id": learning.packet_hash,
                        "reason_codes": ["P5_PACKET_NOT_PROJECTABLE"],
                    }
                ],
            )
            self.assertEqual(
                information_gain.information_gain_status,
                "LEARNING_PACKET_NOT_PROJECTABLE",
            )
            self.assertFalse(information_gain.continuation_eligible)
            self.assertEqual(
                information_gain.disposition_reason,
                "P5_PACKET_NOT_PROJECTABLE",
            )
            self.assertEqual(
                information_gain.learning_packet_hash,
                learning.packet_hash,
            )
            self.assertEqual(decision.decision, "STOP")
            self.assertFalse(decision.continuation_allowed)
            self.assertTrue(
                (
                    root
                    / "research_state/control_plane/learning_packets"
                    / f"{learning.packet_hash}.json"
                ).is_file()
            )

    def test_reused_packet_hash_does_not_count_as_new_information(self) -> None:
        campaign_id = "campaign-controller-reused-learning-packet"
        claim_scope = _scope(generation="generation-1")
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic reusable scoped finding",
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
        budget_limits = CampaignBudgetLimits(
            currency="USD",
            max_cycles=3,
            max_input_tokens=300,
            max_output_tokens=150,
            max_cost="3",
            max_wall_time_ms=_FAKE_CALL_LIMITS.max_wall_time_ms * 3,
            max_tool_attempts=6,
        )
        with _authorized_campaign(campaign_id) as (root, _, journal):
            report, binding, artifact, _, _ = (
                evidence_fixtures.EvidenceLearningVerticalSliceTests()
                ._authority_fixture(
                    root,
                    claim=claim,
                    protocol=_protocol().model_dump(mode="json"),
                )
            )
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=budget_limits,
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-reused-packet", 212, 212_000)
                ),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                    3_000_000,
                    4_000_000,
                    5_000_000,
                ),
            )
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                (
                    first_prepared,
                    first_execution,
                    first_learning,
                    first_information_gain,
                ) = (
                    self._complete_authority_learning_cycle(
                        controller=controller,
                        journal=journal,
                        root=root,
                        cycle_id="cycle-001",
                        cycle_number=1,
                        claim_scope=claim_scope,
                        report=report,
                        binding=binding,
                        artifact=artifact,
                    )
                )
                first_decision = controller.decide_next_cycle(
                    execution=first_execution,
                    information_gain_receipt=first_information_gain,
                )
                (
                    second_prepared,
                    second_execution,
                    second_learning,
                    second_information_gain,
                ) = self._complete_authority_learning_cycle(
                    controller=controller,
                    journal=journal,
                    root=root,
                    cycle_id="cycle-002",
                    cycle_number=2,
                    claim_scope=claim_scope,
                    report=report,
                    binding=binding,
                    artifact=artifact,
                )
                replayed_information_gain = controller.record_information_gain(
                    execution=second_execution,
                )
                second_decision = controller.decide_next_cycle(
                    execution=second_execution,
                    information_gain_receipt=second_information_gain,
                )
                projection_input = CommittedLearningLedgerReader(
                    root
                ).read_projection_input()
            self.assertEqual(first_decision.decision, "CONTINUE")
            self.assertEqual(
                first_information_gain.information_gain_status,
                "ELIGIBLE_LEARNING_COMMITTED",
            )
            self.assertTrue(first_information_gain.continuation_eligible)
            self.assertNotEqual(
                second_prepared.context.projection_input_sha256,
                first_prepared.context.projection_input_sha256,
            )
            self.assertEqual(
                [claim["claim_id"] for claim in projection_input["claims"]],
                [first_learning.packet_hash],
            )
            self.assertEqual(
                second_learning.packet_hash,
                first_learning.packet_hash,
            )
            self.assertEqual(replayed_information_gain, second_information_gain)
            self.assertEqual(
                second_information_gain.information_gain_status,
                "LEARNING_PACKET_NOT_NOVEL",
            )
            self.assertFalse(second_information_gain.continuation_eligible)
            self.assertEqual(
                second_information_gain.disposition_reason,
                "DUPLICATE_LEARNING_PACKET",
            )
            self.assertEqual(
                second_information_gain.learning_packet_hash,
                first_learning.packet_hash,
            )
            self.assertEqual(second_decision.decision, "STOP")
            self.assertFalse(second_decision.continuation_allowed)

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
                ._authority_fixture(
                    root,
                    claim=claim,
                    protocol=_protocol().model_dump(mode="json"),
                )
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
                provider=self._new_owned_fake_provider(
                    _AuthorityEvidenceArtifactBoundFakeProvider,
                    artifact,
                ),
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

            self.assertEqual(first_prepared.reservation.currency, "USD")
            self.assertEqual(first_usage.currency, "USD")
            self.assertEqual(first_settlement.currency, "USD")
            self.assertEqual(controller.budget_snapshot().currency, "USD")
            self.assertEqual(first_evidence.evidence, expected_evidence)
            self.assertEqual(
                first_information_gain.information_gain_status,
                "ELIGIBLE_LEARNING_COMMITTED",
            )
            self.assertTrue(first_information_gain.continuation_eligible)
            self.assertIsNone(first_information_gain.disposition_reason)
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
                provider=self._new_owned_fake_provider(
                    _EvidenceArtifactBoundFakeProvider
                ),
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

            self.assertEqual(second_prepared.reservation.currency, "USD")
            self.assertEqual(second_usage.currency, "USD")
            self.assertEqual(second_settlement.currency, "USD")
            self.assertEqual(recovered.budget_snapshot().currency, "USD")

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
            self.assertEqual(final_recovered.budget_snapshot().currency, "USD")
            self.assertEqual(
                final_recovered.cycle_snapshot("cycle-002").status,
                CycleStatus.COMPLETED,
            )

    def test_derivation_with_final_holdout_output_taint_is_unavailable(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            FinalHoldoutUnavailable,
            "FINAL_HOLDOUT taint is unavailable",
        ):
            AccessEvent(
                event_id="derive-holdout-proof",
                operation=AccessOperation.DERIVE,
                actor_id="trusted",
                actor_type="human",
                invocation_id="p6r2-t11",
                run_id="run-holdout-proof",
                dataset_role=DatasetRole.FINAL_HOLDOUT,
                input_artifact_refs=("artifact:abc",),
                output_artifact_refs=("artifact:def",),
                taint_in=(),
                taint_out=(Taint.FINAL_HOLDOUT,),
            )

    def test_derivation_with_final_holdout_input_taint_is_unavailable(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            FinalHoldoutUnavailable,
            "FINAL_HOLDOUT taint is unavailable",
        ):
            AccessEvent(
                event_id="derive-holdout-proof",
                operation=AccessOperation.DERIVE,
                actor_id="trusted",
                actor_type="human",
                invocation_id="p6r2-t11",
                run_id="run-holdout-proof",
                dataset_role=DatasetRole.FINAL_HOLDOUT,
                input_artifact_refs=("artifact:abc",),
                output_artifact_refs=("artifact:def",),
                taint_in=(Taint.FINAL_HOLDOUT,),
                taint_out=(Taint.CLEAN,),
            )

    def test_final_holdout_unavailable_is_catchable_as_runtime_error(
        self,
    ) -> None:
        self.assertTrue(issubclass(FinalHoldoutUnavailable, RuntimeError))

    def test_synthetic_two_cycle_context_projects_no_final_holdout_references(
        self,
    ) -> None:
        campaign_id = "campaign-controller-final-holdout-exclusion-proof"
        claim_scope = _scope(generation="generation-1")
        claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic scoped finding with no holdout data",
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
            report, binding, artifact, _, _ = (
                evidence_fixtures.EvidenceLearningVerticalSliceTests()
                ._authority_fixture(
                    root,
                    claim=claim,
                    protocol=_protocol().model_dump(mode="json"),
                )
            )
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=_BUDGET_LIMITS,
                identity_provider=_FakeProcessIdentityProvider(
                    ProcessIdentity("host-holdout-proof", 221, 221_000)
                ),
                monotonic_ns=_FakeMonotonicClock(
                    100,
                    1_000_000,
                    2_000_000,
                ),
            )
            with patch(
                "research_automation.control_plane.evidence_learning."
                "AuthorityReader.verify_task_report_binding",
                return_value=binding,
            ):
                _, execution, learning, information_gain = (
                    self._complete_authority_learning_cycle(
                        controller=controller,
                        journal=journal,
                        root=root,
                        cycle_id="cycle-001",
                        cycle_number=1,
                        claim_scope=claim_scope,
                        report=report,
                        binding=binding,
                        artifact=artifact,
                    )
                )
                projection_input = CommittedLearningLedgerReader(
                    root
                ).read_projection_input()
                decision = controller.decide_next_cycle(
                    execution=execution,
                    information_gain_receipt=information_gain,
                )
            projected_text = json.dumps(projection_input, sort_keys=True)
            self.assertNotIn("FINAL_HOLDOUT", projected_text)
            self.assertNotIn("holdout", projected_text.lower())
            self.assertEqual(decision.decision, "CONTINUE")
            synthetic_event = AccessEvent(
                event_id="derive-synthetic-proof",
                operation=AccessOperation.DERIVE,
                actor_id="trusted",
                actor_type="human",
                invocation_id="p6r2-t11",
                run_id="run-holdout-proof",
                dataset_role=DatasetRole.TRAIN,
                input_artifact_refs=("artifact:abc",),
                output_artifact_refs=("artifact:def",),
                taint_in=(),
                taint_out=(Taint.TEST_DERIVED,),
            )
            self.assertEqual(synthetic_event.dataset_role, DatasetRole.TRAIN)
            self.assertNotIn(Taint.FINAL_HOLDOUT, synthetic_event.taint_out)
            self.assertEqual(
                learning.packet_hash,
                projection_input["claims"][0]["claim_id"],
            )


if __name__ == "__main__":
    unittest.main()
