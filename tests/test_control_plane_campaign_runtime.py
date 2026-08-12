"""Tests for the P6R3 authorized Campaign runtime (Task 7)."""

from __future__ import annotations

from dataclasses import replace
import unittest
from unittest.mock import patch

from research_automation.control_plane.campaign_controller import (
    CampaignBudgetLimits,
    CycleReservationLimits,
    OperationalCampaignController,
    OperationalModelCallLimits,
    operational_prompt_sha256,
)
from research_automation.control_plane.campaign_lifecycle import (
    CampaignStatus,
    CycleStatus,
)
from research_automation.control_plane.campaign_runtime import (
    CampaignCommandContext,
    CampaignRuntime,
    CampaignRuntimeError,
    CampaignRuntimeObserver,
    CampaignRuntimePhaseError,
    CampaignRuntimeResult,
    CycleSummary,
    PHASE_CHAIN,
)
from research_automation.control_plane.campaign_store import (
    CampaignLearningCommitSink,
    OperationalCampaignJournal,
)
from research_automation.control_plane.evidence_learning import (
    EvidenceAdapter,
    LearningCommitService,
)
from research_automation.control_plane.campaign_lease import (
    LocalProcessIdentityProvider,
)
from research_automation.foundations.protocols import compile_execution_spec
from research_automation.task_queue import ExperimentTask
from tests.test_control_plane_campaign_controller import (
    _EvidenceArtifactBoundFakeProvider,
    _FAKE_CALL_LIMITS,
    _FakeMonotonicClock,
)
from tests.test_control_plane_campaign_freeze import _protocol_member
from tests.test_control_plane_campaign_preflight import _scope
from tests.test_control_plane_campaign_store import (
    _authorized_campaign,
)
from tests.test_foundations_protocols import _approval, _protocol

_BUDGET_LIMITS = CampaignBudgetLimits(
    currency="USD",
    max_cycles=1,
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


def _make_context(
    *,
    journal: OperationalCampaignJournal,
    root,
    campaign_id: str,
    namespace: str = "formal",
    mode: str = "formal",
    observers=(),
    authority_task_report=None,
):
    controller = OperationalCampaignController(
        journal=journal,
        repository_root=root,
        budget_limits=_BUDGET_LIMITS,
        identity_provider=LocalProcessIdentityProvider(),
        monotonic_ns=_FakeMonotonicClock(1000, 2000, 3000, 4000),
    )
    prompt = {"instruction": "Return deterministic artifact"}
    execution_spec, member = _execution_spec_and_member(prompt)
    task = ExperimentTask(
        task_id="campaign-runtime-cycle-001",
        strategy="b1",
        proposal={
            "hypothesis": "Runtime hypothesis",
            "scope": _scope(generation="runtime-1"),
        },
        source="synthetic-test",
    )
    context = CampaignCommandContext(
        controller=controller,
        task=task,
        execution_spec=execution_spec,
        roster_members=(member,),
        reservation_limits=_RESERVATION_LIMITS,
        call_limits=_FAKE_CALL_LIMITS,
        provider=_EvidenceArtifactBoundFakeProvider(),
        prompt=prompt,
        evidence_adapter=EvidenceAdapter(
            known_runners={"fixture-runner": "1.0.0"},
        ),
        learning_commit_sink=CampaignLearningCommitSink(
            journal=journal,
            service=LearningCommitService(repository_root=root),
        ),
        campaign_id=campaign_id,
        namespace=namespace,
        mode=mode,
        observers=observers,
        authority_task_report=authority_task_report,
    )
    return context


class PhaseChainContractTests(unittest.TestCase):
    def test_phase_chain_is_fixed_order(self) -> None:
        self.assertEqual(
            PHASE_CHAIN,
            (
                "preflight",
                "prepare",
                "start",
                "invoke_required_roster",
                "complete_model",
                "evidence",
                "commit_or_no_learning",
                "settle",
                "information_gain",
                "next_cycle_decision",
                "observers",
            ),
        )

    def test_context_rejects_non_controller(self) -> None:
        with self.assertRaises(TypeError):
            CampaignCommandContext(
                controller=object(),  # type: ignore[arg-type]
                task=object(),  # type: ignore[arg-type]
                execution_spec=object(),  # type: ignore[arg-type]
                roster_members=(),
                reservation_limits=_RESERVATION_LIMITS,
                call_limits=_FAKE_CALL_LIMITS,
                provider=object(),
                prompt={},
                evidence_adapter=object(),  # type: ignore[arg-type]
                learning_commit_sink=object(),  # type: ignore[arg-type]
                campaign_id="x",
                namespace="formal",
                mode="formal",
            )

    def test_context_rejects_roster_conflicting_with_execution_spec(self) -> None:
        with _authorized_campaign("campaign-runtime-conflict") as (root, grant, journal):
            controller = OperationalCampaignController(
                journal=journal,
                repository_root=root,
                budget_limits=_BUDGET_LIMITS,
                identity_provider=LocalProcessIdentityProvider(),
                monotonic_ns=_FakeMonotonicClock(1, 2, 3, 4),
            )
            prompt = {"instruction": "x"}
            execution_spec, member = _execution_spec_and_member(prompt)
            conflicting = replace(member, role="other-role")
            with self.assertRaises(CampaignRuntimeError):
                CampaignCommandContext(
                    controller=controller,
                    task=ExperimentTask(
                        task_id="t",
                        strategy="b1",
                        proposal={
                            "hypothesis": "h",
                            "scope": _scope(generation="conflict"),
                        },
                        source="synthetic-test",
                    ),
                    execution_spec=execution_spec,
                    roster_members=(conflicting,),
                    reservation_limits=_RESERVATION_LIMITS,
                    call_limits=_FAKE_CALL_LIMITS,
                    provider=object(),
                    prompt=prompt,
                    evidence_adapter=EvidenceAdapter(),
                    learning_commit_sink=CampaignLearningCommitSink(
                        journal=journal,
                        service=LearningCommitService(repository_root=root),
                    ),
                    campaign_id="campaign-runtime-conflict",
                    namespace="formal",
                    mode="formal",
                )

    def test_runtime_rejects_non_context(self) -> None:
        with self.assertRaises(TypeError):
            CampaignRuntime(object())  # type: ignore[arg-type]


class RuntimeExecutionTests(unittest.TestCase):
    def test_runtime_runs_no_learning_cycle_to_completion(self) -> None:
        campaign_id = "campaign-runtime-nolearn"
        with _authorized_campaign(campaign_id) as (root, grant, journal):
            context = _make_context(
                journal=journal,
                root=root,
                campaign_id=campaign_id,
            )
            result = CampaignRuntime(context).run(max_cycles=1)

            self.assertIsInstance(result, CampaignRuntimeResult)
            self.assertEqual(result.status, "COMPLETED")
            self.assertEqual(result.cycles_completed, 1)
            self.assertEqual(result.decision, "STOP")
            self.assertEqual(
                result.campaign_snapshot["status"],
                CampaignStatus.COMPLETED.value,
            )
            self.assertEqual(result.mode, "formal")
            self.assertEqual(len(result.cycle_summaries), 1)
            summary = result.cycle_summaries[0]
            self.assertIsInstance(summary, CycleSummary)
            self.assertEqual(summary.decision, "STOP")
            self.assertEqual(summary.model_call_count, 1)
            self.assertTrue(summary.evidence_refs)
            self.assertTrue(summary.event_refs)
            self.assertEqual(
                context.controller.cycle_snapshot(summary.cycle_id).status,
                CycleStatus.COMPLETED,
            )
            payload = result.to_payload()
            self.assertEqual(payload["schema_version"], "control_plane.campaign_runtime_result.v1")

    def test_safe_result_never_leaks_secret_fields(self) -> None:
        campaign_id = "campaign-runtime-safe"
        with _authorized_campaign(campaign_id) as (root, grant, journal):
            context = _make_context(
                journal=journal,
                root=root,
                campaign_id=campaign_id,
            )
            result = CampaignRuntime(context).run(max_cycles=1)
            payload = result.to_payload()
            blob = str(payload)
            self.assertNotIn("root_secret", blob)
            self.assertNotIn("api_key", blob)
            self.assertNotIn("prompt", blob)
            self.assertNotIn("nonce", blob)
            self.assertNotIn("provider_response", blob)
            self.assertNotIn("holdout", blob)
            self.assertNotIn("data bytes", blob)

    def test_observer_after_cycle_settled_requests_pause(self) -> None:
        campaign_id = "campaign-runtime-pause"

        class PausingObserver(CampaignRuntimeObserver):
            def after_cycle_settled(self, summary: CycleSummary) -> None:
                raise RuntimeError("durable pause requested")

        with _authorized_campaign(campaign_id) as (root, grant, journal):
            observer = PausingObserver()
            context = _make_context(
                journal=journal,
                root=root,
                campaign_id=campaign_id,
                observers=(observer,),
            )
            result = CampaignRuntime(context).run(max_cycles=1)
            self.assertEqual(result.status, "PAUSED_BY_OBSERVER")
            self.assertEqual(result.cycles_completed, 1)
            self.assertTrue(
                any("requested pause" in d for d in result.diagnostics)
            )


class RuntimeFailClosedTests(unittest.TestCase):
    def test_skipping_phase_chain_fails_closed(self) -> None:
        campaign_id = "campaign-runtime-phase"
        with _authorized_campaign(campaign_id) as (root, grant, journal):
            context = _make_context(
                journal=journal,
                root=root,
                campaign_id=campaign_id,
            )
            runtime = CampaignRuntime(context)
            # Advancing past preflight without executing it must fail.
            with self.assertRaises(CampaignRuntimePhaseError):
                runtime._require_next_phase("prepare")


if __name__ == "__main__":
    unittest.main()
