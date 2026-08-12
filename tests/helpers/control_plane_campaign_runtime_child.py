"""Fresh-process fixture child for the P6R3 two-cycle proof (Task 8.7).

Usage: python control_plane_campaign_runtime_child.py <campaign-id> <root>

Re-opens the same OperationalCampaignJournal from the given temporary root
(injected via the same store-path patch the parent used), reconstructs a
controller with a *new* process identity, replays the previous cycle decision
and runs the next cycle.  Prints one canonical JSON line: the decision
payload, or {"error": "..."} on failure.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane.campaign_controller import (
    CampaignBudgetLimits,
    CycleReservationLimits,
    OperationalCampaignController,
    OperationalModelCallLimits,
    operational_prompt_sha256,
)
from research_automation.control_plane.campaign_lease import (
    LocalProcessIdentityProvider,
)
from research_automation.control_plane.campaign_store import (
    OperationalCampaignJournal,
)
from research_automation.control_plane.campaign_controller import (
    CampaignLearningCommitSink,
)
from research_automation.control_plane.evidence_learning import (
    LearningCommitService,
)
from research_automation.foundations.protocols import compile_execution_spec
from research_automation.task_queue import ExperimentTask
import research_automation.control_plane.stores as stores_module
from tests.test_control_plane_campaign_store import ROOT_SECRET, _claim_campaign_grant
from tests.test_control_plane_campaign_controller import (
    _EvidenceArtifactBoundFakeProvider,
    _FAKE_CALL_LIMITS,
    _FakeMonotonicClock,
)
from tests.test_control_plane_campaign_freeze import _protocol_member
from tests.test_control_plane_campaign_preflight import _scope
from tests.test_foundations_protocols import _approval, _protocol


def main() -> int:
    campaign_id = sys.argv[1]
    root = Path(sys.argv[2])
    with patch.multiple(
        stores_module,
        _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
        _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
    ):
        stores_module._expected_schema_sha256.cache_clear()
        grant = _claim_campaign_grant(
            campaign_id=campaign_id,
            namespace="formal",
            actor_id="p6-runner-child",
            invocation_id=f"{campaign_id}-child",
            attempt_id=f"{campaign_id}-attempt",
            plan_sha256="a" * 64,
            instruction_sha256="c" * 64,
        )
        journal = OperationalCampaignJournal(
            root_secret=ROOT_SECRET,
            grant=grant,
            namespace="formal",
            campaign_id=campaign_id,
        )
        # The parent committed learning through a patched authority binding;
        # the child must patch the same projection gate so the durable replay
        # of cycle-001's information-gain decision can verify the frozen
        # task report against the shared authority.
        from unittest.mock import patch as _patch

        from tests import test_control_plane_evidence_learning as evidence_fixtures

        _claim = {
            "kind": "NEGATIVE",
            "summary": "Synthetic scoped finding from cycle one",
            "scope": json.dumps(
                {"mechanisms": ["volume-contraction-rebound"],
                 "usage_modes": ["factor-candidate"],
                 "market_regimes": ["all"],
                 "time_windows": [{"start": "2020-01-01", "end": "2026-12-31"}],
                 "universes": ["a-share"],
                 "liquidity_buckets": ["production-minimum"],
                 "label_protocol_families": ["rolling-forward-v1"],
                 "generation_families": ["generation-1"]},
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
        _report, _binding, _artifact, _, _ = (
            evidence_fixtures.EvidenceLearningVerticalSliceTests()
            ._authority_fixture(
                root,
                claim=_claim,
                protocol=_protocol().model_dump(mode="json"),
            )
        )
        _patch(
            "research_automation.control_plane.evidence_learning."
            "AuthorityReader.verify_task_report_binding",
            return_value=_binding,
        ).start()
        controller = OperationalCampaignController(
            journal=journal,
            repository_root=root,
            budget_limits=CampaignBudgetLimits(
                currency="USD",
                max_cycles=2,
                max_input_tokens=200,
                max_output_tokens=100,
                max_cost="2",
                max_wall_time_ms=_FAKE_CALL_LIMITS.max_wall_time_ms * 2,
                max_tool_attempts=4,
            ),
            identity_provider=LocalProcessIdentityProvider(),
            monotonic_ns=_FakeMonotonicClock(100, 200, 300, 400),
        )
        # Fresh process: replay the previous cycle decision (read-only, no lease).
        try:
            previous = controller.replay_next_cycle_decision(cycle_id="cycle-001")
        except Exception as error:  # noqa: BLE001
            import traceback
            detail = "".join(traceback.format_exception(error))
            print(json.dumps({"error": f"replay: {error}", "trace": detail}))
            return 2
        if previous.decision != "CONTINUE" or previous.next_cycle_number != 2:
            print(json.dumps({"error": "previous decision did not continue"}))
            return 3
        prompt = {"instruction": "Return deterministic artifact for cycle 2"}
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        member = _protocol_member()
        # Recompute prompt hash so the frozen roster matches this child prompt.
        from dataclasses import replace

        member = replace(
            member,
            prompt_sha256=operational_prompt_sha256(prompt),
        )
        prepared = controller.prepare_cycle(
            task=ExperimentTask(
                task_id="cycle-002",
                strategy="b1",
                proposal={
                    "hypothesis": "Cycle two recovery hypothesis",
                    "scope": _scope(generation="generation-1"),
                },
                source="synthetic-test",
            ),
            cycle_number=2,
            execution_spec=execution_spec,
            roster_members=(member,),
            reservation_limits=CycleReservationLimits(
                currency="USD",
                max_input_tokens=20,
                max_output_tokens=10,
                max_cost="0.1",
                max_wall_time_ms=_FAKE_CALL_LIMITS.max_wall_time_ms,
                max_tool_attempts=2,
            ),
        )
        execution = controller.start_execution(
            cycle_id=prepared.cycle_id,
            acquisition_id=f"execute-{prepared.cycle_id}",
        )
        controller.invoke_member_json(
            execution=execution,
            member_id=member.member_id,
            provider=_EvidenceArtifactBoundFakeProvider(),
            prompt=prompt,
            limits=_FAKE_CALL_LIMITS,
        )
        usage = controller.complete_model_execution(execution=execution)
        evidence = controller.record_model_evidence(
            execution=execution,
            member_id=member.member_id,
            evidence_adapter=(
                __import__(
                    "research_automation.control_plane.evidence_learning",
                    fromlist=["EvidenceAdapter"],
                ).EvidenceAdapter(known_runners={"fixture-runner": "1.0.0"})
            ),
        )
        settlement = controller.settle_cycle_without_learning(
            execution=execution,
            execution_usage=usage,
            evidence_receipt=evidence,
        )
        information_gain = controller.record_information_gain(
            execution=execution,
            settlement_receipt=settlement,
        )
        decision = controller.decide_next_cycle(
            execution=execution,
            information_gain_receipt=information_gain,
        )
        print(
            json.dumps(
                {
                    "cycle_id": decision.cycle_id,
                    "decision": decision.decision,
                    "reason_code": decision.reason_code,
                    "next_cycle_number": decision.next_cycle_number,
                    "status": controller.cycle_snapshot("cycle-002").status.value,
                }
            )
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
