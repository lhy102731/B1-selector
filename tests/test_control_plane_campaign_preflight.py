from __future__ import annotations

import unittest

from research_automation.control_plane.campaign_preflight import (
    run_campaign_preflight,
)
from research_automation.control_plane.memory import (
    learning_execution_identity,
    learning_semantic_identity,
)
from research_automation.foundations.protocols import (
    IDENTICAL,
    compile_execution_spec,
)
from tests.test_foundations_protocols import _approval, _protocol


def _scope(*, generation: str = "b1-v342") -> dict[str, object]:
    return {
        "mechanisms": ["volume-contraction-rebound"],
        "usage_modes": ["factor-candidate"],
        "market_regimes": ["all"],
        "time_windows": [{"start": "2020-01-01", "end": "2026-12-31"}],
        "universes": ["a-share"],
        "liquidity_buckets": ["production-minimum"],
        "label_protocol_families": ["rolling-forward-v1"],
        "generation_families": [generation],
    }


def _claim(
    *,
    claim_id: str,
    hypothesis: str,
    scope: dict[str, object],
    kind: str = "NEGATIVE",
) -> dict[str, object]:
    return {
        "claim_id": claim_id,
        "kind": kind,
        "execution_identity": learning_execution_identity(hypothesis, scope),
        "semantic_identity": learning_semantic_identity(hypothesis),
        "scope": scope,
        "audit_grade": "PASS",
        "evidence_grade": "STRICT_FORWARD_VALIDATED",
        "taint_refs": [],
        "invalidation_codes": [],
        "parent_claim_ids": [],
        "universal_factor_rejection": False,
    }


class CampaignPreflightTests(unittest.TestCase):
    def test_exact_committed_execution_is_rejected_after_protocol_preflight(self) -> None:
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        hypothesis = "Volume contraction predicts rebound"
        scope = _scope()
        committed = _claim(
            claim_id="committed-negative",
            hypothesis=hypothesis,
            scope=scope,
        )

        result = run_campaign_preflight(
            execution_spec=execution_spec,
            proposal={"hypothesis": hypothesis, "scope": scope},
            committed_claims=[committed],
        )

        self.assertEqual(result["verdict"], "WOULD_REJECT")
        self.assertEqual(result["protocol_conformance"], IDENTICAL)
        self.assertEqual(
            result["rejection_codes"],
            ["LEARNING_HARD_BLOCK"],
        )
        self.assertEqual(
            result["learning_verdict"]["hard_block_claim_ids"],
            ["committed-negative"],
        )

    def test_materially_unapproved_protocol_is_rejected_even_when_scope_allows(self) -> None:
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=None,
            approval=None,
            amendment=None,
        )

        result = run_campaign_preflight(
            execution_spec=execution_spec,
            proposal={
                "hypothesis": "A new bounded mechanism",
                "scope": _scope(),
            },
            committed_claims=[],
        )

        self.assertEqual(result["verdict"], "WOULD_REJECT")
        self.assertEqual(
            result["rejection_codes"],
            ["MATERIAL_PROTOCOL_UNAPPROVED"],
        )
        self.assertEqual(result["learning_verdict"]["enforcement"], "ALLOW")

    def test_semantic_similarity_in_a_distinct_scope_warns_without_rejection(self) -> None:
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        hypothesis = "Volume contraction predicts rebound"
        committed_scope = _scope(generation="b1-v341")

        result = run_campaign_preflight(
            execution_spec=execution_spec,
            proposal={"hypothesis": hypothesis, "scope": _scope()},
            committed_claims=[
                _claim(
                    claim_id="older-generation-negative",
                    hypothesis=hypothesis,
                    scope=committed_scope,
                )
            ],
        )

        self.assertEqual(result["verdict"], "WOULD_ACCEPT")
        self.assertEqual(result["rejection_codes"], [])
        self.assertEqual(
            result["learning_verdict"]["warning_codes"],
            ["SEMANTIC_SIMILARITY_ONLY"],
        )
        self.assertEqual(
            result["learning_verdict"]["matches"][0]["scope_match"],
            "DISJOINT",
        )

    def test_exact_partial_learning_is_reported_as_a_scoped_conflict(self) -> None:
        protocol = _protocol()
        execution_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        hypothesis = "Volume contraction predicts rebound"
        scope = _scope()

        result = run_campaign_preflight(
            execution_spec=execution_spec,
            proposal={"hypothesis": hypothesis, "scope": scope},
            committed_claims=[
                _claim(
                    claim_id="committed-partial",
                    hypothesis=hypothesis,
                    scope=scope,
                    kind="PARTIAL",
                )
            ],
        )

        self.assertEqual(result["verdict"], "WOULD_REJECT")
        self.assertEqual(
            result["rejection_codes"],
            ["LEARNING_SCOPED_BLOCK"],
        )
        self.assertEqual(
            result["learning_verdict"]["scoped_block_claims"][0]["claim_id"],
            "committed-partial",
        )


if __name__ == "__main__":
    unittest.main()
