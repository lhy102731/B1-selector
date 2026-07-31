from __future__ import annotations

import unittest


def scope(*, regime: str) -> dict[str, object]:
    return {
        "mechanisms": ["yellow-line mean reversion"],
        "usage_modes": ["soft_penalty"],
        "market_regimes": [regime],
        "time_windows": [{"start": "2021-01-01", "end": "2023-12-31"}],
        "universes": ["a_share"],
        "liquidity_buckets": ["liquid"],
        "label_protocol_families": ["b1_forward_v1"],
        "generation_families": ["ths_daily_v1"],
    }


class LearningGateTests(unittest.TestCase):
    def test_disjoint_scope_is_not_hard_rejected(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        decision = LearningGate().classify(
            {
                "execution_identity": "proposal-001",
                "semantic_identity": "yellow-line",
                "scope": scope(regime="bear"),
            },
            [
                {
                    "claim_id": "claim-001",
                    "kind": "NEGATIVE",
                    "execution_identity": "prior-001",
                    "semantic_identity": "yellow-line",
                    "scope": scope(regime="bull"),
                    "audit_grade": "PASS",
                    "taint_refs": [],
                    "invalidation_codes": [],
                    "reopen_predicates": [],
                    "universal_factor_rejection": False,
                }
            ],
        )

        self.assertEqual("ALLOW", decision["enforcement"])
        self.assertEqual([], decision["hard_block_claim_ids"])
        self.assertEqual("DISJOINT", decision["matches"][0]["scope_match"])

    def test_partial_scope_applies_only_to_intersection(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        proposal_scope = scope(regime="bear")
        proposal_scope["market_regimes"] = ["bear", "bull"]
        learned_scope = scope(regime="bull")
        learned_scope["market_regimes"] = ["bull", "sideways"]

        decision = LearningGate().classify(
            {
                "execution_identity": "proposal-002",
                "semantic_identity": "yellow-line",
                "scope": proposal_scope,
            },
            [
                {
                    "claim_id": "claim-partial",
                    "kind": "PARTIAL",
                    "execution_identity": "prior-partial",
                    "semantic_identity": "yellow-line",
                    "scope": learned_scope,
                    "audit_grade": "PASS",
                    "taint_refs": [],
                    "invalidation_codes": [],
                    "reopen_predicates": [],
                    "universal_factor_rejection": False,
                }
            ],
        )

        match = decision["matches"][0]
        self.assertEqual("OVERLAP", match["scope_match"])
        self.assertEqual(["bull"], match["applicable_scope"]["market_regimes"])

    def test_universal_rejection_cannot_be_set_manually(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        claim = {
            "claim_id": "claim-universal",
            "kind": "NEGATIVE",
            "execution_identity": "prior-universal",
            "semantic_identity": "yellow-line",
            "scope": scope(regime="bull"),
            "audit_grade": "PASS",
            "taint_refs": [],
            "invalidation_codes": [],
            "reopen_predicates": [],
            "universal_factor_rejection": True,
        }
        with self.assertRaisesRegex(ValueError, "derived"):
            LearningGate().classify(
                {
                    "execution_identity": "proposal-003",
                    "semantic_identity": "yellow-line",
                    "scope": scope(regime="bull"),
                },
                [claim],
            )


if __name__ == "__main__":
    unittest.main()
