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


if __name__ == "__main__":
    unittest.main()
