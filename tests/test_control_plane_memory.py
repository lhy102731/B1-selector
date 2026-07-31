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
    def test_exact_execution_identity_is_hard_rejected(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        decision = LearningGate().classify(
            {
                "execution_identity": "execution-exact",
                "semantic_identity": "yellow-line",
                "scope": scope(regime="bull"),
            },
            [
                {
                    "claim_id": "claim-exact",
                    "kind": "FAILED_USAGE",
                    "execution_identity": "execution-exact",
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

        self.assertEqual("HARD_BLOCK", decision["enforcement"])
        self.assertEqual(["claim-exact"], decision["hard_block_claim_ids"])

    def test_exact_execution_identity_does_not_block_disjoint_scope(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        decision = LearningGate().classify(
            {
                "execution_identity": "execution-shared",
                "semantic_identity": "yellow-line",
                "scope": scope(regime="bear"),
            },
            [
                {
                    "claim_id": "claim-disjoint-exact",
                    "kind": "FAILED_USAGE",
                    "execution_identity": "execution-shared",
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

    def test_exact_partial_identity_blocks_only_intersection(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        proposal_scope = scope(regime="bear")
        proposal_scope["market_regimes"] = ["bear", "bull"]
        learned_scope = scope(regime="bull")
        learned_scope["market_regimes"] = ["bull", "sideways"]
        decision = LearningGate().classify(
            {
                "execution_identity": "execution-partial",
                "semantic_identity": "yellow-line",
                "scope": proposal_scope,
            },
            [
                {
                    "claim_id": "claim-partial-exact",
                    "kind": "PARTIAL",
                    "execution_identity": "execution-partial",
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

        self.assertEqual("SCOPED_BLOCK", decision["enforcement"])
        self.assertEqual([], decision["hard_block_claim_ids"])
        self.assertEqual(
            ["bull"],
            decision["scoped_block_claims"][0]["applicable_scope"][
                "market_regimes"
            ],
        )

    def test_semantic_similarity_warns_only(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        decision = LearningGate().classify(
            {
                "execution_identity": "execution-new",
                "semantic_identity": "yellow-line",
                "scope": scope(regime="bull"),
            },
            [
                {
                    "claim_id": "claim-similar",
                    "kind": "NEGATIVE",
                    "execution_identity": "execution-old",
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
        self.assertIn("SEMANTIC_SIMILARITY_ONLY", decision["warning_codes"])

    def test_malformed_claim_identity_fails_closed(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        claim = {
            "claim_id": "claim-malformed",
            "kind": "FAILED_USAGE",
            "semantic_identity": "yellow-line",
            "scope": scope(regime="bull"),
            "audit_grade": "PASS",
            "taint_refs": [],
            "invalidation_codes": [],
            "reopen_predicates": [],
            "universal_factor_rejection": False,
        }
        with self.assertRaisesRegex(ValueError, "execution_identity"):
            LearningGate().classify(
                {
                    "execution_identity": "execution-malformed",
                    "semantic_identity": "yellow-line",
                    "scope": scope(regime="bull"),
                },
                [claim],
            )

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

    def test_invalid_or_tainted_claim_is_excluded(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        claims = []
        for claim_id, audit_grade, taint_refs in (
            ("claim-invalid", "INVALID", []),
            ("claim-tainted", "PASS", ["holdout-event"]),
        ):
            claims.append(
                {
                    "claim_id": claim_id,
                    "kind": "NEGATIVE",
                    "execution_identity": "proposal-exact",
                    "semantic_identity": "yellow-line",
                    "scope": scope(regime="bull"),
                    "audit_grade": audit_grade,
                    "taint_refs": taint_refs,
                    "invalidation_codes": [],
                    "reopen_predicates": [],
                    "universal_factor_rejection": False,
                }
            )

        decision = LearningGate().classify(
            {
                "execution_identity": "proposal-exact",
                "semantic_identity": "yellow-line",
                "scope": scope(regime="bull"),
            },
            claims,
        )

        self.assertEqual("ALLOW", decision["enforcement"])
        self.assertEqual([], decision["matches"])
        self.assertEqual(
            ["claim-invalid", "claim-tainted"],
            [item["claim_id"] for item in decision["excluded_claims"]],
        )


class LearningConflictTests(unittest.TestCase):
    def test_reproducibility_failure_is_classified_with_owner_event(self) -> None:
        from research_automation.control_plane.memory import ConflictClassifier

        left = {
            "claim_id": "claim-left",
            "kind": "POSITIVE",
            "execution_identity": "execution-same",
            "scope": scope(regime="bull"),
        }
        right = {
            "claim_id": "claim-right",
            "kind": "NEGATIVE",
            "execution_identity": "execution-same",
            "scope": scope(regime="bull"),
        }
        conflict = ConflictClassifier().classify(
            left,
            right,
            actor_event={"event_id": "event-001", "actor_id": "reviewer-001"},
        )

        self.assertEqual("REPRODUCIBILITY_FAILURE", conflict["classification"])
        self.assertEqual("reproducibility_owner", conflict["resolution_owner"])
        self.assertEqual("event-001", conflict["actor_event"]["event_id"])


if __name__ == "__main__":
    unittest.main()
