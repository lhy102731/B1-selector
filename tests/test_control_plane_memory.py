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

    def test_parent_invalidation_excludes_child_claim(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        common = {
            "kind": "FAILED_USAGE",
            "semantic_identity": "yellow-line",
            "scope": scope(regime="bull"),
            "audit_grade": "PASS",
            "taint_refs": [],
            "reopen_predicates": [],
            "universal_factor_rejection": False,
        }
        parent = {
            **common,
            "claim_id": "claim-parent-invalid",
            "execution_identity": "execution-parent",
            "invalidation_codes": ["REVOKED_EVIDENCE"],
            "parent_claim_ids": [],
        }
        child = {
            **common,
            "claim_id": "claim-child",
            "execution_identity": "execution-child",
            "invalidation_codes": [],
            "parent_claim_ids": ["claim-parent-invalid"],
        }
        decision = LearningGate().classify(
            {
                "execution_identity": "execution-child",
                "semantic_identity": "yellow-line",
                "scope": scope(regime="bull"),
            },
            [parent, child],
        )

        self.assertEqual("ALLOW", decision["enforcement"])
        excluded = {item["claim_id"]: item for item in decision["excluded_claims"]}
        self.assertIn("PARENT_INVALIDATED", excluded["claim-child"]["reason_codes"])

    def test_untrusted_parent_invalidates_all_descendants(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        for audit_grade, taint_refs in (
            ("INVALID", []),
            ("PASS", ["holdout-event"]),
        ):
            with self.subTest(audit_grade=audit_grade, taint_refs=taint_refs):
                common = {
                    "kind": "FAILED_USAGE",
                    "semantic_identity": "yellow-line",
                    "scope": scope(regime="bull"),
                    "audit_grade": "PASS",
                    "taint_refs": [],
                    "invalidation_codes": [],
                    "reopen_predicates": [],
                    "universal_factor_rejection": False,
                }
                parent = {
                    **common,
                    "claim_id": "claim-untrusted-parent",
                    "execution_identity": "execution-parent",
                    "audit_grade": audit_grade,
                    "taint_refs": taint_refs,
                    "parent_claim_ids": [],
                }
                child = {
                    **common,
                    "claim_id": "claim-untrusted-child",
                    "execution_identity": "execution-child",
                    "parent_claim_ids": ["claim-untrusted-parent"],
                }
                grandchild = {
                    **common,
                    "claim_id": "claim-untrusted-grandchild",
                    "execution_identity": "execution-grandchild",
                    "parent_claim_ids": ["claim-untrusted-child"],
                }
                decision = LearningGate().classify(
                    {
                        "execution_identity": "execution-grandchild",
                        "semantic_identity": "yellow-line",
                        "scope": scope(regime="bull"),
                    },
                    [parent, child, grandchild],
                )

                self.assertEqual("ALLOW", decision["enforcement"])
                excluded = {
                    item["claim_id"]: item for item in decision["excluded_claims"]
                }
                self.assertIn(
                    "PARENT_INVALIDATED",
                    excluded["claim-untrusted-child"]["reason_codes"],
                )
                self.assertIn(
                    "PARENT_INVALIDATED",
                    excluded["claim-untrusted-grandchild"]["reason_codes"],
                )

    def test_invalid_lineage_graph_fails_closed(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        def claim(claim_id: str, parent_ids: list[str]) -> dict[str, object]:
            return {
                "claim_id": claim_id,
                "kind": "FAILED_USAGE",
                "execution_identity": f"execution-{claim_id}",
                "semantic_identity": "yellow-line",
                "scope": scope(regime="bull"),
                "audit_grade": "PASS",
                "taint_refs": [],
                "invalidation_codes": [],
                "parent_claim_ids": parent_ids,
                "reopen_predicates": [],
                "universal_factor_rejection": False,
            }

        cases = (
            [claim("missing-child", ["missing-parent"])],
            [claim("self-parent", ["self-parent"])],
            [claim("cycle-a", ["cycle-b"]), claim("cycle-b", ["cycle-a"])],
        )
        for claims in cases:
            with self.subTest(claim_ids=[item["claim_id"] for item in claims]):
                with self.assertRaisesRegex(ValueError, "lineage"):
                    LearningGate().classify(
                        {
                            "execution_identity": "execution-proposal",
                            "semantic_identity": "yellow-line",
                            "scope": scope(regime="bull"),
                        },
                        claims,
                    )

    def test_large_legal_lineage_does_not_depend_on_python_recursion(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        claims = []
        claim_count = 1500
        for index in range(claim_count):
            claim_id = f"deep-{index:04d}"
            parent_ids = (
                [f"deep-{index + 1:04d}"] if index + 1 < claim_count else []
            )
            claims.append(
                {
                    "claim_id": claim_id,
                    "kind": "FAILED_USAGE",
                    "execution_identity": f"execution-{claim_id}",
                    "semantic_identity": "yellow-line",
                    "scope": scope(regime="bull"),
                    "audit_grade": "PASS",
                    "taint_refs": [],
                    "invalidation_codes": [],
                    "parent_claim_ids": parent_ids,
                    "reopen_predicates": [],
                    "universal_factor_rejection": False,
                }
            )

        decision = LearningGate().classify(
            {
                "execution_identity": "execution-new",
                "semantic_identity": "unrelated-factor",
                "scope": scope(regime="bull"),
            },
            claims,
        )

        self.assertEqual("ALLOW", decision["enforcement"])
        self.assertEqual(claim_count, len(decision["matches"]))

    def test_large_lineage_cycle_fails_closed_without_recursion(self) -> None:
        from research_automation.control_plane.memory import LearningGate

        claims = []
        claim_count = 1500
        for index in range(claim_count):
            claim_id = f"cycle-deep-{index:04d}"
            parent_id = f"cycle-deep-{(index + 1) % claim_count:04d}"
            claims.append(
                {
                    "claim_id": claim_id,
                    "kind": "FAILED_USAGE",
                    "execution_identity": f"execution-{claim_id}",
                    "semantic_identity": "yellow-line",
                    "scope": scope(regime="bull"),
                    "audit_grade": "PASS",
                    "taint_refs": [],
                    "invalidation_codes": [],
                    "parent_claim_ids": [parent_id],
                    "reopen_predicates": [],
                    "universal_factor_rejection": False,
                }
            )

        with self.assertRaisesRegex(ValueError, "cycle"):
            LearningGate().classify(
                {
                    "execution_identity": "execution-new",
                    "semantic_identity": "yellow-line",
                    "scope": scope(regime="bull"),
                },
                claims,
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

    def test_conflict_requires_valid_opposing_polarity(self) -> None:
        from research_automation.control_plane.memory import ConflictClassifier

        base = {
            "execution_identity": "execution-same-direction",
            "scope": scope(regime="bull"),
        }
        same_direction = ConflictClassifier().classify(
            {**base, "claim_id": "claim-negative", "kind": "NEGATIVE"},
            {**base, "claim_id": "claim-failed", "kind": "FAILED_USAGE"},
            actor_event={"event_id": "event-polarity", "actor_id": "reviewer"},
        )
        self.assertEqual("NONE", same_direction["classification"])
        self.assertIsNone(same_direction["resolution_owner"])

        with self.assertRaisesRegex(ValueError, "kind"):
            ConflictClassifier().classify(
                {**base, "claim_id": "claim-valid", "kind": "POSITIVE"},
                {**base, "claim_id": "claim-unknown", "kind": "UNKNOWN"},
                actor_event={
                    "event_id": "event-unknown-kind",
                    "actor_id": "reviewer",
                },
            )

    def test_conflict_requires_distinct_claims_and_canonical_actor_event(self) -> None:
        from research_automation.control_plane.memory import ConflictClassifier

        claim = {
            "claim_id": "claim-duplicate",
            "kind": "POSITIVE",
            "execution_identity": "execution-duplicate",
            "scope": scope(regime="bull"),
        }
        opposing = {**claim, "kind": "NEGATIVE"}
        with self.assertRaisesRegex(ValueError, "distinct"):
            ConflictClassifier().classify(
                claim,
                opposing,
                actor_event={"event_id": "event-duplicate", "actor_id": "reviewer"},
            )
        for actor_event in (
            {"event_id": "event-only"},
            {"event_id": "event-extra", "actor_id": "reviewer", "extra": "x"},
            {"event_id": "", "actor_id": "reviewer"},
        ):
            with self.subTest(actor_event=actor_event):
                with self.assertRaises(ValueError):
                    ConflictClassifier().classify(
                        {**claim, "claim_id": "claim-left"},
                        {**opposing, "claim_id": "claim-right"},
                        actor_event=actor_event,
                    )

    def test_opposing_specs_are_scope_or_protocol_conflict(self) -> None:
        from research_automation.control_plane.memory import ConflictClassifier

        left = {
            "claim_id": "claim-spec-left",
            "kind": "POSITIVE",
            "execution_identity": "execution-left",
            "semantic_identity": "yellow-line",
            "scope": scope(regime="bull"),
        }
        right = {
            "claim_id": "claim-spec-right",
            "kind": "FAILED_USAGE",
            "execution_identity": "execution-right",
            "semantic_identity": "yellow-line",
            "scope": scope(regime="bear"),
        }
        conflict = ConflictClassifier().classify(
            left,
            right,
            actor_event={"event_id": "event-002", "actor_id": "reviewer-002"},
        )

        self.assertEqual("SCOPE_OR_PROTOCOL_CONFLICT", conflict["classification"])
        self.assertEqual("scope_protocol_owner", conflict["resolution_owner"])

    def test_generation_change_is_data_drift_conflict(self) -> None:
        from research_automation.control_plane.memory import ConflictClassifier

        left_scope = scope(regime="bull")
        right_scope = scope(regime="bull")
        right_scope["generation_families"] = ["ths_daily_v2"]
        conflict = ConflictClassifier().classify(
            {
                "claim_id": "claim-generation-left",
                "kind": "POSITIVE",
                "execution_identity": "execution-generation-left",
                "semantic_identity": "yellow-line",
                "scope": left_scope,
            },
            {
                "claim_id": "claim-generation-right",
                "kind": "NEGATIVE",
                "execution_identity": "execution-generation-right",
                "semantic_identity": "yellow-line",
                "scope": right_scope,
            },
            actor_event={"event_id": "event-003", "actor_id": "reviewer-003"},
        )

        self.assertEqual("DATA_DRIFT_CONFLICT", conflict["classification"])
        self.assertEqual("data_steward", conflict["resolution_owner"])

    def test_legacy_provenance_is_classified_before_scoped_conflict(self) -> None:
        from research_automation.control_plane.memory import ConflictClassifier

        conflict = ConflictClassifier().classify(
            {
                "claim_id": "claim-controller",
                "kind": "POSITIVE",
                "execution_identity": "execution-controller",
                "semantic_identity": "yellow-line",
                "scope": scope(regime="bull"),
                "trust_state": "controller_audited",
            },
            {
                "claim_id": "claim-legacy",
                "kind": "NEGATIVE",
                "execution_identity": "execution-legacy",
                "semantic_identity": "yellow-line",
                "scope": scope(regime="bull"),
                "trust_state": "legacy_unaudited",
            },
            actor_event={"event_id": "event-004", "actor_id": "reviewer-004"},
        )

        self.assertEqual("LEGACY_EVIDENCE_CONFLICT", conflict["classification"])
        self.assertEqual("legacy_evidence_owner", conflict["resolution_owner"])

    def test_conflict_classification_priority_is_deterministic(self) -> None:
        from research_automation.control_plane.memory import ConflictClassifier

        def claim(
            claim_id: str,
            *,
            kind: str,
            execution: str,
            generation: str = "ths_daily_v1",
            trust_state: str = "controller_audited",
            semantic: str = "yellow-line",
        ) -> dict[str, object]:
            claim_scope = scope(regime="bull")
            claim_scope["generation_families"] = [generation]
            return {
                "claim_id": claim_id,
                "kind": kind,
                "execution_identity": execution,
                "semantic_identity": semantic,
                "scope": claim_scope,
                "trust_state": trust_state,
            }

        cases = (
            (
                claim("same-left", kind="POSITIVE", execution="same"),
                claim(
                    "same-right",
                    kind="NEGATIVE",
                    execution="same",
                    generation="ths_daily_v2",
                    trust_state="legacy_unaudited",
                ),
                "REPRODUCIBILITY_FAILURE",
            ),
            (
                claim("legacy-left", kind="POSITIVE", execution="legacy-left"),
                claim(
                    "legacy-right",
                    kind="NEGATIVE",
                    execution="legacy-right",
                    generation="ths_daily_v2",
                    trust_state="legacy_unaudited",
                ),
                "LEGACY_EVIDENCE_CONFLICT",
            ),
            (
                claim("drift-left", kind="POSITIVE", execution="drift-left"),
                claim(
                    "drift-right",
                    kind="NEGATIVE",
                    execution="drift-right",
                    generation="ths_daily_v2",
                ),
                "DATA_DRIFT_CONFLICT",
            ),
            (
                claim("none-left", kind="POSITIVE", execution="none-left"),
                claim(
                    "none-right",
                    kind="NEGATIVE",
                    execution="none-right",
                    semantic="unrelated-factor",
                ),
                "NONE",
            ),
        )
        for left, right, expected in cases:
            with self.subTest(expected=expected):
                decision = ConflictClassifier().classify(
                    left,
                    right,
                    actor_event={
                        "event_id": f"event-{expected.lower()}",
                        "actor_id": "priority-reviewer",
                    },
                )
                self.assertEqual(expected, decision["classification"])
                self.assertEqual(
                    "priority-reviewer", decision["actor_event"]["actor_id"]
                )


class LearningReopenTests(unittest.TestCase):
    def test_declared_new_mechanism_predicate_reopens_claim(self) -> None:
        from research_automation.control_plane.memory import ReopenPredicateEvaluator

        learned_scope = scope(regime="bull")
        proposal_scope = scope(regime="bull")
        proposal_scope["mechanisms"] = ["volume-price divergence"]
        decision = ReopenPredicateEvaluator().evaluate(
            {"scope": proposal_scope},
            {
                "claim_id": "claim-reopen-mechanism",
                "scope": learned_scope,
                "reopen_predicates": ["NEW_MECHANISM"],
            },
        )

        self.assertTrue(decision["qualified"])
        self.assertEqual(["NEW_MECHANISM"], decision["reason_codes"])

    def test_declared_scope_delta_predicates_reopen_claim(self) -> None:
        from research_automation.control_plane.memory import ReopenPredicateEvaluator

        cases = (
            ("usage_modes", ["hard_gate"], "NEW_USAGE_MODE"),
            ("market_regimes", ["bear"], "NEW_MARKET_REGIME"),
            (
                "time_windows",
                [{"start": "2024-01-01", "end": "2024-12-31"}],
                "NEW_TIME_WINDOW",
            ),
            ("universes", ["csi_300"], "NEW_UNIVERSE"),
            ("liquidity_buckets", ["illiquid"], "NEW_LIQUIDITY_BUCKET"),
        )
        for field_name, value, predicate in cases:
            with self.subTest(predicate=predicate):
                proposal_scope = scope(regime="bull")
                proposal_scope[field_name] = value
                decision = ReopenPredicateEvaluator().evaluate(
                    {"scope": proposal_scope},
                    {
                        "claim_id": f"claim-{predicate.lower()}",
                        "scope": scope(regime="bull"),
                        "reopen_predicates": [predicate],
                    },
                )
                self.assertTrue(decision["qualified"])
                self.assertEqual([predicate], decision["reason_codes"])

    def test_declared_data_drift_predicate_reopens_generation_change(self) -> None:
        from research_automation.control_plane.memory import ReopenPredicateEvaluator

        proposal_scope = scope(regime="bull")
        proposal_scope["generation_families"] = ["ths_daily_v2"]
        decision = ReopenPredicateEvaluator().evaluate(
            {"scope": proposal_scope},
            {
                "claim_id": "claim-data-drift",
                "scope": scope(regime="bull"),
                "reopen_predicates": ["DATA_DRIFT"],
            },
        )

        self.assertTrue(decision["qualified"])
        self.assertEqual(["DATA_DRIFT"], decision["reason_codes"])

    def test_declared_stronger_evidence_predicate_requires_grade_upgrade(self) -> None:
        from research_automation.control_plane.memory import ReopenPredicateEvaluator

        evaluator = ReopenPredicateEvaluator()
        claim = {
            "claim_id": "claim-evidence-upgrade",
            "scope": scope(regime="bull"),
            "evidence_grade": "EXPLORATORY",
            "reopen_predicates": ["STRONGER_EVIDENCE"],
        }
        upgraded = evaluator.evaluate(
            {
                "scope": scope(regime="bull"),
                "evidence_grade": "STRICT_FORWARD_VALIDATED",
            },
            claim,
        )
        unchanged = evaluator.evaluate(
            {
                "scope": scope(regime="bull"),
                "evidence_grade": "EXPLORATORY",
            },
            claim,
        )

        self.assertEqual(["STRONGER_EVIDENCE"], upgraded["reason_codes"])
        self.assertFalse(unchanged["qualified"])

    def test_declared_research_gap_requires_matching_structured_reference(self) -> None:
        from research_automation.control_plane.memory import ReopenPredicateEvaluator

        evaluator = ReopenPredicateEvaluator()
        claim = {
            "claim_id": "claim-research-gap",
            "scope": scope(regime="bull"),
            "declared_research_gap_refs": ["gap-001"],
            "reopen_predicates": ["DECLARED_RESEARCH_GAP"],
        }
        matching = evaluator.evaluate(
            {
                "scope": scope(regime="bull"),
                "research_gap_refs": ["gap-001"],
            },
            claim,
        )
        unrelated = evaluator.evaluate(
            {
                "scope": scope(regime="bull"),
                "research_gap_refs": ["gap-999"],
            },
            claim,
        )

        self.assertEqual(["DECLARED_RESEARCH_GAP"], matching["reason_codes"])
        self.assertFalse(unrelated["qualified"])

    def test_empty_research_gap_reference_does_not_reopen(self) -> None:
        from research_automation.control_plane.memory import ReopenPredicateEvaluator

        decision = ReopenPredicateEvaluator().evaluate(
            {
                "scope": scope(regime="bull"),
                "research_gap_refs": [],
            },
            {
                "claim_id": "claim-no-research-gap",
                "scope": scope(regime="bull"),
                "declared_research_gap_refs": ["gap-001"],
                "reopen_predicates": ["DECLARED_RESEARCH_GAP"],
            },
        )

        self.assertFalse(decision["qualified"])
        self.assertEqual([], decision["reason_codes"])

    def test_manual_reopen_bypass_is_rejected(self) -> None:
        from research_automation.control_plane.memory import ReopenPredicateEvaluator

        with self.assertRaisesRegex(ValueError, "manual bypass"):
            ReopenPredicateEvaluator().evaluate(
                {
                    "scope": scope(regime="bull"),
                    "manual_bypass": True,
                },
                {
                    "claim_id": "claim-manual-bypass",
                    "scope": scope(regime="bull"),
                    "reopen_predicates": [],
                },
            )

    def test_unknown_reopen_predicate_fails_closed(self) -> None:
        from research_automation.control_plane.memory import ReopenPredicateEvaluator

        with self.assertRaisesRegex(ValueError, "reopen_predicates"):
            ReopenPredicateEvaluator().evaluate(
                {"scope": scope(regime="bull")},
                {
                    "claim_id": "claim-unknown-predicate",
                    "scope": scope(regime="bull"),
                    "reopen_predicates": ["MANUAL_OVERRIDE"],
                },
            )

    def test_unchanged_scope_does_not_satisfy_reopen_predicates(self) -> None:
        from research_automation.control_plane.memory import ReopenPredicateEvaluator

        for predicate in (
            "NEW_MECHANISM",
            "NEW_USAGE_MODE",
            "NEW_MARKET_REGIME",
            "NEW_TIME_WINDOW",
            "NEW_UNIVERSE",
            "NEW_LIQUIDITY_BUCKET",
            "DATA_DRIFT",
        ):
            with self.subTest(predicate=predicate):
                decision = ReopenPredicateEvaluator().evaluate(
                    {"scope": scope(regime="bull")},
                    {
                        "claim_id": f"claim-unchanged-{predicate.lower()}",
                        "scope": scope(regime="bull"),
                        "reopen_predicates": [predicate],
                    },
                )
                self.assertFalse(decision["qualified"])
                self.assertEqual([], decision["reason_codes"])


if __name__ == "__main__":
    unittest.main()
