from __future__ import annotations

import unittest
from unittest.mock import patch

from research_automation.control_plane.campaign_preflight import (
    BLOCKED_PENDING_C1_ADAPTER_COMMANDS,
    DRY_RUN_READ_ONLY_EXCEPTION_COMMANDS,
    PROGRAMMATIC_CONTEXT_ONLY_COMMANDS,
    READ_ONLY_COMMANDS,
    CampaignBoundaryError,
    command_disposition,
    require_campaign_boundary,
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


class CampaignBoundaryTests(unittest.TestCase):
    def test_legacy_surface_without_campaign_context_fails_closed(self) -> None:
        with self.assertRaises(CampaignBoundaryError) as caught:
            require_campaign_boundary(surface="run_research.py:brainstorm")
        self.assertEqual(
            caught.exception.rejection_codes,
            ("LEGACY_SURFACE_WITHOUT_CAMPAIGN_CONTEXT",),
        )
        self.assertIn("run_research.py:brainstorm", str(caught.exception))

    def test_formal_would_accept_passes_boundary(self) -> None:
        accepted = {"verdict": "WOULD_ACCEPT", "rejection_codes": []}
        with patch(
            "research_automation.control_plane.campaign_preflight."
            "run_campaign_preflight",
            return_value=accepted,
        ) as preflight:
            result = require_campaign_boundary(
                surface="formal-surface",
                execution_spec=object(),
                proposal={"hypothesis": "bounded"},
            )
        self.assertIs(result, accepted)
        preflight.assert_called_once()

    def test_formal_would_reject_raises_with_codes(self) -> None:
        rejected = {
            "verdict": "WOULD_REJECT",
            "rejection_codes": ["LEARNING_HARD_BLOCK"],
        }
        with patch(
            "research_automation.control_plane.campaign_preflight."
            "run_campaign_preflight",
            return_value=rejected,
        ):
            with self.assertRaises(CampaignBoundaryError) as caught:
                require_campaign_boundary(
                    surface="formal-surface",
                    execution_spec=object(),
                    proposal={"hypothesis": "bounded"},
                )
        self.assertEqual(
            caught.exception.rejection_codes,
            ("LEARNING_HARD_BLOCK",),
        )


class CommandDispositionTests(unittest.TestCase):
    def test_read_only_commands_are_allowed(self) -> None:
        for command in ("list", "status", "audit", "doctor", "export"):
            with self.subTest(command=command):
                disposition = command_disposition(command)
                self.assertEqual(
                    disposition["disposition"],
                    "READ_ONLY_ALLOWED",
                )
                self.assertIn(command, READ_ONLY_COMMANDS)

    def test_campaign_requires_programmatic_context(self) -> None:
        disposition = command_disposition("campaign")
        self.assertEqual(
            disposition["disposition"],
            "PROGRAMMATIC_CONTEXT_ONLY",
        )
        self.assertIn("campaign", PROGRAMMATIC_CONTEXT_ONLY_COMMANDS)

    def test_execute_handoff_is_dry_run_read_only_exception(self) -> None:
        disposition = command_disposition("execute-handoff")
        self.assertEqual(
            disposition["disposition"],
            "DRY_RUN_READ_ONLY_EXCEPTION",
        )
        self.assertIn("execute-handoff", DRY_RUN_READ_ONLY_EXCEPTION_COMMANDS)

    def test_network_research_commands_are_blocked_pending_c1_adapter(self) -> None:
        for command in (
            "brainstorm",
            "discover",
            "resume-discover",
            "full-cycle",
            "review",
            "chat",
            "roundtable",
            "interactive",
            "repair-handoff-runner",
        ):
            with self.subTest(command=command):
                disposition = command_disposition(command)
                self.assertEqual(
                    disposition["disposition"],
                    "BLOCKED_PENDING_C1_ADAPTER",
                )
                self.assertIn(command, BLOCKED_PENDING_C1_ADAPTER_COMMANDS)

    def test_unknown_command_fails_closed(self) -> None:
        disposition = command_disposition("unknown-command")
        self.assertEqual(
            disposition["disposition"],
            "BLOCKED_PENDING_C1_ADAPTER",
        )


if __name__ == "__main__":
    unittest.main()
