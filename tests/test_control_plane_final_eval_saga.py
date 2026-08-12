"""Tests for the durable final evaluation saga (P8R3 T7)."""

from __future__ import annotations

import unittest

from research_automation.control_plane.final_eval_saga import (
    FinalEvalSagaOutcomeRejected,
    FinalEvalSagaStep,
    FinalEvalSagaTransitionError,
    SAGA_STATES,
    SAGA_TRANSITIONS,
    TERMINAL_OUTCOMES,
    derive_outcome,
    require_saga_transition,
)


class SagaTransitionTests(unittest.TestCase):
    def test_states_are_fixed_order(self) -> None:
        self.assertEqual(
            SAGA_STATES,
            (
                "REQUEST_FROZEN",
                "AUTHORIZED",
                "CONSUMED",
                "EVALUATING",
                "RESULT_STAGED",
                "CLOSED",
                "AUTHORITY_TERMINAL",
            ),
        )

    def test_transitions_are_exact_edges(self) -> None:
        self.assertEqual(
            SAGA_TRANSITIONS,
            frozenset(
                {
                    ("REQUEST_FROZEN", "AUTHORIZED"),
                    ("AUTHORIZED", "CONSUMED"),
                    ("CONSUMED", "EVALUATING"),
                    ("EVALUATING", "RESULT_STAGED"),
                    ("RESULT_STAGED", "CLOSED"),
                    ("CLOSED", "AUTHORITY_TERMINAL"),
                }
            ),
        )

    def test_legal_transition_passes(self) -> None:
        require_saga_transition("AUTHORIZED", "CONSUMED")

    def test_illegal_transition_fails_closed(self) -> None:
        with self.assertRaises(FinalEvalSagaTransitionError):
            require_saga_transition("REQUEST_FROZEN", "CONSUMED")
        with self.assertRaises(FinalEvalSagaTransitionError):
            require_saga_transition("AUTHORITY_TERMINAL", "REQUEST_FROZEN")

    def test_unknown_state_fails_closed(self) -> None:
        with self.assertRaises(FinalEvalSagaTransitionError):
            require_saga_transition("UNKNOWN", "CONSUMED")


class OutcomeMappingTests(unittest.TestCase):
    def test_caller_outcome_rejected(self) -> None:
        with self.assertRaises(FinalEvalSagaOutcomeRejected):
            derive_outcome(
                worker_payload={"outcome": "SUCCEEDED"},
                caller_outcome="SUCCEEDED",
            )

    def test_valid_worker_outcome_maps_to_succeeded(self) -> None:
        self.assertEqual(
            derive_outcome(worker_payload={"outcome": "SUCCEEDED"}),
            "SUCCEEDED",
        )

    def test_failure_outcomes_map_through(self) -> None:
        self.assertEqual(
            derive_outcome(worker_payload={"outcome": "FAILED"}),
            "FAILED",
        )
        self.assertEqual(
            derive_outcome(worker_payload={"outcome": "TIMEOUT"}),
            "TIMEOUT",
        )
        self.assertEqual(
            derive_outcome(worker_payload={"outcome": "CRASHED"}),
            "CRASHED",
        )

    def test_invalid_worker_outcome_rejected(self) -> None:
        with self.assertRaises(FinalEvalSagaOutcomeRejected):
            derive_outcome(worker_payload={"outcome": "UNKNOWN"})
        with self.assertRaises(FinalEvalSagaOutcomeRejected):
            derive_outcome(worker_payload={})


class SagaStepTests(unittest.TestCase):
    def test_step_payload_is_canonical(self) -> None:
        step = FinalEvalSagaStep(
            step="begin",
            state_before="REQUEST_FROZEN",
            state_after="AUTHORIZED",
            detail={"ticket_id": "t-1"},
        )
        payload = step.to_payload()
        self.assertEqual(payload["state_after"], "AUTHORIZED")
        self.assertEqual(payload["detail"]["ticket_id"], "t-1")


if __name__ == "__main__":
    unittest.main()
