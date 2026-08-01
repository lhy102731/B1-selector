from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import localcontext
import unittest

from research_automation.control_plane.budget import (
    BudgetExceededError,
    BudgetLedger,
)


class BudgetLedgerTests(unittest.TestCase):
    def test_concurrent_reservations_are_atomic(self) -> None:
        ledger = BudgetLedger(
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="1.00",
        )

        def reserve(index: int) -> bool:
            try:
                ledger.reserve(
                    reservation_id=f"reservation-{index}",
                    call_id=f"call-{index}",
                    max_input_tokens=60,
                    max_output_tokens=60,
                    max_cost="0.60",
                )
            except BudgetExceededError:
                return False
            return True

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(reserve, range(8)))

        self.assertEqual(sum(results), 1)
        snapshot = ledger.snapshot()
        self.assertEqual(snapshot.reserved_input_tokens, 60)
        self.assertEqual(snapshot.reserved_output_tokens, 60)
        self.assertEqual(snapshot.reserved_cost, "0.6")

    def test_unknown_settlement_keeps_the_full_reservation(self) -> None:
        ledger = BudgetLedger(
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="1.00",
        )
        ledger.reserve(
            reservation_id="reservation-unknown",
            call_id="call-unknown",
            max_input_tokens=60,
            max_output_tokens=60,
            max_cost="0.60",
        )

        ledger.settle(
            "reservation-unknown",
            input_tokens=None,
            output_tokens=None,
            cost=None,
        )

        snapshot = ledger.snapshot()
        self.assertEqual(snapshot.reserved_input_tokens, 60)
        self.assertEqual(snapshot.reserved_output_tokens, 60)
        self.assertEqual(snapshot.reserved_cost, "0.6")
        self.assertEqual(snapshot.spent_input_tokens, 0)
        with self.assertRaises(BudgetExceededError):
            ledger.reserve(
                reservation_id="reservation-next",
                call_id="call-next",
                max_input_tokens=50,
                max_output_tokens=50,
                max_cost="0.50",
            )

    def test_known_settlement_releases_bound_and_records_actual_spend(self) -> None:
        ledger = BudgetLedger(
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="1.00",
        )
        ledger.reserve(
            reservation_id="reservation-known",
            call_id="call-known",
            max_input_tokens=60,
            max_output_tokens=60,
            max_cost="0.60",
        )

        settlement = ledger.settle(
            "reservation-known",
            input_tokens=20,
            output_tokens=10,
            cost="0.20",
        )
        replay = ledger.settle(
            "reservation-known",
            input_tokens=20,
            output_tokens=10,
            cost="0.20",
        )

        self.assertEqual(settlement.state, "SETTLED")
        self.assertEqual(replay.state, "SETTLED")
        snapshot = ledger.snapshot()
        self.assertEqual(snapshot.reserved_input_tokens, 0)
        self.assertEqual(snapshot.reserved_output_tokens, 0)
        self.assertEqual(snapshot.reserved_cost, "0")
        self.assertEqual(snapshot.spent_input_tokens, 20)
        self.assertEqual(snapshot.spent_output_tokens, 10)
        self.assertEqual(snapshot.spent_cost, "0.2")

    def test_numeric_equivalent_cost_replays_are_idempotent(self) -> None:
        ledger = BudgetLedger(
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="1.0",
        )
        first = ledger.reserve(
            reservation_id="reservation-equivalent",
            call_id="call-equivalent",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.60",
        )
        replay = ledger.reserve(
            reservation_id="reservation-equivalent",
            call_id="call-equivalent",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.6",
        )
        ledger.settle(
            "reservation-equivalent",
            input_tokens=5,
            output_tokens=2,
            cost="0.20",
        )
        settlement_replay = ledger.settle(
            "reservation-equivalent",
            input_tokens=5,
            output_tokens=2,
            cost="0.2",
        )

        self.assertEqual(first, replay)
        self.assertEqual(settlement_replay.state, "SETTLED")

    def test_budget_arithmetic_ignores_ambient_decimal_precision(self) -> None:
        ledger = BudgetLedger(
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="0.999",
        )

        with localcontext() as context:
            context.prec = 2
            for index in range(3):
                ledger.reserve(
                    reservation_id=f"reservation-precision-{index}",
                    call_id=f"call-precision-{index}",
                    max_input_tokens=1,
                    max_output_tokens=1,
                    max_cost="0.333",
                )

        self.assertEqual(ledger.snapshot().reserved_cost, "0.999")

    def test_allowed_exponent_span_is_exact_and_replayable(self) -> None:
        ledger = BudgetLedger(
            max_input_tokens=10,
            max_output_tokens=10,
            max_cost="2E+128",
        )

        first = ledger.reserve(
            reservation_id="reservation-wide-large",
            call_id="call-wide-large",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="1E+128",
        )
        replay = ledger.reserve(
            reservation_id="reservation-wide-large",
            call_id="call-wide-large",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="1e128",
        )
        ledger.reserve(
            reservation_id="reservation-wide-small",
            call_id="call-wide-small",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="1E-128",
        )

        self.assertEqual(first, replay)

        shifted = BudgetLedger(
            max_input_tokens=2,
            max_output_tokens=2,
            max_cost="20E+128",
        )
        shifted_first = shifted.reserve(
            reservation_id="reservation-shifted",
            call_id="call-shifted",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="10E+128",
        )
        shifted_replay = shifted.reserve(
            reservation_id="reservation-shifted",
            call_id="call-shifted",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="10e128",
        )
        self.assertEqual(shifted_first, shifted_replay)
        shifted.reserve(
            reservation_id="reservation-shifted-canonical",
            call_id="call-shifted-canonical",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost=shifted_first.max_cost,
        )


if __name__ == "__main__":
    unittest.main()
