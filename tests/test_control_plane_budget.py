from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import localcontext
import unittest

from research_automation.control_plane.budget import (
    BudgetConflictError,
    BudgetExceededError,
    BudgetLedger,
)


class BudgetLedgerTests(unittest.TestCase):
    def test_currency_is_canonical_and_survives_every_budget_receipt(self) -> None:
        ledger = BudgetLedger(
            currency="USD",
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="1",
        )
        known = ledger.reserve(
            reservation_id="reservation-known-currency",
            call_id="call-known-currency",
            currency="USD",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.2",
        )

        self.assertEqual(known.currency, "USD")
        self.assertEqual(ledger.snapshot().currency, "USD")
        known_settlement = ledger.settle(
            known.reservation_id,
            currency="USD",
            input_tokens=5,
            output_tokens=2,
            cost="0.05",
        )
        unknown = ledger.reserve(
            reservation_id="reservation-unknown-currency",
            call_id="call-unknown-currency",
            currency="USD",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.2",
        )
        unknown_settlement = ledger.settle(
            unknown.reservation_id,
            currency="USD",
            input_tokens=None,
            output_tokens=None,
            cost=None,
        )

        self.assertEqual(known_settlement.currency, "USD")
        self.assertEqual(unknown_settlement.currency, "USD")
        snapshot = ledger.snapshot()
        self.assertEqual(snapshot.currency, "USD")
        self.assertEqual(snapshot.reserved_cost, "0.2")

    def test_invalid_or_mismatched_currency_fails_before_budget_arithmetic(self) -> None:
        class CurrencySubclass(str):
            pass

        for currency in (
            "usd",
            "US",
            "USDD",
            None,
            123,
            CurrencySubclass("USD"),
        ):
            with self.subTest(currency=currency):
                with self.assertRaises(ValueError):
                    BudgetLedger(
                        currency=currency,
                        max_input_tokens=1,
                        max_output_tokens=1,
                        max_cost="1",
                    )

        ledger = BudgetLedger(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=10,
            max_cost="1",
        )
        before_reserve = ledger.snapshot()
        with self.assertRaises(BudgetConflictError):
            ledger.reserve(
                reservation_id="reservation-mismatched-currency",
                call_id="call-mismatched-currency",
                currency="EUR",
                max_input_tokens=1,
                max_output_tokens=1,
                max_cost=object(),
            )
        self.assertEqual(ledger.snapshot(), before_reserve)

        reservation = ledger.reserve(
            reservation_id="reservation-settlement-currency",
            call_id="call-settlement-currency",
            currency="USD",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="0.1",
        )
        before_settle = ledger.snapshot()
        with self.assertRaises(BudgetConflictError):
            ledger.settle(
                reservation.reservation_id,
                currency="EUR",
                input_tokens=0,
                output_tokens=0,
                cost=object(),
            )
        self.assertEqual(ledger.snapshot(), before_settle)

    def test_each_resource_dimension_blocks_cumulative_overreservation(self) -> None:
        resource_fields = (
            ("wall_time_ms", "reserved_wall_time_ms"),
            ("tool_attempts", "reserved_tool_attempts"),
            ("data_exposures", "reserved_data_exposures"),
            ("disk_growth_bytes", "reserved_disk_growth_bytes"),
        )
        for requested_field, snapshot_field in resource_fields:
            with self.subTest(resource=requested_field):
                ledger = BudgetLedger(
                    currency="USD",
                    max_input_tokens=10,
                    max_output_tokens=10,
                    max_cost="1",
                    max_wall_time_ms=100,
                    max_tool_attempts=100,
                    max_data_exposures=100,
                    max_disk_growth_bytes=100,
                )
                first_resources = {
                    "max_wall_time_ms": 0,
                    "max_tool_attempts": 0,
                    "max_data_exposures": 0,
                    "max_disk_growth_bytes": 0,
                }
                first_resources[f"max_{requested_field}"] = 60
                ledger.reserve(
                    currency="USD",
                    reservation_id="reservation-first",
                    call_id="call-first",
                    max_input_tokens=0,
                    max_output_tokens=0,
                    max_cost="0",
                    **first_resources,
                )
                second_resources = dict(first_resources)
                second_resources[f"max_{requested_field}"] = 50

                with self.assertRaises(BudgetExceededError):
                    ledger.reserve(
                        currency="USD",
                        reservation_id="reservation-second",
                        call_id="call-second",
                        max_input_tokens=0,
                        max_output_tokens=0,
                        max_cost="0",
                        **second_resources,
                    )

                self.assertEqual(
                    getattr(ledger.snapshot(), snapshot_field),
                    60,
                )

    def test_concurrent_wall_time_reservations_are_atomic(self) -> None:
        ledger = BudgetLedger(
            currency="USD",
            max_input_tokens=0,
            max_output_tokens=0,
            max_cost="0",
            max_wall_time_ms=100,
        )

        def reserve(index: int) -> bool:
            try:
                ledger.reserve(
                    currency="USD",
                    reservation_id=f"resource-reservation-{index}",
                    call_id=f"resource-call-{index}",
                    max_input_tokens=0,
                    max_output_tokens=0,
                    max_cost="0",
                    max_wall_time_ms=60,
                )
            except BudgetExceededError:
                return False
            return True

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(reserve, range(8)))

        self.assertEqual(sum(results), 1)
        self.assertEqual(ledger.snapshot().reserved_wall_time_ms, 60)

    def test_known_resource_settlement_releases_bounds_and_records_usage(self) -> None:
        ledger = BudgetLedger(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=10,
            max_cost="1",
            max_wall_time_ms=1_000,
            max_tool_attempts=10,
            max_data_exposures=10,
            max_disk_growth_bytes=1_000,
        )
        ledger.reserve(
            currency="USD",
            reservation_id="reservation-resource-known",
            call_id="call-resource-known",
            max_input_tokens=0,
            max_output_tokens=0,
            max_cost="0",
            max_wall_time_ms=600,
            max_tool_attempts=6,
            max_data_exposures=5,
            max_disk_growth_bytes=700,
        )

        settlement = ledger.settle(
            "reservation-resource-known",
            currency="USD",
            input_tokens=0,
            output_tokens=0,
            cost="0",
            wall_time_ms=400,
            tool_attempts=4,
            data_exposures=3,
            disk_growth_bytes=500,
        )
        replay = ledger.settle(
            "reservation-resource-known",
            currency="USD",
            input_tokens=0,
            output_tokens=0,
            cost="0",
            wall_time_ms=400,
            tool_attempts=4,
            data_exposures=3,
            disk_growth_bytes=500,
        )

        self.assertEqual(settlement.state, "SETTLED")
        self.assertEqual(replay.state, "SETTLED")
        snapshot = ledger.snapshot()
        self.assertEqual(snapshot.reserved_wall_time_ms, 0)
        self.assertEqual(snapshot.reserved_tool_attempts, 0)
        self.assertEqual(snapshot.reserved_data_exposures, 0)
        self.assertEqual(snapshot.reserved_disk_growth_bytes, 0)
        self.assertEqual(snapshot.spent_wall_time_ms, 400)
        self.assertEqual(snapshot.spent_tool_attempts, 4)
        self.assertEqual(snapshot.spent_data_exposures, 3)
        self.assertEqual(snapshot.spent_disk_growth_bytes, 500)

    def test_missing_resource_usage_keeps_the_full_reservation(self) -> None:
        ledger = BudgetLedger(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=10,
            max_cost="1",
            max_wall_time_ms=1_000,
            max_tool_attempts=10,
            max_data_exposures=10,
            max_disk_growth_bytes=1_000,
        )
        ledger.reserve(
            currency="USD",
            reservation_id="reservation-resource-unknown",
            call_id="call-resource-unknown",
            max_input_tokens=5,
            max_output_tokens=5,
            max_cost="0.5",
            max_wall_time_ms=600,
            max_tool_attempts=6,
            max_data_exposures=5,
            max_disk_growth_bytes=700,
        )

        settlement = ledger.settle(
            "reservation-resource-unknown",
            currency="USD",
            input_tokens=1,
            output_tokens=1,
            cost="0.1",
        )

        self.assertEqual(settlement.state, "SETTLED_UNKNOWN")
        snapshot = ledger.snapshot()
        self.assertEqual(snapshot.reserved_input_tokens, 5)
        self.assertEqual(snapshot.reserved_cost, "0.5")
        self.assertEqual(snapshot.reserved_wall_time_ms, 600)
        self.assertEqual(snapshot.reserved_tool_attempts, 6)
        self.assertEqual(snapshot.reserved_data_exposures, 5)
        self.assertEqual(snapshot.reserved_disk_growth_bytes, 700)
        self.assertEqual(snapshot.spent_wall_time_ms, 0)

    def test_concurrent_reservations_are_atomic(self) -> None:
        ledger = BudgetLedger(
            currency="USD",
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="1.00",
        )

        def reserve(index: int) -> bool:
            try:
                ledger.reserve(
                    currency="USD",
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
            currency="USD",
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="1.00",
        )
        ledger.reserve(
            currency="USD",
            reservation_id="reservation-unknown",
            call_id="call-unknown",
            max_input_tokens=60,
            max_output_tokens=60,
            max_cost="0.60",
        )

        ledger.settle(
            "reservation-unknown",
            currency="USD",
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
                currency="USD",
                reservation_id="reservation-next",
                call_id="call-next",
                max_input_tokens=50,
                max_output_tokens=50,
                max_cost="0.50",
            )

    def test_known_settlement_releases_bound_and_records_actual_spend(self) -> None:
        ledger = BudgetLedger(
            currency="USD",
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="1.00",
        )
        ledger.reserve(
            currency="USD",
            reservation_id="reservation-known",
            call_id="call-known",
            max_input_tokens=60,
            max_output_tokens=60,
            max_cost="0.60",
        )

        settlement = ledger.settle(
            "reservation-known",
            currency="USD",
            input_tokens=20,
            output_tokens=10,
            cost="0.20",
        )
        replay = ledger.settle(
            "reservation-known",
            currency="USD",
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
            currency="USD",
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="1.0",
        )
        first = ledger.reserve(
            currency="USD",
            reservation_id="reservation-equivalent",
            call_id="call-equivalent",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.60",
        )
        replay = ledger.reserve(
            currency="USD",
            reservation_id="reservation-equivalent",
            call_id="call-equivalent",
            max_input_tokens=20,
            max_output_tokens=10,
            max_cost="0.6",
        )
        ledger.settle(
            "reservation-equivalent",
            currency="USD",
            input_tokens=5,
            output_tokens=2,
            cost="0.20",
        )
        settlement_replay = ledger.settle(
            "reservation-equivalent",
            currency="USD",
            input_tokens=5,
            output_tokens=2,
            cost="0.2",
        )

        self.assertEqual(first, replay)
        self.assertEqual(settlement_replay.state, "SETTLED")

    def test_budget_arithmetic_ignores_ambient_decimal_precision(self) -> None:
        ledger = BudgetLedger(
            currency="USD",
            max_input_tokens=100,
            max_output_tokens=100,
            max_cost="0.999",
        )

        with localcontext() as context:
            context.prec = 2
            for index in range(3):
                ledger.reserve(
                    currency="USD",
                    reservation_id=f"reservation-precision-{index}",
                    call_id=f"call-precision-{index}",
                    max_input_tokens=1,
                    max_output_tokens=1,
                    max_cost="0.333",
                )

        self.assertEqual(ledger.snapshot().reserved_cost, "0.999")

    def test_allowed_exponent_span_is_exact_and_replayable(self) -> None:
        ledger = BudgetLedger(
            currency="USD",
            max_input_tokens=10,
            max_output_tokens=10,
            max_cost="2E+128",
        )

        first = ledger.reserve(
            currency="USD",
            reservation_id="reservation-wide-large",
            call_id="call-wide-large",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="1E+128",
        )
        replay = ledger.reserve(
            currency="USD",
            reservation_id="reservation-wide-large",
            call_id="call-wide-large",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="1e128",
        )
        ledger.reserve(
            currency="USD",
            reservation_id="reservation-wide-small",
            call_id="call-wide-small",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="1E-128",
        )

        self.assertEqual(first, replay)

        shifted = BudgetLedger(
            currency="USD",
            max_input_tokens=2,
            max_output_tokens=2,
            max_cost="20E+128",
        )
        shifted_first = shifted.reserve(
            currency="USD",
            reservation_id="reservation-shifted",
            call_id="call-shifted",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="10E+128",
        )
        shifted_replay = shifted.reserve(
            currency="USD",
            reservation_id="reservation-shifted",
            call_id="call-shifted",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost="10e128",
        )
        self.assertEqual(shifted_first, shifted_replay)
        shifted.reserve(
            currency="USD",
            reservation_id="reservation-shifted-canonical",
            call_id="call-shifted-canonical",
            max_input_tokens=1,
            max_output_tokens=1,
            max_cost=shifted_first.max_cost,
        )


if __name__ == "__main__":
    unittest.main()
