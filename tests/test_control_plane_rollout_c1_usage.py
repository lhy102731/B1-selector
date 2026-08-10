"""C1 usage ledger and budget verdict tests (RED at skeleton)."""

from __future__ import annotations

import unittest

from research_automation.control_plane.rollout_c1_usage import (
    BudgetVerdict,
    DryRunBudget,
    UsageLedger,
    UsageRecord,
)


def _record(model: str = "test-model", status: str = "ok",
            input_tokens: int = 10, output_tokens: int = 5,
            total_tokens: int = 15) -> UsageRecord:
    return UsageRecord(
        model=model, status=status,
        input_tokens=input_tokens, output_tokens=output_tokens,
        total_tokens=total_tokens,
    )


class UsageLedgerRecordTests(unittest.TestCase):
    def test_record_appends_single_record(self) -> None:
        ledger = UsageLedger()
        rec = _record()
        ledger.record(rec)
        self.assertEqual(len(ledger.records()), 1)
        self.assertEqual(ledger.records()[0].model, "test-model")

    def test_records_returns_copy_not_reference(self) -> None:
        ledger = UsageLedger()
        ledger.record(_record())
        copy1 = ledger.records()
        copy2 = ledger.records()
        self.assertIsNot(copy1, copy2)
        # Mutating the returned list must not affect the ledger
        copy1.append(_record(model="extra"))
        self.assertEqual(len(ledger.records()), 1)

    def test_records_preserve_insertion_order(self) -> None:
        ledger = UsageLedger()
        r1 = _record(model="a")
        r2 = _record(model="b")
        r3 = _record(model="c")
        ledger.record(r1)
        ledger.record(r2)
        ledger.record(r3)
        models = [r.model for r in ledger.records()]
        self.assertEqual(models, ["a", "b", "c"])

    def test_none_record_is_rejected(self) -> None:
        ledger = UsageLedger()
        with self.assertRaises(ValueError):
            ledger.record(None)  # type: ignore[arg-type]

    def test_negative_input_tokens_rejected(self) -> None:
        ledger = UsageLedger()
        bad = UsageRecord(model="x", status="ok",
                          input_tokens=-1, output_tokens=0, total_tokens=0)
        with self.assertRaises(ValueError):
            ledger.record(bad)

    def test_negative_output_tokens_rejected(self) -> None:
        ledger = UsageLedger()
        bad = UsageRecord(model="x", status="ok",
                          input_tokens=0, output_tokens=-5, total_tokens=0)
        with self.assertRaises(ValueError):
            ledger.record(bad)

    def test_negative_total_tokens_rejected(self) -> None:
        ledger = UsageLedger()
        bad = UsageRecord(model="x", status="ok",
                          input_tokens=0, output_tokens=0, total_tokens=-3)
        with self.assertRaises(ValueError):
            ledger.record(bad)


class UsageLedgerTotalsTests(unittest.TestCase):
    def test_empty_ledger_totals_are_zero(self) -> None:
        ledger = UsageLedger()
        self.assertEqual(ledger.total_input_tokens(), 0)
        self.assertEqual(ledger.total_output_tokens(), 0)
        self.assertEqual(ledger.total_tokens(), 0)

    def test_totals_sum_across_all_records(self) -> None:
        ledger = UsageLedger()
        ledger.record(_record(input_tokens=10, output_tokens=5, total_tokens=15))
        ledger.record(_record(input_tokens=20, output_tokens=7, total_tokens=27))
        ledger.record(_record(input_tokens=3, output_tokens=2, total_tokens=5))
        self.assertEqual(ledger.total_input_tokens(), 33)
        self.assertEqual(ledger.total_output_tokens(), 14)
        self.assertEqual(ledger.total_tokens(), 47)


class UsageLedgerBudgetTests(unittest.TestCase):
    def test_empty_ledger_passes_budget(self) -> None:
        ledger = UsageLedger()
        budget = DryRunBudget(currency="USD", max_total_tokens=100,
                              max_tokens_per_model=50)
        verdict = ledger.verify_budget(budget)
        self.assertIsInstance(verdict, BudgetVerdict)
        self.assertTrue(verdict.passed)
        self.assertIn("USD", verdict.detail)

    def test_within_budget_passes(self) -> None:
        ledger = UsageLedger()
        ledger.record(_record(model="m1", total_tokens=30))
        ledger.record(_record(model="m2", total_tokens=40))
        budget = DryRunBudget(currency="CNY", max_total_tokens=100,
                              max_tokens_per_model=50)
        verdict = ledger.verify_budget(budget)
        self.assertTrue(verdict.passed)
        self.assertIn("CNY", verdict.detail)

    def test_per_model_over_budget_fails_and_names_model(self) -> None:
        ledger = UsageLedger()
        ledger.record(_record(model="small", total_tokens=10))
        ledger.record(_record(model="big-model", total_tokens=100))
        budget = DryRunBudget(currency="USD", max_total_tokens=500,
                              max_tokens_per_model=50)
        verdict = ledger.verify_budget(budget)
        self.assertFalse(verdict.passed)
        self.assertIn("big-model", verdict.detail)
        self.assertIn("USD", verdict.detail)

    def test_multiple_models_over_budget_all_named(self) -> None:
        ledger = UsageLedger()
        ledger.record(_record(model="alpha", total_tokens=80))
        ledger.record(_record(model="beta", total_tokens=90))
        budget = DryRunBudget(currency="USD", max_total_tokens=1000,
                              max_tokens_per_model=50)
        verdict = ledger.verify_budget(budget)
        self.assertFalse(verdict.passed)
        self.assertIn("alpha", verdict.detail)
        self.assertIn("beta", verdict.detail)

    def test_total_over_budget_fails(self) -> None:
        ledger = UsageLedger()
        ledger.record(_record(model="m1", total_tokens=30))
        ledger.record(_record(model="m2", total_tokens=30))
        ledger.record(_record(model="m3", total_tokens=30))
        budget = DryRunBudget(currency="USD", max_total_tokens=80,
                              max_tokens_per_model=50)
        verdict = ledger.verify_budget(budget)
        self.assertFalse(verdict.passed)
        self.assertIn("total", verdict.detail.lower())
        self.assertIn("USD", verdict.detail)

    def test_both_per_model_and_total_fail_are_reported(self) -> None:
        ledger = UsageLedger()
        ledger.record(_record(model="hog", total_tokens=200))
        ledger.record(_record(model="m2", total_tokens=50))
        budget = DryRunBudget(currency="EUR", max_total_tokens=100,
                              max_tokens_per_model=80)
        verdict = ledger.verify_budget(budget)
        self.assertFalse(verdict.passed)
        self.assertIn("hog", verdict.detail)
        self.assertIn("total", verdict.detail.lower())
        self.assertIn("EUR", verdict.detail)


if __name__ == "__main__":
    unittest.main()
