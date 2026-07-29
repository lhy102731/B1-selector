import unittest

import pandas as pd

from tools.repair_historical_trade_units import repair_frame


class HistoricalTradeUnitRepairTests(unittest.TestCase):
    def test_repairs_volume_and_recomputes_turnover_from_market_cap(self):
        rows = 25
        frame = pd.DataFrame(
            {
                "date": pd.date_range("1991-01-01", periods=rows).strftime("%Y-%m-%d"),
                "open": [14.8] * rows,
                "high": [15.5] * rows,
                "low": [14.5] * rows,
                "close": [15.0] * rows,
                "close_raw": [15.0] * rows,
                "volume": [150_000.0] * rows,
                "amount": [22_500.0] * rows,
                "turnover": [150.0] * rows,
                "market_cap": [1_500_000.0] * rows,
                "pe_dynamic": [10.0] * rows,
            }
        )

        repaired, audit = repair_frame(frame)

        self.assertTrue((repaired["volume"] == 1_500.0).all())
        self.assertTrue((repaired["turnover"] == 1.5).all())
        self.assertTrue((repaired["market_cap"] == 1_500_000.0).all())
        self.assertTrue((repaired["pe_dynamic"] == 10.0).all())
        self.assertEqual(rows, audit["volume_changed"])
        self.assertEqual(rows, audit["turnover_recomputed"])

    def test_isolated_mismatch_is_not_repaired_and_amount_is_cleared(self):
        frame = pd.DataFrame(
            {
                "date": ["1991-01-01"],
                "open": [14.8],
                "high": [15.5],
                "low": [14.5],
                "close": [15.0],
                "close_raw": [15.0],
                "volume": [150_000.0],
                "amount": [22_500.0],
                "turnover": [150.0],
                "market_cap": [1_500_000.0],
            }
        )

        repaired, audit = repair_frame(frame)

        self.assertEqual(150_000.0, repaired.iloc[0]["volume"])
        self.assertTrue(pd.isna(repaired.iloc[0]["amount"]))
        self.assertEqual(0, audit["volume_changed"])
        self.assertEqual(1, audit["amount_cleared"])

    def test_ths_reference_amount_enables_reciprocal_regime_repair(self):
        rows = 20
        frame = pd.DataFrame(
            {
                "date": pd.date_range("1991-01-01", periods=rows).strftime("%Y-%m-%d"),
                "open": [14.8] * rows,
                "high": [15.5] * rows,
                "low": [14.5] * rows,
                "close": [15.0] * rows,
                "close_raw": [15.0] * rows,
                "volume": [37_500.0] * rows,
                "amount": [pd.NA] * rows,
                "turnover": [37.5] * rows,
                "market_cap": [1_500_000.0] * rows,
            }
        )
        amounts = {day: 22_500.0 for day in frame["date"]}

        repaired, audit = repair_frame(frame, amounts)

        self.assertTrue((repaired["volume"] == 1_500.0).all())
        self.assertTrue((repaired["amount"] == 22_500.0).all())
        self.assertEqual(rows, audit["volume_changed"])
        self.assertEqual(rows, audit["amount_filled_from_ths"])

    def test_ths_reference_amount_fills_without_changing_valid_volume(self):
        frame = pd.DataFrame(
            {
                "date": ["1991-01-01"],
                "open": [14.8],
                "high": [15.5],
                "low": [14.5],
                "close": [15.0],
                "close_raw": [15.0],
                "volume": [1_500.0],
                "amount": [pd.NA],
                "turnover": [1.5],
                "market_cap": [1_500_000.0],
            }
        )

        repaired, audit = repair_frame(frame, {"1991-01-01": 22_500.0})

        self.assertEqual(1_500.0, repaired.iloc[0]["volume"])
        self.assertEqual(22_500.0, repaired.iloc[0]["amount"])
        self.assertEqual(0, audit["volume_changed"])
        self.assertEqual(1, audit["amount_filled_from_ths"])


if __name__ == "__main__":
    unittest.main()
