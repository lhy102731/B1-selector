import unittest

import pandas as pd

from tools.repair_amount_ohlc_conflicts_ths import repair_frame


def frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2024-01-03", "2024-01-02", "2024-01-01"],
            "open": [22.0, 20.0, 18.0],
            "high": [22.0, 21.0, 18.0],
            "low": [21.0, 19.0, 18.0],
            "close": [22.0, 20.0, 18.0],
            "close_raw": [11.0, 10.0, 9.0],
            "volume": [1000.0, 5000.0, 1000.0],
            "amount": [11_000.0, pd.NA, 9_000.0],
            "turnover": [1.0, 5.0, 1.0],
            "market_cap": [1_100_000.0, 1_000_000.0, 900_000.0],
            "change": [2.0, 2.0, pd.NA],
            "change_pct": [10.0, 11.111111, pd.NA],
            "amplitude": [5.0, 11.111111, pd.NA],
        }
    )


def full_bar(close: float = 10.0) -> dict[str, float]:
    return {
        "open_raw": 9.5,
        "high_raw": 10.5,
        "low_raw": 9.0,
        "close_raw": close,
        "volume": 5_000.0,
        "amount": 49_500.0,
        "turnover": 5.0,
        "market_cap": close * 100_000.0,
    }


class RepairAmountOhlcConflictsThsTests(unittest.TestCase):
    def test_repairs_with_same_day_close_anchor(self):
        repaired, counts, details = repair_frame(
            frame(),
            {"2024-01-02": full_bar()},
            price_tolerance=0.02,
            raw_close_rtol=0.001,
            raw_close_atol=0.01,
            factor_window=1,
            factor_relative_tolerance=0.01,
        )

        row = repaired.loc[repaired["date"].eq("2024-01-02")].iloc[0]
        self.assertEqual(19.0, row["open"])
        self.assertEqual(21.0, row["high"])
        self.assertEqual(18.0, row["low"])
        self.assertEqual(49_500.0, row["amount"])
        self.assertEqual(1, counts["factor_same_day_close_anchor"])
        self.assertEqual("same_day_close_anchor", details[0]["factor_method"])

    def test_repairs_with_two_sided_factor_when_raw_close_differs(self):
        bar = full_bar(close=8.0)
        bar["open_raw"], bar["high_raw"], bar["low_raw"] = 7.5, 8.5, 7.0
        bar["amount"] = 39_500.0
        bar["market_cap"] = 800_000.0

        repaired, counts, _ = repair_frame(
            frame(),
            {"2024-01-02": bar},
            price_tolerance=0.02,
            raw_close_rtol=0.001,
            raw_close_atol=0.01,
            factor_window=1,
            factor_relative_tolerance=0.01,
        )

        row = repaired.loc[repaired["date"].eq("2024-01-02")].iloc[0]
        self.assertEqual(16.0, row["close"])
        self.assertEqual(8.0, row["close_raw"])
        self.assertEqual(1, counts["factor_two_sided_factor"])

    def test_rejects_full_bar_with_impossible_vwap(self):
        bar = full_bar()
        bar["amount"] = 500.0

        repaired, counts, _ = repair_frame(
            frame(),
            {"2024-01-02": bar},
            price_tolerance=0.02,
            raw_close_rtol=0.001,
            raw_close_atol=0.01,
            factor_window=1,
            factor_relative_tolerance=0.01,
        )

        row = repaired.loc[repaired["date"].eq("2024-01-02")].iloc[0]
        self.assertTrue(pd.isna(row["amount"]))
        self.assertEqual(1, counts["rejected_vwap_outside_raw_ohlc"])


if __name__ == "__main__":
    unittest.main()
