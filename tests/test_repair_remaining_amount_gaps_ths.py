import unittest

import pandas as pd

from tools.repair_remaining_amount_gaps_ths import (
    parse_trade_pairs,
    repair_frame,
)


def current_frame(volume: float = 350_300.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["1992-11-17"],
            "open": [32.36],
            "high": [34.088],
            "low": [30.416],
            "close": [33.008],
            "close_raw": [16.3],
            "volume": [volume],
            "amount": [pd.NA],
            "turnover": [0.6175238931329333],
            "market_cap": [924_642_765.0],
        }
    )


class RemainingAmountGapsThsTests(unittest.TestCase):
    def test_parses_exact_trade_pair(self):
        frame = pd.DataFrame(
            {
                "股票代码": ["000002.SZ"],
                "成交量[19921117]": [1_751_500.0],
                "成交额[19921117]": [27_691_600.0],
            }
        )

        values = parse_trade_pairs(frame, allowed={("000002", "1992-11-17")})

        self.assertEqual(1_751_500.0, values[("000002", "1992-11-17")]["volume"])

    def test_prefers_compatible_ths_pair_and_recomputes_turnover(self):
        repaired, counts = repair_frame(
            current_frame(),
            pd.DataFrame(),
            {"1992-11-17": {"volume": 1_751_500.0, "amount": 27_691_600.0}},
            price_tolerance=0.001,
            amount_match_rtol=0.001,
            amount_match_atol=1000.0,
        )

        self.assertEqual(1_751_500.0, repaired.iloc[0]["volume"])
        self.assertEqual(27_691_600.0, repaired.iloc[0]["amount"])
        self.assertEqual(1, counts["filled_ths_pair"])
        self.assertEqual(1, counts["turnover_recomputed"])

    def test_uses_legacy_volume_only_when_amount_agrees_and_vwap_passes(self):
        legacy = pd.DataFrame(
            {
                "date": ["1992-11-17"],
                "volume": [1_751_500.0],
                "amount": [27_691_000.0],
            }
        )
        repaired, counts = repair_frame(
            current_frame(),
            legacy,
            {"1992-11-17": {"volume": 400_000.0, "amount": 27_691_600.0}},
            price_tolerance=0.001,
            amount_match_rtol=0.001,
            amount_match_atol=1000.0,
        )

        self.assertEqual(1_751_500.0, repaired.iloc[0]["volume"])
        self.assertEqual(1, counts["filled_legacy_volume_ths_amount"])

    def test_rejects_legacy_fallback_when_amounts_disagree(self):
        legacy = pd.DataFrame(
            {
                "date": ["1992-11-17"],
                "volume": [1_751_500.0],
                "amount": [20_000_000.0],
            }
        )
        repaired, counts = repair_frame(
            current_frame(),
            legacy,
            {"1992-11-17": {"volume": 400_000.0, "amount": 27_691_600.0}},
            price_tolerance=0.001,
            amount_match_rtol=0.001,
            amount_match_atol=1000.0,
        )

        self.assertTrue(pd.isna(repaired.iloc[0]["amount"]))
        self.assertEqual(1, counts["unresolved"])

    def test_normalises_unique_ths_volume_unit_before_filling_amount(self):
        current = pd.DataFrame(
            {
                "date": ["1992-01-13"],
                "open": [86.4],
                "high": [86.4],
                "low": [86.4],
                "close": [86.4],
                "close_raw": [86.4],
                "volume": [15_000.0],
                "amount": [pd.NA],
                "turnover": [1.15119],
                "market_cap": [112_579_200.0],
            }
        )

        repaired, counts = repair_frame(
            current,
            pd.DataFrame(),
            {"1992-01-13": {"volume": 15_000.0, "amount": 129_600.0}},
            price_tolerance=0.02,
            amount_match_rtol=0.001,
            amount_match_atol=1000.0,
        )

        self.assertEqual(1_500.0, repaired.iloc[0]["volume"])
        self.assertEqual(129_600.0, repaired.iloc[0]["amount"])
        self.assertEqual(1, counts["filled_ths_pair_normalised_volume"])
        self.assertEqual(1, counts["volume_multiplier_0p1"])
        self.assertEqual(1, counts["turnover_recomputed"])

    def test_normalises_unique_legacy_volume_unit_when_ths_has_no_pair(self):
        current = pd.DataFrame(
            {
                "date": ["1990-12-19"],
                "open": [365.7],
                "high": [384.0],
                "low": [365.7],
                "close": [384.0],
                "close_raw": [384.0],
                "volume": [116_000.0],
                "amount": [pd.NA],
                "turnover": [23.625255],
                "market_cap": [188_544_000.0],
            }
        )
        legacy = pd.DataFrame(
            {"date": ["1990-12-19"], "volume": [116_000.0], "amount": [443_000.0]}
        )

        repaired, counts = repair_frame(
            current,
            legacy,
            {"1990-12-19": {}},
            price_tolerance=0.02,
            amount_match_rtol=0.001,
            amount_match_atol=1000.0,
        )

        self.assertEqual(1_160.0, repaired.iloc[0]["volume"])
        self.assertEqual(443_000.0, repaired.iloc[0]["amount"])
        self.assertEqual(1, counts["filled_legacy_pair_normalised_volume"])
        self.assertEqual(1, counts["volume_multiplier_0p01"])

    def test_rejects_pair_when_more_than_one_unit_multiplier_fits(self):
        current = current_frame()
        current.loc[0, "low"] = 1.0
        current.loc[0, "high"] = 100.0
        current.loc[0, "close"] = 50.0
        current.loc[0, "close_raw"] = 50.0

        repaired, counts = repair_frame(
            current,
            pd.DataFrame(),
            {"1992-11-17": {"volume": 1_000.0, "amount": 10_000.0}},
            price_tolerance=0.02,
            amount_match_rtol=0.001,
            amount_match_atol=1000.0,
        )

        self.assertTrue(pd.isna(repaired.iloc[0]["amount"]))
        self.assertEqual(1, counts["unresolved"])


if __name__ == "__main__":
    unittest.main()
