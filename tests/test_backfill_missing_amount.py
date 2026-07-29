import unittest

import pandas as pd

from tools.backfill_missing_amount import merge_amount


class MissingAmountBackfillTests(unittest.TestCase):
    def current(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": ["2001-04-20", "2001-04-19"],
                "low": [471.179, 470.0],
                "high": [480.665, 480.0],
                "close": [471.811, 475.0],
                "close_raw": [16.2, 16.3],
                "volume": [10_589_100.0, 10_000_000.0],
                "amount": [pd.NA, 160_000_000.0],
                "turnover": [1.0, 1.0],
            }
        )

    def test_exact_date_compatible_amount_is_filled(self):
        legacy = pd.DataFrame(
            {
                "date": ["2001-04-20"],
                "volume": [10_589_130.0],
                "amount": [172_437_825.22],
            }
        )

        merged, stats = merge_amount(
            self.current(),
            legacy,
            max_volume_relative_diff=0.01,
            price_tolerance=0.001,
        )

        self.assertEqual(172_437_825.22, merged.iloc[0]["amount"])
        self.assertEqual(1, stats["filled_amount"])
        self.assertEqual(160_000_000.0, merged.iloc[1]["amount"])

    def test_large_volume_mismatch_is_rejected(self):
        legacy = pd.DataFrame(
            {
                "date": ["2001-04-20"],
                "volume": [5_000_000.0],
                "amount": [172_437_825.22],
            }
        )

        merged, stats = merge_amount(
            self.current(),
            legacy,
            max_volume_relative_diff=0.01,
            price_tolerance=0.001,
        )

        self.assertTrue(pd.isna(merged.iloc[0]["amount"]))
        self.assertEqual(1, stats["rejected_volume_mismatch"])

    def test_vwap_outside_ths_raw_ohlc_is_rejected(self):
        legacy = pd.DataFrame(
            {
                "date": ["2001-04-20"],
                "volume": [10_589_100.0],
                "amount": [300_000_000.0],
            }
        )

        merged, stats = merge_amount(
            self.current(),
            legacy,
            max_volume_relative_diff=0.01,
            price_tolerance=0.001,
        )

        self.assertTrue(pd.isna(merged.iloc[0]["amount"]))
        self.assertEqual(1, stats["rejected_vwap_outside_raw_ohlc"])


if __name__ == "__main__":
    unittest.main()
