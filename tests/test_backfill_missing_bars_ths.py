import unittest

import pandas as pd

from tools.backfill_missing_bars_ths import insert_bars, parse_wencai_bars


class MissingBarsThsTests(unittest.TestCase):
    def test_parse_wencai_raw_bar(self):
        frame = pd.DataFrame(
            {
                "股票代码": ["000001.SZ"],
                "开盘价:不复权[19970218]": [19.0],
                "最高价:不复权[19970218]": [19.09],
                "最低价:不复权[19970218]": [17.3],
                "收盘价:不复权[19970218]": [17.59],
                "成交量[19970218]": [63_657_292],
                "成交额[19970218]": [1_145_976_680],
                "换手率[19970218]": [8.91030887],
                "a股市值(不含限售股)[19970218]": [12_566_700_000],
            }
        )

        values = parse_wencai_bars(
            frame, allowed_pairs={("000001", "1997-02-18")}
        )

        bar = values[("000001", "1997-02-18")]
        self.assertEqual(17.59, bar["close_raw"])
        self.assertEqual(63_657_292.0, bar["volume"])

    def test_insert_uses_two_sided_factor_and_recomputes_next_change(self):
        current = pd.DataFrame(
            {
                "date": ["1997-02-19", "1997-02-17"],
                "open": [300.0, 310.0],
                "high": [310.0, 315.0],
                "low": [295.0, 305.0],
                "close": [306.892, 311.757],
                "close_raw": [18.92, 19.22],
                "volume": [45_450_401, 20_000_000],
                "amount": [842_649_085, 380_000_000],
                "turnover": [6.361834, 2.506498],
                "change_pct": [-1.0, 1.0],
                "pe_dynamic": [10.0, 10.0],
                "pb": [1.0, 1.0],
                "ps": [2.0, 2.0],
                "pcf": [3.0, 3.0],
                "market_cap": [13_516_880_000, 13_731_210_000],
                "amplitude": [1.0, 1.0],
                "change": [-3.0, 3.0],
            }
        )
        bar = {
            "open_raw": 19.0,
            "high_raw": 19.09,
            "low_raw": 17.3,
            "close_raw": 17.59,
            "volume": 63_657_292.0,
            "amount": 1_145_976_680.0,
            "turnover": 8.91030887,
            "market_cap": 12_566_700_000.0,
        }

        merged, stats, _ = insert_bars(
            current,
            "000001",
            {"1997-02-18": bar},
            factor_window=5,
            factor_relative_tolerance=0.01,
            price_tolerance=0.001,
        )

        self.assertEqual(1, stats["inserted"])
        inserted = merged.loc[merged["date"].eq("1997-02-18")].iloc[0]
        self.assertEqual(17.59, inserted["close_raw"])
        self.assertTrue(pd.isna(inserted["pe_dynamic"]))
        following = merged.loc[merged["date"].eq("1997-02-19")].iloc[0]
        self.assertAlmostEqual(
            (following["close"] / inserted["close"] - 1.0) * 100.0,
            following["change_pct"],
        )

    def test_one_sided_history_is_rejected(self):
        current = pd.DataFrame(
            {
                "date": ["1997-02-19"],
                "open": [300.0],
                "high": [310.0],
                "low": [295.0],
                "close": [306.892],
                "close_raw": [18.92],
                "volume": [45_450_401],
                "amount": [842_649_085],
                "turnover": [6.361834],
                "change_pct": [pd.NA],
                "pe_dynamic": [pd.NA],
                "pb": [pd.NA],
                "ps": [pd.NA],
                "pcf": [pd.NA],
                "market_cap": [13_516_880_000],
                "amplitude": [pd.NA],
                "change": [pd.NA],
            }
        )
        bar = {
            "open_raw": 19.0,
            "high_raw": 19.09,
            "low_raw": 17.3,
            "close_raw": 17.59,
            "volume": 63_657_292.0,
            "amount": 1_145_976_680.0,
            "turnover": 8.91030887,
        }

        _, stats, _ = insert_bars(
            current,
            "000001",
            {"1997-02-18": bar},
            factor_window=5,
            factor_relative_tolerance=0.01,
            price_tolerance=0.001,
        )

        self.assertEqual(1, stats["rejected_missing_two_sided_factor_anchor"])


if __name__ == "__main__":
    unittest.main()
