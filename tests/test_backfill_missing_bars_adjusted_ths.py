import unittest

import pandas as pd

from tools.backfill_missing_bars_adjusted_ths import (
    calibrate_adjusted_scale,
    insert_adjusted_bars,
    parse_adjusted_opens,
)


class MissingBarsAdjustedThsTests(unittest.TestCase):
    def test_parse_post_adjusted_open(self):
        frame = pd.DataFrame(
            {
                "股票代码": ["000028.SZ"],
                "开盘价:后复权[19930809]": [10.1],
                "开盘价:不复权[19930809]": [9.85],
            }
        )

        values = parse_adjusted_opens(
            frame, allowed_pairs={("000028", "1993-08-09")}
        )

        self.assertEqual(10.1, values[("000028", "1993-08-09")])

    def test_calibration_aligns_provider_scale_and_rejects_one_bad_anchor(self):
        anchors = {
            "000529": {
                "1996-04-16": 12.0,
                "1996-04-17": 13.2,
                "1996-04-18": 14.4,
            }
        }
        values = {
            ("000529", "1996-04-16"): 10.0,
            ("000529", "1996-04-17"): 11.0,
            ("000529", "1996-04-18"): 12.0,
        }

        scales, details = calibrate_adjusted_scale(
            anchors,
            values,
            relative_tolerance=0.01,
            absolute_tolerance=0.001,
            minimum_matches=3,
            minimum_return_ratio=1.0,
        )

        self.assertAlmostEqual(1.2, scales["000529"])
        self.assertTrue(details["000529"]["passed"])
        values[("000529", "1996-04-17")] = 10.5
        scales, details = calibrate_adjusted_scale(
            anchors,
            values,
            relative_tolerance=0.01,
            absolute_tolerance=0.001,
            minimum_matches=3,
            minimum_return_ratio=1.0,
        )
        self.assertIsNone(scales["000529"])
        self.assertFalse(details["000529"]["passed"])

    def test_calibration_never_lowers_minimum_anchor_count(self):
        for anchors, values in (
            ({"000517": {}}, {}),
            (
                {"000517": {"1996-04-16": 5.763}},
                {("000517", "1996-04-16"): 5.76},
            ),
        ):
            scales, details = calibrate_adjusted_scale(
                anchors,
                values,
                relative_tolerance=0.01,
                absolute_tolerance=0.001,
                minimum_matches=2,
                minimum_return_ratio=0.5,
            )
            self.assertIsNone(scales["000517"])
            self.assertFalse(details["000517"]["passed"])

    def test_calibration_allows_missing_values_only_above_fixed_gate(self):
        anchors = {
            "000517": {
                "1996-04-16": 5.76,
                "1996-04-17": 5.82,
                "1996-04-18": 5.90,
                "1996-04-19": 6.00,
            }
        }
        values = {
            ("000517", "1996-04-17"): 5.82,
            ("000517", "1996-04-19"): 6.00,
        }

        scales, details = calibrate_adjusted_scale(
            anchors,
            values,
            relative_tolerance=0.01,
            absolute_tolerance=0.001,
            minimum_matches=2,
            minimum_return_ratio=0.5,
        )

        self.assertAlmostEqual(1.0, scales["000517"])
        self.assertEqual(2, details["000517"]["available_anchors"])

    @staticmethod
    def current_frame():
        return pd.DataFrame(
            {
                "date": ["1993-08-11"],
                "open": [12.0],
                "high": [13.0],
                "low": [11.5],
                "close": [12.7],
                "close_raw": [12.7],
                "volume": [1_120_300.0],
                "amount": [12_625_415.0],
                "turnover": [6.78969697],
                "change_pct": [pd.NA],
                "pe_dynamic": [10.0],
                "pb": [1.0],
                "ps": [2.0],
                "pcf": [3.0],
                "market_cap": [20_000_000.0],
                "amplitude": [pd.NA],
                "change": [pd.NA],
            }
        )

    @staticmethod
    def raw_bars():
        return {
            "1993-08-09": {
                "open_raw": 9.85,
                "high_raw": 10.3,
                "low_raw": 9.5,
                "close_raw": 10.1,
                "volume": 1_170_200.0,
                "amount": 11_533_905.0,
                "turnover": 7.09212121,
            },
            "1993-08-10": {
                "open_raw": 10.2,
                "high_raw": 10.8,
                "low_raw": 10.2,
                "close_raw": 10.7,
                "volume": 1_334_200.0,
                "amount": 14_206_950.0,
                "turnover": 8.08606061,
            },
        }

    def test_uses_adjusted_open_over_raw_open_and_recomputes_successor(self):
        merged, stats, _ = insert_adjusted_bars(
            self.current_frame(),
            "000028",
            self.raw_bars(),
            {"1993-08-09": 19.7, "1993-08-10": 20.4},
            calibration_scale=0.5,
            price_tolerance=0.001,
        )

        self.assertEqual(2, stats["inserted"])
        inserted = merged.loc[merged["date"].eq("1993-08-09")].iloc[0]
        self.assertAlmostEqual(9.85, inserted["open"])
        self.assertAlmostEqual(10.1, inserted["close"])
        self.assertTrue(pd.isna(inserted["pe_dynamic"]))
        existing = merged.loc[merged["date"].eq("1993-08-11")].iloc[0]
        self.assertAlmostEqual((12.7 / 10.7 - 1.0) * 100.0, existing["change_pct"])

    def test_rejects_adjusted_open_close_factor_mismatch(self):
        bars = {"1993-08-09": self.raw_bars()["1993-08-09"]}
        _, stats, _ = insert_adjusted_bars(
            self.current_frame(),
            "000028",
            bars,
            {"1993-08-09": 9.85},
            calibration_scale=1.0,
            adjusted_close_by_day={"1993-08-09": 20.2},
            factor_crosscheck_tolerance=0.01,
            price_tolerance=0.001,
        )

        self.assertEqual(1, stats["rejected_adjusted_open_close_factor_mismatch"])

    def test_rejects_rows_when_adjustment_anchors_fail(self):
        bars = {"1993-08-09": self.raw_bars()["1993-08-09"]}
        _, stats, _ = insert_adjusted_bars(
            self.current_frame(),
            "000028",
            bars,
            {"1993-08-09": 9.85},
            calibration_scale=None,
            price_tolerance=0.001,
        )

        self.assertEqual(1, stats["rejected_anchor_validation"])


if __name__ == "__main__":
    unittest.main()
