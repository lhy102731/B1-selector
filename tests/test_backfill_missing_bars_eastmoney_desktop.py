import unittest

import pandas as pd

from tools.backfill_missing_bars_eastmoney_desktop import (
    AffineTransform,
    bracketed_local_transform,
    canonical_affine_for_date,
    corporate_action_transform_from_calibration,
    corporate_action_transform,
    fit_affine,
    insert_rows,
    nearby_trade_fingerprint,
    strict_trade_source_gate,
    validated_trade_source,
    validate_adjusted_bar,
)


class EastmoneyDesktopMissingBarsTests(unittest.TestCase):
    @staticmethod
    def source_row() -> pd.Series:
        return pd.Series(
            {
                "em_open_raw": 9.2,
                "em_high_raw": 9.8,
                "em_low_raw": 9.1,
                "em_close_raw": 9.55,
                "em_volume": 100_000.0,
                "em_amount": 950_000.0,
                "legacy_volume": 100_000.0,
                "legacy_amount": 950_000.0,
                "ths_raw_status": "complete",
            }
        )

    def test_trade_gate_accepts_rounded_ths_prices_but_rejects_real_conflict(self):
        source = self.source_row()
        ths = pd.Series(
            {
                "open_raw": 9.2,
                "high_raw": 9.8,
                "low_raw": 9.1,
                "close_raw": 9.56,
            }
        )

        passed, _, _ = strict_trade_source_gate(source, ths)
        self.assertTrue(passed)
        ths["close_raw"] = 9.65
        passed, reason, _ = strict_trade_source_gate(source, ths)
        self.assertFalse(passed)
        self.assertEqual("em_ths_price_conflict", reason)

    def test_complete_ths_bar_replaces_a_conflicting_eastmoney_bar(self):
        source = self.source_row()
        source["em_amount"] = 930_000.0
        ths = pd.Series(
            {
                "open_raw": 9.2,
                "high_raw": 9.8,
                "low_raw": 9.1,
                "close_raw": 9.55,
                "volume": 100_000.0,
                "amount": 950_000.0,
            }
        )

        selected, label, passed, reason, _ = validated_trade_source(source, ths)

        self.assertTrue(passed)
        self.assertEqual("trade_source_passed", reason)
        self.assertEqual("ths_wencai_fallback", label)
        self.assertEqual(950_000.0, selected["em_amount"])

    def test_human_approved_source_can_skip_only_the_legacy_trade_crosscheck(self):
        source = self.source_row()
        source["ths_raw_status"] = "absent"
        source["em_amount"] = 930_000.0

        passed, reason, _ = strict_trade_source_gate(
            source,
            None,
            require_legacy_trade_crosscheck=False,
        )

        self.assertTrue(passed)
        self.assertEqual("trade_source_passed", reason)

    def test_affine_fit_allows_half_mill_quantization_at_low_prices(self):
        x = pd.Series([2.0 + index * 0.01 for index in range(20)])
        frame = pd.DataFrame(
            {
                "raw": x,
                "adjusted": x
                + pd.Series(
                    [
                        0.0005,
                        -0.0005,
                        -0.0005,
                        -0.0005,
                        0.0005,
                        0.0005,
                        -0.0005,
                        0.0005,
                        0.0005,
                        0.0005,
                        -0.0005,
                        0.0005,
                        -0.0005,
                        -0.0005,
                        0.0005,
                        -0.0005,
                        -0.0005,
                        -0.0005,
                        0.0005,
                        0.0005,
                    ]
                ),
            }
        )

        rejected, _ = fit_affine(
            frame,
            x_column="raw",
            y_column="adjusted",
            min_points=20,
            min_variation=0.005,
            max_absolute_error=0.002,
            max_relative_error=0.0002,
        )
        accepted, diagnostics = fit_affine(
            frame,
            x_column="raw",
            y_column="adjusted",
            min_points=20,
            min_variation=0.005,
            max_absolute_error=0.002,
            max_relative_error=0.0002,
            quantization_floor=0.00055,
        )

        self.assertIsNone(rejected)
        self.assertIsNotNone(accepted)
        self.assertEqual("stable_affine", diagnostics["fit_status"])

    def test_target_adjustment_crosscheck_uses_ths_raw_price(self):
        actions = pd.DataFrame(
            {
                "date": [pd.Timestamp("1995-01-01")],
                "bonus_ratio": [4.0],
                "cash_per_share": [0.0],
                "rights_ratio": [0.0],
                "rights_price": [0.0],
                "consideration_stock_ratio": [0.0],
                "consideration_cash_per_share": [0.0],
            }
        )
        calibration = (
            1.0,
            0.0,
            {
                "fit_max_absolute_error": 0.0005,
                "holdout_max_absolute_error": 0.0005,
            },
        )

        transform = corporate_action_transform_from_calibration(
            calibration,
            actions,
            target_date="1996-04-01",
            target_raw_close=9.99,
            target_validation_raw_close=10.0,
            target_adjusted_close=50.0,
        )

        self.assertIsNotNone(transform)
        assert transform is not None
        self.assertAlmostEqual(49.95, transform.slope * 9.99 + transform.intercept)
        self.assertAlmostEqual(0.0, transform.diagnostics["target_adjusted_close_error"])

    def test_bracketed_local_transform_requires_anchors_on_both_sides(self):
        before = pd.date_range("1996-03-20", periods=5, freq="D")
        after = pd.date_range("1996-04-02", periods=5, freq="D")
        raw = pd.Series([7.0 + index * 0.1 for index in range(10)])
        trusted = pd.DataFrame(
            {
                "date": before.append(after),
                "close_raw": raw,
                "close": (35.36 * raw - 43.0639).round(3),
            }
        )

        transform = bracketed_local_transform(
            trusted,
            target_date="1996-04-01",
            target_raw_close=7.48,
            target_adjusted_close=221.43,
            minimum_each_side=5,
        )
        one_sided = bracketed_local_transform(
            trusted.loc[pd.to_datetime(trusted["date"]).lt("1996-04-01")],
            target_date="1996-04-01",
            target_raw_close=7.48,
            target_adjusted_close=221.43,
            minimum_each_side=5,
        )

        self.assertIsNotNone(transform)
        self.assertIsNone(one_sided)

    def test_affine_action_chain_handles_rights_bonus_and_cash(self):
        actions = pd.DataFrame(
            {
                "date": pd.to_datetime(["1994-03-19", "1994-03-21", "1995-08-09"]),
                "bonus_ratio": [0.0, 0.5, 0.0],
                "cash_per_share": [0.0, 0.0, 0.1],
                "rights_ratio": [0.3, 0.0, 0.0],
                "rights_price": [3.8, 0.0, 0.0],
                "consideration_stock_ratio": [0.0, 0.0, 0.0],
                "consideration_cash_per_share": [0.0, 0.0, 0.0],
                "description": ["rights", "bonus", "cash"],
            }
        )

        slope, intercept = canonical_affine_for_date(actions, "1996-04-16")

        self.assertAlmostEqual(1.95, slope)
        self.assertAlmostEqual(-0.945, intercept)

    def test_action_transform_is_validated_on_untouched_history(self):
        actions = pd.DataFrame(
            {
                "date": pd.to_datetime(["1994-03-19", "1994-03-21", "1995-08-09"]),
                "bonus_ratio": [0.0, 0.5, 0.0],
                "cash_per_share": [0.0, 0.0, 0.1],
                "rights_ratio": [0.3, 0.0, 0.0],
                "rights_price": [3.8, 0.0, 0.0],
                "consideration_stock_ratio": [0.0, 0.0, 0.0],
                "consideration_cash_per_share": [0.0, 0.0, 0.0],
                "description": ["rights", "bonus", "cash"],
            }
        )
        dates = pd.date_range("1996-04-16", periods=30, freq="D")
        raw = pd.Series([3.0 + index * 0.05 for index in range(30)])
        trusted = pd.DataFrame(
            {
                "date": dates,
                "close_raw": raw,
                "close": (1.95 * raw - 0.945).round(3),
            }
        )

        transform = corporate_action_transform(
            trusted,
            actions,
            target_date="1993-08-06",
            target_raw_close=9.55,
            target_adjusted_close=None,
        )

        self.assertIsNotNone(transform)
        assert transform is not None
        self.assertAlmostEqual(1.0, transform.slope, places=3)
        self.assertAlmostEqual(0.0, transform.intercept, places=3)

    def test_unsupported_split_action_fails_closed(self):
        trusted = pd.DataFrame(
            {
                "date": pd.date_range("1991-03-20", periods=30, freq="D"),
                "close_raw": [10.0 + index for index in range(30)],
                "close": [10.0 + index for index in range(30)],
            }
        )
        actions = pd.DataFrame(
            {
                "date": [pd.Timestamp("1991-03-11")],
                "bonus_ratio": [0.0],
                "cash_per_share": [0.0],
                "rights_ratio": [0.0],
                "rights_price": [0.0],
                "consideration_stock_ratio": [0.0],
                "consideration_cash_per_share": [0.0],
                "description": ["每十股拆成50股"],
            }
        )

        self.assertIsNone(
            corporate_action_transform(
                trusted,
                actions,
                target_date="1990-12-19",
                target_raw_close=100.0,
                target_adjusted_close=None,
            )
        )

    def test_nearby_fingerprint_catches_date_shift(self):
        current = pd.DataFrame(
            {
                "date": ["1992-04-18"],
                "close_raw": [15.0],
                "volume": [760_500.0],
                "amount": [11_407_500.0],
            }
        )

        duplicate = nearby_trade_fingerprint(
            current,
            target_date="1992-04-20",
            close_raw=15.0,
            volume=760_500.0,
            amount=11_407_500.0,
        )

        self.assertEqual("1992-04-18", duplicate)

    def test_insert_recomputes_inserted_and_successor_returns(self):
        current = pd.DataFrame(
            {
                "date": ["1997-02-19", "1997-02-17"],
                "open": [12.0, 10.0],
                "high": [13.0, 11.0],
                "low": [11.0, 9.0],
                "close": [12.5, 10.0],
                "close_raw": [12.5, 10.0],
                "volume": [100.0, 100.0],
                "amount": [1_200.0, 1_000.0],
                "turnover": [1.0, 1.0],
                "change_pct": [999.0, 999.0],
                "pe_dynamic": [pd.NA, pd.NA],
                "pb": [pd.NA, pd.NA],
                "ps": [pd.NA, pd.NA],
                "pcf": [pd.NA, pd.NA],
                "market_cap": [1_250.0, 1_000.0],
                "amplitude": [999.0, 999.0],
                "change": [999.0, 999.0],
            }
        )
        row = {column: pd.NA for column in current.columns}
        row.update(
            {
                "date": "1997-02-18",
                "open": 10.5,
                "high": 12.0,
                "low": 10.0,
                "close": 11.0,
                "close_raw": 11.0,
                "volume": 100.0,
                "amount": 1_100.0,
                "turnover": 1.0,
                "market_cap": 1_100.0,
            }
        )

        result = insert_rows(current, [row])

        inserted = result.loc[result["date"].eq("1997-02-18")].iloc[0]
        successor = result.loc[result["date"].eq("1997-02-19")].iloc[0]
        self.assertAlmostEqual(10.0, inserted["change_pct"])
        self.assertAlmostEqual((12.5 / 11.0 - 1.0) * 100.0, successor["change_pct"])

    def test_adjusted_affine_preserves_ohlc_envelope(self):
        transform = AffineTransform(1.95, -0.945, "test", {})
        adjusted, reason = validate_adjusted_bar(
            transform,
            {"open": 3.4, "high": 3.8, "low": 3.2, "close": 3.6},
        )

        self.assertIsNone(reason)
        assert adjusted is not None
        self.assertGreaterEqual(adjusted["high"], adjusted["close"])
        self.assertLessEqual(adjusted["low"], adjusted["open"])


if __name__ == "__main__":
    unittest.main()
