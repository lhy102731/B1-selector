import unittest

import pandas as pd

from tools.backfill_missing_bars_actions_ths import reconstruct_adjusted_closes


def current_frame(start: str, rows: int, raw: float, scale: float) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=rows)
    return pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "close": [raw * scale] * rows,
            "close_raw": [raw] * rows,
        }
    )


def raw_bar(close: float) -> dict[str, float]:
    return {
        "open_raw": close,
        "high_raw": close * 1.01,
        "low_raw": close * 0.99,
        "close_raw": close,
        "volume": 1_000.0,
        "amount": close * 1_000.0,
        "turnover": 1.0,
    }


class MissingBarsActionsThsTests(unittest.TestCase):
    def test_accepts_full_overlap_stable_scale(self):
        current = current_frame("2000-01-03", 30, raw=10.0, scale=1.2)
        bars = {"1999-12-30": raw_bar(9.5)}

        values, audit = reconstruct_adjusted_closes(
            current,
            bars,
            pd.DataFrame(),
            minimum_overlap_rows=20,
            full_p99_tolerance=0.001,
            full_max_tolerance=0.005,
            leading_tolerance=0.001,
        )

        self.assertEqual("full_overlap_stable_scale", audit["method"])
        self.assertAlmostEqual(11.4, values["1999-12-30"])

    def test_accepts_clean_prefix_before_first_action(self):
        current = current_frame("2000-01-03", 30, raw=10.0, scale=1.0)
        bars = {"1999-12-30": raw_bar(9.5)}
        actions = pd.DataFrame(
            {
                "date": [pd.Timestamp("2000-02-07")],
                "bonus_ratio": [1.0],
                "cash_per_share": [0.0],
                "rights_ratio": [0.0],
                "rights_price": [0.0],
                "consideration_stock_ratio": [0.0],
                "consideration_cash_per_share": [0.0],
            }
        )

        values, audit = reconstruct_adjusted_closes(
            current,
            bars,
            actions,
            minimum_overlap_rows=20,
            full_p99_tolerance=0.001,
            full_max_tolerance=0.005,
            leading_tolerance=0.001,
        )

        self.assertEqual("clean_pre_action_prefix", audit["method"])
        self.assertAlmostEqual(9.5, values["1999-12-30"])

    def test_rejects_unverified_action_inside_leading_gap(self):
        current = current_frame("2000-01-03", 30, raw=10.0, scale=1.0)
        current.loc[20:, "close"] = 11.0
        bars = {"1999-12-30": raw_bar(9.5)}
        actions = pd.DataFrame(
            {
                "date": [pd.Timestamp("1999-12-29")],
                "bonus_ratio": [1.0],
                "cash_per_share": [0.0],
                "rights_ratio": [0.0],
                "rights_price": [0.0],
                "consideration_stock_ratio": [0.0],
                "consideration_cash_per_share": [0.0],
            }
        )

        values, audit = reconstruct_adjusted_closes(
            current,
            bars,
            actions,
            minimum_overlap_rows=20,
            full_p99_tolerance=0.001,
            full_max_tolerance=0.005,
            leading_tolerance=0.001,
        )

        self.assertEqual({}, values)
        self.assertEqual("adjustment_scale_not_proven", audit["reason"])


if __name__ == "__main__":
    unittest.main()
