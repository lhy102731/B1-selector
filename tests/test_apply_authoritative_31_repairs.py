import unittest

import pandas as pd

from tools.apply_authoritative_31_repairs import (
    CORRECTIONS,
    DELETIONS,
    Correction,
    Deletion,
    repair_frame,
)


def frame(rows):
    values = pd.DataFrame(rows)
    defaults = {
        "amount": pd.NA,
        "turnover": 1.0,
        "market_cap": 50_000.0,
        "change": 0.0,
        "change_pct": 0.0,
        "amplitude": 0.0,
        "pe_dynamic": 12.34,
    }
    for column, value in defaults.items():
        if column not in values:
            values[column] = value
    return values.sort_values("date", ascending=False).reset_index(drop=True)


class Authoritative31RepairTests(unittest.TestCase):
    def test_adjudication_has_expected_unique_scope(self):
        keys = [(item.code, item.day) for item in (*CORRECTIONS, *DELETIONS)]

        self.assertEqual(26, len(CORRECTIONS))
        self.assertEqual(5, len(DELETIONS))
        self.assertEqual(31, len(set(keys)))
        self.assertEqual(10, len({code for code, _ in keys}))

    def test_every_authoritative_trade_pair_passes_raw_vwap_gate(self):
        for item in CORRECTIONS:
            vwap = item.amount_yuan / item.volume_shares
            self.assertLessEqual(item.raw_low, vwap, (item.code, item.day))
            self.assertLessEqual(vwap, item.raw_high, (item.code, item.day))

    def test_three_special_volume_repairs_use_database_share_units(self):
        mapped = {(item.code, item.day): item for item in CORRECTIONS}

        self.assertEqual(2_764_000, mapped[("600608", "1992-10-28")].volume_shares)
        self.assertEqual(987_000, mapped[("600608", "1993-09-08")].volume_shares)
        special = mapped[("002042", "2006-12-06")]
        self.assertEqual(219_700_000, special.volume_shares)
        self.assertEqual(219_700_000, special.float_shares)
        self.assertEqual(100.0, special.turnover_override)

    def test_updates_trade_fields_and_preserves_non_target_column(self):
        current = frame(
            [
                {"date": "2020-01-01", "open": 10, "high": 10, "low": 10, "close": 10,
                 "close_raw": 5, "volume": 100, "market_cap": 50_000},
                {"date": "2020-01-02", "open": 11, "high": 11, "low": 11, "close": 11,
                 "close_raw": 5, "volume": 100, "market_cap": 50_000},
                {"date": "2020-01-03", "open": 12, "high": 12, "low": 12, "close": 12,
                 "close_raw": 6, "volume": 100, "market_cap": 60_000},
            ]
        )
        correction = Correction(
            "000001", "2020-01-02", 5.0, 5.3, 4.9, 5.0,
            100, 10_000, 51_000, 10.0, 12.0,
        )

        repaired, counts, _, errors = repair_frame(
            "000001", current, corrections=(correction,), deletions=()
        )
        row = repaired.loc[repaired["date"].eq("2020-01-02")].iloc[0]

        self.assertEqual([], errors)
        self.assertEqual(1, counts["updated_rows"])
        self.assertEqual(10_000, row["volume"])
        self.assertEqual(51_000, row["amount"])
        self.assertEqual(100.0, row["turnover"])
        self.assertEqual(10.0, row["low"])
        self.assertEqual(12.0, row["high"])
        self.assertEqual(20.0, row["amplitude"])
        self.assertEqual(12.34, row["pe_dynamic"])

    def test_deletion_recomputes_successor_derived_fields(self):
        current = frame(
            [
                {"date": "2020-01-01", "open": 10, "high": 11, "low": 9, "close": 10,
                 "close_raw": 10, "volume": 100},
                {"date": "2020-01-02", "open": 20, "high": 21, "low": 19, "close": 20,
                 "close_raw": 20, "volume": 200},
                {"date": "2020-01-03", "open": 15, "high": 16, "low": 14, "close": 15,
                 "close_raw": 15, "volume": 300},
            ]
        )
        deletion = Deletion("000001", "2020-01-02", 20, 200, "test")

        repaired, counts, _, errors = repair_frame(
            "000001", current, corrections=(), deletions=(deletion,)
        )
        successor = repaired.loc[repaired["date"].eq("2020-01-03")].iloc[0]

        self.assertEqual([], errors)
        self.assertEqual(1, counts["deleted_rows"])
        self.assertNotIn("2020-01-02", repaired["date"].tolist())
        self.assertEqual(5.0, successor["change"])
        self.assertEqual(50.0, successor["change_pct"])
        self.assertEqual(20.0, successor["amplitude"])

    def test_explicit_close_override_recomputes_target_and_successor(self):
        current = frame(
            [
                {"date": "2020-01-01", "open": 10, "high": 10, "low": 10, "close": 10,
                 "close_raw": 5, "volume": 100},
                {"date": "2020-01-02", "open": 9, "high": 9, "low": 9, "close": 9,
                 "close_raw": 5, "volume": 100, "market_cap": 50_000},
                {"date": "2020-01-03", "open": 15, "high": 16, "low": 14, "close": 15,
                 "close_raw": 6, "volume": 100, "market_cap": 60_000},
            ]
        )
        correction = Correction(
            "000001", "2020-01-02", 5.0, 5.3, 4.9, 5.0,
            100, 10_000, 51_000, 10.0, 12.0,
            adjusted_open=11.0, adjusted_close=12.0,
        )

        repaired, _, _, errors = repair_frame(
            "000001", current, corrections=(correction,), deletions=()
        )
        target = repaired.loc[repaired["date"].eq("2020-01-02")].iloc[0]
        successor = repaired.loc[repaired["date"].eq("2020-01-03")].iloc[0]

        self.assertEqual([], errors)
        self.assertEqual(12.0, target["close"])
        self.assertEqual(2.0, target["change"])
        self.assertAlmostEqual(20.0, target["change_pct"])
        self.assertEqual(3.0, successor["change"])
        self.assertEqual(25.0, successor["change_pct"])

    def test_002042_override_replaces_sentinel_and_share_anchor(self):
        special = next(item for item in CORRECTIONS if item.code == "002042")
        current = frame(
            [
                {"date": "2006-12-05", "open": 4.4, "high": 4.4, "low": 4.4, "close": 4.4,
                 "close_raw": 3.3, "volume": 1_000, "market_cap": 171_600_000},
                {"date": "2006-12-06", "open": 4.433, "high": 4.433, "low": 4.433,
                 "close": 4.433, "close_raw": 3.33, "volume": 2_147_483_648,
                 "market_cap": 173_160_000},
            ]
        )

        repaired, _, _, errors = repair_frame(
            "002042", current, corrections=(special,), deletions=()
        )
        row = repaired.loc[repaired["date"].eq("2006-12-06")].iloc[0]

        self.assertEqual([], errors)
        self.assertEqual(219_700_000, row["volume"])
        self.assertEqual(740_495_000, row["amount"])
        self.assertEqual(100.0, row["turnover"])
        self.assertEqual(731_601_000, row["market_cap"])


if __name__ == "__main__":
    unittest.main()
