import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from tools.backfill_valuation_fields import (
    PS_SENTINEL,
    _apply_source,
    _atomic_csv,
    _load_progress,
    _read_csv,
    merge_legacy_sources,
    valid_valuation,
)


class ValuationBackfillTests(unittest.TestCase):
    def test_resume_uses_latest_status_and_accumulates_completed_counts(self):
        with TemporaryDirectory() as temporary:
            progress = Path(temporary) / "progress.jsonl"
            rows = [
                {"code": "000001", "status": "failed", "filled_pb": 9},
                {
                    "code": "000001",
                    "status": "updated",
                    "filled_pe_dynamic": 2,
                    "filled_pb": 3,
                },
                {"code": "000002", "status": "no_target_rows"},
                {"code": "000003", "status": "failed", "filled_pb": 7},
            ]
            progress.write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )

            completed, aggregate = _load_progress(progress)

            self.assertEqual({"000001", "000002"}, completed)
            self.assertEqual(2, aggregate["filled_pe_dynamic"])
            self.assertEqual(3, aggregate["filled_pb"])

    def test_csv_round_trip_preserves_unrelated_float_tokens(self):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "600519.csv"
            original = (
                "date,close,change,market_cap,pe_dynamic\n"
                "2026-07-24,1410.01,30.389999999999418,"
                "1518699135422.8801,\n"
            )
            path.write_text(original, encoding="gbk")

            frame = _read_csv(path)
            frame.loc[0, "pe_dynamic"] = 19.5263
            _atomic_csv(frame, path)

            tokens = path.read_text(encoding="gbk").splitlines()[1].split(",")
            self.assertEqual("30.389999999999418", tokens[2])
            self.assertEqual("1518699135422.8801", tokens[3])

    def test_legacy_merge_prefers_pepb_cache_and_never_forward_fills(self):
        target = pd.DataFrame(
            {
                "date": ["2026-05-29", "2026-05-28", "2026-05-27"],
                "close": [10.0, 9.0, 8.0],
                "pe_dynamic": [pd.NA, pd.NA, 99.0],
                "pb": [pd.NA, pd.NA, pd.NA],
                "ps": [pd.NA, pd.NA, pd.NA],
                "pcf": [pd.NA, pd.NA, pd.NA],
            }
        )
        legacy = pd.DataFrame(
            {
                "date": ["2026-05-29", "2026-05-28"],
                "pe_dynamic": [11.0, 12.0],
                "pb": [1.1, 1.2],
                "ps": [2.1, 2.2],
                "pcf": [-3.1, -3.2],
            }
        )
        cache = pd.DataFrame(
            {
                "date": ["2026-05-29", "2026-05-28"],
                "peTTM": [21.0, 22.0],
                "pbMRQ": [4.1, 4.2],
            }
        )

        merged, stats = merge_legacy_sources(target, legacy, cache)

        self.assertEqual([21.0, 22.0, 99.0], merged["pe_dynamic"].tolist())
        self.assertEqual([4.1, 4.2], merged["pb"].iloc[:2].tolist())
        self.assertEqual([2.1, 2.2], merged["ps"].iloc[:2].tolist())
        self.assertEqual([-3.1, -3.2], merged["pcf"].iloc[:2].tolist())
        self.assertTrue(pd.isna(merged.iloc[2]["pb"]))
        self.assertEqual(2, stats["filled_pe_dynamic"])

    def test_cutoff_blocks_contaminated_recent_legacy_rows(self):
        target = pd.DataFrame(
            {"date": ["2026-06-01", "2026-05-29"], "pe_dynamic": [pd.NA, pd.NA]}
        )
        legacy = pd.DataFrame(
            {"date": ["2026-06-01", "2026-05-29"], "pe_dynamic": [10.0, 9.0]}
        )

        merged, _ = merge_legacy_sources(
            target, legacy, None, cutoff="2026-05-29"
        )

        self.assertTrue(pd.isna(merged.iloc[0]["pe_dynamic"]))
        self.assertEqual(9.0, merged.iloc[1]["pe_dynamic"])

    def test_zero_and_ps_vendor_sentinel_are_not_migrated(self):
        target = pd.DataFrame(
            {
                "date": ["2024-01-02"],
                "pe_dynamic": [pd.NA],
                "pb": [pd.NA],
                "ps": [pd.NA],
                "pcf": [pd.NA],
            }
        )
        source = pd.DataFrame(
            {
                "date": ["2024-01-02"],
                "pe_dynamic": [0.0],
                "pb": [0.0],
                "ps": [PS_SENTINEL],
                "pcf": [-7.0],
            }
        )

        merged, stats = _apply_source(target, source)

        self.assertTrue(pd.isna(merged.iloc[0]["pe_dynamic"]))
        self.assertTrue(pd.isna(merged.iloc[0]["pb"]))
        self.assertTrue(pd.isna(merged.iloc[0]["ps"]))
        self.assertEqual(-7.0, merged.iloc[0]["pcf"])
        self.assertEqual(1, stats["filled_pcf"])

    def test_valid_valuation_keeps_negative_fundamentals(self):
        values = pd.Series([-10.0, 0.0, float("inf"), 2.0])
        self.assertEqual(
            [True, False, False, True], valid_valuation(values, "pe_dynamic").tolist()
        )


if __name__ == "__main__":
    unittest.main()
