from __future__ import annotations

import unittest

from tools.attribute_market_data_repairs import (
    classify_source_matches,
    detect_scale_sandwich,
    matching_fields,
)


class AttributeMarketDataRepairsTests(unittest.TestCase):
    def test_matching_fields_distinguishes_full_ohlc_from_close_only(self):
        current = {
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
        }
        exact = dict(current)
        close_only = {**current, "high": 11.2}

        self.assertEqual((True, True), matching_fields(current, exact))
        self.assertEqual((False, True), matching_fields(current, close_only))

    def test_source_classification_preserves_ambiguous_provenance(self):
        classification = classify_source_matches(
            full_matches=["tencent_backup", "em_bulk_backup"],
            close_matches=["tencent_backup", "em_bulk_backup", "em_today_update_backup"],
        )

        self.assertEqual("MULTI_SOURCE_FULL_MATCH", classification)

    def test_source_classification_reports_close_only_and_unattributed(self):
        self.assertEqual(
            "CLOSE_ONLY_MATCH",
            classify_source_matches([], ["em_bulk_backup"]),
        )
        self.assertEqual("UNATTRIBUTED", classify_source_matches([], []))

    def test_new_old_new_adjustment_scale_is_detected(self):
        rows = [
            {
                "date": "2026-06-23",
                "close": 1500.258544,
                "change_pct": -7.0109,
            },
            {
                "date": "2026-06-24",
                "close": 1436.623,
                "change_pct": 1.84,
            },
            {
                "date": "2026-06-25",
                "close": 1506.77282452,
                "change_pct": -1.3824,
            },
        ]

        result = detect_scale_sandwich(rows, "2026-06-24", "2026-06-25")

        self.assertTrue(result["eligible"])
        self.assertTrue(result["detected"])
        self.assertEqual("2026-06-23", result["left_date"])
        self.assertAlmostEqual(
            result["left_to_middle_scale_ratio"],
            result["right_to_middle_scale_ratio"],
            places=4,
        )
        self.assertGreater(result["left_to_middle_scale_ratio"], 1.05)

    def test_consistent_scale_is_not_a_sandwich(self):
        rows = [
            {"date": "2026-06-23", "close": 100.0, "change_pct": 1.0},
            {"date": "2026-06-24", "close": 102.0, "change_pct": 2.0},
            {
                "date": "2026-06-25",
                "close": 103.02,
                "change_pct": 1.0,
            },
        ]

        result = detect_scale_sandwich(rows, "2026-06-24", "2026-06-25")

        self.assertTrue(result["eligible"])
        self.assertFalse(result["detected"])
        self.assertEqual("below_min_scale_break", result["reason"])

    def test_scale_break_threshold_matches_semantic_audit_boundary(self):
        factor = 1.003
        rows = [
            {"date": "2026-06-23", "close": 100.0 * factor},
            {"date": "2026-06-24", "close": 102.0, "change_pct": 2.0},
            {
                "date": "2026-06-25",
                "close": 102.0 * factor * 1.01,
                "change_pct": 1.0,
            },
        ]

        result = detect_scale_sandwich(rows, "2026-06-24", "2026-06-25")

        self.assertTrue(result["detected"])
        self.assertEqual("scale_sandwich", result["reason"])

    def test_sandwich_requires_adjacent_trading_rows(self):
        rows = [
            {"date": "2026-06-23", "close": 100.0, "change_pct": 0.0},
            {"date": "2026-06-24", "close": 90.0, "change_pct": 1.0},
            {"date": "2026-06-24", "close": 91.0, "change_pct": 1.0},
            {"date": "2026-06-25", "close": 100.0, "change_pct": 1.0},
        ]

        result = detect_scale_sandwich(rows, "2026-06-24", "2026-06-25")

        self.assertFalse(result["eligible"])
        self.assertEqual("duplicate_dates", result["reason"])


if __name__ == "__main__":
    unittest.main()
