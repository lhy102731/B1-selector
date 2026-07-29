from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from tools.audit_market_data_semantics import scan_data_dir
from utils.market_data_semantics import audit_frame, summarize_checks


class MarketDataSemanticsTests(unittest.TestCase):
    def test_consistent_adjusted_rows_pass(self):
        frame = pd.DataFrame(
            {
                "date": ["2026-06-23", "2026-06-24", "2026-06-25"],
                "open": [10.0, 10.5, 11.5],
                "high": [10.5, 11.5, 12.5],
                "low": [9.5, 10.5, 11.5],
                "close": [10.0, 11.0, 12.0],
                "change_pct": [None, 10.0, 9.0909090909],
                "amplitude": [None, 10.0, 9.0909090909],
            }
        )

        checks = audit_frame(frame, code="000001")

        comparable = [row for row in checks if row["return_eligible"]]
        self.assertEqual(2, len(comparable))
        self.assertFalse(any(row["return_bad"] for row in comparable))
        self.assertFalse(any(row["amplitude_bad"] for row in comparable))

    def test_mixed_price_scale_is_reported(self):
        frame = pd.DataFrame(
            {
                "date": ["2026-06-23", "2026-06-24"],
                "open": [100.0, 94.0],
                "high": [101.0, 97.0],
                "low": [99.0, 92.0],
                "close": [100.0, 95.0],
                "change_pct": [None, 2.0],
                "amplitude": [None, 2.0],
            }
        )

        checks = audit_frame(frame, code="300274")
        row = checks[-1]

        self.assertTrue(row["return_bad"])
        self.assertAlmostEqual(-5.0, row["calculated_change_pct"], places=8)
        self.assertAlmostEqual(7.0, row["return_error_pp"], places=8)
        self.assertTrue(row["amplitude_bad"])
        self.assertAlmostEqual(5.0, row["calculated_amplitude_pct"], places=8)

    def test_cross_sectional_spike_quarantines_dataset(self):
        frames = []
        for code in ("000001", "000002"):
            frame = pd.DataFrame(
                {
                    "date": ["2026-06-23", "2026-06-24"],
                    "high": [101.0, 97.0],
                    "low": [99.0, 92.0],
                    "close": [100.0, 95.0],
                    "change_pct": [None, 2.0],
                    "amplitude": [None, 2.0],
                }
            )
            frames.extend(audit_frame(frame, code=code))

        summary = summarize_checks(
            frames,
            cross_sectional_spike_ratio=0.5,
            min_eligible=2,
        )

        self.assertEqual("SEMANTIC_QUARANTINE", summary["status"])
        day = next(item for item in summary["dates"] if item["date"] == "2026-06-24")
        self.assertEqual(2, day["return_bad"])
        self.assertEqual(1.0, day["return_bad_ratio"])
        self.assertIn("RETURN_CROSS_SECTIONAL_SPIKE", day["flags"])

    def test_missing_optional_fields_are_not_treated_as_valid(self):
        frame = pd.DataFrame(
            {
                "date": ["2026-06-23", "2026-06-24"],
                "high": [10.0, 11.0],
                "low": [9.0, 10.0],
                "close": [9.5, 10.5],
            }
        )

        checks = audit_frame(frame, code="000001")

        self.assertFalse(checks[-1]["return_eligible"])
        self.assertFalse(checks[-1]["amplitude_eligible"])

    def test_data_dir_scan_reports_market_wide_incident_without_changing_sources(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            stock_dir = data_dir / "00"
            stock_dir.mkdir(parents=True)
            source_hashes = {}
            for code in ("000001", "000002"):
                path = stock_dir / f"{code}.csv"
                pd.DataFrame(
                    {
                        "date": ["2026-06-24", "2026-06-23"],
                        "high": [97.0, 101.0],
                        "low": [92.0, 99.0],
                        "close": [95.0, 100.0],
                        "change_pct": [2.0, None],
                        "amplitude": [2.0, None],
                    }
                ).to_csv(path, index=False, encoding="gbk")
                source_hashes[path] = path.read_bytes()

            summary, bad_rows = scan_data_dir(
                data_dir,
                recent_rows=10,
                cross_sectional_spike_ratio=0.5,
                min_eligible=2,
            )

            self.assertEqual("SEMANTIC_QUARANTINE", summary["status"])
            self.assertEqual(2, summary["files_scanned"])
            self.assertEqual(0, summary["files_failed"])
            self.assertEqual(4, len(bad_rows))
            for path, original in source_hashes.items():
                self.assertEqual(original, path.read_bytes())

    def test_overlay_scan_projects_replacements_without_changing_sources(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            overlay_dir = Path(directory) / "overlay"
            source_dir = data_dir / "00"
            replacement_dir = overlay_dir / "00"
            source_dir.mkdir(parents=True)
            replacement_dir.mkdir(parents=True)
            source_bytes = {}
            for code in ("000001", "000002"):
                source = source_dir / f"{code}.csv"
                bad = pd.DataFrame(
                    {
                        "date": ["2026-06-24", "2026-06-23"],
                        "high": [97.0, 101.0],
                        "low": [92.0, 99.0],
                        "close": [95.0, 100.0],
                        "change_pct": [2.0, None],
                        "amplitude": [2.0, None],
                    }
                )
                bad.to_csv(source, index=False, encoding="gbk")
                source_bytes[source] = source.read_bytes()
                corrected = bad.copy()
                corrected.loc[0, ["high", "low", "close"]] = [103.0, 101.0, 102.0]
                corrected.to_csv(
                    replacement_dir / source.name,
                    index=False,
                    encoding="gbk",
                )

            summary, bad_rows = scan_data_dir(
                data_dir,
                overlay_dir=overlay_dir,
                recent_rows=10,
                cross_sectional_spike_ratio=0.5,
                min_eligible=2,
            )

            self.assertEqual("NO_MARKET_WIDE_SPIKE", summary["status"])
            self.assertEqual(2, summary["overlay_files_used"])
            self.assertEqual([], bad_rows)
            for path, original in source_bytes.items():
                self.assertEqual(original, path.read_bytes())


if __name__ == "__main__":
    unittest.main()
