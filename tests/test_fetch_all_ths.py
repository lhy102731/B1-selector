from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from tools.fetch_all_ths import _effective_date_bounds, fetch_all


class _Source:
    def fetch_stock_universe(self):
        return {"000002": {"ths_code": "USZA000002", "name": "sample"}}

    def fetch_history(self, code, start, end):
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-02", "2020-01-03"]),
                "open": [10.0, 11.0],
                "high": [11.0, 12.0],
                "low": [9.0, 10.0],
                "close": [10.0, 11.0],
                "close_raw": [10.0, 11.0],
                "volume": [1000.0, 2000.0],
                "amount": [10000.0, 22000.0],
                "turnover": [2.0, 4.0],
                "market_cap": [500000.0, 550000.0],
            }
        )


class _NoFetchSource(_Source):
    def fetch_history(self, code, start, end):
        raise AssertionError(f"unexpected refetch: {code}")


class _EmptyNewListingSource(_Source):
    def fetch_history(self, code, start, end):
        if code == "000002":
            return pd.DataFrame()
        return super().fetch_history(code, start, end)


class FetchAllTHSTests(unittest.TestCase):
    def test_known_source_gap_uses_verified_ths_start_but_keeps_archive_end(self):
        bounds = {
            "000028": (
                pd.Timestamp("1993-08-09"),
                pd.Timestamp("2026-07-24"),
            )
        }

        effective = _effective_date_bounds("000028", bounds)

        self.assertEqual(pd.Timestamp("1996-04-16"), effective[0])
        self.assertEqual(pd.Timestamp("2026-07-24"), effective[1])

    def test_writes_new_same_prefix_tree_without_touching_old_data(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            old = root / "data"
            output = root / "data_ths"
            (old / "00").mkdir(parents=True)
            old_file = old / "00" / "000001.csv"
            old_file.write_text("date,close\n2020-01-03,1\n", encoding="gbk")
            before = old_file.read_bytes()

            result = fetch_all(
                archive_data_dir=old,
                output_dir=output,
                source=_Source(),
            )

            self.assertEqual(0, result)
            self.assertEqual(before, old_file.read_bytes())
            self.assertTrue((output / "00" / "000001.csv").exists())
            self.assertTrue((output / "00" / "000002.csv").exists())
            self.assertTrue(all((output / prefix).is_dir() for prefix in ("00", "30", "60", "68")))
            manifest = (output / ".ths_dataset_manifest.json").read_text(encoding="utf-8")
            self.assertIn('"source": "thsdk+yuanhang"', manifest)

    def test_resumes_only_files_written_by_an_interrupted_quality_run(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            old = root / "data"
            output = root / "data_ths"
            (old / "00").mkdir(parents=True)
            (old / "00" / "000001.csv").write_text(
                "date,close\n2020-01-03,1\n", encoding="gbk"
            )
            self.assertEqual(
                0,
                fetch_all(archive_data_dir=old, output_dir=output, source=_Source()),
            )
            manifest_path = output / ".ths_dataset_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["data_quality_version"] = 0
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            result = fetch_all(
                archive_data_dir=old,
                output_dir=output,
                source=_NoFetchSource(),
                resume=True,
            )

            self.assertEqual(0, result)
            report = json.loads((output / ".ths_fetch_report.json").read_text(encoding="utf-8"))
            self.assertEqual(2, report["skipped"])
            self.assertEqual(0, report["written"])

    def test_empty_current_listing_is_recorded_as_no_history_not_failure(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            old = root / "data"
            output = root / "data_ths"
            (old / "00").mkdir(parents=True)
            (old / "00" / "000001.csv").write_text(
                "date,close\n2020-01-03,1\n", encoding="gbk"
            )

            result = fetch_all(
                archive_data_dir=old,
                output_dir=output,
                source=_EmptyNewListingSource(),
            )

            self.assertEqual(0, result)
            report = json.loads((output / ".ths_fetch_report.json").read_text(encoding="utf-8"))
            self.assertEqual(0, report["failed"])
            self.assertEqual(["000002"], report["no_history_codes"])


if __name__ == "__main__":
    unittest.main()
