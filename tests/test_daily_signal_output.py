from __future__ import annotations

import unittest
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from backtest_brick_v2 import RAW_SIGNAL_COLUMNS, save_raw_signals
from filter_exec_reduce import main as filter_exec_reduce_main


class DailySignalOutputTests(unittest.TestCase):
    def test_zero_signal_day_replaces_stale_artifact_with_header_only_csv(self):
        with TemporaryDirectory() as temp:
            path = Path(temp) / "signals_today.csv"
            path.write_text("code,entry_date\n000001,1999-01-01\n", encoding="gbk")

            count = save_raw_signals([], path)

            self.assertEqual(0, count)
            frame = pd.read_csv(path, encoding="gbk")
            self.assertTrue(frame.empty)
            self.assertEqual(RAW_SIGNAL_COLUMNS, frame.columns.tolist())

    def test_signal_codes_are_published_as_six_digit_strings(self):
        with TemporaryDirectory() as temp:
            path = Path(temp) / "signals_today.csv"

            count = save_raw_signals(
                [
                    {"code": 1, "entry_date": "2026-07-24"},
                    {"code": "11", "entry_date": "2026-07-24"},
                ],
                path,
            )

            self.assertEqual(2, count)
            frame = pd.read_csv(path, encoding="gbk", dtype={"code": str})
            self.assertEqual(["000001", "000011"], frame["code"].tolist())
            self.assertEqual(RAW_SIGNAL_COLUMNS, frame.columns.tolist())

    def test_reduction_filter_preserves_six_digit_codes(self):
        with TemporaryDirectory() as temp:
            path = Path(temp) / "signals_today.csv"
            pd.DataFrame(
                [
                    {"code": 1, "entry_date": "2026-07-24"},
                    {"code": 11, "entry_date": "2026-07-24"},
                ]
            ).to_csv(path, index=False, encoding="gbk")

            argv = ["filter_exec_reduce.py", "--signals", str(path), "--date", "2026-07-24"]
            with patch.object(sys, "argv", argv), patch(
                "filter_exec_reduce.get_reduce_codes", return_value={"999999"}
            ):
                filter_exec_reduce_main()

            frame = pd.read_csv(path, encoding="gbk", dtype={"code": str})
            self.assertEqual(["000001", "000011"], frame["code"].tolist())


if __name__ == "__main__":
    unittest.main()
