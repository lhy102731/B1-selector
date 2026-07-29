from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import run_b1_v3 as runner
from strategy import b1_v3_strategy as strategy


class B1V3SelectDateTests(unittest.TestCase):
    def test_select_uses_latest_locally_completed_data_date(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            update_cache = root / ".update_cache.json"
            update_cache.write_text(
                json.dumps({
                    "last_update_completed_date": "2026-07-24",
                    "last_update_source": "thsdk",
                }),
                encoding="utf-8",
            )
            extract = unittest.mock.Mock(return_value=[])
            args = SimpleNamespace(max_stocks=0, date=None)

            with (
                patch.object(runner, "UPDATE_CACHE_PATH", update_cache),
                patch.object(runner, "DATASET_MANIFEST_PATH", root / "missing.json"),
                patch.object(runner, "get_stock_list", return_value=["000001"]),
                patch.object(strategy, "extract_signals_single", extract),
            ):
                runner.cmd_select(args)

            self.assertEqual("2026-07-17", extract.call_args.args[1])
            self.assertEqual("2026-07-24", extract.call_args.args[2])

    def test_select_rejects_requested_date_beyond_completed_local_data(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            update_cache = root / ".update_cache.json"
            update_cache.write_text(
                json.dumps({
                    "last_update_completed_date": "2026-07-24",
                    "last_update_source": "thsdk",
                }),
                encoding="utf-8",
            )
            args = SimpleNamespace(max_stocks=0, date="2026-07-25")

            with (
                patch.object(runner, "UPDATE_CACHE_PATH", update_cache),
                patch.object(runner, "DATASET_MANIFEST_PATH", root / "missing.json"),
                patch.object(runner, "get_stock_list", return_value=["000001"]),
            ):
                with self.assertRaisesRegex(RuntimeError, "newer than completed data"):
                    runner.cmd_select(args)

    def test_failed_dataset_manifest_is_not_treated_as_completed_data(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / ".ths_dataset_manifest.json"
            manifest.write_text(
                json.dumps({"status": "FAILED", "end": "2026-07-29"}),
                encoding="utf-8",
            )

            with (
                patch.object(runner, "UPDATE_CACHE_PATH", root / "missing.json"),
                patch.object(runner, "DATASET_MANIFEST_PATH", manifest),
            ):
                with self.assertRaisesRegex(RuntimeError, "no locally completed"):
                    runner._latest_completed_data_date()

    def test_future_completion_marker_is_rejected(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            update_cache = root / ".update_cache.json"
            update_cache.write_text(
                json.dumps({
                    "last_update_completed_date": "2099-01-01",
                    "last_update_source": "thsdk",
                }),
                encoding="utf-8",
            )

            with (
                patch.object(runner, "UPDATE_CACHE_PATH", update_cache),
                patch.object(runner, "DATASET_MANIFEST_PATH", root / "missing.json"),
            ):
                with self.assertRaisesRegex(RuntimeError, "future date"):
                    runner._latest_completed_data_date()


if __name__ == "__main__":
    unittest.main()
