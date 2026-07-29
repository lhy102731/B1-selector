import tempfile
import unittest
from pathlib import Path
import sys
import json
from unittest.mock import patch

import pandas as pd

from tools import update_today_em_client as daily_update
from tools import update_today_ths as ths_daily_update
import daily_run as daily_runner
from utils.akshare_fetcher import AKShareFetcher


class DailyCheckpointValidationTests(unittest.TestCase):
    def test_suspended_no_today_bar_is_legal_and_optional_nulls_are_ignored(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            backup = root / "checkpoint" / "backup"
            stock_files = []
            rows = []
            for index in range(200):
                code = f"{index:06d}"
                source = root / "data" / "00" / f"{code}.csv"
                stock_files.append(source)
                if index == 199:
                    rows.append({"code": code, "status": "no_today_bar",
                                 "close": pd.NA, "volume": pd.NA})
                    continue
                target = backup / "00" / f"{code}.csv"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("backup", encoding="utf-8")
                rows.append({"code": code, "status": "inserted", "close": 10.0,
                             "volume": 0, "amount": pd.NA, "turnover": pd.NA})

            result = daily_update.validate_checkpoint_run(rows, stock_files, backup)

            self.assertTrue(result["valid"], result["reasons"])
            self.assertEqual(result["suspended_no_today_bar"], 1)
            self.assertEqual(result["successful"], 199)

    def test_missing_backup_fails_closed(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stock = root / "data" / "00" / "000001.csv"
            result = daily_update.validate_checkpoint_run(
                [{"code": "000001", "status": "updated", "close": 10.0, "volume": 1}],
                [stock],
                root / "missing-backup",
            )
            self.assertFalse(result["valid"])
            self.assertIn("missing_backups=1", result["reasons"])


class DailyCheckpointCleanupTests(unittest.TestCase):
    def test_prune_keeps_current_and_unknown_directory_only(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            current = root / "2026-07-23"
            old = root / "2026-07-22"
            repair = root / "repair_2026-07-02_2026-07-03"
            stale = root / "stale_indicators_cache"
            unknown = root / "manual_evidence"
            for directory in (current, old, repair, stale, unknown):
                directory.mkdir(parents=True)
                (directory / "payload.bin").write_bytes(b"x")

            result = daily_update.prune_checkpoint_history(root, current, retention=1)

            self.assertTrue(current.exists())
            self.assertTrue(unknown.exists())
            self.assertFalse(old.exists())
            self.assertFalse(repair.exists())
            self.assertFalse(stale.exists())
            self.assertEqual(result["removed_files"], 3)

    def test_mark_cache_stale_deletes_regenerable_cache(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cache = root / "indicators_cache" / "000001.parquet"
            cache.parent.mkdir(parents=True)
            cache.write_bytes(b"derived")
            with patch.object(daily_update, "DATA_DIR", root):
                daily_update.mark_cache_stale("000001")
            self.assertFalse(cache.exists())


class DailyUpdateFailurePropagationTests(unittest.TestCase):
    def test_cached_update_is_read_from_the_fetchers_data_directory(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stock_dir = root / "00"
            stock_dir.mkdir(parents=True)
            (stock_dir / "000001.csv").write_text(
                "date,close\n2026-07-24,10\n",
                encoding="gbk",
            )
            (root / ths_daily_update.DATASET_MANIFEST).write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang",
                        "status": "COMPLETED",
                        "schema_version": 3,
                        "data_quality_version": 4,
                        "stock_count": 1,
                    }
                ),
                encoding="utf-8",
            )
            (root / ".update_cache.json").write_text(
                json.dumps(
                    {
                        "last_update_date": ths_daily_update.TODAY_STR,
                        "last_update_source": "thsdk",
                    }
                ),
                encoding="utf-8",
            )
            fetcher = AKShareFetcher(data_dir=td)
            with (
                patch.object(
                    ths_daily_update,
                    "UPDATE_CACHE_PATH",
                    root / "not-the-fetcher-cache.json",
                ),
                patch.object(ths_daily_update, "run") as update,
            ):
                result = fetcher.daily_update()

            self.assertEqual(result, 0)
            update.assert_not_called()

    def test_nonzero_checkpoint_result_reaches_the_main_update_caller(self):
        with tempfile.TemporaryDirectory() as td:
            cache_path = Path(td) / ".update_cache.json"
            fetcher = AKShareFetcher(data_dir=td)
            with (
                patch.object(ths_daily_update, "UPDATE_CACHE_PATH", cache_path),
                patch.object(ths_daily_update, "TODAY_STR", "2026-07-24"),
                patch.object(ths_daily_update, "run", return_value=2),
            ):
                with self.assertRaisesRegex(RuntimeError, "exit code 2"):
                    fetcher.daily_update()

    def test_ths_exception_does_not_invoke_a_legacy_quote_fallback(self):
        with tempfile.TemporaryDirectory() as td:
            fetcher = AKShareFetcher(data_dir=td)
            with (
                patch.object(
                    ths_daily_update,
                    "run",
                    side_effect=RuntimeError("THS unavailable"),
                ),
                patch.object(fetcher, "_fetch_quote_batch_tencent") as legacy_quotes,
            ):
                with self.assertRaisesRegex(RuntimeError, "THS unavailable"):
                    fetcher.daily_update(max_stocks=1)

            legacy_quotes.assert_not_called()

    def test_daily_pipeline_stops_before_cache_rebuild_when_update_fails(self):
        with (
            patch.object(
                sys,
                "argv",
                ["daily_run.py", "--skip-b1", "--skip-brick"],
            ),
            patch.object(
                daily_runner,
                "run",
                side_effect=[False, True, True],
            ) as execute,
        ):
            exit_code = daily_runner.main()

        self.assertEqual(exit_code, 1)
        self.assertEqual(execute.call_count, 1)
        self.assertEqual(
            execute.call_args.args[0],
            [sys.executable, "main.py", "update"],
        )

    def test_daily_pipeline_updates_typed_market_assets_before_derived_caches(self):
        with (
            patch.object(sys, "argv", ["daily_run.py", "--skip-b1", "--skip-brick"]),
            patch(
                "run_b1_v3._effective_select_date",
                return_value=pd.Timestamp("2026-07-24").date(),
            ),
            patch.object(daily_runner, "run", side_effect=[True, False]) as execute,
        ):
            exit_code = daily_runner.main()

        self.assertEqual(1, exit_code)
        self.assertEqual(2, execute.call_count)
        self.assertEqual(
            [
                sys.executable,
                "tools/update_ths_market_assets.py",
                "--asset-types",
                "etf",
                "--end",
                "2026-07-24",
            ],
            execute.call_args_list[1].args[0],
        )


if __name__ == "__main__":
    unittest.main()
