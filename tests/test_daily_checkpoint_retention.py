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
from utils.checkpoint_retention import prune_checkpoint_history


class _CheckpointTHSSource:
    def __init__(self, *, fail: bool = False):
        self.fail = fail

    def fetch_realtime_batch(self, codes):
        return {
            code: {
                "turnover": 2.0,
                "market_cap": 500_000.0,
                "pe_dynamic": 12.5,
                "pb": 1.75,
                "ps": 2.25,
            }
            for code in codes
        }

    def fetch_realtime(self, code):
        return self.fetch_realtime_batch([code])[code]

    def fetch_klines(self, code, start, end):
        if self.fail:
            raise RuntimeError("simulated THS failure")
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-23", "2026-07-24"]),
                "open": [9.0, 9.0],
                "high": [11.0, 11.0],
                "low": [8.0, 8.0],
                "close": [10.0, 10.0],
                "volume": [1000.0, 1000.0],
                "amount": [10000.0, 10000.0],
                "close_raw": [10.0, 10.0],
            }
        )

    def fetch_turnover_history(self, code, start, end):
        return pd.DataFrame(columns=["date", "turnover"])


def _write_checkpoint_ths_stock(path: Path) -> None:
    pd.DataFrame(
        {
            "date": ["2026-07-23"],
            "open": [9.0],
            "high": [11.0],
            "low": [8.0],
            "close": [10.0],
            "volume": [1000.0],
            "amount": [10000.0],
            "turnover": [1.0],
            "market_cap": [0.0],
        }
    ).to_csv(path, index=False, encoding="gbk")


def _write_ths_manifest(root: Path) -> None:
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
            manual_repair = root / "repair_manual_evidence"
            for directory in (current, old, repair, stale, unknown, manual_repair):
                directory.mkdir(parents=True)
                (directory / "payload.bin").write_bytes(b"x")

            result = prune_checkpoint_history(root, current, retention=1)

            self.assertTrue(current.exists())
            self.assertTrue(unknown.exists())
            self.assertTrue(manual_repair.exists())
            self.assertFalse(old.exists())
            self.assertFalse(repair.exists())
            self.assertFalse(stale.exists())
            self.assertEqual(result["removed_files"], 3)

    def test_prune_preserves_failed_and_malformed_dated_checkpoints(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            current = root / "2026-07-24"
            failed = root / "2026-07-23"
            malformed = root / "2026-07-22"
            legacy = root / "2026-07-21"
            failed_repair = root / "repair_2026-07-19_2026-07-20"
            for directory in (current, failed, malformed, legacy, failed_repair):
                directory.mkdir(parents=True)
                (directory / "payload.bin").write_bytes(b"x")
            (current / "checkpoint_validation.json").write_text(
                json.dumps({"valid": True}),
                encoding="utf-8",
            )
            (failed / "checkpoint_validation.json").write_text(
                json.dumps({"valid": False}),
                encoding="utf-8",
            )
            (malformed / "checkpoint_validation.json").write_text(
                "not-json",
                encoding="utf-8",
            )
            (failed_repair / "checkpoint_validation.json").write_text(
                json.dumps({"valid": False}),
                encoding="utf-8",
            )

            result = prune_checkpoint_history(root, current, retention=1)

            self.assertTrue(current.exists())
            self.assertTrue(failed.exists())
            self.assertTrue(malformed.exists())
            self.assertTrue(failed_repair.exists())
            self.assertFalse(legacy.exists())
            self.assertEqual(
                {str(failed), str(malformed), str(failed_repair)},
                set(result["protected"]),
            )

    def test_prune_rejects_a_current_checkpoint_outside_the_managed_root(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "managed"
            outside = Path(td) / "2026-07-24"
            root.mkdir()
            outside.mkdir()

            with self.assertRaisesRegex(ValueError, "direct child"):
                prune_checkpoint_history(root, outside, retention=1)

    def test_mark_cache_stale_deletes_regenerable_cache(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cache = root / "indicators_cache" / "000001.parquet"
            cache.parent.mkdir(parents=True)
            cache.write_bytes(b"derived")
            with patch.object(daily_update, "DATA_DIR", root):
                daily_update.mark_cache_stale("000001")
            self.assertFalse(cache.exists())

    def test_full_valid_ths_run_prunes_generated_history(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "data"
            stock_dir = root / "00"
            stock_dir.mkdir(parents=True)
            _write_checkpoint_ths_stock(stock_dir / "000001.csv")
            _write_ths_manifest(root)

            update_root = root / "_daily_updates"
            old = update_root / "2026-07-01"
            repair = update_root / "repair_2026-07-01_2026-07-02"
            stale = update_root / "stale_indicators_cache"
            manual = update_root / "manual_evidence"
            for directory in (old, repair, stale, manual):
                directory.mkdir(parents=True)
                (directory / "payload.bin").write_bytes(b"x")

            result = ths_daily_update.run(root, source=_CheckpointTHSSource())

            current = update_root / ths_daily_update.TODAY_STR
            self.assertEqual(0, result)
            self.assertTrue(current.exists())
            self.assertTrue((current / "checkpoint_cleanup.json").exists())
            self.assertFalse(old.exists())
            self.assertFalse(repair.exists())
            self.assertFalse(stale.exists())
            self.assertTrue(manual.exists())

    def test_failed_ths_run_retains_old_and_current_checkpoints(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "data"
            stock_dir = root / "00"
            stock_dir.mkdir(parents=True)
            _write_checkpoint_ths_stock(stock_dir / "000001.csv")
            _write_ths_manifest(root)

            old = root / "_daily_updates" / "2026-07-01"
            repair = root / "_daily_updates" / "repair_2026-07-01_2026-07-02"
            stale = root / "_daily_updates" / "stale_indicators_cache"
            for directory in (old, repair, stale):
                directory.mkdir(parents=True)
                (directory / "payload.bin").write_bytes(b"x")

            result = ths_daily_update.run(
                root,
                source=_CheckpointTHSSource(fail=True),
            )

            current = root / "_daily_updates" / ths_daily_update.TODAY_STR
            self.assertEqual(2, result)
            self.assertTrue(current.exists())
            self.assertFalse((current / "checkpoint_cleanup.json").exists())
            self.assertTrue(old.exists())
            self.assertTrue(repair.exists())
            self.assertTrue(stale.exists())

    def test_bounded_valid_ths_run_does_not_prune_history(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "data"
            stock_dir = root / "00"
            stock_dir.mkdir(parents=True)
            _write_checkpoint_ths_stock(stock_dir / "000001.csv")
            _write_ths_manifest(root)

            old = root / "_daily_updates" / "2026-07-01"
            old.mkdir(parents=True)
            (old / "payload.bin").write_bytes(b"x")

            result = ths_daily_update.run(
                root,
                max_stocks=1,
                source=_CheckpointTHSSource(),
            )

            current = root / "_daily_updates" / ths_daily_update.TODAY_STR
            self.assertEqual(0, result)
            self.assertTrue(current.exists())
            self.assertFalse((current / "checkpoint_cleanup.json").exists())
            self.assertTrue(old.exists())


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
