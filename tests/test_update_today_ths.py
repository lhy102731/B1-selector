from __future__ import annotations

import unittest
from unittest.mock import patch
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import subprocess
import sys

import pandas as pd

from tools import update_today_ths as updater
from utils.process_lock import process_lock
from utils.ths_data_source import THSHistoryPermissionError


class UpdateTodayCsvRoundTripTests(unittest.TestCase):
    def test_read_and_atomic_write_preserve_existing_float_tokens(self):
        with TemporaryDirectory() as temp:
            path = Path(temp) / "600519.csv"
            path.write_text(
                "date,close,change,market_cap,pe_dynamic\n"
                "2026-07-24,1410.01,30.389999999999418,"
                "1518699135422.8801,19.607886\n",
                encoding="gbk",
            )

            frame = updater._read(path)
            updater._atomic_csv(frame, path)

            tokens = path.read_text(encoding="gbk").splitlines()[1].split(",")
            self.assertEqual("30.389999999999418", tokens[2])
            self.assertEqual("1518699135422.8801", tokens[3])


class _FakeSource:
    def __init__(self, gap: bool = False):
        self.gap = gap

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
        return {
            "turnover": 2.0,
            "market_cap": 500_000.0,
            "pe_dynamic": 12.5,
            "pb": 1.75,
            "ps": 2.25,
        }

    def fetch_klines(self, code, start, end):
        dates = ["2026-07-23", "2026-07-24"]
        if self.gap:
            dates = ["2026-07-24", "2026-07-25"]
        return pd.DataFrame(
            {
                "date": pd.to_datetime(dates),
                "open": [9.0] * len(dates),
                "high": [11.0] * len(dates),
                "low": [8.0] * len(dates),
                "close": [10.0] * len(dates),
                "volume": [1000] * len(dates),
                "amount": [10000.0] * len(dates),
                "close_raw": [10.0] * len(dates),
            }
        )

    def fetch_turnover_history(self, code, start, end):
        if self.gap:
            raise THSHistoryPermissionError("guest")
        return pd.DataFrame(columns=["date", "turnover"])


class _SuspendedNullSource(_FakeSource):
    def fetch_realtime_batch(self, codes):
        return {code: {"turnover": 0.0, "market_cap": 0.0} for code in codes}

    def fetch_realtime(self, code):
        return {"turnover": 0.0, "market_cap": 0.0}

    def fetch_klines(self, code, start, end):
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-23", "2026-07-24"]),
                "open": [9.0, float("nan")],
                "high": [11.0, float("nan")],
                "low": [8.0, float("nan")],
                "close": [10.0, float("nan")],
                "volume": [1000.0, 0.0],
                "amount": [10000.0, 0.0],
                "close_raw": [10.0, float("nan")],
            }
        )


class _UniverseSource(_FakeSource):
    """THS-only source used to exercise new-listing reconciliation."""

    def __init__(self, invalid: bool = False, fail_existing: bool = False):
        super().__init__()
        self.invalid = invalid
        self.fail_existing = fail_existing
        self.history_calls = []

    def fetch_stock_universe(self):
        return {
            "000001": {"ths_code": "USHA000001", "name": "existing"},
            "000002": {"ths_code": "USHA000002", "name": "new"},
            "000003": {"ths_code": "USHA000003", "name": "not listed yet"},
        }

    def fetch_history(self, code, start, end):
        self.history_calls.append(code)
        if code == "000003":
            return pd.DataFrame()
        if self.invalid:
            # Non-empty but deliberately fails the existing amount/cap gate.
            return pd.DataFrame(
                {
                    "date": pd.to_datetime(["2026-07-23"]),
                    "open": [10.0], "high": [11.0], "low": [9.0],
                    "close": [10.0], "close_raw": [10.0],
                    "volume": [1000.0], "amount": [1.0],
                    "turnover": [2.0], "market_cap": [500000.0],
                }
            )
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-23", "2026-07-24"]),
                "open": [9.0, 10.0], "high": [11.0, 12.0],
                "low": [8.0, 9.0], "close": [10.0, 11.0],
                "close_raw": [10.0, 11.0],
                "volume": [1000.0, 2000.0],
                "amount": [10000.0, 22000.0],
                "turnover": [2.0, 4.0],
                "market_cap": [500000.0, 550000.0],
            }
        )

    def fetch_klines(self, code, start, end):
        if self.fail_existing:
            raise RuntimeError("simulated existing-file failure")
        return super().fetch_klines(code, start, end)


def _write_old(path: Path):
    pd.DataFrame(
        {
            "date": ["2026-07-23"],
            "open": [9.0],
            "high": [11.0],
            "low": [8.0],
            "close": [10.0],
            "volume": [1000],
            "amount": [10000.0],
            "turnover": [1.0],
            "market_cap": [0.0],
        }
    ).to_csv(path, index=False, encoding="gbk")


class THSDailyUpdateTests(unittest.TestCase):
    def test_run_rejects_concurrent_process_for_same_dataset(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            (root / updater.DATASET_MANIFEST).write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                    }
                ),
                encoding="utf-8",
            )
            lock_path = root / updater.UPDATE_LOCK_FILENAME

            with process_lock(lock_path, "test THS writer"):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        (
                            "import sys; "
                            "from tools import update_today_ths as updater; "
                            "raise SystemExit(updater.run(sys.argv[1]))"
                        ),
                        str(root),
                    ],
                    cwd=Path(__file__).resolve().parents[1],
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("THS daily update already active", result.stderr)

    def test_rebases_short_adjusted_slice_to_committed_overlap(self):
        local = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-23", "2026-07-24"]),
                "close": [20.0, 22.0],
            }
        )
        remote = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-23", "2026-07-24", "2026-07-27"]),
                "open": [9.0, 10.0, 11.0], "high": [11.0, 12.0, 13.0],
                "low": [8.0, 9.0, 10.0], "close": [10.0, 11.0, 12.0],
            }
        )

        rebased = updater._rebase_remote_adjusted_ohlc(local, remote)

        self.assertEqual([20.0, 22.0, 24.0], rebased["close"].tolist())
        self.assertEqual([18.0, 20.0, 22.0], rebased["open"].tolist())
        self.assertEqual("adjusted", rebased.attrs["price_rebase_mode"])

    def test_rebases_reconstructed_history_from_raw_ohlc_when_it_fits_better(self):
        local = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-23", "2026-07-24"]),
                "open": [28.474969, 29.041635],
                "high": [29.136080, 29.088857],
                "low": [28.333303, 28.286081],
                "close": [29.088857, 28.333303],
            }
        )
        factor = 28.333303 / 6.0
        remote = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-23", "2026-07-24", "2026-07-27"]),
                "open": [23.207, 23.816, 24.0],
                "high": [23.918, 23.867, 24.5],
                "low": [23.055, 23.004, 23.5],
                "close": [23.867, 23.055, 24.2],
                "open_raw": [6.03, 6.15, 6.20],
                "high_raw": [6.17, 6.16, 6.30],
                "low_raw": [6.00, 5.99, 6.10],
                "close_raw": [6.16, 6.00, 6.25],
            }
        )

        rebased = updater._rebase_remote_adjusted_ohlc(local, remote)

        self.assertEqual("raw", rebased.attrs["price_rebase_mode"])
        self.assertAlmostEqual(6.25 * factor, rebased.iloc[-1]["close"], places=5)
        self.assertAlmostEqual(6.30 * factor, rebased.iloc[-1]["high"], places=5)

    def test_accepts_completed_yuanhang_dataset_manifest(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data_ths"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            (root / updater.DATASET_MANIFEST).write_text(
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

            manifest = updater.validate_dataset_manifest(root)

            self.assertEqual("thsdk+yuanhang", manifest["source"])

    def test_rejects_manifest_below_required_schema_or_quality_version(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data_ths"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            baseline = {
                "source": "thsdk+yuanhang",
                "status": "COMPLETED",
                "schema_version": 3,
                "data_quality_version": 4,
                "stock_count": 1,
            }

            for field, stale_version in (
                ("schema_version", 2),
                ("data_quality_version", 1),
            ):
                with self.subTest(field=field):
                    manifest = {**baseline, field: stale_version}
                    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                    with self.assertRaisesRegex(RuntimeError, rf"{field}>="):
                        updater.validate_dataset_manifest(root)

    def test_live_update_requires_committed_ths_baseline_manifest(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            path = root / "00" / "000001.csv"
            _write_old(path)
            before = path.read_bytes()

            with self.assertRaisesRegex(RuntimeError, "fully rebuilt THS baseline"):
                updater.run(root, source=_FakeSource())

            self.assertEqual(before, path.read_bytes())

    def test_adds_close_raw_and_derives_latest_cap_without_other_sources(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            path = root / "00" / "000001.csv"
            _write_old(path)

            result = updater.run(root, source=_FakeSource(), require_ths_manifest=False)

            self.assertEqual(0, result)
            frame = pd.read_csv(path, encoding="gbk")
            self.assertIn("close_raw", frame.columns)
            latest = frame.iloc[0]
            self.assertEqual(500_000.0, latest["market_cap"])
            self.assertEqual(10.0, latest["close_raw"])
            self.assertEqual(12.5, latest["pe_dynamic"])
            self.assertEqual(1.75, latest["pb"])
            self.assertEqual(2.25, latest["ps"])
            self.assertTrue(pd.isna(latest["pcf"]))
            self.assertTrue((root / "_daily_updates" / updater.TODAY_STR / "backup" / "00" / "000001.csv").exists())
            cache = json.loads((root / ".update_cache.json").read_text(encoding="utf-8"))
            self.assertEqual("2026-07-24", cache["last_update_completed_date"])
            self.assertEqual(updater.TODAY_STR, cache["last_update_attempt_date"])

    def test_suspended_null_snapshot_is_not_written_as_a_daily_bar(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            path = root / "00" / "000001.csv"
            _write_old(path)
            before = path.read_bytes()

            result = updater.run(
                root,
                source=_SuspendedNullSource(),
                require_ths_manifest=False,
            )

            self.assertEqual(0, result)
            self.assertEqual(before, path.read_bytes())
            report = pd.read_csv(
                root / "_daily_updates" / updater.TODAY_STR / "ths_update_report.csv",
                encoding="utf-8-sig",
            )
            self.assertEqual("no_today_bar", report.iloc[0]["status"])

    def test_history_permission_failure_does_not_fallback_or_write(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            path = root / "00" / "000001.csv"
            _write_old(path)
            before = path.read_bytes()

            result = updater.run(root, source=_FakeSource(gap=True), require_ths_manifest=False)

            self.assertEqual(2, result)
            self.assertEqual(before, path.read_bytes())
            report = (root / "_daily_updates" / updater.TODAY_STR / "ths_update_report.csv").read_text(encoding="utf-8-sig")
            self.assertIn("THSHistoryPermissionError", report)

    def test_bounded_update_does_not_mark_the_full_dataset_current(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")

            result = updater.run(
                root,
                max_stocks=1,
                source=_FakeSource(),
                require_ths_manifest=False,
            )

            self.assertEqual(0, result)
            self.assertFalse((root / ".update_cache.json").exists())

    def test_unbounded_reconciles_new_history_and_keeps_no_history_codes(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang",
                        "status": "COMPLETED",
                        "schema_version": 3,
                        "data_quality_version": 4,
                        "stock_count": 1,
                        "start": "1990-01-01",
                        "no_history_codes": ["000003"],
                    }
                ),
                encoding="utf-8",
            )
            source = _UniverseSource()

            result = updater.run(root, source=source)

            self.assertEqual(0, result)
            self.assertEqual(["000002", "000003"], source.history_calls)
            self.assertTrue((root / "00" / "000002.csv").exists())
            self.assertFalse((root / "00" / "000003.csv").exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(2, manifest["stock_count"])
            self.assertEqual(["000003"], manifest["no_history_codes"])
            self.assertFalse(any(path.suffix == ".tmp" for path in root.iterdir()))

    def test_bounded_run_does_not_fetch_or_add_universe_difference(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            baseline = {
                "source": "thsdk+yuanhang", "status": "COMPLETED",
                "schema_version": 3, "data_quality_version": 4,
                "stock_count": 1, "no_history_codes": ["000003"],
            }
            manifest_path.write_text(json.dumps(baseline), encoding="utf-8")
            source = _UniverseSource()

            result = updater.run(root, max_stocks=1, source=source)

            self.assertEqual(0, result)
            self.assertEqual([], source.history_calls)
            self.assertFalse((root / "00" / "000002.csv").exists())
            self.assertEqual(baseline, json.loads(manifest_path.read_text(encoding="utf-8")))

    def test_invalid_new_history_is_not_written_and_fails_closed(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            (root / updater.DATASET_MANIFEST).write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "last_daily_update": "2026-07-01",
                        "last_daily_update_source": "thsdk",
                    }
                ),
                encoding="utf-8",
            )
            source = _UniverseSource(invalid=True)

            result = updater.run(root, source=source)

            self.assertEqual(2, result)
            self.assertFalse((root / "00" / "000002.csv").exists())
            report = pd.read_csv(
                root / "_daily_updates" / updater.TODAY_STR / "ths_update_report.csv",
                encoding="utf-8-sig",
            )
            added = report.loc[report["code"].astype(str).str.zfill(6) == "000002"]
            self.assertEqual("failed_validation", added.iloc[0]["status"])
            manifest = json.loads(
                (root / updater.DATASET_MANIFEST).read_text(encoding="utf-8")
            )
            self.assertEqual("2026-07-01", manifest["last_daily_update"])

    def test_manifest_count_stays_consistent_when_existing_merge_fails(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "last_daily_update": "2026-07-01",
                        "last_daily_update_source": "thsdk",
                    }
                ),
                encoding="utf-8",
            )

            result = updater.run(root, source=_UniverseSource(fail_existing=True))

            self.assertEqual(2, result)
            self.assertTrue((root / "00" / "000002.csv").exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(2, manifest["stock_count"])
            self.assertEqual("2026-07-01", manifest["last_daily_update"])

    def test_no_history_orphan_without_pending_intent_fails_closed(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            source = _UniverseSource()
            history = updater._normalise_history(
                pd.DataFrame(
                    {
                        "date": pd.to_datetime(["2026-07-23"]),
                        "open": [9.0], "high": [11.0], "low": [8.0],
                        "close": [10.0], "close_raw": [10.0],
                        "volume": [1000.0], "amount": [10000.0],
                        "turnover": [2.0], "market_cap": [500000.0],
                    }
                )
            )
            updater._atomic_csv(history, root / "00" / "000002.csv")
            source.history_calls.clear()
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "inventory_codes": ["000001"],
                        "start": "1990-01-01",
                        "no_history_codes": ["000002", "000003"],
                    }
                ),
                encoding="utf-8",
            )

            before = manifest_path.read_bytes()

            with self.assertRaisesRegex(RuntimeError, "stock_count"):
                updater.run(root, source=source)

            self.assertEqual([], source.history_calls)
            self.assertEqual(before, manifest_path.read_bytes())

    def test_new_listing_inventory_is_committed_before_existing_file_loop(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "start": "1990-01-01",
                        "no_history_codes": ["000003"],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(
                updater,
                "_fetch_realtime_batch_or_empty",
                side_effect=KeyboardInterrupt("simulated interruption"),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    updater.run(root, source=_UniverseSource())

            self.assertTrue((root / "00" / "000002.csv").exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(2, manifest["stock_count"])
            self.assertEqual(["000003"], manifest["no_history_codes"])

    def test_resumes_new_listing_interrupted_before_csv_write(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "start": "1990-01-01",
                        "no_history_codes": ["000003"],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(
                updater,
                "_atomic_csv",
                side_effect=KeyboardInterrupt("simulated interruption before CSV commit"),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    updater.run(root, source=_UniverseSource())

            self.assertFalse((root / "00" / "000002.csv").exists())
            interrupted_manifest = json.loads(
                manifest_path.read_text(encoding="utf-8")
            )
            self.assertEqual(1, interrupted_manifest["stock_count"])
            self.assertEqual(["000001"], interrupted_manifest["inventory_codes"])
            self.assertEqual(
                ["000002"],
                interrupted_manifest["pending_inventory_additions"],
            )

            resumed_source = _UniverseSource()
            result = updater.run(root, source=resumed_source)

            self.assertEqual(0, result)
            self.assertEqual(["000002", "000003"], resumed_source.history_calls)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(2, manifest["stock_count"])
            self.assertEqual(["000001", "000002"], manifest["inventory_codes"])
            self.assertNotIn("pending_inventory_additions", manifest)

    def test_recovers_brand_new_listing_interrupted_after_csv_write(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "start": "1990-01-01",
                        "no_history_codes": ["000003"],
                    }
                ),
                encoding="utf-8",
            )
            class SingleRowUniverseSource(_UniverseSource):
                def fetch_history(self, code, start, end):
                    self.history_calls.append(code)
                    if code == "000003":
                        return pd.DataFrame()
                    return pd.DataFrame(
                        {
                            "date": pd.to_datetime(["2026-07-23"]),
                            "open": [9.0], "high": [11.0], "low": [8.0],
                            "close": [10.0], "close_raw": [10.0],
                            "volume": [1000.0], "amount": [10000.0],
                            "turnover": [2.0], "market_cap": [500000.0],
                        }
                    )

            real_atomic_csv = updater._atomic_csv

            def write_then_interrupt(frame, path):
                real_atomic_csv(frame, path)
                if path.stem == "000002":
                    raise KeyboardInterrupt("simulated interruption after CSV commit")

            with patch.object(updater, "_atomic_csv", side_effect=write_then_interrupt):
                with self.assertRaises(KeyboardInterrupt):
                    updater.run(root, source=SingleRowUniverseSource())

            self.assertTrue((root / "00" / "000002.csv").exists())
            interrupted_manifest = json.loads(
                manifest_path.read_text(encoding="utf-8")
            )
            self.assertEqual(1, interrupted_manifest["stock_count"])
            self.assertEqual(["000001"], interrupted_manifest["inventory_codes"])
            self.assertEqual(
                ["000002"],
                interrupted_manifest["pending_inventory_additions"],
            )
            resumed_source = SingleRowUniverseSource()
            result = updater.run(root, source=resumed_source)

            self.assertEqual(0, result)
            self.assertEqual(["000003"], resumed_source.history_calls)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(2, manifest["stock_count"])
            self.assertEqual(["000003"], manifest["no_history_codes"])
            self.assertNotIn("pending_inventory_additions", manifest)

    def test_inventory_codes_reject_mixed_missing_and_untracked_files(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            history = updater._normalise_history(
                pd.DataFrame(
                    {
                        "date": pd.to_datetime(["2026-07-23"]),
                        "open": [9.0], "high": [11.0], "low": [8.0],
                        "close": [10.0], "close_raw": [10.0],
                        "volume": [1000.0], "amount": [10000.0],
                        "turnover": [2.0], "market_cap": [500000.0],
                    }
                )
            )
            updater._atomic_csv(history, root / "00" / "000002.csv")
            updater._atomic_csv(history, root / "00" / "000999.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 2,
                        "inventory_codes": ["000001", "000004"],
                        "no_history_codes": ["000002"],
                    }
                ),
                encoding="utf-8",
            )
            before = manifest_path.read_bytes()

            with self.assertRaisesRegex(RuntimeError, "stock_count|inventory"):
                updater.validate_dataset_manifest(
                    root,
                    recover_interrupted_new_history=True,
                )

            self.assertEqual(before, manifest_path.read_bytes())

    def test_invalid_pending_recovery_candidate_fails_without_manifest_write(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            invalid_history = updater._normalise_history(
                pd.DataFrame(
                    {
                        "date": pd.to_datetime(["2026-07-23"]),
                        "open": [10.0], "high": [11.0], "low": [9.0],
                        "close": [10.0], "close_raw": [10.0],
                        "volume": [1000.0], "amount": [1.0],
                        "turnover": [2.0], "market_cap": [500000.0],
                    }
                )
            )
            updater._atomic_csv(invalid_history, root / "00" / "000002.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "inventory_codes": ["000001"],
                        "pending_inventory_additions": ["000002"],
                    }
                ),
                encoding="utf-8",
            )
            before = manifest_path.read_bytes()

            with self.assertRaisesRegex(RuntimeError, "stock_count"):
                updater.validate_dataset_manifest(
                    root,
                    recover_interrupted_new_history=True,
                )

            self.assertEqual(before, manifest_path.read_bytes())

    def test_legacy_manifest_refuses_ambiguous_count_mismatch_recovery(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            history = updater._normalise_history(
                pd.DataFrame(
                    {
                        "date": pd.to_datetime(["2026-07-23"]),
                        "open": [9.0], "high": [11.0], "low": [8.0],
                        "close": [10.0], "close_raw": [10.0],
                        "volume": [1000.0], "amount": [10000.0],
                        "turnover": [2.0], "market_cap": [500000.0],
                    }
                )
            )
            updater._atomic_csv(history, root / "00" / "000002.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "no_history_codes": ["000002"],
                    }
                ),
                encoding="utf-8",
            )
            before = manifest_path.read_bytes()

            with self.assertRaisesRegex(RuntimeError, "lacks inventory_codes"):
                updater.validate_dataset_manifest(
                    root,
                    recover_interrupted_new_history=True,
                )

            self.assertEqual(before, manifest_path.read_bytes())

    def test_unbounded_run_bootstraps_inventory_codes_for_legacy_manifest(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                    }
                ),
                encoding="utf-8",
            )

            result = updater.run(root, source=_FakeSource())

            self.assertEqual(0, result)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(["000001"], manifest["inventory_codes"])

    def test_unbounded_run_records_completed_bar_date_when_inventory_is_unchanged(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "inventory_codes": ["000001"],
                        "last_daily_update": "2026-07-01",
                        "last_daily_update_source": "thsdk",
                    }
                ),
                encoding="utf-8",
            )

            result = updater.run(root, source=_FakeSource())

            self.assertEqual(0, result)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual("2026-07-24", manifest["last_daily_update"])
            self.assertEqual("thsdk", manifest["last_daily_update_source"])

    def test_unbounded_run_uses_local_date_when_remote_history_is_empty(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            _write_old(root / "00" / "000001.csv")
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "inventory_codes": ["000001"],
                        "last_daily_update": "2026-07-23",
                        "last_daily_update_source": "thsdk",
                    }
                ),
                encoding="utf-8",
            )

            class EmptyKlineSource(_FakeSource):
                def fetch_klines(self, code, start, end):
                    return pd.DataFrame()

            result = updater.run(root, source=EmptyKlineSource())

            self.assertEqual(0, result)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual("2026-07-23", manifest["last_daily_update"])
            validation = json.loads(
                (
                    root
                    / "_daily_updates"
                    / updater.TODAY_STR
                    / "checkpoint_validation.json"
                ).read_text(encoding="utf-8")
            )
            self.assertTrue(validation["valid"])
            self.assertEqual("2026-07-23", validation["latest_completed_data_date"])

    def test_unbounded_run_does_not_regress_date_on_stale_remote_history(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            (root / "00").mkdir(parents=True)
            path = root / "00" / "000001.csv"
            local = _FakeSource().fetch_klines("000001", "2026-07-23", "2026-07-24")
            local["turnover"] = [1.0, 2.0]
            local["market_cap"] = [500000.0, 550000.0]
            updater._atomic_csv(local, path)
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 1,
                        "inventory_codes": ["000001"],
                        "last_daily_update": "2026-07-24",
                        "last_daily_update_source": "thsdk",
                    }
                ),
                encoding="utf-8",
            )

            class StaleKlineSource(_FakeSource):
                def fetch_klines(self, code, start, end):
                    return super().fetch_klines(code, start, end).iloc[:1].copy()

            result = updater.run(root, source=StaleKlineSource())

            self.assertEqual(0, result)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual("2026-07-24", manifest["last_daily_update"])
            validation = json.loads(
                (
                    root
                    / "_daily_updates"
                    / updater.TODAY_STR
                    / "checkpoint_validation.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual("2026-07-24", validation["latest_completed_data_date"])
            cache = json.loads((root / ".update_cache.json").read_text(encoding="utf-8"))
            self.assertEqual("2026-07-24", cache["last_update_completed_date"])

    def test_all_new_listing_reconciliation_does_not_issue_empty_snapshot(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            for prefix in ("00", "30", "60", "68"):
                (root / prefix).mkdir(parents=True)
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 0,
                    }
                ),
                encoding="utf-8",
            )
            source = _UniverseSource()

            result = updater.run(root, source=source)

            self.assertEqual(0, result)
            self.assertTrue((root / "00" / "000001.csv").exists())
            self.assertTrue((root / "00" / "000002.csv").exists())
            self.assertEqual(2, json.loads(manifest_path.read_text(encoding="utf-8"))["stock_count"])

    def test_empty_baseline_reports_invalid_new_history_instead_of_raising(self):
        with TemporaryDirectory() as temp:
            root = Path(temp) / "data"
            for prefix in ("00", "30", "60", "68"):
                (root / prefix).mkdir(parents=True)
            manifest_path = root / updater.DATASET_MANIFEST
            manifest_path.write_text(
                json.dumps(
                    {
                        "source": "thsdk+yuanhang", "status": "COMPLETED",
                        "schema_version": 3, "data_quality_version": 4,
                        "stock_count": 0,
                    }
                ),
                encoding="utf-8",
            )

            result = updater.run(root, source=_UniverseSource(invalid=True))

            self.assertEqual(2, result)
            self.assertFalse((root / "00" / "000001.csv").exists())
            self.assertTrue((root / "_daily_updates" / updater.TODAY_STR / "ths_update_report.csv").exists())


if __name__ == "__main__":
    unittest.main()
