from __future__ import annotations

import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from tools import update_ths_market_assets as updater
from utils.market_asset_store import MarketAssetStore
from utils.ths_data_source import THSDataSourceError


class _FakeSource:
    def __init__(self) -> None:
        self.history_calls = []

    def fetch_etf_universe(self):
        return {
            "510300": {
                "code": "510300",
                "ths_code": "USHJ510300",
                "name": "沪深300ETF",
                "asset_type": "etf",
                "t0": False,
                "selection_eligible": True,
            },
            "165513": {
                "code": "165513",
                "ths_code": "USZJ165513",
                "name": "商品LOF",
                "asset_type": "etf",
                "t0": True,
                "selection_eligible": False,
            },
        }

    def fetch_index_catalog(self, kind):
        raise AssertionError(kind)

    def fetch_market_history(self, ths_code, start, end, *, asset_type):
        self.history_calls.append((ths_code, str(start), str(end), asset_type))
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
                "open": [3.5, 3.6],
                "high": [3.6, 3.7],
                "low": [3.4, 3.5],
                "close": [3.55, 3.65],
                "open_raw": [3.5, 3.6], "high_raw": [3.6, 3.7], "low_raw": [3.4, 3.5],
                "volume": [1000, 2000],
                "amount": [3550, 7300],
                "close_raw": [3.55, 3.65],
                "market_cap": [pd.NA, pd.NA],
                "turnover": [pd.NA, pd.NA],
                "pe_dynamic": [pd.NA, pd.NA],
                "pb": [pd.NA, pd.NA],
                "ps": [pd.NA, pd.NA],
                "pcf": [pd.NA, pd.NA],
                "asset_type": ["etf", "etf"],
                "ths_code": [ths_code, ths_code],
            }
        )

    def close(self):
        return None


class _IncrementalSource(_FakeSource):
    def fetch_market_history(self, ths_code, start, end, *, asset_type):
        self.history_calls.append((ths_code, str(start), str(end), asset_type))
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-03", "2024-01-04"]),
                "open": [3.6, 3.65], "high": [3.7, 3.8], "low": [3.5, 3.6],
                "close": [3.65, 3.75], "close_raw": [3.65, 3.75],
                "open_raw": [3.6, 3.65], "high_raw": [3.7, 3.8], "low_raw": [3.5, 3.6],
                "volume": [2000, 2500], "amount": [7300, 9375],
                "asset_type": ["etf", "etf"], "ths_code": [ths_code, ths_code],
            }
        )


class _IndexSource(_FakeSource):
    def fetch_index_catalog(self, kind):
        return {
            "881165": {
                "code": "881165", "ths_code": "URFI881165", "name": "综合", "asset_type": kind,
            }
        }

    def fetch_market_history(self, ths_code, start, end, *, asset_type):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02"]),
                "open": [100.0], "high": [102.0], "low": [99.0], "close": [101.0],
                "open_raw": [100.0], "high_raw": [102.0], "low_raw": [99.0], "close_raw": [101.0],
                "volume": [1000], "amount": [101000],
                "asset_type": [asset_type], "ths_code": [ths_code],
            }
        )
        frame.attrs["source"] = "yuanhang"
        return frame


class _NoHistorySource(_FakeSource):
    def fetch_market_history(self, ths_code, start, end, *, asset_type):
        raise THSDataSourceError("THSDK etf_raw_klines failed: not data")


class THSMarketAssetUpdaterTests(unittest.TestCase):
    def test_sync_persists_catalog_and_history_in_one_public_run(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            source = _FakeSource()

            result = updater.run(
                data_dir=data_dir,
                asset_types=("etf",),
                start="2024-01-01",
                end="2024-01-03",
                source=source,
            )

            self.assertEqual(0, result)
            store = MarketAssetStore(data_dir)
            self.assertTrue(store.catalog_path("etf").exists())
            self.assertTrue(store.history_path("etf", "510300").exists())
            self.assertFalse(store.history_path("etf", "165513").exists())
            self.assertEqual("USHJ510300", source.history_calls[0][0])
            self.assertEqual(1, len(source.history_calls))
            summary_path = data_dir / "_market_assets" / "latest_sync.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual("thsdk", summary["source"])
            self.assertEqual("completed", summary["status"])

    def test_sync_resumes_from_last_saved_date_and_merges_the_overlap(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            store = MarketAssetStore(data_dir)
            existing = pd.DataFrame(
                {
                    "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
                    "open": [3.5, 3.6], "high": [3.6, 3.7], "low": [3.4, 3.5],
                    "close": [3.55, 3.65], "close_raw": [3.55, 3.65],
                    "open_raw": [3.5, 3.6], "high_raw": [3.6, 3.7], "low_raw": [3.4, 3.5],
                    "volume": [1000, 2000], "amount": [3550, 7300],
                    "asset_type": ["etf", "etf"], "ths_code": ["USHJ510300", "USHJ510300"],
                }
            )
            store.write_history("etf", "510300", existing)
            source = _IncrementalSource()

            result = updater.run(
                data_dir=data_dir,
                asset_types=("etf",),
                start="1990-01-01",
                end="2024-01-04",
                source=source,
            )

            self.assertEqual(0, result)
            self.assertEqual("2024-01-03", source.history_calls[0][1])
            persisted = pd.read_csv(store.history_path("etf", "510300"), encoding="gbk")
            self.assertEqual(["2024-01-04", "2024-01-03", "2024-01-02"], persisted["date"].tolist())

    def test_sync_manifest_records_yuanhang_only_when_it_served_history(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"

            result = updater.run(
                data_dir=data_dir,
                asset_types=("industry",),
                start="2024-01-01",
                end="2024-01-03",
                source=_IndexSource(),
            )

            self.assertEqual(0, result)
            summary = json.loads(
                (data_dir / "_market_assets" / "latest_sync.json").read_text(encoding="utf-8")
            )
            self.assertEqual("thsdk+yuanhang", summary["source"])
            report = pd.read_csv(data_dir / "_market_assets" / "latest_sync.csv")
            self.assertEqual("yuanhang", report.iloc[0]["source"])

    def test_resume_can_reuse_saved_catalog_without_reconnecting_thsdk(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            store = MarketAssetStore(data_dir)
            store.write_catalog(
                "industry",
                {
                    "881165": {
                        "code": "881165", "ths_code": "URFI881165", "name": "综合",
                        "asset_type": "industry",
                    }
                },
            )
            source = _IndexSource()
            source.fetch_index_catalog = lambda kind: (_ for _ in ()).throw(AssertionError(kind))

            result = updater.run(
                data_dir=data_dir,
                asset_types=("industry",),
                start="2024-01-01",
                end="2024-01-03",
                source=source,
                use_cached_catalogs=True,
            )

            self.assertEqual(0, result)
            self.assertTrue(store.history_path("industry", "881165").exists())

    def test_catalog_member_with_no_bars_is_recorded_without_failing_the_sync(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"

            result = updater.run(
                data_dir=data_dir,
                asset_types=("etf",),
                start="2024-01-01",
                end="2024-01-03",
                source=_NoHistorySource(),
                max_assets=1,
            )

            self.assertEqual(0, result)
            summary = json.loads(
                (data_dir / "_market_assets" / "latest_sync.json").read_text(encoding="utf-8")
            )
            self.assertEqual(1, summary["no_history"])
            report = pd.read_csv(data_dir / "_market_assets" / "latest_sync.csv")
            self.assertEqual("no_history", report.iloc[0]["status"])

    def test_targeted_refresh_refetches_full_requested_range(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            store = MarketAssetStore(data_dir)
            store.write_catalog("etf", _FakeSource().fetch_etf_universe())
            existing = _FakeSource().fetch_market_history(
                "USHJ510300", "2024-01-01", "2024-01-03", asset_type="etf"
            )
            store.write_history("etf", "510300", existing)
            source = _IncrementalSource()

            result = updater.run(
                data_dir=data_dir,
                asset_types=("etf",),
                start="2020-01-01",
                end="2024-01-04",
                source=source,
                use_cached_catalogs=True,
                refresh=True,
                codes=("510300",),
            )

            self.assertEqual(0, result)
            self.assertEqual("2020-01-01", source.history_calls[0][1])
            self.assertEqual(1, len(source.history_calls))


if __name__ == "__main__":
    unittest.main()
