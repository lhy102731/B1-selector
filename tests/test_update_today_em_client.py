from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd
import requests

from tools import update_today_em_client as updater
from utils import akshare_fetcher
from utils.eastmoney_fetcher import EastmoneyFetcher


class UpdateTodayEastmoneyClientTests(unittest.TestCase):
    def test_market_data_sessions_ignore_the_desktop_proxy(self):
        self.assertFalse(EastmoneyFetcher().session.trust_env)
        self.assertFalse(akshare_fetcher.session.trust_env)

    def test_eastmoney_connection_failure_uses_validated_tencent_hfq(self):
        fallback = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-23", "2026-07-24"]),
                "open": [10.0, 10.1],
                "high": [10.2, 10.3],
                "low": [9.9, 10.0],
                "close": [10.1, 10.2],
                "volume": [1_000, 1_200],
            }
        )
        with (
            patch.object(
                updater,
                "fetch_hfq_rows",
                side_effect=requests.exceptions.ProxyError(
                    "desktop proxy unavailable"
                ),
            ),
            patch.object(
                updater,
                "fetch_tencent_hfq_rows",
                return_value=fallback,
            ) as tencent,
        ):
            frame, provenance = updater.fetch_hfq_rows_with_fallback(
                "000001",
                "20260723",
                "20260724",
            )

        self.assertEqual(len(frame), 2)
        self.assertEqual(provenance["provider"], "tencent")
        self.assertEqual(provenance["fallback_reason"], "ProxyError")
        tencent.assert_called_once_with("000001", "20260723", "20260724")

    def test_hfq_anchor_supports_nonzero_affine_offset(self):
        remote = pd.DataFrame(
            {
                "date": ["2026-07-20", "2026-07-21", "2026-07-22"],
                "close": [97.0, 98.0, 99.0],
                "volume": [1_000, 1_000, 1_000],
            }
        )
        local = pd.DataFrame(
            {
                "date": ["2026-07-20", "2026-07-21", "2026-07-22"],
                "close": [204.0, 206.0, 208.0],
                "volume": [1_000, 1_000, 1_000],
            }
        )

        transform, diagnostics = updater.estimate_hfq_affine_transform(
            local,
            remote,
            target_date=20260723,
        )

        self.assertEqual("stable_affine", diagnostics["anchor_status"])
        self.assertIsNotNone(transform)
        slope, intercept = transform
        self.assertAlmostEqual(2.0, slope)
        self.assertAlmostEqual(10.0, intercept)

    def test_tencent_hfq_adapter_derives_returns_without_fake_amounts(self):
        source = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-21", "2026-07-22", "2026-07-23"]),
                "open": [99.0, 100.0, 102.0],
                "high": [101.0, 103.0, 107.0],
                "low": [98.0, 99.0, 101.0],
                "close": [100.0, 102.0, 105.0],
                "volume": [1_000, 1_100, 1_200],
                "amount": [0.0, 0.0, 0.0],
                "turnover": [0.0, 0.0, 0.0],
                "change_pct": [0.0, 0.0, 0.0],
            }
        )
        with patch.object(
            updater.AKShareFetcher,
            "_fetch_stock_update_tencent",
            return_value=source,
        ):
            result = updater.fetch_tencent_hfq_rows(
                "000001",
                "20260721",
                "20260723",
            )

        latest = result.iloc[0]
        self.assertAlmostEqual((105.0 / 102.0 - 1.0) * 100.0, latest["change_pct"])
        self.assertAlmostEqual((107.0 - 101.0) / 102.0 * 100.0, latest["amplitude"])
        self.assertAlmostEqual(3.0, latest["change"])
        self.assertTrue(pd.isna(latest["amount"]))
        self.assertTrue(pd.isna(latest["turnover"]))

    def test_inserted_row_recomputes_derived_fields_and_does_not_copy_unknowns(self):
        remote = pd.DataFrame(
            [
                {"date": "2026-07-20", "open": 97.0, "high": 99.0, "low": 96.0, "close": 98.0, "volume": 800, "amount": 80_000.0, "turnover": 0.8, "change_pct": 1.0, "amplitude": 3.0, "change": 1.0},
                {"date": "2026-07-21", "open": 98.0, "high": 100.0, "low": 97.0, "close": 99.0, "volume": 900, "amount": 90_000.0, "turnover": 0.9, "change_pct": 1.0, "amplitude": 3.0, "change": 1.0},
                {"date": "2026-07-22", "open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0, "volume": 1_000, "amount": 100_000.0, "turnover": 1.0, "change_pct": 1.0, "amplitude": 3.0, "change": 1.0},
                {"date": "2026-07-23", "open": 101.0, "high": 110.0, "low": 99.0, "close": 105.0, "volume": 2_000, "amount": 210_000.0, "turnover": 2.0, "change_pct": 5.0, "amplitude": 11.0, "change": 5.0},
            ]
        )

        with TemporaryDirectory() as directory:
            path = Path(directory) / "000001.csv"
            pd.DataFrame(
                [
                    {
                        "date": "2026-07-20",
                        "open": 194.0,
                        "high": 198.0,
                        "low": 192.0,
                        "close": 196.0,
                        "volume": 800,
                        "amount": 80_000.0,
                        "turnover": 0.8,
                        "change_pct": 1.0,
                        "amplitude": 3.0,
                        "change": 2.0,
                        "pe_dynamic": 10.0,
                        "pb": 1.3,
                        "market_cap": 121_000_000,
                        "main_net_flow_x": 997.0,
                    },
                    {
                        "date": "2026-07-21",
                        "open": 196.0,
                        "high": 200.0,
                        "low": 194.0,
                        "close": 198.0,
                        "volume": 900,
                        "amount": 90_000.0,
                        "turnover": 0.9,
                        "change_pct": 1.0,
                        "amplitude": 3.0,
                        "change": 2.0,
                        "pe_dynamic": 11.0,
                        "pb": 1.4,
                        "market_cap": 122_000_000,
                        "main_net_flow_x": 998.0,
                    },
                    {
                        "date": "2026-07-22",
                        "open": 198.0,
                        "high": 202.0,
                        "low": 196.0,
                        "close": 200.0,
                        "volume": 1_000,
                        "amount": 100_000.0,
                        "turnover": 1.0,
                        "change_pct": 1.0,
                        "amplitude": 77.0,
                        "change": 88.0,
                        "pe_dynamic": 12.0,
                        "pb": 1.5,
                        "market_cap": 123_000_000,
                        "main_net_flow_x": 999.0,
                    }
                ]
            ).to_csv(path, index=False, encoding="gbk")

            with (
                patch.object(updater, "TODAY", 20260723),
                patch.object(updater, "TODAY_STR", "2026-07-23"),
                patch.object(updater, "fetch_hfq_rows", return_value=remote),
                patch.object(updater, "backup_file"),
                patch.object(updater, "mark_cache_stale"),
            ):
                result = updater.update_one(path, quote=None)

            self.assertEqual("inserted", result["status"])
            actual = pd.read_csv(path, encoding="gbk")
            inserted = actual.loc[actual["date"] == "2026-07-23"].iloc[0]
            self.assertAlmostEqual(210.0, inserted["close"])
            self.assertAlmostEqual(5.0, inserted["change_pct"])
            self.assertAlmostEqual(11.0, inserted["amplitude"])
            self.assertAlmostEqual(10.0, inserted["change"])
            self.assertTrue(pd.isna(inserted["main_net_flow_x"]))
            self.assertTrue(pd.isna(inserted["pe_dynamic"]))
            self.assertTrue(pd.isna(inserted["pb"]))
            self.assertTrue(pd.isna(inserted["market_cap"]))

    def test_mixed_scale_anchor_is_rejected_without_writing(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "000001.csv"
            local = pd.DataFrame(
                {
                    "date": ["2026-07-22", "2026-07-21", "2026-07-20"],
                    "open": [200.0, 198.0, 147.0],
                    "high": [202.0, 200.0, 148.5],
                    "low": [196.0, 194.0, 144.0],
                    "close": [200.0, 198.0, 147.0],
                    "volume": [1_000, 900, 800],
                    "amount": [100_000.0, 90_000.0, 80_000.0],
                    "turnover": [1.0, 0.9, 0.8],
                    "change_pct": [1.0, 1.0, 1.0],
                }
            )
            local.to_csv(path, index=False, encoding="gbk")
            before = path.read_bytes()
            remote = pd.DataFrame(
                {
                    "date": ["2026-07-20", "2026-07-21", "2026-07-22", "2026-07-23"],
                    "open": [98.0, 99.0, 100.0, 101.0],
                    "high": [99.0, 100.0, 101.0, 110.0],
                    "low": [96.0, 97.0, 98.0, 99.0],
                    "close": [98.0, 99.0, 100.0, 105.0],
                    "volume": [800, 900, 1_000, 2_000],
                    "amount": [80_000.0, 90_000.0, 100_000.0, 210_000.0],
                    "turnover": [0.8, 0.9, 1.0, 2.0],
                    "change_pct": [1.0, 1.0, 1.0, 5.0],
                    "amplitude": [3.0, 3.0, 3.0, 11.0],
                    "change": [1.0, 1.0, 1.0, 5.0],
                }
            )

            with (
                patch.object(updater, "TODAY", 20260723),
                patch.object(updater, "TODAY_STR", "2026-07-23"),
                patch.object(updater, "fetch_hfq_rows", return_value=remote),
                patch.object(updater, "backup_file") as backup,
                patch.object(updater, "mark_cache_stale"),
            ):
                result = updater.update_one(path, quote=None)

            self.assertEqual("unstable_affine", result["status"])
            self.assertEqual(before, path.read_bytes())
            backup.assert_not_called()


if __name__ == "__main__":
    unittest.main()
