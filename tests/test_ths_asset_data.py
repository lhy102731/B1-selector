from __future__ import annotations

import unittest
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from utils.ths_data_source import THSDataSource
from utils.market_asset_store import MarketAssetStore
from utils.selection_universe import SelectionUniverse


class _CatalogClient:
    def __init__(self):
        self.calls = []

    def connect(self):
        return SimpleNamespace(success=True, error="", data=[])

    def disconnect(self):
        return None

    def ths_industry(self):
        return SimpleNamespace(
            success=True,
            error="",
            data=[{5: "URFI881165", 55: "综合"}],
        )

    def ths_concept(self):
        return SimpleNamespace(
            success=True,
            error="",
            data=[{5: "URFI885580", 55: "足球概念"}],
        )

    def fund_etf_lists(self):
        return SimpleNamespace(
            success=True,
            error="",
            data=[
                {5: "USHJ510300", 55: "沪深300ETF"},
                {5: "USZJ165513", 55: "中信保诚商品LOF"},
            ],
        )

    def fund_etf_t0_lists(self):
        return SimpleNamespace(
            success=True,
            error="",
            data=[{5: "USHJ510300", 55: "沪深300ETF"}],
        )

    def call(self, method, params):
        self.calls.append((method, params.copy()))
        scale = 10 if params.get("adjust") == "backward" else 1
        return SimpleNamespace(
            success=True,
            error="",
            data=[
                {1: 20240102, 7: 3.50 * scale, 8: 3.60 * scale, 9: 3.40 * scale, 11: 3.55 * scale, 13: 1_000, 19: 3_550},
                {1: 20240103, 7: 3.55 * scale, 8: 3.70 * scale, 9: 3.50 * scale, 11: 3.65 * scale, 13: 2_000, 19: 7_300},
            ],
        )


class _IndexHistoryBridge:
    def __init__(self):
        self.requests = []

    def query(self, request):
        self.requests.append(request)
        return [
            {"1": "20240102", "7": "100.0", "8": "100.99", "9": "99.0", "11": "101.0", "13": "1000", "19": "101000"},
            {"1": "20240103", "7": "101.0", "8": "103.0", "9": "100.0", "11": "102.0", "13": "2000", "19": "204000"},
        ]

    def close(self):
        return None


class _RoundedETFClient(_CatalogClient):
    def call(self, method, params):
        self.calls.append((method, params.copy()))
        return SimpleNamespace(
            success=True,
            error="",
            data=[{1: 20240102, 7: 1.888, 8: 1.888, 9: 1.888, 11: 1.892, 13: 3400, 19: 6419}],
        )


class _MissingETFAmountClient(_CatalogClient):
    def call(self, method, params):
        self.calls.append((method, params.copy()))
        return SimpleNamespace(
            success=True,
            error="",
            data=[{1: 20240102, 7: 100.0, 8: 100.0, 9: 100.0, 11: 100.0, 13: 1000, 19: 2147483648}],
        )


class _InvalidETFAmountClient(_CatalogClient):
    def call(self, method, params):
        self.calls.append((method, params.copy()))
        return SimpleNamespace(
            success=True,
            error="",
            data=[{1: 20240102, 7: 1.305, 8: 1.309, 9: 1.291, 11: 1.291, 13: 94006, 19: 120348}],
        )


class THSAssetCatalogTests(unittest.TestCase):
    def test_default_client_hydrates_user_credentials_before_thsdk_construction(self):
        expected = {
            "THS_USERNAME": "configured-user",
            "THS_PASSWORD": "configured-password",
            "THS_MAC": "configured-mac",
        }

        class _CredentialAwareTHS:
            def __init__(self):
                self.seen = {name: os.environ.get(name) for name in expected}
                if self.seen != expected:
                    raise AssertionError(self.seen)

        module = SimpleNamespace(THS=_CredentialAwareTHS)
        with (
            patch.dict(os.environ, {name: "" for name in expected}),
            patch("utils.ths_yuanhang_bridge._windows_user_env", side_effect=expected.get),
            patch("utils.ths_data_source.importlib.import_module", return_value=module),
        ):
            client = THSDataSource._default_client_factory()

        self.assertEqual(expected, client.seen)

    def test_catalogs_expose_normalized_asset_metadata(self):
        source = THSDataSource(client_factory=_CatalogClient, sleeper=lambda _: None)

        industry = source.fetch_index_catalog("industry")
        concept = source.fetch_index_catalog("concept")
        etfs = source.fetch_etf_universe()

        self.assertEqual(
            {"code": "881165", "ths_code": "URFI881165", "name": "综合", "asset_type": "industry"},
            industry["881165"],
        )
        self.assertEqual("concept", concept["885580"]["asset_type"])
        self.assertEqual("etf", etfs["510300"]["asset_type"])
        self.assertTrue(etfs["510300"]["t0"])
        self.assertEqual("etf", etfs["510300"]["subtype"])
        self.assertTrue(etfs["510300"]["selection_eligible"])
        self.assertEqual("lof", etfs["165513"]["subtype"])
        self.assertFalse(etfs["165513"]["selection_eligible"])

    def test_market_history_uses_raw_ths_code_and_preserves_asset_semantics(self):
        client = _CatalogClient()
        source = THSDataSource(client_factory=lambda: client, sleeper=lambda _: None)

        result = source.fetch_market_history(
            "USHJ510300", "2024-01-01", "2024-01-03", asset_type="etf"
        )

        self.assertEqual([35.5, 36.5], result["close"].tolist())
        self.assertEqual([3.55, 3.65], result["close_raw"].tolist())
        self.assertEqual(["etf", "etf"], result["asset_type"].tolist())
        self.assertTrue(result["market_cap"].isna().all())
        self.assertEqual("klines", client.calls[0][0])
        self.assertEqual("USHJ510300", client.calls[0][1]["code"])
        self.assertCountEqual(["backward", ""], [call[1]["adjust"] for call in client.calls])
        self.assertEqual("2024-01-01 00:00:00", client.calls[0][1]["start_time"])

    def test_market_history_expands_a_small_etf_tick_envelope_gap(self):
        source = THSDataSource(client_factory=_RoundedETFClient, sleeper=lambda _: None)

        result = source.fetch_market_history(
            "USHJ510680", "2024-01-01", "2024-01-03", asset_type="etf"
        )

        self.assertEqual(1.892, result.iloc[0]["high"])
        self.assertEqual(1.892, result.iloc[0]["high_raw"])
        self.assertEqual(1, result.attrs["ohlc_envelope_repaired_rows"])

    def test_market_history_normalizes_etf_amount_sentinel_to_missing(self):
        source = THSDataSource(client_factory=_MissingETFAmountClient, sleeper=lambda _: None)

        result = source.fetch_market_history(
            "USHJ511650", "2024-01-01", "2024-01-03", asset_type="etf"
        )

        self.assertTrue(pd.isna(result.iloc[0]["amount"]))
        self.assertEqual(1, result.attrs["amount_missing_rows"])

    def test_market_history_rejects_an_isolated_etf_amount_outside_one_tick(self):
        source = THSDataSource(client_factory=_InvalidETFAmountClient, sleeper=lambda _: None)

        result = source.fetch_market_history(
            "USHJ510560", "2024-01-01", "2024-01-03", asset_type="etf"
        )

        self.assertTrue(pd.isna(result.iloc[0]["amount"]))
        self.assertEqual(1, result.attrs["amount_vwap_rejected_rows"])

    def test_index_history_uses_fast_yuanhang_daily_protocol(self):
        client = _CatalogClient()
        bridge = _IndexHistoryBridge()
        source = THSDataSource(
            client_factory=lambda: client,
            sleeper=lambda _: None,
            history_bridge_factory=lambda: bridge,
        )

        result = source.fetch_market_history(
            "URFI881165", "2024-01-01", "2024-01-03", asset_type="industry"
        )

        self.assertEqual([101.0, 102.0], result["close"].tolist())
        self.assertEqual(101.0, result.iloc[0]["high"])
        self.assertEqual(1, result.attrs["ohlc_envelope_repaired_rows"])
        self.assertEqual("yuanhang", result.attrs["source"])
        self.assertEqual([], client.calls)
        self.assertEqual(
            "id=210&market=URFI&code=881165&start=20240101&end=20240103&datatype=1,7,8,9,11,13,19&period=16384",
            bridge.requests[0],
        )

    def test_asset_store_keeps_indices_and_etfs_in_separate_trees(self):
        bars = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
                "open": [3.5, 3.6],
                "high": [3.6, 3.7],
                "low": [3.4, 3.5],
                "close": [3.55, 3.65],
                "open_raw": [3.5, 3.6],
                "high_raw": [3.6, 3.7],
                "low_raw": [3.4, 3.5],
                "close_raw": [3.55, 3.65],
                "volume": [1000, 2000],
                "amount": [3550, 7300],
                "asset_type": ["etf", "etf"],
                "ths_code": ["USHJ510300", "USHJ510300"],
            }
        )
        with TemporaryDirectory() as directory:
            store = MarketAssetStore(Path(directory) / "data")
            store.write_catalog(
                "etf",
                {"510300": {"code": "510300", "ths_code": "USHJ510300", "name": "沪深300ETF", "asset_type": "etf"}},
            )
            path = store.write_history("etf", "510300", bars)

            self.assertEqual(Path(directory) / "data" / "etf" / "51" / "510300.csv", path)
            self.assertEqual(Path(directory) / "data" / "etf" / "metadata.json", store.catalog_path("etf"))
            self.assertEqual(Path(directory) / "data" / "indices" / "industry" / "metadata.json", store.catalog_path("industry"))
            persisted = pd.read_csv(path, encoding="gbk", dtype={"code": str})
            self.assertEqual(["2024-01-03", "2024-01-02"], persisted["date"].tolist())

    def test_asset_store_rejects_etf_trade_units_outside_raw_price_range(self):
        corrupt = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02"]),
                "open": [35.0], "high": [36.0], "low": [34.0], "close": [35.5],
                "open_raw": [3.5], "high_raw": [3.6], "low_raw": [3.4], "close_raw": [3.55],
                "volume": [1000], "amount": [35_500],
                "asset_type": ["etf"], "ths_code": ["USHJ510300"],
            }
        )
        with TemporaryDirectory() as directory:
            store = MarketAssetStore(Path(directory) / "data")
            with self.assertRaisesRegex(ValueError, "VWAP"):
                store.write_history("etf", "510300", corrupt)

    def test_asset_store_allows_one_yuan_amount_rounding_on_tiny_etf_volume(self):
        tiny_trade = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02"]),
                "open": [0.756], "high": [0.756], "low": [0.756], "close": [0.756],
                "open_raw": [0.756], "high_raw": [0.756], "low_raw": [0.756], "close_raw": [0.756],
                "volume": [1], "amount": [1],
                "asset_type": ["etf"], "ths_code": ["USHJ510090"],
            }
        )
        with TemporaryDirectory() as directory:
            store = MarketAssetStore(Path(directory) / "data")

            path = store.write_history("etf", "510090", tiny_trade)

            self.assertTrue(path.exists())

    def test_asset_store_preserves_missing_etf_amount_instead_of_a_sentinel(self):
        missing_amount = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02"]),
                "open": [100.0], "high": [100.0], "low": [100.0], "close": [100.0],
                "open_raw": [100.0], "high_raw": [100.0], "low_raw": [100.0], "close_raw": [100.0],
                "volume": [1000], "amount": [float("nan")],
                "asset_type": ["etf"], "ths_code": ["USHJ511650"],
            }
        )
        with TemporaryDirectory() as directory:
            store = MarketAssetStore(Path(directory) / "data")

            path = store.write_history("etf", "511650", missing_amount)

            persisted = pd.read_csv(path, encoding="gbk")
            self.assertTrue(pd.isna(persisted.iloc[0]["amount"]))

    def test_asset_store_allows_one_price_tick_of_etf_amount_rounding(self):
        rounded = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02"]),
                "open": [99.999], "high": [99.999], "low": [99.986], "close": [99.998],
                "open_raw": [99.999], "high_raw": [99.999], "low_raw": [99.986], "close_raw": [99.998],
                "volume": [1_918_815], "amount": [191_880_360],
                "asset_type": ["etf"], "ths_code": ["USHJ511830"],
            }
        )
        with TemporaryDirectory() as directory:
            store = MarketAssetStore(Path(directory) / "data")

            path = store.write_history("etf", "511830", rounded)

            self.assertTrue(path.exists())

    def test_selection_universe_returns_stocks_and_etfs_with_asset_types(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            stock_path = data_dir / "00" / "000001.csv"
            stock_path.parent.mkdir(parents=True)
            stock_path.write_text("date,close\n2024-01-03,10\n", encoding="gbk")
            store = MarketAssetStore(data_dir)
            store.write_catalog(
                "etf",
                {
                    "510300": {
                        "code": "510300", "ths_code": "USHJ510300", "name": "沪深300ETF",
                        "asset_type": "etf", "subtype": "etf", "selection_eligible": True,
                    },
                    "165513": {
                        "code": "165513", "ths_code": "USZJ165513", "name": "商品LOF",
                        "asset_type": "etf", "subtype": "lof", "selection_eligible": False,
                    },
                },
            )
            etf_path = store.history_path("etf", "510300")
            etf_path.parent.mkdir(parents=True)
            etf_path.write_text("date,close\n2024-01-03,3.6\n", encoding="gbk")
            lof_path = store.history_path("etf", "165513")
            lof_path.parent.mkdir(parents=True)
            lof_path.write_text("date,close\n2024-01-03,1.0\n", encoding="gbk")

            assets = SelectionUniverse(data_dir).list_assets(include_etfs=True)

            self.assertEqual(["000001", "510300"], [asset.code for asset in assets])
            self.assertEqual(["stock", "etf"], [asset.asset_type for asset in assets])
            self.assertEqual("sh", assets[1].exchange)
            self.assertEqual(etf_path, assets[1].path)


if __name__ == "__main__":
    unittest.main()
