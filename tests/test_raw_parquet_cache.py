from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import pandas as pd

from backtest_optimized import OptimizedBacktester, _indicator_cache_path
from build_indicators_cache import build_etf_one, build_one, build_raw_one, result_exit_code, safe_cache_name
from utils.market_asset_store import MarketAssetStore
from utils.raw_parquet_cache import RawParquetCache, normalize_raw_stock_frame


def _write_stock_csv(data_dir: Path, code: str, rows: int = 70) -> Path:
    stock_dir = data_dir / code[:2]
    stock_dir.mkdir(parents=True, exist_ok=True)
    dates = pd.date_range("2024-01-01", periods=rows, freq="D")
    df = pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
        "open": [10.0 + i * 0.01 for i in range(rows)],
        "high": [10.5 + i * 0.01 for i in range(rows)],
        "low": [9.8 + i * 0.01 for i in range(rows)],
        "close": [10.2 + i * 0.01 for i in range(rows)],
        "volume": [1000 + i for i in range(rows)],
    })
    df.loc[0, "volume"] = 0
    df = df.sort_values("date", ascending=False)
    path = stock_dir / f"{code}.csv"
    df.to_csv(path, index=False, encoding="gbk")
    return path


class FakeStrategy:
    def __init__(self):
        self.params = {}

    def calculate_indicators(self, df):
        out = df.copy()
        out["white_line"] = out["close"]
        out["yellow_line"] = out["close"]
        return out


class RawParquetCacheTests(unittest.TestCase):
    def test_normalize_raw_stock_frame_sorts_and_filters(self):
        df = pd.DataFrame({
            "date": ["2024-01-03", "2024-01-01", "bad"],
            "open": [3, 1, 2],
            "high": [3, 1, 2],
            "low": [3, 1, 2],
            "close": [3, 1, 2],
            "volume": [30, 10, 0],
        })

        out = normalize_raw_stock_frame(df)

        self.assertEqual(out["date"].dt.strftime("%Y-%m-%d").tolist(), ["2024-01-01", "2024-01-03"])
        self.assertTrue((out["volume"] > 0).all())

    def test_raw_parquet_cache_builds_and_reuses_current_file(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            _write_stock_csv(data_dir, "000001")
            cache = RawParquetCache(data_dir)

            first = cache.read_stock("000001")
            second_status = build_raw_one("000001", data_dir)

            self.assertFalse(first.empty)
            self.assertTrue(cache.parquet_path("000001").exists())
            self.assertTrue(cache.is_current("000001"))
            self.assertIn("raw parquet current", second_status)

    def test_build_one_can_use_raw_parquet_cache(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            cache_dir = data_dir / "indicators_cache"
            _write_stock_csv(data_dir, "000001")

            with patch("build_indicators_cache.UnifiedB1Strategy", FakeStrategy):
                status = build_one("000001", data_dir, cache_dir, use_raw_cache=True)

            self.assertTrue(status.startswith("OK"), status)
            self.assertTrue((data_dir / "raw_parquet" / "00" / "000001.parquet").exists())
            result = pd.read_parquet(cache_dir / "000001.parquet")
            self.assertIn("white_line", result.columns)
            self.assertEqual(result["date"].is_monotonic_increasing, True)

    def test_etf_indicator_cache_is_typed_and_isolated_from_stock_cache_files(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            dates = pd.date_range("2024-01-01", periods=70, freq="D")
            close_raw = pd.Series([3.0 + i * 0.01 for i in range(70)])
            frame = pd.DataFrame(
                {
                    "date": dates,
                    "open": close_raw, "high": close_raw + 0.05,
                    "low": close_raw - 0.05, "close": close_raw,
                    "open_raw": close_raw, "high_raw": close_raw + 0.05,
                    "low_raw": close_raw - 0.05, "close_raw": close_raw,
                    "volume": [1000] * 70, "amount": close_raw * 1000,
                    "asset_type": ["etf"] * 70, "ths_code": ["USHJ510300"] * 70,
                }
            )
            MarketAssetStore(data_dir).write_history("etf", "510300", frame)

            with patch("build_indicators_cache.UnifiedB1Strategy", FakeStrategy):
                status = build_etf_one("510300", data_dir)

            self.assertTrue(status.startswith("OK"), status)
            cache_path = data_dir / "indicators_cache" / "etf" / "510300.parquet"
            self.assertTrue(cache_path.exists())
            result = pd.read_parquet(cache_path)
            self.assertTrue((result["asset_type"] == "etf").all())
            self.assertTrue((result["instrument_id"] == "etf:510300").all())
            self.assertFalse((data_dir / "indicators_cache" / "510300.parquet").exists())

    def test_research_indicator_cache_is_separate(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            cache_dir = data_dir / "research_indicators_cache"
            _write_stock_csv(data_dir, "000001")

            with patch("build_indicators_cache.UnifiedB1Strategy", FakeStrategy):
                status = build_one("000001", data_dir, cache_dir, use_raw_cache=True)

            self.assertTrue(status.startswith("OK"), status)
            self.assertTrue((data_dir / "research_indicators_cache" / "000001.parquet").exists())
            self.assertFalse((data_dir / "indicators_cache" / "000001.parquet").exists())
            self.assertEqual(
                data_dir / "research_indicators_cache" / "000001.parquet",
                _indicator_cache_path(data_dir, "research_indicators_cache", "000001"),
            )

    def test_cache_name_rejects_path_escape(self):
        self.assertEqual("research_indicators_cache", safe_cache_name("research_indicators_cache"))
        with self.assertRaises(ValueError):
            safe_cache_name("../indicators_cache")

    def test_cache_builder_exits_nonzero_on_any_worker_failure(self):
        self.assertEqual(0, result_exit_code(["OK   000001", "SKIP 000002 (insufficient data)"]))
        self.assertEqual(2, result_exit_code(["OK   000001", "FAIL 000002 (broken parquet)"]))

    def test_backtester_reads_explicit_research_indicator_cache(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            _write_stock_csv(data_dir, "000001")
            research_cache = data_dir / "research_indicators_cache"
            research_cache.mkdir(parents=True)
            pd.DataFrame({
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "close": [10.0, 11.0],
                "white_gt_yellow": [True, True],
                "J": [10.0, 10.0],
                "volume": [1000, 1000],
                "DIF": [1.0, 1.0],
                "doubled": [False, False],
            }).to_parquet(research_cache / "000001.parquet", index=False)

            backtester = OptimizedBacktester(
                data_dir=data_dir,
                indicators_cache_name="research_indicators_cache",
            )
            out = backtester._get_realtime_indicators("000001", "2024-01-02")

            self.assertFalse(out.empty)
            self.assertEqual(11.0, out.iloc[0]["close"])
            self.assertEqual("research_indicators_cache", backtester.indicators_cache_name)


if __name__ == "__main__":
    unittest.main()
