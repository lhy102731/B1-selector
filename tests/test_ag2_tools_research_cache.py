from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

from ag2_research import tools as ag2_tools


class FakeBacktester:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.strategy = MagicMock()
        self.last_summary = {"status": "completed", "total_return_pct": 1.2}
        FakeBacktester.instances.append(self)

    def run(self, **kwargs):
        self.run_kwargs = kwargs


class AG2ToolsResearchCacheTests(unittest.TestCase):
    def setUp(self):
        FakeBacktester.instances.clear()

    def test_run_backtest_defaults_to_research_indicator_cache(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            cache_dir = root / "data" / "research_indicators_cache"
            cache_dir.mkdir(parents=True)
            (cache_dir / "000001.parquet").write_text("", encoding="utf-8")

            with (
                patch.object(ag2_tools, "_PROJECT_ROOT", root),
                patch("backtest_optimized.OptimizedBacktester", FakeBacktester),
            ):
                result = json.loads(ag2_tools.run_backtest(
                    start_date="2024-01-01",
                    end_date="2024-12-31",
                ))

            self.assertEqual("research_indicators_cache", FakeBacktester.instances[0].kwargs["indicators_cache_name"])
            self.assertEqual("research_indicators_cache", result["indicator_cache"]["name"])
            self.assertEqual(1, result["indicator_cache"]["files"])

    def test_run_backtest_allows_explicit_production_cache(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(ag2_tools, "_PROJECT_ROOT", root),
                patch("backtest_optimized.OptimizedBacktester", FakeBacktester),
            ):
                result = json.loads(ag2_tools.run_backtest(
                    start_date="2024-01-01",
                    end_date="2024-12-31",
                    indicators_cache_name="indicators_cache",
                ))

            self.assertEqual("indicators_cache", FakeBacktester.instances[0].kwargs["indicators_cache_name"])
            self.assertEqual("indicators_cache", result["indicator_cache"]["name"])

    def test_run_backtest_rejects_unsafe_cache_name(self):
        result = json.loads(ag2_tools.run_backtest(
            start_date="2024-01-01",
            end_date="2024-12-31",
            indicators_cache_name="../indicators_cache",
        ))

        self.assertIn("error", result)
        self.assertIn("indicator cache name", result["error"])

    def test_list_available_data_reports_research_and_production_caches(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir = root / "data"
            (data_dir / "00").mkdir(parents=True)
            (data_dir / "00" / "000001.csv").write_text("date,close\n2024-01-01,10\n", encoding="gbk")
            research_cache = data_dir / "research_indicators_cache"
            production_cache = data_dir / "indicators_cache"
            signal_cache = data_dir / "signal_cache"
            research_cache.mkdir()
            production_cache.mkdir()
            signal_cache.mkdir()
            (research_cache / "000001.parquet").write_text("", encoding="utf-8")
            (research_cache / "000002.parquet").write_text("", encoding="utf-8")
            (production_cache / "000001.parquet").write_text("", encoding="utf-8")
            (signal_cache / "cache.pkl").write_text("", encoding="utf-8")

            with patch.object(ag2_tools, "_PROJECT_ROOT", root):
                result = json.loads(ag2_tools.list_available_data())

        self.assertEqual("research_indicators_cache", result["default_indicator_cache"])
        self.assertEqual(1, result["total_stocks"])
        self.assertEqual(2, result["indicator_cache_files"])
        self.assertEqual(2, result["research_indicator_cache"]["files"])
        self.assertEqual(1, result["production_indicator_cache"]["files"])
        self.assertEqual(1, result["signal_cache_files"])


if __name__ == "__main__":
    unittest.main()
