from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from tools.select_etf_candidates import scan_etf_candidates
from utils.market_asset_store import MarketAssetStore


class _FakeStrategy:
    def __init__(self):
        self.calls = []

    def select_stocks(self, frame, name, *, asset_type):
        self.calls.append((frame.copy(), name, asset_type))
        return [{"close": 3.65, "J": 12.0, "reasons": ["technical pullback"]}]


class ETFSelectionTests(unittest.TestCase):
    def test_scanner_emits_typed_etf_candidates_without_stock_ranking(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            store = MarketAssetStore(data_dir)
            store.write_catalog(
                "etf",
                {
                    "510300": {
                        "code": "510300", "ths_code": "USHJ510300", "name": "沪深300ETF",
                        "asset_type": "etf", "selection_eligible": True,
                    }
                },
            )
            history_path = store.history_path("etf", "510300")
            history_path.parent.mkdir(parents=True)
            history_path.write_text("date,close\n2024-01-03,3.65\n", encoding="gbk")
            cache_dir = data_dir / "indicators_cache" / "etf"
            cache_dir.mkdir(parents=True)
            pd.DataFrame(
                {
                    "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
                    "close": [3.55, 3.65], "J": [15.0, 12.0],
                    "asset_type": ["etf", "etf"],
                }
            ).to_parquet(cache_dir / "510300.parquet", index=False)
            strategy = _FakeStrategy()

            candidates = scan_etf_candidates(
                data_dir=data_dir,
                signal_date="2024-01-03",
                strategy=strategy,
            )

            self.assertEqual(1, len(candidates))
            self.assertEqual("etf:510300", candidates[0]["instrument_id"])
            self.assertEqual("research_only", candidates[0]["validation_status"])
            self.assertEqual("etf", strategy.calls[0][2])
            self.assertTrue(strategy.calls[0][0]["date"].is_monotonic_decreasing)


if __name__ == "__main__":
    unittest.main()
