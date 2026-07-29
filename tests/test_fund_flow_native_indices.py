from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from utils.fund_flow_collector import FundFlowCollector


class FundFlowNativeIndexTests(unittest.TestCase):
    def test_native_index_update_delegates_to_ths_without_member_synthesis(self):
        with TemporaryDirectory() as directory:
            block_dir = Path(directory) / "data" / "block"
            collector = FundFlowCollector(block_dir)

            with patch("tools.update_ths_market_assets.run", return_value=0) as run:
                result = collector.collect_native_indices("2024-01-03")

            self.assertEqual(0, result)
            run.assert_called_once_with(
                data_dir=block_dir.parent,
                asset_types=("industry", "concept"),
                end="2024-01-03",
            )

    def test_failed_layer_does_not_mark_block_cache_current(self):
        with TemporaryDirectory() as directory:
            block_dir = Path(directory) / "data" / "block"
            collector = FundFlowCollector(block_dir)

            with (
                patch.object(collector, "_is_trading_day", return_value=False),
                patch.object(collector, "_check_block_cache", return_value=False),
                patch.object(
                    collector,
                    "collect_concept_fund_flow",
                    side_effect=RuntimeError("source unavailable"),
                ),
                patch.object(collector, "collect_stock_fund_flow"),
                patch.object(collector, "collect_big_deal"),
                patch.object(collector, "collect_native_indices"),
                patch.object(collector, "_write_block_cache") as write_cache,
            ):
                result = collector.collect_all("2024-01-03")

            self.assertEqual(2, result)
            write_cache.assert_not_called()


if __name__ == "__main__":
    unittest.main()
