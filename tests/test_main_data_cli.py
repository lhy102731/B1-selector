from __future__ import annotations

import unittest
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import main as main_module
from main import QuantSystem


class MainDataCliTests(unittest.TestCase):
    def test_init_propagates_nonzero_thsdk_rebuild_result(self):
        system = object.__new__(QuantSystem)
        system.fetcher = Mock()
        system.fetcher.init_full_data.return_value = 2

        with self.assertRaisesRegex(RuntimeError, "THSDK full rebuild stopped"):
            system.init_data(max_stocks=None, force_full=True)

    def test_no_baostock_compatibility_flag_reaches_update_path(self):
        with (
            patch.object(sys, "argv", ["main.py", "update", "--no-baostock"]),
            patch.object(main_module, "QuantSystem") as system_type,
        ):
            main_module.main()

        system_type.return_value.update_data.assert_called_once_with(
            max_stocks=None,
            skip_baostock=True,
        )

    def test_update_uses_configured_data_tree_and_propagates_fund_flow_failure(self):
        with TemporaryDirectory() as temp:
            system = object.__new__(QuantSystem)
            system.data_dir = temp
            system.fetcher = Mock()
            collector = Mock()
            collector.collect_all.return_value = 2

            with patch(
                "utils.fund_flow_collector.FundFlowCollector",
                return_value=collector,
            ) as collector_type:
                with self.assertRaisesRegex(RuntimeError, "fund-flow update failed"):
                    system.update_data(max_stocks=12, skip_baostock=True)

            system.fetcher.daily_update.assert_called_once_with(
                max_stocks=12,
                skip_baostock=True,
            )
            collector_type.assert_called_once_with(Path(temp) / "block")


if __name__ == "__main__":
    unittest.main()
