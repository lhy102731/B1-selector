from __future__ import annotations

import unittest

import pandas as pd

from backtest_optimized import OptimizedBacktester


def indicator_rows(dates: list[str], *, open_price: float = 10.0, close: float = 10.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(dates),
            "open": [open_price] * len(dates),
            "high": [close + 1.0] * len(dates),
            "low": [close - 1.0] * len(dates),
            "close": [close] * len(dates),
            "volume": [1_000] * len(dates),
            "yellow_line": [close + 5.0] * len(dates),
            "white_line": [close + 5.0] * len(dates),
            "J": [10.0] * len(dates),
        }
    )


class StrictTradeDateTests(unittest.TestCase):
    def make_backtester(self) -> OptimizedBacktester:
        backtester = OptimizedBacktester.__new__(OptimizedBacktester)
        backtester.trading_days = ["2024-01-02", "2024-01-03", "2024-01-04"]
        backtester.position_pct = 0.10
        backtester.commission = 0.0003
        backtester.cash = 1_000_000.0
        backtester.positions = []
        backtester.closed_trades = []
        return backtester

    def test_next_session_suspension_does_not_buy_at_signal_day_open(self):
        backtester = self.make_backtester()
        prior_only = indicator_rows(["2024-01-02"])
        backtester._get_realtime_indicators = lambda code, date: prior_only.copy()
        stock_info = {
            "code": "000001",
            "name": "fixture",
            "b1_score": 80.0,
            "is_washout": False,
            "signal_day_low": 9.0,
        }

        bought = backtester.buy_stock("2024-01-02", stock_info, 1_000_000.0)

        self.assertFalse(bought)
        self.assertEqual([], backtester.positions)
        self.assertEqual(1_000_000.0, backtester.cash)

    def test_suspended_session_does_not_execute_exit_on_stale_close(self):
        backtester = self.make_backtester()
        stale = indicator_rows(
            ["2024-01-02", "2024-01-01", "2023-12-29", "2023-12-28", "2023-12-27"],
            close=5.0,
        )
        backtester._get_realtime_indicators = lambda code, date: stale.copy()
        backtester._is_volume_shrink = lambda frame: False
        backtester.positions = [
            {
                "code": "000001",
                "shares": 1_000,
                "cost": 10_000.0,
                "buy_price": 10.0,
                "buy_date": "2024-01-02",
                "actual_buy_date": "2024-01-02",
                "batch_prices": [10.0],
                "stop_loss_ref": 9.0,
                "stop_loss_ref_active": 9.0,
                "surge_start_date": None,
            }
        ]

        backtester.check_exits_master("2024-01-03")

        self.assertEqual(1, len(backtester.positions))
        self.assertEqual([], backtester.closed_trades)
        self.assertEqual(1_000_000.0, backtester.cash)


if __name__ == "__main__":
    unittest.main()
