import unittest
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pandas as pd

import backtest_brick_v2_research as brick
from backtest_brick_v2_research import apply_kbase_overlay, apply_market_regime_topn


class BrickKBaseOverlayTests(unittest.TestCase):
    def test_none_overlay_preserves_scores(self) -> None:
        df = pd.DataFrame({
            "entry_date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "score": [0.9, 0.1],
        })
        out = apply_kbase_overlay(df, overlay="none", weight=0.5)
        self.assertEqual(out["score"].tolist(), [0.9, 0.1])

    def test_turnover_extreme_overlay_can_change_daily_rank(self) -> None:
        df = pd.DataFrame({
            "entry_date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "score": [0.9, 0.8],
            "turnover_extreme_score": [0.1, 1.0],
            "turnover_to_60d_max": [0.1, 1.0],
            "volume_to_60d_max": [0.1, 1.0],
        })
        out = apply_kbase_overlay(df, overlay="turnover_extreme", weight=1.0)
        top_idx = out["score"].idxmax()
        self.assertEqual(top_idx, 1)
        self.assertIn("base_score", out.columns)
        self.assertIn("kbase_overlay_score", out.columns)

    def test_turnover_calm_overlay_favors_lower_extreme_score(self) -> None:
        df = pd.DataFrame({
            "entry_date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "score": [0.9, 0.8],
            "turnover_extreme_score": [0.1, 1.0],
            "turnover_to_60d_max": [0.1, 1.0],
            "volume_to_60d_max": [0.1, 1.0],
        })
        out = apply_kbase_overlay(df, overlay="turnover_calm", weight=1.0)
        self.assertEqual(out["score"].idxmax(), 0)

    def test_turnover_mid_overlay_favors_middle_extreme_score(self) -> None:
        df = pd.DataFrame({
            "entry_date": pd.to_datetime(["2024-01-02"] * 3),
            "score": [0.9, 0.8, 0.7],
            "turnover_extreme_score": [0.1, 0.65, 1.0],
            "turnover_to_60d_max": [0.1, 0.65, 1.0],
            "volume_to_60d_max": [0.1, 0.65, 1.0],
        })
        out = apply_kbase_overlay(df, overlay="turnover_mid", weight=1.0)
        self.assertEqual(out["score"].idxmax(), 1)

    def test_active_cap_regime_can_reduce_daily_topn(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "active_cap.csv"
            dates = pd.date_range("2023-12-01", periods=25, freq="B")
            caps = [100.0] * 24 + [50.0]
            pd.DataFrame({"date": dates, "active_cap": caps}).to_csv(path, index=False)
            signal_date = dates[-1]
            df = pd.DataFrame({
                "signal_date": [signal_date] * 3,
                "entry_date": [signal_date + pd.Timedelta(days=1)] * 3,
                "score": [0.9, 0.8, 0.7],
            })
            out = apply_market_regime_topn(
                df, top_n=3, mode="active_cap_topn", path=str(path)
            )
            self.assertEqual(len(out), 2)
            self.assertTrue((out["_dynamic_top_n"] == 2).all())

    def test_active_cap_max_can_disable_overlay(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "active_cap.csv"
            dates = pd.date_range("2023-12-01", periods=25, freq="B")
            caps = [100.0] * 24 + [200.0]
            pd.DataFrame({"date": dates, "active_cap": caps}).to_csv(path, index=False)
            signal_date = dates[-1]
            df = pd.DataFrame({
                "signal_date": [signal_date] * 2,
                "entry_date": [signal_date + pd.Timedelta(days=1)] * 2,
                "score": [0.9, 0.8],
                "turnover_extreme_score": [0.1, 1.0],
                "turnover_to_60d_max": [0.1, 1.0],
                "volume_to_60d_max": [0.1, 1.0],
            })
            df.index = [10, 20]
            out = apply_kbase_overlay(
                df,
                overlay="turnover_extreme",
                weight=1.0,
                active_cap_max=1.15,
                active_cap_path=str(path),
            )
            self.assertEqual(out.index.tolist(), [10, 20])
            self.assertEqual(out["score"].idxmax(), 10)

    def test_account_nav_uses_previous_trading_close_after_weekend(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            data_dir = tmp_path / "data"
            (data_dir / "00").mkdir(parents=True)
            pd.DataFrame({
                "date": ["2024-01-05", "2024-01-08"],
                "close": [100.0, 112.0],
            }).to_csv(data_dir / "00" / "000001.csv", index=False, encoding="gbk")
            trades = pd.DataFrame([{
                "code": "000001",
                "entry_date": "2024-01-05",
                "entry_price": 100.0,
                "exit_date": "2024-01-08",
                "exit_price": 110.0,
            }])
            args = SimpleNamespace(
                commission=0.0,
                slippage=0.0,
                stamp=0.0,
                top_n=1,
                output_suffix="weekend_regression",
                output_dir=tmp_path,
            )

            old_data_dir = brick.DATA_DIR
            old_cwd = os.getcwd()
            try:
                brick.DATA_DIR = data_dir
                os.chdir(tmp_path)
                brick.build_account_nav(
                    trades,
                    pd.Timestamp("2024-01-05"),
                    pd.Timestamp("2024-01-08"),
                    args,
                )
                nav = pd.read_csv(tmp_path / "backtest_brick_nav_weekend_regression.csv")
            finally:
                os.chdir(old_cwd)
                brick.DATA_DIR = old_data_dir

            monday_ret = nav.loc[nav["date"] == "2024-01-08", "ret"].iloc[0]
            self.assertAlmostEqual(monday_ret, 0.10)


if __name__ == "__main__":
    unittest.main()
