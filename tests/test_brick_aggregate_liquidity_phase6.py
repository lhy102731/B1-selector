from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from research.brick_aggregate_liquidity_modulation_phase6 import (
    VOLUME_FEATURES,
    add_state_percentiles,
    bootstrap_delta,
    model_matrix,
)


class BrickAggregateLiquidityPhase6Tests(unittest.TestCase):
    def test_state_score_is_equal_weight_mean_of_historical_percentiles(self) -> None:
        rows = 80
        daily = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=rows, freq="B"),
            "total_amount": np.arange(1, rows + 1, dtype=float),
            "advancers": np.arange(101, 101 + rows, dtype=float),
            "decliners": np.arange(180, 180 - rows, -1, dtype=float),
            "participants": np.arange(51, 51 + rows, dtype=float),
            "participation_eligible": np.full(rows, 200.0),
            "amount_rows": np.full(rows, 198.0),
            "universe_rows": np.full(rows, 200.0),
        })

        state = add_state_percentiles(daily)
        last = state.iloc[-1]

        expected = np.mean([
            last["market_amount_pctile_60d"],
            last["advance_decline_pctile_60d"],
            last["participation_breadth_pctile_60d"],
        ])
        self.assertAlmostEqual(expected, last["aggregate_liquidity_state_score"])
        self.assertAlmostEqual(0.99, last["amount_stock_coverage"])

    def test_model_matrix_modulates_only_locked_volume_features(self) -> None:
        frame = pd.DataFrame({
            "aggregate_liquidity_state_score": [0.25, 0.50],
            "red_height": [2.0, 4.0],
            "vol_ratio_5": [8.0, 10.0],
            "vol_ratio_20": [4.0, 6.0],
            "turnover_ratio_5": [2.0, 3.0],
        })

        baseline = model_matrix(frame, modulated=False)
        modulated = model_matrix(frame, modulated=True)

        self.assertTrue(np.allclose(baseline["red_height"], modulated["red_height"]))
        for feature in VOLUME_FEATURES:
            self.assertTrue(np.allclose(
                modulated[feature],
                baseline[feature] * frame["aggregate_liquidity_state_score"],
            ))

    def test_bootstrap_delta_detects_separated_samples(self) -> None:
        result = bootstrap_delta(
            np.full(30, 0.04),
            np.full(30, 0.01),
            seed=42,
            draws=500,
        )

        self.assertAlmostEqual(0.03, result["delta"], places=6)
        self.assertGreater(result["ci_low"], 0.0)
        self.assertLess(result["p_value"], 0.05)


if __name__ == "__main__":
    unittest.main()
