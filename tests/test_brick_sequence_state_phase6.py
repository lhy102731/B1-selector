from __future__ import annotations

import unittest

import numpy as np

from research.brick_sequence_state_phase6 import _sequence_state_from_brick


class BrickSequenceStatePhase6Tests(unittest.TestCase):
    def test_transition_signal_has_zero_exclusive_same_color_and_one_recency(self):
        # Colors from deltas are: up, up, down, down, up.
        state = _sequence_state_from_brick(
            np.array([1.0, 2.0, 3.0, 2.0, 1.0, 2.0], dtype=float)
        )

        self.assertEqual(0.0, state["same_color_exclusive"][5])
        self.assertEqual(1.0, state["reversal_recency"][5])
        self.assertEqual(0.5, state["run_length_ratio_raw"][5])

    def test_zero_change_day_is_not_counted_as_a_brick(self):
        state = _sequence_state_from_brick(
            np.array([1.0, 2.0, 2.0, 3.0], dtype=float)
        )

        self.assertTrue(np.isnan(state["reversal_recency"][2]))
        self.assertEqual(2.0, state["reversal_recency"][3])


if __name__ == "__main__":
    unittest.main()
