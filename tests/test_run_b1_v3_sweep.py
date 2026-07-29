from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import run_b1_v3 as runner


class B1V3SweepTests(unittest.TestCase):
    def setUp(self):
        output_patch = patch.object(runner.pd.DataFrame, "to_csv")
        output_patch.start()
        self.addCleanup(output_patch.stop)

    def test_each_grid_combination_builds_raw_signals_with_its_own_parameters(self):
        observed_j_max = []

        def build_raw_cache(codes, start, end, params):
            observed_j_max.append(params.j_max)
            return {}, codes

        with TemporaryDirectory() as temp:
            args = SimpleNamespace(
                max_stocks=0,
                start="2024-01-01",
                end="2024-06-30",
                sweep=["j_max=20,30"],
                sweep_preset=None,
                output_dir=str(Path(temp) / "outputs"),
            )
            with (
                patch.object(runner, "get_stock_list", return_value=[]),
                patch.object(runner, "build_raw_cache", side_effect=build_raw_cache),
            ):
                results = runner.cmd_sweep(args)

        self.assertEqual([20, 30], observed_j_max)
        self.assertEqual(2, len(results))

    def test_post_extraction_grid_values_reuse_one_raw_signal_build(self):
        raw_builds = []

        def build_raw_cache(codes, start, end, params):
            raw_builds.append(params.max_hold_days)
            return {}, codes

        with TemporaryDirectory() as temp:
            args = SimpleNamespace(
                max_stocks=0,
                start="2024-01-01",
                end="2024-06-30",
                sweep=["max_hold_days=20,35"],
                sweep_preset=None,
                output_dir=str(Path(temp) / "outputs"),
            )
            with (
                patch.object(runner, "get_stock_list", return_value=[]),
                patch.object(runner, "build_raw_cache", side_effect=build_raw_cache),
            ):
                results = runner.cmd_sweep(args)

        self.assertEqual(1, len(raw_builds))
        self.assertEqual(2, len(results))

    def test_unknown_grid_parameter_is_rejected(self):
        with TemporaryDirectory() as temp:
            args = SimpleNamespace(
                max_stocks=0,
                start="2024-01-01",
                end="2024-06-30",
                sweep=["j_mxa=20"],
                sweep_preset=None,
                output_dir=str(Path(temp) / "outputs"),
            )
            with patch.object(runner, "get_stock_list", return_value=[]):
                with self.assertRaisesRegex(ValueError, "unknown B1 V3 parameter"):
                    runner.cmd_sweep(args)

    def test_preset_can_be_used_without_explicit_grid_values(self):
        with TemporaryDirectory() as temp:
            args = SimpleNamespace(
                max_stocks=0,
                start="2024-01-01",
                end="2024-06-30",
                sweep=None,
                sweep_preset="max_hold",
                output_dir=str(Path(temp) / "outputs"),
            )
            with (
                patch.object(runner, "get_stock_list", return_value=[]),
                patch.object(runner, "build_raw_cache", return_value=({}, [])),
            ):
                results = runner.cmd_sweep(args)

        self.assertEqual(len(runner.SWEEP_PRESETS["max_hold"]["max_hold_days"]), len(results))


if __name__ == "__main__":
    unittest.main()
