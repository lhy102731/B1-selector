from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import run_b1_v3 as runner


class B1V3OutputTests(unittest.TestCase):
    def test_sweep_writes_generated_csv_under_requested_output_directory(self):
        with TemporaryDirectory() as temp:
            output_dir = Path(temp) / "artifacts" / "b1-v3"
            args = SimpleNamespace(
                max_stocks=0,
                start="2024-01-01",
                end="2024-06-30",
                sweep=["max_hold_days=20"],
                sweep_preset=None,
                output_dir=str(output_dir),
            )
            with (
                patch.object(runner, "get_stock_list", return_value=[]),
                patch.object(runner, "build_raw_cache", return_value=({}, [])),
            ):
                runner.cmd_sweep(args)

            output_path = output_dir / "sweep_results_v3.csv"
            self.assertTrue(output_path.is_file())
            self.assertEqual(output_dir.resolve(), output_path.parent.resolve())


if __name__ == "__main__":
    unittest.main()
