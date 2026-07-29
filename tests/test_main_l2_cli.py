from __future__ import annotations

import sys
import unittest
from unittest.mock import patch

import main as main_module


class MainL2CliTests(unittest.TestCase):
    def test_l2_cli_command_routes_without_starting_the_gui(self):
        with (
            patch.object(
                sys,
                "argv",
                ["main.py", "l2", "--cli", "--stock", "600366,000001"],
            ),
            patch.object(main_module, "QuantSystem") as system_type,
        ):
            main_module.main()

        system_type.return_value.run_l2.assert_called_once_with(
            cli_mode=True,
            stock="600366,000001",
            backfill_days=None,
        )


if __name__ == "__main__":
    unittest.main()
