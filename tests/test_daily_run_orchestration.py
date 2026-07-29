from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import daily_run as daily_runner
from utils.process_lock import process_lock


class DailyRunOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self._lock_directory = TemporaryDirectory()
        self.addCleanup(self._lock_directory.cleanup)
        lock_patch = patch.object(
            daily_runner,
            "DAILY_RUN_LOCK_PATH",
            Path(self._lock_directory.name) / "daily.lock",
        )
        lock_patch.start()
        self.addCleanup(lock_patch.stop)

    def test_windows_launcher_uses_its_own_repository_directory(self):
        launcher = Path("run_select.bat").read_text(encoding="utf-8")

        self.assertIn('cd /d "%~dp0"', launcher)
        self.assertIn('daily_run.py', launcher)
        self.assertNotIn('D:\\workspace\\', launcher)

    def test_pipeline_uses_completed_local_date_and_avoids_duplicate_native_index_update(self):
        commands = []

        def record(command, description):
            commands.append(command)
            return True

        with (
            patch.object(
                sys,
                "argv",
                ["daily_run.py", "--skip-brick", "--skip-etf"],
            ),
            patch.object(daily_runner, "run", side_effect=record),
            patch.object(
                daily_runner,
                "_get_latest_trading_day",
                side_effect=AssertionError("network calendar must not be used"),
                create=True,
            ),
            patch(
                "run_b1_v3._effective_select_date",
                return_value=date(2026, 7, 24),
            ),
        ):
            result = daily_runner.main()

        self.assertEqual(0, result)
        self.assertEqual([sys.executable, "main.py", "update"], commands[0])
        self.assertIn(
            [
                sys.executable,
                "tools/update_ths_market_assets.py",
                "--asset-types",
                "etf",
                "--end",
                "2026-07-24",
            ],
            commands,
        )
        self.assertIn(
            [
                sys.executable,
                "run_b1_v3.py",
                "select",
                "--date",
                "2026-07-24",
            ],
            commands,
        )

    def test_second_daily_pipeline_fails_before_starting_any_step(self):
        with TemporaryDirectory() as temp:
            lock_path = Path(temp) / "daily.lock"
            with (
                process_lock(lock_path, "daily pipeline"),
                patch.object(
                    sys,
                    "argv",
                    ["daily_run.py", "--skip-update", "--skip-b1", "--skip-brick"],
                ),
                patch.object(
                    daily_runner,
                    "DAILY_RUN_LOCK_PATH",
                    lock_path,
                    create=True,
                ),
                patch.object(daily_runner, "run") as execute,
                patch(
                    "run_b1_v3._effective_select_date",
                    return_value=date(2026, 7, 24),
                ),
            ):
                result = daily_runner.main()

            self.assertEqual(3, result)
            execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
