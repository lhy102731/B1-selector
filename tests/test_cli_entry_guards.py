from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import run_research
from research_automation.control_plane.cli_registry import authorize_cli_command
from research_automation.control_plane.sink_guard import ExecutionAuthorizationError


class CliEntryGuardTests(unittest.TestCase):
    def test_network_command_is_blocked_before_orchestrator_construction(self) -> None:
        with patch.object(
            run_research,
            "_orchestrator_class",
            side_effect=AssertionError("Orchestrator constructed"),
        ):
            status = run_research.main(
                ["brainstorm", "--topic", "must not start"]
            )
        self.assertEqual(3, status)

    def test_repair_dry_run_is_blocked_before_output_or_repair_import(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "repair"
            handoff = Path(directory) / "handoff.yaml"
            with patch.dict(
                "sys.modules",
                {"research_automation.handoff_runner_repair": None},
            ):
                status = run_research.main(
                    [
                        "repair-handoff-runner",
                        "--handoff-path",
                        str(handoff),
                        "--output-dir",
                        str(output),
                        "--dry-run",
                    ]
                )
            self.assertEqual(3, status)
            self.assertFalse(output.exists())

    def test_execute_handoff_dry_run_preflight_is_read_only(self) -> None:
        result = authorize_cli_command(
            None,
            command="execute-handoff",
            argv=("run_research.py", "execute-handoff", "--dry-run"),
            dry_run=True,
        )
        self.assertIsNone(result)

    def test_execute_handoff_without_dry_run_requires_adapter(self) -> None:
        with self.assertRaises(ExecutionAuthorizationError):
            authorize_cli_command(
                None,
                command="execute-handoff",
                argv=("run_research.py", "execute-handoff"),
                dry_run=False,
            )

    def test_list_remains_read_only_without_authority(self) -> None:
        config = MagicMock(default_profile="test", profiles={})
        config.list_profiles.return_value = []
        config.list_agents.return_value = []
        config.list_workflows.return_value = []
        config_class = MagicMock(return_value=config)
        with patch.object(run_research, "_research_config_class", return_value=config_class):
            self.assertEqual(0, run_research.main(["list"]))
        config_class.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
