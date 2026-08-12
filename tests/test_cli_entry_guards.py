from __future__ import annotations

import argparse
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

    def test_discover_is_blocked_before_orchestrator_construction(self) -> None:
        with patch.object(
            run_research,
            "_orchestrator_class",
            side_effect=AssertionError("Orchestrator constructed"),
        ):
            status = run_research.main(
                ["discover", "--topic", "must not start"]
            )
        self.assertEqual(3, status)

    def test_full_cycle_is_blocked_before_orchestrator_construction(self) -> None:
        with patch.object(
            run_research,
            "_orchestrator_class",
            side_effect=AssertionError("Orchestrator constructed"),
        ):
            status = run_research.main(
                [
                    "full-cycle",
                    "--topic",
                    "must not start",
                    "--dry-run",
                ]
            )
        self.assertEqual(3, status)


class CampaignBoundaryCliTests(unittest.TestCase):
    def test_authorized_legacy_command_still_requires_campaign_boundary(self) -> None:
        with (
            patch.object(
                run_research,
                "_cli_preflight",
                return_value=object(),
            ),
            patch.object(
                run_research,
                "_orchestrator_class",
                side_effect=AssertionError("Orchestrator constructed"),
            ),
        ):
            status = run_research.main(
                ["brainstorm", "--topic", "must not start"]
            )
        self.assertEqual(3, status)

    def test_execute_handoff_dry_run_skips_campaign_boundary(self) -> None:
        args = argparse.Namespace(dry_run=True)
        with patch.object(
            run_research,
            "require_campaign_boundary",
            side_effect=AssertionError("dry-run must not cross the boundary"),
        ) as boundary:
            run_research._campaign_boundary(
                args,
                "execute-handoff",
                dry_run=True,
            )
        boundary.assert_not_called()

    def test_execute_handoff_without_dry_run_requires_campaign_boundary(self) -> None:
        args = argparse.Namespace(dry_run=False)
        with patch.object(
            run_research,
            "require_campaign_boundary",
        ) as boundary:
            run_research._campaign_boundary(
                args,
                "execute-handoff",
                dry_run=False,
            )
        boundary.assert_called_once_with(
            surface="run_research.py:execute-handoff"
        )

    def test_full_cycle_dry_run_is_not_a_read_only_exception(self) -> None:
        args = argparse.Namespace(dry_run=True)
        with patch.object(
            run_research,
            "require_campaign_boundary",
        ) as boundary:
            run_research._campaign_boundary(
                args,
                "full-cycle",
                dry_run=True,
            )
        boundary.assert_called_once_with(surface="run_research.py:full-cycle")


class CampaignCliTests(unittest.TestCase):
    def test_campaign_without_programmatic_context_exits_3(self) -> None:
        with patch.object(
            run_research,
            "_orchestrator_class",
            side_effect=AssertionError("provider constructed"),
        ):
            status = run_research.main(
                ["campaign", "--campaign-id", "c1", "--mode", "formal"]
            )
        self.assertEqual(3, status)

    def test_campaign_with_context_but_no_authorization_exits_3(self) -> None:
        fake_context = MagicMock(campaign_id="c1", mode="formal")
        fake_context.run.return_value = MagicMock(
            status="COMPLETED",
            to_payload=lambda: {"schema_version": "x", "status": "COMPLETED"},
        )
        with patch.object(
            run_research,
            "_orchestrator_class",
            side_effect=AssertionError("provider constructed"),
        ):
            status = run_research.main(
                ["campaign", "--campaign-id", "c1", "--mode", "formal"],
                campaign_context=fake_context,
            )
        self.assertEqual(3, status)

    def test_campaign_context_requires_cli_preflight_authorization(self) -> None:
        # _cli_preflight raises ExecutionAuthorizationError without a
        # programmatic CliAuthorizationContext, so the command is blocked
        # (main maps it to exit 3) before the runtime is touched even when a
        # campaign context exists.
        fake_context = MagicMock(campaign_id="c1", mode="formal")
        with patch.object(
            run_research,
            "_orchestrator_class",
            side_effect=AssertionError("provider constructed"),
        ):
            status = run_research.main(
                ["campaign", "--campaign-id", "c1", "--mode", "formal"],
                campaign_context=fake_context,
            )
        self.assertEqual(3, status)
        fake_context.run.assert_not_called()

    def test_campaign_bounds_cannot_expand_injected_context(self) -> None:
        fake_context = MagicMock(campaign_id="c1", mode="formal")
        fake_context.run.return_value = MagicMock(
            status="COMPLETED",
            to_payload=lambda: {"schema_version": "x", "status": "COMPLETED"},
        )
        with patch.object(
            run_research,
            "_cli_preflight",
            return_value=object(),
        ):
            status = run_research.main(
                ["campaign", "--campaign-id", "other-id", "--mode", "formal"],
                campaign_context=fake_context,
            )
        self.assertEqual(3, status)
        fake_context.run.assert_not_called()

    def test_campaign_mode_cannot_expand_injected_context(self) -> None:
        fake_context = MagicMock(campaign_id="c1", mode="formal")
        with patch.object(
            run_research,
            "_cli_preflight",
            return_value=object(),
        ):
            status = run_research.main(
                ["campaign", "--campaign-id", "c1", "--mode", "dry-run"],
                campaign_context=fake_context,
            )
        self.assertEqual(3, status)
        fake_context.run.assert_not_called()

    def test_campaign_with_authorized_context_runs_and_prints_safe_json(self) -> None:
        fake_context = MagicMock(campaign_id="c1", mode="formal")
        fake_context.run.return_value = MagicMock(
            status="COMPLETED",
            to_payload=lambda: {
                "schema_version": "control_plane.campaign_runtime_result.v1",
                "campaign_id": "c1",
                "namespace": "formal",
                "mode": "FORMAL",
                "status": "COMPLETED",
                "cycles_completed": 1,
                "decision": "STOP",
                "reason_code": "CYCLE_BUDGET_EXHAUSTED",
                "campaign_snapshot": {},
                "budget_summary": {},
                "cycle_summaries": [],
                "diagnostics": [],
            },
        )
        with patch.object(
            run_research,
            "_cli_preflight",
            return_value=object(),
        ):
            status = run_research.main(
                ["campaign", "--campaign-id", "c1", "--mode", "formal"],
                campaign_context=fake_context,
            )
        self.assertEqual(0, status)
        fake_context.run.assert_called_once_with(max_cycles=None)


if __name__ == "__main__":
    unittest.main()
