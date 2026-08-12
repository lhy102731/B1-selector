from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

import run_research


class AG2CliRoutingTests(unittest.TestCase):
    @patch("run_research._cli_preflight")
    @patch("run_research._campaign_boundary")
    @patch("research_automation.kbase_ag2_full_cycle.run_kbase_ag2_full_cycle")
    def test_full_cycle_defaults_to_roundtable_discovery(self, full_cycle, _preflight, _boundary):
        full_cycle.return_value = {"status": "DRY_RUN_READY"}
        args = argparse.Namespace(
            profile=None,
            context=None,
            context_file=None,
            topic="liquidity gap",
            strategy="brick",
            handoff_path=None,
            output_dir=None,
            sequential=False,
            no_auto_repair=False,
            dry_run=True,
            claude_binary="claude",
            repair_timeout=900,
        )

        run_research.cmd_full_cycle(args)

        self.assertEqual(
            "kbase_roundtable_discovery",
            full_cycle.call_args.kwargs["workflow_id"],
        )

    @patch("run_research._cli_preflight")
    @patch("run_research._campaign_boundary")
    @patch("research_automation.kbase_ag2_full_cycle.run_kbase_ag2_full_cycle")
    def test_full_cycle_sequential_override_uses_legacy_discovery(
        self, full_cycle, _preflight, _boundary
    ):
        full_cycle.return_value = {"status": "DISCOVERY_STOP"}
        args = argparse.Namespace(
            profile=None,
            context=None,
            context_file=None,
            topic="legacy discovery",
            strategy="brick",
            handoff_path=None,
            output_dir=None,
            sequential=True,
            no_auto_repair=False,
            dry_run=True,
            claude_binary="claude",
            repair_timeout=900,
        )

        run_research.cmd_full_cycle(args)

        self.assertEqual("kbase_discovery", full_cycle.call_args.kwargs["workflow_id"])

    @patch("run_research._cli_preflight")
    @patch("run_research._campaign_boundary")
    @patch("run_research.Orchestrator")
    def test_brainstorm_uses_configured_workflow_dispatch(
        self, orchestrator_cls, _preflight, _boundary
    ):
        orchestrator = MagicMock(profile="default")
        orchestrator_cls.return_value = orchestrator
        args = argparse.Namespace(
            profile=None, context=None, context_file=None, topic="test gap"
        )

        run_research.cmd_brainstorm(args)

        orchestrator.run_workflow.assert_called_once_with(
            "brainstorm", topic="test gap", research_context=""
        )
        orchestrator.run_brainstorm.assert_not_called()

    @patch("run_research._cli_preflight")
    @patch("run_research._campaign_boundary")
    @patch("run_research.save_discovery_handoff")
    @patch("run_research.Orchestrator")
    def test_discover_routes_to_source_first_kbase_workflow(
        self, orchestrator_cls, save_handoff, _preflight, _boundary
    ):
        orchestrator = MagicMock(profile="default")
        orchestrator.run_workflow.return_value = {
            "status": "APPROVED", "reason": "all gates passed; no memory writes performed"
        }
        orchestrator_cls.return_value = orchestrator
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(
            profile=None,
            context=None,
            context_file=None,
            topic="post-market review value",
            strategy="b1",
            output_dir=tmp,
            sequential=False,
        )

            save_handoff.return_value = Path(tmp) / "handoff.yaml"
            saved = run_research.cmd_discover(args)

            self.assertEqual(save_handoff.return_value, saved)
            save_handoff.assert_called_once_with(
                orchestrator.run_workflow.return_value,
                topic="post-market review value",
                strategy_id="b1",
                output_dir=tmp,
            )

        orchestrator.run_workflow.assert_called_once_with(
            "kbase_source_first_discovery",
            topic="post-market review value",
            research_context="",
            strategy_id="b1",
        )

    @patch("run_research._cli_preflight")
    @patch("run_research._campaign_boundary")
    @patch("run_research.save_discovery_handoff")
    @patch("run_research.Orchestrator")
    def test_discover_roundtable_first_flag_keeps_legacy_route(
        self, orchestrator_cls, save_handoff, _preflight, _boundary
    ):
        orchestrator = MagicMock(profile="default")
        orchestrator.run_workflow.return_value = {"status": "REJECTED", "reason": "test"}
        orchestrator_cls.return_value = orchestrator
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(
                profile=None,
                context=None,
                context_file=None,
                topic="legacy route",
                strategy="b1",
                output_dir=tmp,
                sequential=False,
                roundtable_first=True,
            )
            save_handoff.return_value = Path(tmp) / "handoff.yaml"
            run_research.cmd_discover(args)

            save_handoff.assert_called_once()

        orchestrator.run_workflow.assert_called_once_with(
            "kbase_roundtable_discovery",
            topic="legacy route",
            research_context="",
            strategy_id="b1",
        )

    @patch("run_research._cli_preflight")
    @patch("run_research._campaign_boundary")
    @patch("run_research.save_discovery_handoff")
    @patch("run_research.Orchestrator")
    def test_resume_discover_routes_to_checkpoint_resume(
        self, orchestrator_cls, save_handoff, _preflight, _boundary
    ):
        orchestrator = MagicMock(profile="default")
        orchestrator.resume_source_first_discovery.return_value = {
            "status": "ESCALATE_TO_USER", "reason": "quota"
        }
        orchestrator_cls.return_value = orchestrator
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = Path(tmp) / "checkpoint.yaml"
            checkpoint.write_text(yaml.safe_dump({
                "handoff_type": "kbase_discovery",
                "strategy_id": "brick",
                "topic": "resume",
                "result": {},
            }), encoding="utf-8")
            args = argparse.Namespace(
                profile=None,
                handoff_path=str(checkpoint),
                output_dir=tmp,
            )
            save_handoff.return_value = Path(tmp) / "handoff.yaml"
            saved = run_research.cmd_resume_discover(args)

            self.assertEqual(save_handoff.return_value, saved)
            save_handoff.assert_called_once()
        orchestrator.resume_source_first_discovery.assert_called_once_with(checkpoint.resolve())

    def test_discovery_handoff_rejects_unsafe_strategy_identifier(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                run_research.save_discovery_handoff(
                    {"status": "APPROVED", "transcript": []},
                    topic="x", strategy_id="../escape", output_dir=Path(tmp),
                )


if __name__ == "__main__":
    unittest.main()
