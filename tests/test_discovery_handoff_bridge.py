from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import yaml

from ag2_research.discovery_handoff import (
    extract_discovery_transcript,
    load_latest_approved_discovery,
    render_discovery_context,
    save_discovery_handoff,
)
from ag2_research.agents import create_agents
from ag2_research.config import ResearchConfig
from research_automation.control_plane.contracts import SideEffect
from research_automation.control_plane.sink_guard import ExecutionAuthorizationError
from research_automation.control_plane.sink_guard import ExecutionInvocation, RunnerIdentity
from research_automation.discovery_execution_bridge import (
    DiscoveryExecutionPlan,
    build_execution_plan,
    execute_plan,
)
from research_automation.autonomous_runner import AutonomousRunnerV1


def _document(status="APPROVED", factors=None):
    return {
        "handoff_type": "kbase_discovery",
        "schema_version": 1,
        "created_at": "2026-07-04T00:00:00+00:00",
        "strategy_id": "b1",
        "topic": "test gap",
        "status": status,
        "result": {"transcript": [
            {"stage": "source_librarian", "output": {"brief_id": "brief-1"}},
            {"stage": "alpha_hunter", "output": {"proposed_generator": {"family": "flow"}}},
            {"stage": "factor_engineer", "output": {"factor_batch": factors or []}},
        ]},
    }


def _roundtable_document(status="APPROVED", strategy_id="brick", factors=None):
    return {
        "handoff_type": "kbase_discovery",
        "schema_version": 1,
        "created_at": "2026-07-08T00:00:00+00:00",
        "strategy_id": strategy_id,
        "topic": "roundtable gap",
        "status": status,
        "result": {
            "status": status,
            "discovery": {
                "transcript": [
                    {"stage": "source_librarian", "output": {"brief_id": "brief-1"}},
                    {"stage": "alpha_hunter", "output": {"mechanism": "relative"}},
                    {"stage": "factor_engineer", "output": {"factor_batch": factors or []}},
                ],
            },
        },
    }


class DiscoveryHandoffBridgeTests(unittest.TestCase):
    def _authorized_execution_fixture(self, root: Path):
        """Build a plan/invocation pair without touching the real authority store."""
        handoff = root / "handoff.yaml"
        handoff.write_text("handoff_type: kbase_discovery\n", encoding="utf-8")
        output = root / "research-output"
        runner_path = Path(__file__).resolve()
        project_root = Path(__file__).resolve().parent.parent
        runner_ref = runner_path.relative_to(project_root).as_posix()
        plan = DiscoveryExecutionPlan(
            handoff_path=str(handoff),
            strategy_id="brick",
            runner_id="test-runner",
            runner_script=str(runner_path),
            output_dir=str(output),
            factor_names=["test_factor"],
            reason="test",
            command=[
                sys.executable,
                str(runner_path),
                "--handoff-path",
                str(handoff),
                "--output-dir",
                str(output),
            ],
        )
        invocation = ExecutionInvocation(
            intent_ref="research_state/control_plane/p0r2/intents/discovery.json",
            entry_id="callable:research_automation.discovery_execution_bridge:execute_plan",
            effect=SideEffect.START_SUBPROCESS,
            operation="SUBPROCESS",
            argv=tuple(plan.command),
            cwd=str(project_root),
            runner=RunnerIdentity(
                module="tests.test_discovery_handoff_bridge",
                callable_name="main",
                source_ref=runner_ref,
                source_sha256="a" * 64,
            ),
            resource_paths=(str(runner_path), str(handoff), str(output)),
        )
        permit = SimpleNamespace(
            operation="SUBPROCESS",
            effect=SideEffect.START_SUBPROCESS,
            argv=tuple(plan.command),
            resource_paths=tuple(
                path.resolve() for path in (runner_path, handoff, output)
            ),
        )
        return plan, invocation, permit, output

    def test_execute_plan_requires_authority_before_creating_output_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "research-output"
            plan = DiscoveryExecutionPlan(
                handoff_path=str(root / "handoff.yaml"),
                strategy_id="brick",
                runner_id="test-runner",
                runner_script=str(Path(__file__).resolve()),
                output_dir=str(output_dir),
                factor_names=["test_factor"],
                reason="test",
                command=[sys.executable, "-c", "print('must not run')"],
            )

            with patch(
                "research_automation.discovery_execution_bridge.subprocess.run"
            ) as runner:
                with self.assertRaises(ExecutionAuthorizationError):
                    execute_plan(plan)

            self.assertFalse(output_dir.exists())
            runner.assert_not_called()

    def test_save_discovery_handoff_requires_authority_before_directory_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "handoffs"
            with self.assertRaises(ExecutionAuthorizationError):
                save_discovery_handoff(
                    {"status": "APPROVED", "transcript": []},
                    topic="unauthorized",
                    strategy_id="brick",
                    output_dir=destination,
                    created_at=datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc),
                )

            self.assertFalse(destination.exists())

    def test_save_discovery_handoff_binds_directory_target_and_temp_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "handoffs"
            created_at = datetime(2026, 7, 23, 12, 0, 1, tzinfo=timezone.utc)
            sink = MagicMock()
            sink.authorize.return_value = object()
            lease = object()
            invocation = object()

            with patch(
                "ag2_research.discovery_handoff.AuthorizedPathMutation",
                return_value=sink,
            ):
                target = save_discovery_handoff(
                    {"status": "APPROVED", "transcript": []},
                    topic="authorized",
                    strategy_id="brick",
                    output_dir=destination,
                    created_at=created_at,
                    lease=lease,
                    invocation=invocation,
                    authority_reader=MagicMock(),
                    repository_root=Path(directory),
                )

            temporary = destination / f".{target.name}.tmp"
            self.assertTrue(target.is_file())
            self.assertFalse(temporary.exists())
            authorization = sink.authorize.call_args.kwargs
            self.assertIs(lease, sink.authorize.call_args.args[0])
            self.assertIs(invocation, sink.authorize.call_args.args[1])
            self.assertEqual("KBASE_WRITE", authorization["operation"])
            self.assertIs(SideEffect.WRITE_KBASE, authorization["effect"])
            self.assertEqual(
                authorization["paths"],
                (destination, target, temporary),
            )

    def test_execute_plan_dry_run_without_authority_has_no_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = DiscoveryExecutionPlan(
                handoff_path=str(root / "handoff.yaml"),
                strategy_id="brick",
                runner_id="test-runner",
                runner_script=str(Path(__file__).resolve()),
                output_dir=str(root / "research-output"),
                factor_names=["test_factor"],
                reason="test",
                command=[sys.executable, "-V"],
            )
            with (
                patch("research_automation.discovery_execution_bridge.AuthorityReader") as reader,
                patch("research_automation.discovery_execution_bridge.ExecutionSinkGuard") as guard,
                patch("research_automation.discovery_execution_bridge.AuthorizedSubprocess") as sink,
                patch("research_automation.discovery_execution_bridge.subprocess.run") as runner,
            ):
                self.assertIsNone(execute_plan(plan, dry_run=True))

            self.assertFalse(Path(plan.output_dir).exists())
            reader.assert_not_called()
            guard.assert_not_called()
            sink.assert_not_called()
            runner.assert_not_called()

    def test_execute_plan_valid_authorization_rechecks_identity_before_effect(self):
        with tempfile.TemporaryDirectory() as directory:
            plan, invocation, permit, output = self._authorized_execution_fixture(Path(directory))
            lease = object()
            guard = MagicMock()

            def authorize(checked_lease, checked_invocation):
                self.assertIs(lease, checked_lease)
                self.assertIs(invocation, checked_invocation)
                self.assertFalse(output.exists())
                return permit

            guard.authorize.side_effect = authorize
            sink = MagicMock()
            sink.run.side_effect = lambda checked_lease, checked_invocation: (
                self.assertTrue(output.is_dir())
                or "completed"
            )
            with (
                patch("research_automation.discovery_execution_bridge.ExecutionSinkGuard", return_value=guard),
                patch("research_automation.discovery_execution_bridge.AuthorizedSubprocess", return_value=sink),
            ):
                result = execute_plan(
                    plan,
                    execution_lease=lease,
                    execution_invocation=invocation,
                )

            self.assertEqual("completed", result)
            self.assertTrue(output.is_dir())
            guard.authorize.assert_called_once_with(lease, invocation)
            sink.run.assert_called_once_with(lease, invocation)

    def test_execute_plan_rejects_command_or_resource_tampering_before_mkdir(self):
        with tempfile.TemporaryDirectory() as directory:
            plan, invocation, permit, output = self._authorized_execution_fixture(Path(directory))
            plan.command[1] = str(Path(__file__).resolve().parent / "other.py")
            guard = MagicMock()
            guard.authorize.return_value = permit
            sink = MagicMock()
            with (
                patch("research_automation.discovery_execution_bridge.ExecutionSinkGuard", return_value=guard),
                patch("research_automation.discovery_execution_bridge.AuthorizedSubprocess", return_value=sink),
            ):
                with self.assertRaises(ExecutionAuthorizationError):
                    execute_plan(
                        plan,
                        execution_lease=object(),
                        execution_invocation=invocation,
                    )

            self.assertFalse(output.exists())
            sink.assert_not_called()

            # A valid command is still denied when the immutable intent omits
            # the exact output resource that would be created.
            plan, invocation, permit, output = self._authorized_execution_fixture(Path(directory))
            permit.resource_paths = permit.resource_paths[:-1]
            guard.authorize.return_value = permit
            with patch(
                "research_automation.discovery_execution_bridge.ExecutionSinkGuard",
                return_value=guard,
            ):
                with self.assertRaises(ExecutionAuthorizationError):
                    execute_plan(
                        plan,
                        execution_lease=object(),
                        execution_invocation=invocation,
                    )

            self.assertFalse(output.exists())

    def test_only_complete_approved_handoff_is_loaded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rejected = root / "discovery_b1_REJECTED_20260704T010000Z.yaml"
            rejected.write_text(yaml.safe_dump(_document("REJECTED")), encoding="utf-8")
            approved = root / "discovery_b1_APPROVED_20260704T020000Z.yaml"
            approved.write_text(yaml.safe_dump(_document(factors=[{
                "name": "flow_pressure", "expression": "volume * return"
            }])), encoding="utf-8")

            loaded = load_latest_approved_discovery("b1", handoff_dir=root)
            context = render_discovery_context("b1", handoff_dir=root)

            self.assertEqual(str(approved), loaded["path"])
            self.assertIn("flow_pressure", context)
            self.assertIn("inspiration, not validation", context)

    def test_roundtable_nested_transcript_is_loaded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            factors = [{
                "name": "sector_relative_volume_contraction_5d",
                "expression": "1 - rank(volume_trend)",
                "data_requirements": ["volume", "sector_classification"],
            }]
            path = root / "discovery_brick_APPROVED_20260708T010000Z.yaml"
            document = _roundtable_document(factors=factors)
            path.write_text(yaml.safe_dump(document), encoding="utf-8")

            transcript = extract_discovery_transcript(document)
            loaded = load_latest_approved_discovery("brick", handoff_dir=root)
            context = render_discovery_context("brick", handoff_dir=root)

            self.assertEqual(3, len(transcript))
            self.assertEqual(str(path), loaded["path"])
            self.assertIn("sector_relative_volume_contraction_5d", context)

    def test_incomplete_approved_handoff_is_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "discovery_b1_APPROVED_20260704T020000Z.yaml"
            path.write_text(yaml.safe_dump(_document()), encoding="utf-8")
            self.assertIsNone(load_latest_approved_discovery("b1", handoff_dir=root))

    def test_sector_relative_handoff_builds_phase6_execution_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "discovery_brick_APPROVED_20260708T010000Z.yaml"
            path.write_text(yaml.safe_dump(_roundtable_document(factors=[
                {
                    "name": "sector_relative_volume_contraction_5d",
                    "expression": "1 - rank(volume_trend)",
                    "data_requirements": ["volume", "sector_classification"],
                },
                {
                    "name": "sector_relative_rsi_14d",
                    "expression": "1 - rank(rsi)",
                    "data_requirements": ["close", "sector_classification"],
                },
            ])), encoding="utf-8")

            plan = build_execution_plan(path, timestamp="20260708_000000")

            self.assertEqual("brick_sector_relative_phase6", plan.runner_id)
            self.assertIn("brick_sector_relative_phase6.py", plan.runner_script)
            self.assertIn("--handoff-path", plan.command)
            self.assertIn(str(path.resolve()), plan.command)

    def test_volume_authenticity_handoff_builds_phase6_execution_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "discovery_brick_APPROVED_20260708T020000Z.yaml"
            path.write_text(yaml.safe_dump(_roundtable_document(factors=[
                {
                    "name": "stock_volume_contraction_20d",
                    "expression": "mean(volume,t-4..t)/mean(volume,t-19..t-5)",
                    "data_requirements": ["daily_volume"],
                },
                {
                    "name": "market_volume_contraction_20d",
                    "expression": "mean(market_volume,t-4..t)/mean(market_volume,t-19..t-5)",
                    "data_requirements": ["daily_volume"],
                },
                {
                    "name": "volume_shrinkage_authenticity_rank",
                    "expression": "rank(stock_volume_contraction_20d / market_volume_contraction_20d)",
                    "data_requirements": ["daily_volume"],
                },
            ])), encoding="utf-8")

            plan = build_execution_plan(path, timestamp="20260708_000000")

            self.assertEqual("brick_volume_authenticity_phase6", plan.runner_id)
            self.assertIn("brick_volume_authenticity_phase6.py", plan.runner_script)
            self.assertIn(str(path.resolve()), plan.command)

    def test_brick_sequence_handoff_builds_dedicated_phase6_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "discovery_brick_APPROVED_sequence.yaml"
            factors = [
                {
                    "name": name,
                    "expression": "signal-day brick history only",
                    "data_requirements": ["brick_history_color_sequence"],
                }
                for name in (
                    "brick_same_color_run_length",
                    "brick_reversal_recency",
                    "brick_run_length_ratio",
                )
            ]
            path.write_text(
                yaml.safe_dump(_roundtable_document(factors=factors)),
                encoding="utf-8",
            )

            plan = build_execution_plan(path, timestamp="20260723_000000")

            self.assertEqual("brick_sequence_state_phase6", plan.runner_id)
            self.assertIn("brick_sequence_state_phase6.py", plan.runner_script)

    def test_aggregate_liquidity_research_mechanism_routes_to_dedicated_runner(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "discovery_brick_APPROVED_mechanism.yaml"
            document = _roundtable_document(factors=[])
            document["result"]["project_state_packet"] = {
                "research_history": "old label_reconstruction hold_days experiment"
            }
            document["result"]["discovery"]["transcript"][-1]["output"] = {
                "research_mechanism": {
                    "name": "aggregate_liquidity_state_volume_feature_modulation",
                    "family": "Feature Weight Conditioning",
                    "mechanism": "condition volume features on signal-day aggregate state",
                    "runner_id": "backtest_brick_v2_research.py",
                    "validation_plan": ["three fixed rolling folds"],
                    "stop_conditions": ["no fold-stable lift"],
                }
            }
            path.write_text(yaml.safe_dump(document), encoding="utf-8")

            plan = build_execution_plan(path, timestamp="20260723_000000")

            self.assertEqual("brick_aggregate_liquidity_modulation_phase6", plan.runner_id)
            self.assertIn("brick_aggregate_liquidity_modulation_phase6.py", plan.runner_script)
            self.assertNotIn("label_reconstruction", plan.runner_id)

    def test_known_research_mechanism_routes_from_final_factor_output_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "discovery_brick_APPROVED_label_mechanism.yaml"
            document = _roundtable_document(factors=[])
            document["result"]["discovery"]["transcript"][-1]["output"] = {
                "research_mechanism": {
                    "name": "label_reconstruction_hold_days_decay",
                    "mechanism": "label reconstruction with hold_days decay weights",
                }
            }
            path.write_text(yaml.safe_dump(document), encoding="utf-8")

            plan = build_execution_plan(path, timestamp="20260723_000000")

            self.assertEqual("brick_label_reconstruction_phase6", plan.runner_id)

    def test_ag2_candidate_round_receives_discovery_context(self):
        runner = AutonomousRunnerV1.__new__(AutonomousRunnerV1)
        runner.strategy = "b1"
        runner.AG2_CANDIDATE_WORKFLOW = "proposal_gate"
        orchestrator = MagicMock()
        orchestrator.run_sequential_workflow.return_value = {"status": "APPROVED"}
        with (
            patch("ag2_research.orchestrator.Orchestrator", return_value=orchestrator),
            patch("ag2_research.knowledge_bridge.build_combined_research_context", return_value="PROJECT"),
            patch("ag2_research.discovery_handoff.render_discovery_context", return_value="\nHANDOFF"),
        ):
            runner._ag2_round(1)

        context = orchestrator.run_sequential_workflow.call_args.kwargs["research_context"]
        self.assertEqual("PROJECT\nHANDOFF", context)

    def test_research_proposer_system_message_receives_context(self):
        with patch("ag2_research.agents.autogen.AssistantAgent") as agent_cls:
            agent_cls.return_value = MagicMock()
            create_agents(
                ResearchConfig(), ["research_proposer"],
                {"config_list": [{"model": "test"}]}, "HANDOFF-CONTEXT",
            )

        system_message = agent_cls.call_args.kwargs["system_message"]
        self.assertIn("HANDOFF-CONTEXT", system_message)
        self.assertNotIn("{research_context}", system_message)


if __name__ == "__main__":
    unittest.main()
