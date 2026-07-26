from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

import yaml

from research_automation.discovery_execution_bridge import DiscoveryExecutionPlan
from research_automation.handoff_runner_repair import RepairResult
from research_automation.kbase_ag2_full_cycle import run_kbase_ag2_full_cycle


def _handoff(status: str = "APPROVED") -> dict:
    return {
        "handoff_type": "kbase_discovery",
        "schema_version": 1,
        "strategy_id": "brick",
        "topic": "full cycle test",
        "status": status,
        "result": {"status": status, "transcript": []},
    }


class KBaseAG2FullCycleTests(unittest.TestCase):
    def test_dry_run_without_execution_lease_has_no_cycle_side_effect(self):
        with tempfile.TemporaryDirectory() as directory:
            cycle = Path(directory) / "cycle"
            with (
                patch("research_automation.kbase_ag2_full_cycle.Orchestrator") as orchestrator,
                patch(
                    "research_automation.kbase_ag2_full_cycle.repair_handoff_runner"
                ) as repair_runner,
                patch(
                    "research_automation.kbase_ag2_full_cycle._execute_with_logs"
                ) as execute,
            ):
                result = run_kbase_ag2_full_cycle(
                    strategy_id="brick",
                    topic="bounded preview",
                    output_dir=cycle,
                    dry_run=True,
                )

            self.assertEqual("UNAUTHORIZED", result["status"])
            self.assertFalse(cycle.exists())
            orchestrator.assert_not_called()
            repair_runner.assert_not_called()
            execute.assert_not_called()

    def test_approved_handoff_executes_and_archives_not_promoted_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            cycle = root / "cycle"
            execution = cycle / "execution"
            execution.mkdir(parents=True)
            handoff.write_text(yaml.safe_dump(_handoff()), encoding="utf-8")
            (execution / "status.json").write_text(
                json.dumps(
                    {
                        "status": "complete",
                        "research_status": "PREFLIGHT_STOP",
                        "promotion_gate_passed": False,
                    }
                ),
                encoding="utf-8",
            )
            plan = DiscoveryExecutionPlan(
                handoff_path=str(handoff),
                strategy_id="brick",
                runner_id="registered_runner",
                runner_script=str(root / "runner.py"),
                output_dir=str(execution),
                factor_names=["factor"],
                reason="registered",
                command=["python", "runner.py"],
            )
            fingerprint = {
                "files": {"backtest_brick_v2.py": {"sha256": "same"}},
                "indicator_cache": {"count": 5200, "metadata_sha256": "same"},
            }

            with (
                patch(
                    "research_automation.kbase_ag2_full_cycle.fingerprint_production_boundary",
                    return_value=fingerprint,
                ),
                patch(
                    "research_automation.kbase_ag2_full_cycle.build_execution_plan",
                    return_value=plan,
                ),
                patch(
                    "research_automation.kbase_ag2_full_cycle._execute_with_logs",
                    return_value=CompletedProcess(plan.command, 0),
                ),
            ):
                result = run_kbase_ag2_full_cycle(
                    strategy_id="brick",
                    handoff_path=handoff,
                    output_dir=cycle,
                )

            self.assertEqual("COMPLETED_NOT_PROMOTED", result["status"])
            self.assertEqual("PREFLIGHT_STOP", result["research_status"])
            self.assertFalse(result["promotion_gate_passed"])
            self.assertTrue(result["production_boundary_unchanged"])
            self.assertFalse(result["production_promotion_performed"])
            archived = json.loads((cycle / "cycle_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(result["status"], archived["status"])

    def test_non_approved_handoff_stops_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            cycle = root / "cycle"
            handoff.write_text(
                yaml.safe_dump(_handoff("REJECTED")),
                encoding="utf-8",
            )
            fingerprint = {"files": {}, "indicator_cache": {"count": 0}}

            with (
                patch(
                    "research_automation.kbase_ag2_full_cycle.fingerprint_production_boundary",
                    return_value=fingerprint,
                ),
                patch(
                    "research_automation.kbase_ag2_full_cycle.build_execution_plan"
                ) as build_plan,
            ):
                result = run_kbase_ag2_full_cycle(
                    strategy_id="brick",
                    handoff_path=handoff,
                    output_dir=cycle,
                )

            self.assertEqual("DISCOVERY_STOP", result["status"])
            build_plan.assert_not_called()

    def test_missing_runner_repairs_then_retries_execution_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            cycle = root / "cycle"
            execution = cycle / "execution"
            execution.mkdir(parents=True)
            handoff.write_text(yaml.safe_dump(_handoff()), encoding="utf-8")
            (execution / "status.json").write_text(
                json.dumps(
                    {
                        "status": "complete",
                        "research_status": "VALIDATED",
                        "promotion_gate_passed": False,
                    }
                ),
                encoding="utf-8",
            )
            plan = DiscoveryExecutionPlan(
                handoff_path=str(handoff),
                strategy_id="brick",
                runner_id="repaired_runner",
                runner_script=str(root / "runner.py"),
                output_dir=str(execution),
                factor_names=["factor"],
                reason="repaired",
                command=["python", "runner.py"],
            )
            repair = RepairResult(
                ok=True,
                status="repaired",
                handoff_path=str(handoff),
                output_dir=str(cycle / "runner_repair"),
            )
            fingerprint = {"files": {}, "indicator_cache": {"count": 5200}}

            with (
                patch(
                    "research_automation.kbase_ag2_full_cycle.fingerprint_production_boundary",
                    return_value=fingerprint,
                ),
                patch(
                    "research_automation.kbase_ag2_full_cycle.build_execution_plan",
                    side_effect=[
                        ValueError("no registered Phase 6 runner for mechanism"),
                        plan,
                    ],
                ) as build_plan,
                patch(
                    "research_automation.kbase_ag2_full_cycle.repair_handoff_runner",
                    return_value=repair,
                ) as repair_runner,
                patch(
                    "research_automation.kbase_ag2_full_cycle._execute_with_logs",
                    return_value=CompletedProcess(plan.command, 0),
                ),
            ):
                result = run_kbase_ag2_full_cycle(
                    strategy_id="brick",
                    handoff_path=handoff,
                    output_dir=cycle,
                )

            self.assertEqual("COMPLETED_NOT_PROMOTED", result["status"])
            self.assertEqual(2, build_plan.call_count)
            repair_runner.assert_called_once()

    def test_repair_failure_is_archived_without_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            cycle = root / "cycle"
            handoff.write_text(yaml.safe_dump(_handoff()), encoding="utf-8")
            repair = RepairResult(
                ok=False,
                status="code_reviewer_rejected",
                handoff_path=str(handoff),
                output_dir=str(cycle / "runner_repair"),
                error="structured verdict was REJECT",
            )
            fingerprint = {"files": {}, "indicator_cache": {"count": 5200}}

            with (
                patch(
                    "research_automation.kbase_ag2_full_cycle.fingerprint_production_boundary",
                    return_value=fingerprint,
                ),
                patch(
                    "research_automation.kbase_ag2_full_cycle.build_execution_plan",
                    side_effect=ValueError("no registered Phase 6 runner"),
                ),
                patch(
                    "research_automation.kbase_ag2_full_cycle.repair_handoff_runner",
                    return_value=repair,
                ),
                patch(
                    "research_automation.kbase_ag2_full_cycle._execute_with_logs"
                ) as execute,
            ):
                result = run_kbase_ag2_full_cycle(
                    strategy_id="brick",
                    handoff_path=handoff,
                    output_dir=cycle,
                )

            self.assertEqual("RUNNER_REPAIR_FAILED", result["status"])
            self.assertIn("structured verdict", result["reason"])
            execute.assert_not_called()

    def test_dry_run_archives_plan_without_repair_or_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            cycle = root / "cycle"
            handoff.write_text(yaml.safe_dump(_handoff()), encoding="utf-8")
            plan = DiscoveryExecutionPlan(
                handoff_path=str(handoff),
                strategy_id="brick",
                runner_id="registered_runner",
                runner_script=str(root / "runner.py"),
                output_dir=str(cycle / "execution"),
                factor_names=["factor"],
                reason="registered",
                command=["python", "runner.py"],
            )
            fingerprint = {"files": {}, "indicator_cache": {"count": 5200}}

            with (
                patch(
                    "research_automation.kbase_ag2_full_cycle.fingerprint_production_boundary",
                    return_value=fingerprint,
                ),
                patch(
                    "research_automation.kbase_ag2_full_cycle.build_execution_plan",
                    return_value=plan,
                ),
                patch(
                    "research_automation.kbase_ag2_full_cycle.repair_handoff_runner"
                ) as repair_runner,
                patch(
                    "research_automation.kbase_ag2_full_cycle._execute_with_logs"
                ) as execute,
            ):
                result = run_kbase_ag2_full_cycle(
                    strategy_id="brick",
                    handoff_path=handoff,
                    output_dir=cycle,
                    dry_run=True,
                )

            self.assertEqual("DRY_RUN_READY", result["status"])
            self.assertEqual("registered_runner", result["execution_plan"]["runner_id"])
            self.assertTrue((cycle / "execution_plan.json").is_file())
            repair_runner.assert_not_called()
            execute.assert_not_called()

    def test_production_boundary_change_overrides_cycle_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            cycle = root / "cycle"
            handoff.write_text(yaml.safe_dump(_handoff()), encoding="utf-8")
            plan = DiscoveryExecutionPlan(
                handoff_path=str(handoff),
                strategy_id="brick",
                runner_id="registered_runner",
                runner_script=str(root / "runner.py"),
                output_dir=str(cycle / "execution"),
                factor_names=["factor"],
                reason="registered",
                command=["python", "runner.py"],
            )
            before = {
                "files": {"backtest_brick_v2.py": {"sha256": "before"}},
                "indicator_cache": {"count": 5200, "metadata_sha256": "same"},
            }
            after = {
                "files": {"backtest_brick_v2.py": {"sha256": "after"}},
                "indicator_cache": {"count": 5200, "metadata_sha256": "same"},
            }

            with (
                patch(
                    "research_automation.kbase_ag2_full_cycle.fingerprint_production_boundary",
                    side_effect=[before, after],
                ),
                patch(
                    "research_automation.kbase_ag2_full_cycle.build_execution_plan",
                    return_value=plan,
                ),
            ):
                result = run_kbase_ag2_full_cycle(
                    strategy_id="brick",
                    handoff_path=handoff,
                    output_dir=cycle,
                    dry_run=True,
                )

            self.assertEqual("PRODUCTION_BOUNDARY_CHANGED", result["status"])
            self.assertFalse(result["production_boundary_unchanged"])
            self.assertEqual(before, result["production_boundary_before"])
            self.assertEqual(after, result["production_boundary_after"])


if __name__ == "__main__":
    unittest.main()
