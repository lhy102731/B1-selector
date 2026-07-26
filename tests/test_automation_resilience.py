import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.task_queue import ExperimentTask, QueuePersistenceError, TaskQueue
from research_automation.autonomous_runner import AutonomousRunnerV1, _kbase_writeback_enabled
from research_automation.automation_controller import AutomationController
from research_automation.experiment import StandardMetrics
from research_automation.experiment_runner import RealBacktestExecutor
from research_automation.patch_executor import (
    ClaudePatchExecutor,
    _apply_patch_to_workspace,
    compile_gate,
)
from research_automation.safety import UnsafeWriteError, assert_safe_path
from research_automation.control_plane.entry_guard import AuthorizationError


class ControlPlaneSafetyTests(unittest.TestCase):
    def test_control_plane_state_root_is_an_allowed_write_boundary(self):
        path = assert_safe_path(Path("research_state") / "control_plane" / "gate_report.json")
        self.assertTrue(path.as_posix().endswith("research_state/control_plane/gate_report.json"))

    def test_control_plane_cannot_use_legacy_memory_filenames(self):
        with self.assertRaises(UnsafeWriteError):
            assert_safe_path(Path("research_state") / "control_plane" / "registry_brick.yaml")


class KnowledgeGateFailClosedTests(unittest.TestCase):
    def test_automatic_kb_validation_error_is_rejected(self):
        from research_automation.kb_gate import gate_proposal_kb

        with patch(
            "ag2_research.knowledge_base.validate_proposal",
            side_effect=RuntimeError("validator unavailable"),
        ):
            verdict = gate_proposal_kb("b1_v3", {"hypothesis": "x"})

        self.assertEqual(verdict["verdict"], "reject")
        self.assertIn("KB_VALIDATION_ERROR", verdict["warnings"])

    def test_proposal_only_kb_validation_error_requests_evidence(self):
        from research_automation.kb_gate import gate_proposal_kb

        with patch(
            "ag2_research.knowledge_base.validate_proposal",
            side_effect=RuntimeError("validator unavailable"),
        ):
            verdict = gate_proposal_kb(
                "b1_v3",
                {"hypothesis": "x"},
                proposal_only=True,
            )

        self.assertEqual(verdict["verdict"], "needs_evidence")
        self.assertIn("KB_VALIDATION_ERROR", verdict["needs_evidence"])


class TaskQueueRecoveryTests(unittest.TestCase):
    def test_corrupt_persistence_fails_closed(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "queue.json"
            path.write_text("{not-json", encoding="utf-8")
            with self.assertRaisesRegex(QueuePersistenceError, "cannot load persisted task queue"):
                TaskQueue(path)

    def test_in_flight_task_is_requeued_after_restart(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "queue.json"
            q = TaskQueue(path)
            q.enqueue(ExperimentTask("t1", "b1", {"hypothesis": "x"}))
            self.assertEqual(q.dequeue().task_id, "t1")
            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertIn("t1", saved["in_flight"])
            recovered = TaskQueue(path)
            self.assertEqual(recovered.pending_count(), 1)
            self.assertEqual(recovered.dequeue().task_id, "t1")

    def test_done_and_failed_leave_no_in_flight_task(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "queue.json"
            q = TaskQueue(path)
            for tid in ("ok", "bad"):
                q.enqueue(ExperimentTask(tid, "b1", {}))
            q.dequeue(); q.mark_done("ok")
            q.dequeue(); q.mark_failed("bad", "boom")
            data = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(data["in_flight"], {})
            self.assertEqual(data["done"], ["ok"])
            self.assertEqual(data["failed"], {"bad": "boom"})


class BaselineFailFastTests(unittest.TestCase):
    def test_controller_without_execution_lease_fails_before_output(self):
        with tempfile.TemporaryDirectory() as td:
            output_root = Path(td) / "experiments"
            controller = AutomationController(output_root=output_root)
            result = controller.run_from_proposal(
                "unauthorized",
                {"hypothesis": "bounded", "scope": {}},
            )

            self.assertEqual("FAILED", result.status.value)
            self.assertTrue(any("unauthorized" in item.lower() for item in result.logs))
            self.assertFalse(output_root.exists())

    def test_failed_baseline_raises(self):
        runner = object.__new__(AutonomousRunnerV1)
        runner.project_root = None; runner.keep_scratch = False
        runner.strategy = "b1"; runner._scope = {}; runner._champion = {}
        with tempfile.TemporaryDirectory() as td, patch(
            "research_automation.autonomous_runner.RealBacktestExecutor.execute",
            return_value={"success": False, "error": "broken"},
        ):
            with self.assertRaisesRegex(RuntimeError, "baseline backtest failed"):
                runner._run_baseline(Path(td))

    def test_metric_validity_rejects_empty_parse(self):
        self.assertFalse(AutonomousRunnerV1._has_valid_metrics(StandardMetrics(source="none")))
        self.assertTrue(AutonomousRunnerV1._has_valid_metrics(
            StandardMetrics(source="metrics_json", sharpe=0.0)))

    def test_kbase_writeback_default_is_denied(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(_kbase_writeback_enabled())

    def test_legacy_runner_without_ticket_is_blocked_before_writes(self):
        runner = object.__new__(AutonomousRunnerV1)
        with patch.object(Path, "mkdir", side_effect=AssertionError("write attempted")):
            with self.assertRaises(AuthorizationError):
                runner.run(dry_run=True)

    def test_legacy_cli_is_blocked_before_runner_construction(self):
        import run_research_cycle

        with (
            patch.object(sys, "argv", ["run_research_cycle.py", "--dry-run"]),
            patch.object(
                run_research_cycle,
                "AutonomousRunnerV1",
                side_effect=AssertionError("runner constructed"),
            ),
        ):
            self.assertEqual(run_research_cycle.main(), 3)

    def test_patch_executor_without_execution_lease_fails_before_claude(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            task = root / "task.md"
            task.write_text("change one bounded constant", encoding="utf-8")
            workspace = root / "workspace"
            workspace.mkdir()
            executor = ClaudePatchExecutor(binary="claude")
            with patch("research_automation.patch_executor.subprocess.run") as run:
                result = executor.apply(task, workspace)

            self.assertFalse(result.ok)
            self.assertIn("execution lease", (result.error or "").lower())
            run.assert_not_called()

    def test_legacy_patch_helper_fails_closed_without_authorized_sink(self):
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td) / "workspace"
            target = workspace / "strategy" / "candidate.py"
            target.parent.mkdir(parents=True)
            target.write_text("before\n", encoding="utf-8")
            diff = (
                "--- a/strategy/candidate.py\n"
                "+++ b/strategy/candidate.py\n"
                "@@ -1 +1 @@\n"
                "-before\n"
                "+after\n"
            )

            result = _apply_patch_to_workspace(diff, workspace)

            self.assertFalse(result["ok"])
            self.assertIn("authorized", result["error"].lower())
            self.assertEqual(target.read_text(encoding="utf-8"), "before\n")

    def test_compile_gate_without_authorized_runner_fails_closed(self):
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            (workspace / "strategy").mkdir()

            result = compile_gate(workspace)

            self.assertFalse(result["ok"])
            self.assertIn("authorized", result["output"].lower())


class OutputOwnershipTests(unittest.TestCase):
    def test_real_backtest_without_execution_lease_fails_before_subprocess(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            executor = RealBacktestExecutor(project_root=root)
            with patch("research_automation.experiment_runner.subprocess.run") as run:
                result = executor.execute(
                    {"strategy": "B1", "params": {}},
                    result_dir=root / "result",
                )

            self.assertFalse(result["success"])
            self.assertIn("execution lease", result["error"].lower())
            run.assert_not_called()

    def test_failed_run_does_not_archive_or_delete_preexisting_output(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "run_b1_v3.py").write_text("pass", encoding="utf-8")
            old = root / "artifacts" / "backtests" / "b1_v3" / "backtest_equity_v3.csv"
            old.parent.mkdir(parents=True)
            old.write_text("old-output", encoding="utf-8")
            result = root / "result"
            proc = type("Proc", (), {"returncode": 1, "stdout": "", "stderr": "failed"})()
            ex = RealBacktestExecutor(project_root=root, keep_scratch=False)
            def overwrite_then_fail(*args, **kwargs):
                old.write_text("failed-run-output", encoding="utf-8")
                return proc
            with patch("research_automation.experiment_runner.subprocess.run", side_effect=overwrite_then_fail):
                res = ex.execute({"strategy": "B1", "params": {}}, result_dir=result)
            self.assertFalse(res["success"])
            self.assertEqual(old.read_text(encoding="utf-8"), "old-output")
            self.assertFalse((result / "equity.csv").exists())
            self.assertEqual(res["scratch_cleaned"], [])

    def test_success_archives_new_output_then_restores_preexisting_file(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "run_b1_v3.py").write_text("pass", encoding="utf-8")
            shared = root / "artifacts" / "backtests" / "b1_v3" / "backtest_equity_v3.csv"
            shared.parent.mkdir(parents=True)
            shared.write_text("old-output", encoding="utf-8")
            result = root / "result"
            proc = type("Proc", (), {"returncode": 0, "stdout": "", "stderr": ""})()
            def overwrite_then_succeed(*args, **kwargs):
                shared.write_text("new-output", encoding="utf-8")
                return proc
            ex = RealBacktestExecutor(project_root=root, keep_scratch=False)
            with patch("research_automation.experiment_runner.subprocess.run", side_effect=overwrite_then_succeed):
                res = ex.execute({"strategy": "B1", "params": {}}, result_dir=result)
            self.assertTrue(res["success"])
            self.assertEqual((result / "equity.csv").read_text(encoding="utf-8"), "new-output")
            self.assertEqual(shared.read_text(encoding="utf-8"), "old-output")
            self.assertEqual(res["scratch_cleaned"], [])


if __name__ == "__main__":
    unittest.main()
