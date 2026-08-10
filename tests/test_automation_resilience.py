import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from research_automation.task_queue import ExperimentTask, QueuePersistenceError, TaskQueue
from research_automation.autonomous_runner import (
    AutonomousRunnerV1,
    _kbase_writeback_enabled,
    _require_legacy_candidate_stamp,
)
from research_automation.automation_controller import AutomationController
from research_automation.experiment import StandardMetrics
from research_automation.experiment_runner import RealBacktestExecutor
from research_automation.patch_executor import (
    ClaudePatchExecutor,
    _apply_patch_to_workspace,
    _parse_code_review_response,
    compile_gate,
)
from research_automation.safety import UnsafeWriteError, assert_safe_path
from research_automation.control_plane.entry_guard import AuthorizationError
from research_automation.control_plane.contracts import Actor, Phase, SideEffect
from research_automation.control_plane.sink_guard import (
    ExecutionInvocation,
    RunnerIdentity,
)
from research_automation.control_plane.stores import (
    AuthorityIdentity,
    TaskExecutionLease,
    _BearerSecret,
)


class ControlPlaneSafetyTests(unittest.TestCase):
    def test_control_plane_state_root_is_an_allowed_write_boundary(self):
        path = assert_safe_path(Path("research_state") / "control_plane" / "gate_report.json")
        self.assertTrue(path.as_posix().endswith("research_state/control_plane/gate_report.json"))

    def test_control_plane_cannot_use_legacy_memory_filenames(self):
        with self.assertRaises(UnsafeWriteError):
            assert_safe_path(Path("research_state") / "control_plane" / "registry_brick.yaml")


class KnowledgeGateFailClosedTests(unittest.TestCase):
    def test_code_review_response_requires_closed_approve_schema(self):
        approved = """code_review:
  implements_design: pass
  drift_detected: none
  side_effects: []
  architectural_violation: none
  test_coverage_change: increased
  verdict: APPROVE
  rationale: The bounded patch matches the reviewed design.
"""
        self.assertEqual("APPROVE", _parse_code_review_response(approved)[0])

        for invalid in (None, "", "APPROVE", "code_review: {verdict: APPROVE}"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    _parse_code_review_response(invalid)

        request_changes = approved.replace("verdict: APPROVE", "verdict: REQUEST_CHANGES")
        self.assertEqual(
            "REQUEST_CHANGES",
            _parse_code_review_response(request_changes)[0],
        )
        contradictory_approval = approved.replace(
            "implements_design: pass",
            "implements_design: fail",
        ).replace(
            "drift_detected: none",
            "drift_detected: design drift present",
        ).replace(
            "side_effects: []",
            "side_effects: [unapproved side effect]",
        ).replace(
            "architectural_violation: none",
            "architectural_violation: boundary violated",
        ).replace(
            "test_coverage_change: increased",
            "test_coverage_change: decreased",
        )
        with self.assertRaisesRegex(ValueError, "contradictory"):
            _parse_code_review_response(contradictory_approval)

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

    def test_patch_executor_fails_closed_when_review_context_is_unavailable(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            task = root / "task.md"
            task.write_text("change one bounded constant", encoding="utf-8")
            workspace = root / "workspace"
            target = workspace / "strategy" / "brick_chart_strategy.py"
            target.parent.mkdir(parents=True)
            target.write_text("VALUE = 1\n", encoding="utf-8")
            executor = ClaudePatchExecutor(binary="claude")
            prompt = executor._build_patch_prompt(
                task.read_text(encoding="utf-8"),
                target.read_text(encoding="utf-8"),
                "strategy/brick_chart_strategy.py",
            )
            actor = Actor("patch-test", "automation", "patch-test-invocation")
            identity = AuthorityIdentity("a" * 64, "b" * 64, "c" * 64)
            lease = TaskExecutionLease(
                lease_id="lease-test",
                ticket_id="ticket-test",
                grant_id="grant-test",
                authorization_ref="auth-test",
                phase=Phase.P4,
                attempt_id="p4-test",
                task_id="PATCH-TEST",
                entry_policy_sha256="d" * 64,
                allowed_side_effects=(
                    SideEffect.GIT_MUTATION,
                    SideEffect.NETWORK_EGRESS,
                ),
                actor=actor,
                identity=identity,
                _bearer_secret=_BearerSecret("test-secret"),
            )
            runner = RunnerIdentity(
                module="research_automation.patch_executor",
                callable_name="ClaudePatchExecutor.apply",
                source_ref="research_automation/patch_executor.py",
                source_sha256="e" * 64,
            )

            def invocation(effect, argv):
                return ExecutionInvocation(
                    intent_ref=f"intent-{effect.value}",
                    entry_id=f"entry-{effect.value}",
                    effect=effect,
                    operation="PATCH_APPLY",
                    argv=tuple(argv),
                    cwd=str(workspace),
                    runner=runner,
                    resource_paths=(),
                )

            git_invocation = invocation(SideEffect.GIT_MUTATION, ("apply",))
            llm_invocation = invocation(
                SideEffect.NETWORK_EGRESS,
                ("claude", "-p", prompt),
            )
            review_invocation = invocation(
                SideEffect.NETWORK_EGRESS,
                ("review",),
            )
            experiment = {
                "scope": {
                    "code_change": {
                        "file": "strategy/brick_chart_strategy.py",
                    }
                }
            }

            with (
                patch(
                    "research_automation.patch_executor.ExecutionSinkGuard.authorize",
                    side_effect=lambda _lease, item: SimpleNamespace(
                        operation="PATCH_APPLY", effect=item.effect
                    ),
                ),
                patch(
                    "research_automation.patch_executor.AuthorizedSubprocess.run",
                    return_value=SimpleNamespace(
                        returncode=0,
                        stdout="FIND: VALUE = 1\nREPLACE WITH: VALUE = 2\n",
                        stderr="",
                    ),
                ),
                patch(
                    "research_automation.patch_executor.AuthorizedPatchApplier.apply"
                ),
                patch(
                    "research_automation.patch_executor.compile_gate",
                    return_value={"ok": True, "output": ""},
                ),
                patch(
                    "research_automation.kb_gate.gate_proposal_kb",
                    return_value={"verdict": "pass"},
                ),
                patch(
                    "research_automation.control_plane.memory."
                    "CommittedLearningLedgerReader.read_projection_input",
                    side_effect=RuntimeError("committed context unavailable"),
                ),
            ):
                result = executor.apply(
                    task,
                    workspace,
                    experiment=experiment,
                    lease=lease,
                    invocation=git_invocation,
                    llm_lease=lease,
                    llm_invocation=llm_invocation,
                    patch_lease=lease,
                    patch_invocation=git_invocation,
                    compile_lease=lease,
                    compile_invocation=git_invocation,
                    review_lease=lease,
                    review_invocation=review_invocation,
                    repository_root=root,
                )

        self.assertFalse(result.ok)
        self.assertIn("committed context unavailable", result.error or "")
        self.assertIn("discard the isolated workspace", " ".join(result.logs))

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
            ex = RealBacktestExecutor(project_root=root, keep_scratch=False)
            with patch("research_automation.experiment_runner.subprocess.run") as run:
                res = ex.execute({"strategy": "B1", "params": {}}, result_dir=result)
            self.assertFalse(res["success"])
            self.assertIn("execution lease", res["error"].lower())
            self.assertEqual(old.read_text(encoding="utf-8"), "old-output")
            self.assertFalse((result / "equity.csv").exists())
            run.assert_not_called()

    def test_unauthorized_run_cannot_overwrite_preexisting_output(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "run_b1_v3.py").write_text("pass", encoding="utf-8")
            shared = root / "artifacts" / "backtests" / "b1_v3" / "backtest_equity_v3.csv"
            shared.parent.mkdir(parents=True)
            shared.write_text("old-output", encoding="utf-8")
            result = root / "result"
            ex = RealBacktestExecutor(project_root=root, keep_scratch=False)
            with patch("research_automation.experiment_runner.subprocess.run") as run:
                res = ex.execute({"strategy": "B1", "params": {}}, result_dir=result)
            self.assertFalse(res["success"])
            self.assertIn("execution lease", res["error"].lower())
            self.assertEqual(shared.read_text(encoding="utf-8"), "old-output")
            run.assert_not_called()


class CampaignBoundaryAutomationTests(unittest.TestCase):
    def test_autonomous_runner_without_campaign_context_fails_before_writes(self):
        runner = object.__new__(AutonomousRunnerV1)
        with patch.object(Path, "mkdir", side_effect=AssertionError("write attempted")):
            with self.assertRaises(AuthorizationError) as caught:
                runner.run(dry_run=True)
        self.assertIn("campaign boundary", str(caught.exception).lower())

    def test_controller_without_campaign_context_fails_before_output(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            output_root = root / "experiments"
            controller = AutomationController(output_root=output_root)
            actor = Actor(
                "controller-boundary-test",
                "automation",
                "controller-boundary-invocation",
            )
            identity = AuthorityIdentity("a" * 64, "b" * 64, "c" * 64)
            lease = TaskExecutionLease(
                lease_id="lease-boundary",
                ticket_id="ticket-boundary",
                grant_id="grant-boundary",
                authorization_ref="auth-boundary",
                phase=Phase.P6,
                attempt_id="p6-boundary",
                task_id="BOUNDARY-TEST",
                entry_policy_sha256="d" * 64,
                allowed_side_effects=(SideEffect.RUN_RESEARCH,),
                actor=actor,
                identity=identity,
                _bearer_secret=_BearerSecret("test-secret"),
            )
            runner = RunnerIdentity(
                module="research_automation.automation_controller",
                callable_name="AutomationController.run_from_proposal",
                source_ref="research_automation/automation_controller.py",
                source_sha256="e" * 64,
            )
            invocation = ExecutionInvocation(
                intent_ref="intent-boundary",
                entry_id="entry-boundary",
                effect=SideEffect.RUN_RESEARCH,
                operation="AUTONOMOUS",
                argv=("run_from_proposal",),
                cwd=str(root),
                runner=runner,
                resource_paths=(str(output_root.resolve()),),
            )
            controller.execution_lease = lease
            controller.execution_invocation = invocation
            permit = SimpleNamespace(
                operation="AUTONOMOUS",
                effect=SideEffect.RUN_RESEARCH,
                resource_paths=(output_root.resolve(),),
            )
            with patch(
                "research_automation.automation_controller."
                "ExecutionSinkGuard.authorize",
                return_value=permit,
            ):
                result = controller.run_from_proposal(
                    "boundary-blocked",
                    {"hypothesis": "bounded", "scope": {}},
                )
            self.assertEqual("FAILED", result.status.value)
            self.assertTrue(
                any("campaign boundary" in item.lower() for item in result.logs)
            )
            self.assertFalse(output_root.exists())


class LegacyCandidateQuarantineTests(unittest.TestCase):
    def test_unstamped_candidate_dict_is_refused(self) -> None:
        with self.assertRaises(AuthorizationError):
            _require_legacy_candidate_stamp({"experiment_id": "unstamped"})

    def test_stamped_candidate_dict_passes(self) -> None:
        stamped = {
            "experiment_id": "stamped",
            "controller_created": False,
            "trust_state": "legacy_unaudited",
            "promotion_eligible": False,
        }
        self.assertIsNone(_require_legacy_candidate_stamp(stamped))

    def test_drain_refuses_unstamped_pool_entry_before_further_writes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            queue = TaskQueue(persist_path=root / "queue.json")
            queue.enqueue(ExperimentTask("t1", "b1", {"hypothesis": "bounded"}))
            runner = object.__new__(AutonomousRunnerV1)
            runner.strategy = "b1"
            runner._current_cycle_id = "cycle-quarantine"
            runner._current_round = 0
            runner.evaluator = SimpleNamespace(
                evaluate=lambda exp, baseline: "tested"
            )
            runner.pool = SimpleNamespace(
                add=lambda *args, **kwargs: {"experiment_id": "unstamped"}
            )
            metrics = StandardMetrics(
                source="metrics_json", sharpe=1.2, extra={"total_return": 0.1}
            )
            exp = SimpleNamespace(
                status=SimpleNamespace(value="COMPLETED"),
                metrics=metrics,
            )
            controller = SimpleNamespace(
                run_from_proposal=lambda *a, **k: exp
            )
            baseline = StandardMetrics(
                source="metrics_json", sharpe=1.0, extra={"total_return": 0.0}
            )
            with patch(
                "research_automation.cycle_log.write_cycle_log",
                side_effect=AssertionError("cycle log must not be written"),
            ) as clog:
                with self.assertRaises(AuthorizationError):
                    runner._drain(queue, controller, baseline, 0, None)
            clog.assert_not_called()

    def test_drain_keeps_concentration_analytics_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            queue = TaskQueue(persist_path=root / "queue.json")
            queue.enqueue(ExperimentTask("t1", "b1", {"hypothesis": "bounded"}))
            runner = object.__new__(AutonomousRunnerV1)
            runner.strategy = "b1"
            runner._current_cycle_id = "cycle-quarantine"
            runner._current_round = 0
            runner.evaluator = SimpleNamespace(
                evaluate=lambda exp, baseline: "tested"
            )
            stamped = {
                "experiment_id": "stamped",
                "controller_created": False,
                "trust_state": "legacy_unaudited",
                "promotion_eligible": False,
            }
            runner.pool = SimpleNamespace(
                add=lambda *args, **kwargs: dict(stamped)
            )
            metrics = StandardMetrics(
                source="metrics_json", sharpe=1.2, extra={"total_return": 0.1}
            )
            exp = SimpleNamespace(
                status=SimpleNamespace(value="COMPLETED"),
                metrics=metrics,
            )
            controller = SimpleNamespace(
                run_from_proposal=lambda *a, **k: exp
            )
            baseline = StandardMetrics(
                source="metrics_json", sharpe=1.0, extra={"total_return": 0.0}
            )
            with (
                patch(
                    "research_automation.autonomous_runner._kbase_writeback_enabled",
                    return_value=False,
                ),
                patch(
                    "research_automation.cycle_log.write_cycle_log",
                    return_value=root / "cycle_log.yaml",
                ),
                patch(
                    "research_automation.capital_tracker.record_experiment",
                    side_effect=AssertionError("capital write attempted"),
                ) as cap,
                patch(
                    "research_automation.coverage_map.update_from_entry",
                    side_effect=AssertionError("coverage write attempted"),
                ) as cov,
            ):
                out = runner._drain(queue, controller, baseline, 0, None)
            cap.assert_not_called()
            cov.assert_not_called()
            self.assertEqual(1, len(out))
            self.assertEqual("stamped", out[0]["experiment_id"])

    def test_recent_candidates_refuses_unstamped_pool_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pool_path = Path(td) / "candidate_pool.yaml"
            pool_path.write_text(
                yaml.safe_dump(
                    {"candidates": [{"experiment_id": "unstamped"}]}
                ),
                encoding="utf-8",
            )
            runner = object.__new__(AutonomousRunnerV1)
            runner.pool = SimpleNamespace(path=pool_path)
            with self.assertRaises(AuthorizationError):
                runner._recent_candidates(5)

    def test_recent_candidates_accepts_stamped_pool_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pool_path = Path(td) / "candidate_pool.yaml"
            entry = {
                "experiment_id": "stamped",
                "promotion_status": "tested",
                "params": {"pe": 30},
                "controller_created": False,
                "trust_state": "legacy_unaudited",
                "promotion_eligible": False,
            }
            pool_path.write_text(
                yaml.safe_dump({"candidates": [entry]}),
                encoding="utf-8",
            )
            runner = object.__new__(AutonomousRunnerV1)
            runner.pool = SimpleNamespace(path=pool_path)
            recent = runner._recent_candidates(5)
            self.assertEqual(
                [
                    {
                        "experiment_id": "stamped",
                        "promotion_status": "tested",
                        "params": {"pe": 30},
                    }
                ],
                recent,
            )

    def test_concentration_signals_remain_advisory(self) -> None:
        runner = object.__new__(AutonomousRunnerV1)
        runner.strategy = "b1"
        events = {
            "info_gain_zero_streak": 0,
            "max_surprise_last_5": 0.0,
            "escalation_fired_this_round": False,
            "phase_concentration_over_limit": True,
            "revision_loop_hit_limit": False,
            "capital_concentration_violation": {"over_limit": True},
            "coverage_imbalance": {"imbalance": True},
        }
        should, reasons = runner._should_invoke_director(1, events)
        self.assertTrue(should)
        self.assertIn("capital_concentration_70pct", reasons)
        self.assertIn("coverage_imbalance", reasons)

    def test_collect_recent_events_reads_concentration_as_analytics(self) -> None:
        runner = object.__new__(AutonomousRunnerV1)
        runner.strategy = "b1"
        with (
            patch(
                "research_automation.capital_tracker.check_concentration_violation",
                return_value={"subject": "b1_v3", "over_limit": True},
            ),
            patch(
                "research_automation.coverage_map.check_coverage_imbalance",
                return_value={"subject": "b1_v3", "imbalance": True},
            ),
        ):
            events = runner._collect_recent_events()
        self.assertEqual(
            {"subject": "b1_v3", "over_limit": True},
            events["capital_concentration_violation"],
        )
        self.assertEqual(
            {"subject": "b1_v3", "imbalance": True},
            events["coverage_imbalance"],
        )


if __name__ == "__main__":
    unittest.main()
