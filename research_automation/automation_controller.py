"""automation_controller.py -- Phase 1 controller driving the Experiment lifecycle.

Wires together: TaskQueue -> Experiment -> (pre-registry gate) -> Claude Code stub
-> backtest -> result parser -> report -> registry/snapshot/handoff deltas -> COMPLETED.
The Human Approval Gate can short-circuit any stage to ESCALATED_TO_USER.

Everything is dependency-injected so the loop runs fully offline with stub executors;
no real API and no mutation of existing Research Memory / Registry.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

from ag2_research.orchestrator import MemoryRouter

from .approval_gate import ApprovalGate
from .experiment import Experiment, ExperimentStatus, Proposal
from .experiment_runner import (
    BacktestExecutor, CodeChangeExecutor, StubBacktestExecutor, StubCodeChangeExecutor,
    generate_experiment_task_md,
)
from .handoff_updater import HandoffUpdater
from .registry_updater import RegistryUpdater
from .report_generator import ReportGenerator
from .result_parser import BacktestResultParser
from .snapshot_updater import SnapshotUpdater
from .task_queue import ExperimentTask, TaskQueue
from .control_plane.contracts import SideEffect
from .control_plane.sink_guard import (
    ExecutionAuthorizationError,
    ExecutionInvocation,
    ExecutionSinkGuard,
)
from .control_plane.stores import AuthorityReader, TaskExecutionLease


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class AutomationController:
    def __init__(
        self,
        strategy_id: str = "b1",
        workspace: str | Path = ".",
        output_root: str | Path = "research_automation/_output/runs/_adhoc/experiments",
        code_executor: CodeChangeExecutor | None = None,
        backtest_executor: BacktestExecutor | None = None,
        approval_gate: ApprovalGate | None = None,
        memory_router: MemoryRouter | None = None,
        workspace_manager=None,
        workspace_mode: bool = False,
        execution_lease=None,
        execution_invocation=None,
        authority_reader=None,
        repository_root: str | Path | None = None,
    ):
        self.strategy_id = strategy_id
        self.workspace = Path(workspace)
        self.output_root = Path(output_root)
        self.router = memory_router or MemoryRouter(strategy_id)
        # injectable boundaries (default to offline stubs)
        self.code_executor = code_executor or StubCodeChangeExecutor()
        self.backtest_executor = backtest_executor or StubBacktestExecutor()
        self.gate = approval_gate or ApprovalGate()
        # Sandbox: when enabled, each experiment gets an isolated workspace copy
        # of strategy/ + config/ + the entrypoint script; code changes land in the
        # workspace copy only, production code is never touched.
        self.workspace_manager = workspace_manager
        self.workspace_mode = workspace_mode
        self.execution_lease = execution_lease
        self.execution_invocation = execution_invocation
        self.authority_reader = authority_reader
        self.repository_root = Path(repository_root).resolve() if repository_root else Path(__file__).resolve().parent.parent
        # collaborators
        self.parser = BacktestResultParser()
        self.reporter = ReportGenerator()
        self.registry_updater = RegistryUpdater(strategy_id, router=self.router)
        self.snapshot_updater = SnapshotUpdater()
        self.handoff_updater = HandoffUpdater()

    # ---- public API -------------------------------------------------------
    def _authorize_controller(self) -> None:
        if not isinstance(self.execution_lease, TaskExecutionLease) or not isinstance(
            self.execution_invocation, ExecutionInvocation
        ):
            raise ExecutionAuthorizationError(
                "execution lease and invocation are required before controller execution"
            )
        reader = self.authority_reader if isinstance(self.authority_reader, AuthorityReader) else AuthorityReader()
        permit = ExecutionSinkGuard(
            authority_reader=reader,
            repository_root=self.repository_root,
        ).authorize(self.execution_lease, self.execution_invocation)
        if (
            permit.operation != "AUTONOMOUS"
            or permit.effect is not SideEffect.RUN_RESEARCH
        ):
            raise ExecutionAuthorizationError(
                "controller requires a RUN_RESEARCH AUTONOMOUS intent"
            )
        if self.output_root.resolve() not in permit.resource_paths:
            raise ExecutionAuthorizationError(
                "controller output root is not bound by the execution intent"
            )

    def run_from_proposal(self, experiment_id: str, proposal: dict) -> Experiment:
        try:
            self._authorize_controller()
        except (ExecutionAuthorizationError, OSError, ValueError) as error:
            exp = Experiment(
                experiment_id=experiment_id,
                strategy=self.strategy_id,
                proposal=Proposal(**{k: proposal.get(k, "") for k in
                                     ("hypothesis", "alpha_source", "scope", "success_criteria")} ),
                start_time=_now(),
            )
            exp.fail(f"controller unauthorized: {error}")
            exp.end_time = _now()
            return exp
        exp = Experiment(
            experiment_id=experiment_id,
            strategy=self.strategy_id,
            proposal=Proposal(**{k: proposal.get(k, "") for k in
                                 ("hypothesis", "alpha_source", "scope", "success_criteria")}),
            start_time=_now(),
        )
        out_dir = self.output_root / experiment_id
        try:
            self._drive(exp, out_dir)
        except Exception as e:  # any unhandled step error -> FAILED
            exp.fail(f"controller exception: {e}")
        exp.end_time = _now()
        (out_dir).mkdir(parents=True, exist_ok=True)
        (out_dir / "experiment.json").write_text(exp.to_json(), encoding="utf-8")
        return exp

    def drain_queue(self, queue: TaskQueue) -> list[Experiment]:
        self._authorize_controller()
        results = []
        n = 0
        while True:
            task = queue.dequeue()
            if task is None:
                break
            n += 1
            eid = task.task_id or f"{self.strategy_id}-auto-{int(time.time())}-{n:03d}"
            exp = self.run_from_proposal(eid, task.proposal)
            results.append(exp)
            if exp.status == ExperimentStatus.COMPLETED:
                queue.mark_done(task.task_id)
            else:
                queue.mark_failed(task.task_id, exp.status.value)
        return results

    # ---- lifecycle driver -------------------------------------------------
    def _drive(self, exp: Experiment, out_dir: Path) -> None:
        # PROPOSED -> pre-registry gate -> APPROVED / REJECTED / ESCALATED
        ref = self.registry_updater.classify(exp.proposal.hypothesis)
        exp.registry_reference = ref
        reg_decision = self.gate.check_registry(ref)
        frozen = (self.router.build_packet().get("snapshot", {}) or {}).get("frozen_directions")
        mem_decision = self.gate.check_memory_conflict(exp, frozen)
        if reg_decision.escalate or mem_decision.escalate:
            # registry conflict (duplicate/failed/verified) escalates per Phase 9
            exp.escalate(reg_decision.reasons + mem_decision.reasons)
            return
        if ref.action == "reject":
            exp.reject(f"registry action=reject ({ref.registry_status})")
            return
        exp.transition(ExperimentStatus.APPROVED, note=f"registry={ref.registry_status}")

        # APPROVED -> IMPLEMENTING (Claude Code abstraction)
        task_path = generate_experiment_task_md(exp, out_dir)
        exp.transition(ExperimentStatus.IMPLEMENTING)
        # Sandbox: create the isolated workspace BEFORE the code-change step so the
        # code executor (future Claude Code) edits the workspace copy, never prod.
        ws = None
        if self.workspace_mode and self.workspace_manager is not None:
            from .experiment_runner import _STRATEGY_SPECS, _OPT_SPEC
            spec = _STRATEGY_SPECS.get(self.strategy_id.upper(), _OPT_SPEC)
            ws = self.workspace_manager.create_workspace(exp, spec)
            exp.logs.append(f"workspace created: {ws}")
        cc = self.code_executor.apply(task_path, ws if ws is not None else self.workspace, experiment=exp)
        exp.logs.extend(cc.logs)
        if not cc.ok:
            exp.fail(cc.error or "code change failed")
            return
        exp.changed_files = cc.changed_files
        exp.deleted_files = cc.deleted_files
        exp.schema_changes = cc.schema_changes
        exp.git_commit = cc.git_commit
        code_decision = self.gate.check_code_change(exp)
        if code_decision.escalate:
            exp.escalate(code_decision.reasons)
            return

        # IMPLEMENTING -> BACKTESTING
        exp.transition(ExperimentStatus.BACKTESTING)
        result_dir = (out_dir / "outputs") if ws is not None else (out_dir / "result")
        result = self.backtest_executor.run(exp, ws if ws is not None else self.workspace, result_dir)
        exp.logs.extend(result.logs)
        if not result.ok:
            exp.fail(result.error or "backtest failed")
            return
        exp.metrics = self.parser.parse(result.result_dir)
        bt_decision = self.gate.check_backtest(result, exp)
        if bt_decision.escalate:
            exp.escalate(bt_decision.reasons)
            return

        # BACKTESTING -> REPORTING
        exp.transition(ExperimentStatus.REPORTING)
        # write lineage.json BEFORE report so the report can reference it
        from .lineage import write_lineage_json
        write_lineage_json(exp, out_dir)
        self.reporter.generate(exp, out_dir)

        # REPORTING -> REGISTRY_UPDATE
        exp.transition(ExperimentStatus.REGISTRY_UPDATE)
        self.registry_updater.build_entry(exp)
        self.registry_updater.write_delta(exp, out_dir)

        # REGISTRY_UPDATE -> SNAPSHOT_UPDATE
        exp.transition(ExperimentStatus.SNAPSHOT_UPDATE)
        self.snapshot_updater.build_delta(exp)
        self.snapshot_updater.write_delta(exp, out_dir)

        # SNAPSHOT_UPDATE -> HANDOFF_UPDATE
        exp.transition(ExperimentStatus.HANDOFF_UPDATE)
        self.handoff_updater.build_delta(exp)
        self.handoff_updater.write_delta(exp, out_dir)

        # HANDOFF_UPDATE -> COMPLETED
        exp.transition(ExperimentStatus.COMPLETED, note="loop closed; ready to return to AG2")

    # ---- AG2 hand-back ----------------------------------------------------
    def ag2_feedback_packet(self, exp: Experiment) -> dict:
        """Compact result to feed back into the AG2 discussion (closes the loop)."""
        return {
            "experiment_id": exp.experiment_id,
            "status": exp.status.value,
            "registry_status": exp.registry_reference.registry_status,
            "metrics": {
                "sharpe": exp.metrics.sharpe, "cagr": exp.metrics.cagr,
                "max_drawdown": exp.metrics.max_drawdown, "trades": exp.metrics.trades,
            },
            "report_path": exp.report_path,
            "escalation_reasons": exp.escalation_reasons,
            "deltas": {
                "registry_entry": exp.registry_update,
                "snapshot_delta": exp.snapshot_update,
                "handoff_delta": exp.handoff_update,
            },
        }
