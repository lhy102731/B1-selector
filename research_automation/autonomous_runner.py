"""autonomous_runner.py -- AutonomousRunnerV1.

Drives the closed research loop with: hybrid task sourcing (your ideas first, then
auto), stateless rounds with a BOUNDED memory_packet (no context growth), per-experiment
STOP checks, champion-baseline delta, candidate pool + nightly report, and --resume.

Safety: every experiment runs through AutomationController (registry gate + approval gate),
which only writes staging deltas. Champion / Registry / Snapshot / Handoff are never mutated.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

from ag2_research.orchestrator import MemoryRouter
from .automation_controller import AutomationController
from .ag2_task_adapter import AG2TaskAdapter
from .experiment import StandardMetrics
from .experiment_runner import RealBacktestExecutor, NoOpCodeChangeExecutor
from .nightly_report import NightlyReport
from .patch_executor import ClaudePatchExecutor
from .promotion import CandidatePool, PromotionEvaluator
from .proposer import ParameterProposer
from .research_director import ResearchDirector
from .result_parser import BacktestResultParser
from .workspace_manager import WorkspaceManager
from .safety import output_root
from .strategies import require_supported
from .task_queue import TaskQueue
from .control_plane.contracts import SideEffect
from .control_plane.entry_guard import AuthorizationError, EntryGuard
from .control_plane.sink_guard import (
    ExecutionAuthorizationError,
    ExecutionInvocation,
    ExecutionSinkGuard,
)
from .control_plane.stores import AuthorityReader, TaskExecutionLease


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _kbase_writeback_enabled() -> bool:
    return os.environ.get("KBASE_WRITEBACK", "0").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


class AutonomousRunnerV1:
    # AG2 proposes and validates candidates here; execution belongs solely to
    # the automation queue so each experiment is run exactly once.
    AG2_CANDIDATE_WORKFLOW = "proposal_gate"
    STOP_FILE = output_root() / "STOP"

    def __init__(self, strategy: str = "b1", source: str = "hybrid", auto_source: str = "proposer",
                 ideas: list | None = None, search_space_path: str | Path | None = None,
                 keep_scratch: bool = False, memory_packet_recent_n: int = 8,
                 project_root: str | Path | None = None, workspace_mode: bool = True,
                 research_mode: str = "parameter", execution_lease=None,
                 execution_invocation=None, authority_reader=None):
        self.strategy = strategy.lower()
        # validate selection up front: unsupported strategies (e.g. b3) raise with a reason
        self.profile = require_supported(self.strategy)
        self.source = source
        self.auto_source = auto_source
        self.ideas = ideas or []
        self.keep_scratch = keep_scratch
        self.recent_n = memory_packet_recent_n
        self.workspace_mode = workspace_mode
        self.research_mode = research_mode  # "parameter" (default, unchanged) | "code"
        self.router = MemoryRouter(self.strategy)
        self.proposer = ParameterProposer(self.strategy, search_space_path, memory_router=self.router)
        self._scope = self.proposer._space.get("scope", {}) or {}
        self._grid = self.proposer._space.get("grid", {}) or {}
        self._champion = self.proposer._space.get("champion_params", {}) or {}
        self.adapter = AG2TaskAdapter(
            self.strategy, default_scope=self._scope,
            cli_params=self.profile.cli_params, normalize_map=self.profile.normalize_map,
            champion_params=self._champion,
        )
        self.evaluator = PromotionEvaluator()
        self.pool = CandidatePool()
        self.reporter = NightlyReport()
        self.parser = BacktestResultParser()
        self.project_root = Path(project_root) if project_root else None
        self.execution_lease = execution_lease
        self.execution_invocation = execution_invocation
        self.authority_reader = authority_reader

    # ---- stop control -----------------------------------------------------
    def _stop_requested(self) -> bool:
        return self.STOP_FILE.exists()

    def _clear_stop(self) -> None:
        try:
            self.STOP_FILE.unlink()
        except OSError:
            pass

    # ---- baseline (champion, frozen, read-only) ---------------------------
    def _run_baseline(self, cycle_dir: Path) -> StandardMetrics:
        ex = RealBacktestExecutor(project_root=self.project_root, keep_scratch=self.keep_scratch)
        # baseline = champion config (CLI-reproducible subset); empty for strategies w/o one
        scope = {"strategy": self.strategy.upper(), **self._scope, "params": dict(self._champion)}
        result_dir = cycle_dir / "baseline" / "result"
        res = ex.execute(scope, result_dir=result_dir)
        if not res.get("success"):
            raise RuntimeError(f"baseline backtest failed: {res.get('error') or res.get('stderr') or 'unknown error'}")
        metrics = self.parser.parse(result_dir)
        if not self._has_valid_metrics(metrics):
            raise RuntimeError("baseline backtest produced no valid metrics")
        return metrics

    @staticmethod
    def _has_valid_metrics(metrics: StandardMetrics) -> bool:
        primary = (metrics.sharpe, metrics.cagr, metrics.win_rate,
                   metrics.max_drawdown, metrics.trades)
        return metrics.source != "none" and (any(v is not None for v in primary)
                                             or (metrics.extra or {}).get("total_return") is not None)

    # ---- main -------------------------------------------------------------
    def run(self, max_rounds: int = 5, per_round: int = 4, max_minutes: int | None = None,
            resume: bool = False, dry_run: bool = False) -> dict:
        entry_guard = getattr(self, "entry_guard", None)
        if not isinstance(entry_guard, EntryGuard):
            raise AuthorizationError("AutonomousRunnerV1 requires a control-plane entry ticket")
        entry_guard.assert_side_effect(SideEffect.RUN_RESEARCH)
        execution_lease = getattr(self, "execution_lease", None)
        execution_invocation = getattr(self, "execution_invocation", None)
        if not isinstance(execution_lease, TaskExecutionLease) or not isinstance(
            execution_invocation, ExecutionInvocation
        ):
            raise AuthorizationError(
                "AutonomousRunnerV1 requires a P0R2 execution lease and invocation"
            )
        try:
            reader = self.authority_reader if isinstance(self.authority_reader, AuthorityReader) else AuthorityReader()
            repository_root = self.project_root or Path(__file__).resolve().parent.parent
            permit = ExecutionSinkGuard(
                authority_reader=reader,
                repository_root=repository_root,
            ).authorize(execution_lease, execution_invocation)
            if (
                permit.operation != "AUTONOMOUS"
                or permit.effect is not SideEffect.RUN_RESEARCH
            ):
                raise ExecutionAuthorizationError(
                    "autonomous runner requires a RUN_RESEARCH AUTONOMOUS intent"
                )
        except (ExecutionAuthorizationError, OSError, ValueError) as error:
            raise AuthorizationError(f"AutonomousRunnerV1 authority rejected: {error}") from error
        t0 = time.time()
        runs_root = output_root() / "runs"
        runs_root.mkdir(parents=True, exist_ok=True)

        if resume:
            existing = sorted([p for p in runs_root.glob("*") if p.is_dir()])
            cycle_dir = existing[-1] if existing else runs_root / _now_stamp()
        else:
            cycle_dir = runs_root / _now_stamp()
        cycle_dir.mkdir(parents=True, exist_ok=True)
        cycle_id = cycle_dir.name
        # v4.1: expose cycle_id to inner helpers (cycle_log writer + director)
        self._current_cycle_id = cycle_id
        self._current_round = 0
        queue = TaskQueue(persist_path=cycle_dir / "queue.json")

        print(f"[AutonomousRunnerV1] cycle={cycle_id}  source={self.source}  auto={self.auto_source}")
        print(f"  strategy={self.strategy} capability={self.profile.capability}  research_mode={self.research_mode}")
        if self.profile.caveat:
            print(f"  CAVEAT: {self.profile.caveat}")
        print(f"  STOP anytime: double-click stop.bat  |  create {self.STOP_FILE}  |  Ctrl+C")

        # ---- select code executor based on research_mode ----
        if self.research_mode == "code":
            code_exec = ClaudePatchExecutor()
            print("  code_executor: ClaudePatchExecutor")
        else:
            code_exec = NoOpCodeChangeExecutor()
            # default: silent (parameter experiments change no code)

        controller = AutomationController(
            strategy_id=self.strategy,
            output_root=cycle_dir / "experiments",
            code_executor=code_exec,
            backtest_executor=RealBacktestExecutor(
                project_root=self.project_root, keep_scratch=self.keep_scratch,
                workspace_mode=self.workspace_mode,
                workspace_manager=WorkspaceManager(
                    workspace_root=cycle_dir / "experiments",
                    project_root=self.project_root,
                ) if self.workspace_mode else None,
            ),
            memory_router=self.router,
            workspace_mode=self.workspace_mode,
            workspace_manager=WorkspaceManager(
                workspace_root=cycle_dir / "experiments",
                project_root=self.project_root,
            ) if self.workspace_mode else None,
            execution_lease=execution_lease,
            execution_invocation=execution_invocation,
            authority_reader=reader,
            repository_root=repository_root,
        )

        # ---- phase A: your ideas first (priority 0) ----
        if self.source in ("hybrid", "idea") and self.ideas:
            for t in self.adapter.from_human_idea(self.ideas, priority=0, fallback_grid=self._grid):
                queue.enqueue(t)
            print(f"  phase A: enqueued {queue.pending_count()} idea tasks (priority 0)")

        if dry_run:
            return self._dry_run(queue, max_rounds, per_round, cycle_id)

        baseline = self._run_baseline(cycle_dir)
        print(f"  baseline: sharpe={baseline.sharpe} total_return={(baseline.extra or {}).get('total_return')}")

        candidates: list[dict] = []
        try:
            # drain any idea tasks first (phase A)
            candidates += self._drain(queue, controller, baseline, t0, max_minutes)

            # ---- phase B: auto rounds ----
            if self.source in ("hybrid", "proposer", "ag2"):
                # code mode: Research Director + Proposal Generator + Claude
                if self.research_mode == "code":
                    candidates += self._run_code_rounds(max_rounds, per_round, controller,
                                                        baseline, t0, max_minutes, cycle_dir)
                else:
                    # parameter mode: unchanged
                    for r in range(1, max_rounds + 1):
                        self._current_round = r   # v4.1: cycle_log + director hooks read this
                        if self._stop_requested() or self._time_up(t0, max_minutes):
                            print("  stop requested / time up -> ending rounds")
                            break
                        pkt = self._round_memory_packet()
                        print(f"  round {r}: memory_packet={len(json.dumps(pkt, ensure_ascii=False))}B "
                              f"recent_candidates={len(pkt['recent_candidates'])}")
                        tasks = self._generate_auto(per_round, controller)
                        for t in tasks:
                            queue.enqueue(t)
                        if queue.pending_count() == 0:
                            print(f"  round {r}: no new tasks (dedup/registry) -> stop")
                            break
                        candidates += self._drain(queue, controller, baseline, t0, max_minutes)
                        # v4.1: end-of-round director hook (periodic / event-driven)
                        self._maybe_invoke_director(r)
        except KeyboardInterrupt:
            print("\n  KeyboardInterrupt -> finishing cleanly, writing report")

        report_path = self.reporter.generate(
            {"cycle_id": cycle_id, "strategy": self.strategy, "rounds": max_rounds,
             "baseline": {"sharpe": baseline.sharpe, "max_drawdown": baseline.max_drawdown,
                          "win_rate": baseline.win_rate, "trades": baseline.trades,
                          "total_return": (baseline.extra or {}).get("total_return")},
             "candidates": candidates},
            _today())
        # ---- per-cycle lineage tree (aggregates all experiment parent/child edges) ---
        from .lineage import build_lineage_tree
        build_lineage_tree(candidates, cycle_dir / "lineage_tree.json")

        # ---- campaign summary (stability tracking for Champion Pool) ----
        _campaign_summary = {
            "rounds": len(candidates),
            "completed": sum(1 for c in candidates
                             if c.get("_experiment_status") == "COMPLETED"),
            "failed": sum(1 for c in candidates
                          if c.get("_experiment_status") == "FAILED"),
            "rejected": len(candidates) - sum(1 for c in candidates
                                                if c.get("_experiment_status") == "COMPLETED")
                        - sum(1 for c in candidates
                              if c.get("_experiment_status") == "FAILED"),
            "best_return": max(((c.get("metrics") or {}).get("total_return") or 0)
                               for c in candidates) if candidates else 0,
            "best_experiment": max(candidates, key=lambda c:
                                   ((c.get("metrics") or {}).get("total_return") or 0))["experiment_id"]
            if candidates else None,
        }
        from .safety import assert_safe_path
        _cs_path = assert_safe_path(cycle_dir / "campaign_summary.json")
        _cs_path.write_text(
            json.dumps(_campaign_summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  campaign_summary: {_cs_path}   rounds={_campaign_summary['rounds']} "
              f"completed={_campaign_summary['completed']} rejected={_campaign_summary['rejected']} "
              f"failed={_campaign_summary['failed']}")

        self._clear_stop()
        print(f"  done: {len(candidates)} experiments -> {report_path}")
        return {"cycle_id": cycle_id, "candidates": candidates, "report": str(report_path),
                "stopped": self._stop_requested()}

    # ---- helpers ----------------------------------------------------------
    def _time_up(self, t0: float, max_minutes: int | None) -> bool:
        return max_minutes is not None and (time.time() - t0) > max_minutes * 60

    def _drain(self, queue: TaskQueue, controller: AutomationController,
               baseline: StandardMetrics, t0: float, max_minutes: int | None) -> list[dict]:
        out = []
        # v4.1: cycle_log writer subject + cycle_id
        from .cycle_log import write_cycle_log, _info_gain_from_entry
        # v4.2: governance trackers
        from .capital_tracker import record_experiment as cap_record
        from .coverage_map import update_from_entry as cov_update
        kb_subject = "b1_v3" if self.strategy == "b1" else self.strategy
        while True:
            if self._stop_requested() or self._time_up(t0, max_minutes):
                print("  STOP/time -> halting before next experiment")
                break
            task = queue.dequeue()
            if task is None:
                break
            exp = controller.run_from_proposal(task.task_id, task.proposal)
            if exp.status.value != "COMPLETED":
                queue.mark_failed(task.task_id, exp.status.value)
                continue
            status = self.evaluator.evaluate(exp, baseline)
            delta = self._delta(exp.metrics, baseline)
            entry = self.pool.add(exp, status, baseline, delta)
            entry["_experiment_status"] = exp.status.value  # real status (COMPLETED/FAILED), not promotion
            out.append(entry)
            queue.mark_done(task.task_id)
            # v4.1: persist cycle_log for Research_Director event triggers
            try:
                cycle_log_path = write_cycle_log(
                    subject=kb_subject,
                    cycle_id=getattr(self, "_current_cycle_id", "unknown"),
                    round_n=getattr(self, "_current_round", 0),
                    entry=entry,
                    baseline=baseline,
                )
            except Exception as e:
                cycle_log_path = None
                print(f"    cycle_log write failed (non-fatal): {e}")
            # Knowledge Bridge v1: write experiment evidence back to KBase.
            # This is output-only and cannot promote claims or mutate hard constraints.
            if _kbase_writeback_enabled():
                try:
                    from ag2_research.knowledge_bridge import write_experiment_output

                    artifacts = [item for item in (cycle_log_path, entry.get("report_path")) if item]
                    kbase_output = write_experiment_output(
                        subject=kb_subject,
                        cycle_id=getattr(self, "_current_cycle_id", "unknown"),
                        round_n=getattr(self, "_current_round", 0),
                        entry=entry,
                        baseline={
                            "sharpe": baseline.sharpe,
                            "max_drawdown": baseline.max_drawdown,
                            "win_rate": baseline.win_rate,
                            "trades": baseline.trades,
                            "total_return": (baseline.extra or {}).get("total_return"),
                            "profit_factor": (baseline.extra or {}).get("profit_factor"),
                        },
                        artifact_paths=artifacts,
                        project_root=self.project_root or Path.cwd(),
                    )
                    entry["kbase_output_path"] = str(kbase_output)
                except Exception as e:
                    print(f"    KBase writeback failed (non-fatal): {e}")
            # v4.2: governance — capital + coverage tracking
            try:
                ig = _info_gain_from_entry(entry)
                cap_record(kb_subject,
                           getattr(self, "_current_cycle_id", "unknown"),
                           getattr(self, "_current_round", 0),
                           entry, info_gain=ig)
                cov_update(kb_subject, entry,
                           cycle_id=getattr(self, "_current_cycle_id", "unknown"),
                           info_gain=ig)
            except Exception as e:
                print(f"    capital/coverage write failed (non-fatal): {e}")
            print(f"    {task.task_id}: {exp.status.value} -> promotion={status} delta={delta}")
        return out

    def _generate_auto(self, per_round: int, controller: AutomationController) -> list:
        if self.auto_source == "ag2":
            try:
                res = self._ag2_round(per_round)
                tasks = self.adapter.from_ag2_discussion(res, priority=100, fallback_grid=self._grid) if res else []
                if tasks:
                    return tasks[:per_round]
            except Exception as e:
                raise RuntimeError(
                    f"AG2 candidate generation failed; no proposer downgrade allowed: {e}"
                ) from e
            raise RuntimeError(
                "AG2 candidate generation failed; no executable tasks were produced and "
                "no proposer downgrade is allowed"
            )
        # over-generate so registry-dropped/duplicate proposals are topped up;
        # adapter dedup (_seen_keys) advances through the grid across rounds.
        proposals = self.proposer.propose(max(per_round * 6, per_round))
        return self.adapter.from_proposer(proposals, priority=100, limit=per_round)

    def _run_code_rounds(self, max_rounds: int, per_round: int,
                         controller: AutomationController, baseline: StandardMetrics,
                         t0: float, max_minutes: int | None, cycle_dir: Path) -> list[dict]:
        """Code mode: Director → Proposal Generator → Claude → Backtest → feedback loop."""
        from .research_proposal_generator import ResearchProposalGenerator
        from .task_queue import ExperimentTask

        director = ResearchDirector()
        prop_gen = ResearchProposalGenerator()
        candidates: list[dict] = []
        # seed history with a baseline entry so round-1 Director has data to analyse
        history: list[dict] = [{
            "experiment_id": "baseline",
            "total_return": (baseline.extra or {}).get("total_return", 0) or 0,
            "trades": baseline.trades,
            "win_rate": baseline.win_rate,
            "code_change": {"symbol": "height_ratio", "value": 2.0 / 3.0},
            "hypothesis": "baseline: height_ratio=2/3 (champion)",
            "param_value": 2.0 / 3.0,
            "generation": 0,
        }]

        for r in range(1, max_rounds + 1):
            self._current_round = r   # v4.1: cycle_log + director hooks read this
            if self._stop_requested() or self._time_up(t0, max_minutes):
                print("  stop requested / time up -> ending rounds")
                break

            # ---- Director analyses history ----
            analysis = director.analyze(history if history else
                                        [{"experiment_id": "baseline", "total_return": 0,
                                          "code_change": {"symbol": "height_ratio"}}])
            print(f"  round {r}: findings={len(analysis.findings)} "
                  f"exhausted={[d.symbol for d in analysis.exhausted_dimensions]} "
                  f"hypotheses={len(analysis.hypotheses)}")

            # ---- Generate proposals from ranked output ----
            raw_proposals = []
            exhausted_symbols = {d.symbol for d in analysis.exhausted_dimensions}
            for rp in analysis.ranked_proposals[:per_round]:
                if rp.code_change.get("symbol") in exhausted_symbols:
                    continue  # skip exhausted
                raw_proposals.append(rp)

            # If Director didn't produce enough, supplement with ProposalGenerator
            if len(raw_proposals) < per_round:
                extra = prop_gen.generate(history if history else
                    [{"experiment_id": "baseline", "total_return": 0,
                      "code_change": {"symbol": "height_ratio", "value": 2.0/3.0}}],
                    max_proposals=per_round - len(raw_proposals))
                for ep in extra:
                    raw_proposals.append(ep)

            executable_proposals = []
            seen_code_changes = set()
            for rp in raw_proposals:
                cc = getattr(rp, 'code_change', None)
                if cc is None and isinstance(rp, dict):
                    cc = rp.get("code_change")
                symbol = cc.get("symbol") if isinstance(cc, dict) else None
                value = cc.get("value") if isinstance(cc, dict) else None
                if symbol in exhausted_symbols:
                    continue
                if not self._is_supported_code_change(cc):
                    print(f"    filtered unsupported code_change: symbol={symbol} value={value}")
                    continue
                # ---- KB hard-constraint gate (Phase 15 closure) ----
                from .kb_gate import gate_proposal_kb
                kb_subject = "b1_v3" if self.strategy == "b1" else self.strategy
                kb_verdict = gate_proposal_kb(kb_subject, {
                    "subject": kb_subject,
                    "hypothesis": getattr(rp, "hypothesis", None)
                                  or (rp.get("hypothesis") if isinstance(rp, dict) else "") or "",
                    "scope": {
                        "code_change": cc,
                        "params": (rp.get("scope", {}) if isinstance(rp, dict) else {}).get("params", {}),
                    },
                })
                if kb_verdict["verdict"] == "reject":
                    print(f"    KB rejected ({kb_verdict.get('kb_version')}): "
                          f"violations={kb_verdict['violations']} -- "
                          f"{kb_verdict['reasons'][0] if kb_verdict['reasons'] else ''}")
                    continue
                if kb_verdict["verdict"] == "needs_evidence":
                    print(f"    KB needs_evidence: {kb_verdict['needs_evidence']} -- skipping in code-mode")
                    continue
                key = (symbol, value)
                if key in seen_code_changes:
                    continue
                seen_code_changes.add(key)
                executable_proposals.append(rp)
            raw_proposals = executable_proposals

            if not raw_proposals:
                print(f"  round {r}: no executable proposals -> stop")
                break

            # ---- Convert to tasks + drain ----
            queue = TaskQueue(persist_path=cycle_dir / f"queue_round_{r}.json")
            for i, rp in enumerate(raw_proposals[:per_round]):
                tid = f"{self.strategy}-code-{cycle_dir.name}-r{r:02d}-{i:02d}"
                # build scope: window params from search_space + code_change from Director
                task_scope = dict(self._scope) if self._scope else {}
                task_scope["code_change"] = rp.code_change
                task = ExperimentTask(
                    task_id=tid, strategy=self.strategy,
                    proposal={"hypothesis": rp.hypothesis if hasattr(rp, 'hypothesis')
                              else rp.code_change.get("symbol", "?"),
                              "scope": task_scope},
                    source="director", priority=100,
                )
                queue.enqueue(task)

            print(f"  round {r}: enqueued {queue.pending_count()} tasks")
            round_candidates = self._drain(queue, controller, baseline, t0, max_minutes)
            candidates.extend(round_candidates)

            # v4.1: end-of-round director hook (periodic / event-driven)
            # In code mode the director gets a chance every DIRECTOR_INTERVAL
            # rounds (default 5) or when surprise/info_gain triggers fire.
            self._maybe_invoke_director(r)

            # ---- Feedback: accumulate history for next round ----
            # even failed experiments produce a history entry (with score=0) so
            # the Director sees the attempt and doesn't repeat the same dead end.
            for idx, c in enumerate(round_candidates):
                metrics_dict = c.get("metrics") or {}
                pool_params = c.get("params") or {}
                orig = raw_proposals[idx] if idx < len(raw_proposals) else None
                orig_cc = getattr(orig, 'code_change', None) if orig is not None else None
                if orig_cc is None and isinstance(orig, dict):
                    orig_cc = orig.get("code_change")
                cc = orig_cc if orig_cc else {
                    "symbol": pool_params.get("symbol", "?"),
                    "value": pool_params.get("value"),
                }
                entry = {
                    "experiment_id": c.get("experiment_id", f"round{r}-{idx}"),
                    "total_return": (metrics_dict.get("total_return") or 0) if metrics_dict else 0,
                    "trades": metrics_dict.get("trades") if metrics_dict else None,
                    "win_rate": metrics_dict.get("win_rate") if metrics_dict else None,
                    "code_change": cc,
                    "hypothesis": c.get("hypothesis", ""),
                    "param_value": cc.get("value", 0),
                    "generation": r,
                    "status": c.get("promotion_status", "unknown"),
                }
                history.append(entry)

        return candidates

    @staticmethod
    def _is_supported_code_change(code_change: dict | None) -> bool:
        if not isinstance(code_change, dict):
            return False
        symbol = code_change.get("symbol")
        value = code_change.get("value")
        return (
            code_change.get("change_type") == "modify_constant"
            and code_change.get("file") == "strategy/brick_chart_strategy.py"
            and isinstance(symbol, str)
            and "+" not in symbol
            and "," not in symbol
            and isinstance(value, (int, float))
        )

    def _ag2_round(self, per_round: int) -> dict | None:
        """Optional: drive one AG2 sequential discussion to produce experiment_spec(s).
        Requires LLM API; returns the run_sequential_workflow result or None."""
        from ag2_research.orchestrator import Orchestrator
        strategy_label = self.strategy.upper()
        if self.strategy == "brick":
            strategy_objective = (
                "Brick V2 ranking-backtest parameter experiment for brick_signal "
                "candidate ranking. Stay inside Brick: candidate ranking, dynamic "
                "TopN, entry MA source, per-industry cap, or score threshold only. "
                "Do not import B1 pullback/retest or B3 limit-up relay logic."
            )
        else:
            strategy_objective = (
                f"{strategy_label} in-boundary parameter experiment. Stay inside "
                "the selected strategy memory and its CLI-tunable parameters."
            )
        # Inject project closure first, then validated external evidence.
        kb_ctx = ""
        try:
            from ag2_research.knowledge_bridge import build_combined_research_context
            from ag2_research.discovery_handoff import render_discovery_context
            kb_subject = "b1_v3" if self.strategy == "b1" else self.strategy
            topic = (
                f"Propose ONE executable in-boundary {strategy_label} parameter "
                "experiment as experiment_spec{param,values}."
            )
            kb_ctx = build_combined_research_context(kb_subject, query=topic, project_mode="brief")
            kb_ctx += render_discovery_context(self.strategy)
        except Exception as _e:
            kb_ctx = ""
        orch = Orchestrator()
        topic = (
            f"Propose ONE executable in-boundary {strategy_label} parameter experiment "
            f"as experiment_spec{{param,values}}. {strategy_objective} Use an approved "
            "KBase discovery handoff only when it maps exactly to an existing CLI "
            "parameter; never disguise a new factor as a threshold tweak."
        )
        # Candidate generation must not execute a backtest inside AG2.  The
        # controller queues the approved experiment and executes it exactly
        # once later, so use the proposal-only gated workflow here.
        return orch.run_sequential_workflow(
            self.AG2_CANDIDATE_WORKFLOW, topic=topic, strategy_id=self.strategy,
            research_context=kb_ctx,
        )

    # ----------------------------------------------------------------
    # v4.1: Research Director hook (called between rounds, NOT per stage)
    # ----------------------------------------------------------------
    # The director sets STRATEGIC direction (mode / state / channel mix /
    # termination). pipeline_controller does mechanical control. The
    # director runs:
    #   - every DIRECTOR_INTERVAL cycles (periodic)
    #   - or when any trigger fires (event-driven)
    # See ag2_research/config.yaml::agents.research_director for the full
    # contract.
    DIRECTOR_INTERVAL = 2

    def _should_invoke_director(self, round_n: int, recent_events: dict) -> tuple[bool, list[str]]:
        """Return (should_run, reasons[]) for the director invocation."""
        reasons = []
        if round_n > 0 and round_n % self.DIRECTOR_INTERVAL == 0:
            reasons.append("interval")
        if recent_events.get("info_gain_zero_streak", 0) >= 3:
            reasons.append("info_gain_zero_streak")
        if recent_events.get("max_surprise_last_5", 0) >= 0.5:
            reasons.append("surprise")
        if recent_events.get("escalation_fired_this_round", False):
            reasons.append("escalation")
        if recent_events.get("phase_concentration_over_limit", False):
            reasons.append("concentration")
        if recent_events.get("revision_loop_hit_limit", False):
            reasons.append("revision_limit")
        # v4.2 governance triggers
        if recent_events.get("capital_concentration_violation"):
            reasons.append("capital_concentration_70pct")
        if recent_events.get("coverage_imbalance"):
            reasons.append("coverage_imbalance")
        return (len(reasons) > 0, reasons)

    def _collect_recent_events(self) -> dict:
        """Aggregate signals from cycle_log files + v4.2 governance trackers.

        Reads research_state/<subject>/cycle_log_*.yaml if present, else
        returns zeros. Always safe — never blocks the pipeline if files
        are missing.
        """
        out = {
            "info_gain_zero_streak": 0,
            "max_surprise_last_5": 0.0,
            "escalation_fired_this_round": False,
            "phase_concentration_over_limit": False,
            "revision_loop_hit_limit": False,
            "cycle_logs_read": 0,
            # v4.2 governance
            "capital_concentration_violation": None,
            "coverage_imbalance": None,
        }
        kb_subject = "b1_v3" if self.strategy == "b1" else self.strategy
        try:
            from pathlib import Path
            state_dir = Path("research_state") / kb_subject
            if state_dir.exists():
                logs = sorted(state_dir.glob("cycle_log_*.yaml"))[-5:]
                out["cycle_logs_read"] = len(logs)
                zero_streak = 0
                max_surprise = 0.0
                for log_path in logs:
                    try:
                        data = yaml.safe_load(log_path.read_text(encoding="utf-8")) or {}
                        ig = (data.get("info_gain") or {}).get("info_gain_score", 0)
                        if ig == 0:
                            zero_streak += 1
                        else:
                            zero_streak = 0
                        sur = (data.get("surprise") or {}).get("max_surprise_score", 0.0)
                        if sur > max_surprise:
                            max_surprise = sur
                    except Exception:
                        pass
                out["info_gain_zero_streak"] = zero_streak
                out["max_surprise_last_5"] = max_surprise
        except Exception:
            pass

        # v4.2: governance modules
        try:
            from .capital_tracker import check_concentration_violation
            from .coverage_map import check_coverage_imbalance
            out["capital_concentration_violation"] = check_concentration_violation(kb_subject)
            out["coverage_imbalance"] = check_coverage_imbalance(kb_subject)
        except Exception:
            pass

        return out

    def _invoke_director(self, round_n: int, reasons: list[str]) -> dict | None:
        """Run the director_only AG2 workflow and persist its decision.

        Returns the director_decision dict (or None on failure). The
        decision is written to research_state/<subject>/director_decisions.yaml
        as a rolling log; pipeline_controller is expected to consume the
        latest entry on the next cycle's memory_packet.
        """
        try:
            from ag2_research.orchestrator import Orchestrator
            from ag2_research.knowledge_base import build_context, list_subjects
            from pathlib import Path

            kb_subject = "b1_v3" if self.strategy == "b1" else self.strategy
            kb_ctx = ""
            if kb_subject in list_subjects():
                kb_ctx = build_context(kb_subject, mode="brief")

            # v4.2: inject capital + coverage summaries into the director's context
            try:
                from .capital_tracker import (summary_for_director as cap_summary,
                                              aggregate_to_json, category_spend_estimate)
                from .coverage_map import summary_for_director as cov_summary
                from .strategy_state import summary_for_director as state_summary
                # Step 6: refresh agent_performance.json on each director call.
                # Cheap (one jsonl read) and gives Director up-to-date efficiency numbers.
                try:
                    aggregate_to_json(kb_subject)
                except Exception as _e:
                    pass
                cap_sum = cap_summary(kb_subject)
                cov_sum = cov_summary(kb_subject)
                state_sum = state_summary(kb_subject)
                spend_warn = category_spend_estimate(kb_subject)
                gov_block = "\n\n## v4.2 Governance Inputs (MUST consult)\n\n"
                gov_block += "### Strategy State + Open Questions\n```yaml\n"
                gov_block += yaml.safe_dump(state_sum, sort_keys=False, allow_unicode=True)
                gov_block += "```\n\n### Capital Tracker summary\n```yaml\n"
                gov_block += yaml.safe_dump(cap_sum, sort_keys=False, allow_unicode=True)
                gov_block += "```\n\n### Coverage Map summary\n```yaml\n"
                gov_block += yaml.safe_dump(cov_sum, sort_keys=False, allow_unicode=True)
                gov_block += "```\n\n### Category Spend Estimate (USD)\n```yaml\n"
                gov_block += yaml.safe_dump(spend_warn, sort_keys=False, allow_unicode=True)
                gov_block += "```\n"
                kb_ctx = (kb_ctx or "") + gov_block
            except Exception as _e:
                pass

            topic = (
                f"Director invocation at round={round_n}. "
                f"Triggers fired: {', '.join(reasons)}. "
                "Produce ONE director_decision following the v4.1 schema. "
                "Your channel_allocation MUST cite specific lines from the Capital "
                "Tracker and Coverage Map governance inputs above."
            )
            orch = Orchestrator()
            res = orch.run_sequential_workflow(
                "director_only", topic=topic, strategy_id=self.strategy,
                research_context=kb_ctx,
            )

            # Persist the raw result for audit. pipeline_controller / other
            # readers can pick this up on next cycle.
            state_dir = Path("research_state") / kb_subject
            state_dir.mkdir(parents=True, exist_ok=True)
            decisions_path = state_dir / "director_decisions.yaml"
            existing = []
            if decisions_path.exists():
                try:
                    existing = yaml.safe_load(decisions_path.read_text(encoding="utf-8")) or []
                except Exception:
                    existing = []
            entry = {
                "at": datetime.now(timezone.utc).isoformat(),
                "round": round_n,
                "triggers": reasons,
                "raw_result": res if isinstance(res, dict) else {"text": str(res)},
            }
            existing.append(entry)
            decisions_path.write_text(
                yaml.safe_dump(existing[-20:], sort_keys=False, allow_unicode=True),
                encoding="utf-8",
            )
            print(f"  director invoked (reasons={reasons}) -> {decisions_path.name}")

            # v4.1: if Director decision carries structured state/mode/allocation
            # deltas, apply them back to strategy_state.yaml immediately so
            # Pipeline_Controller's next memory_packet reflects the change.
            try:
                from .strategy_state import apply_director_delta
                rd = res if isinstance(res, dict) else {}
                dec = rd.get("director_decision") or rd
                apply_director_delta(
                    kb_subject,
                    state_delta=(dec.get("state_recommendation") or {}),
                    mode_delta=(dec.get("mode_recommendation") or {}),
                    allocation_delta=(dec.get("channel_allocation") or {}),
                    reason="director round=" + str(round_n),
                )
            except Exception as _e:
                pass

            return entry
        except Exception as e:
            print(f"  director invocation failed: {e}")
            return None

    def _maybe_invoke_director(self, round_n: int) -> dict | None:
        """Called at end of each round. Returns the director entry or None."""
        events = self._collect_recent_events()
        should, reasons = self._should_invoke_director(round_n, events)
        if not should:
            return None
        return self._invoke_director(round_n, reasons)

    def _round_memory_packet(self) -> dict:
        pkt = self.router.build_packet()
        snap = pkt.get("snapshot", {}) or {}
        hand = pkt.get("handoff", {}) or {}
        recent = self._recent_candidates(self.recent_n)
        out = {
            "champion": (snap.get("current_champion") or {}),
            "next_priority": snap.get("next_priority"),
            "do_not_repeat": hand.get("do_not_repeat"),
            "recent_candidates": recent,   # capped -> bounded packet size
        }
        # v4.1: embed strategy_state + latest director_directives so worker agents
        # operate under the same lifecycle constraints as Pipeline_Controller.
        try:
            from .strategy_state import load as load_state, read_latest_director_decision
            kb_subject = "b1_v3" if self.strategy == "b1" else self.strategy
            out["strategy_state"] = load_state(kb_subject)
            out["director_directives"] = read_latest_director_decision(kb_subject)
        except Exception:
            pass
        return out

    def _recent_candidates(self, n: int) -> list[dict]:
        path = self.pool.path
        if not path.exists():
            return []
        pool = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("candidates", [])
        return [{"experiment_id": c["experiment_id"], "promotion_status": c.get("promotion_status"),
                 "params": c.get("params")} for c in pool[-n:]]

    @staticmethod
    def _delta(m: StandardMetrics, baseline: StandardMetrics) -> dict:
        def d(a, b):
            return None if (a is None or b is None) else round(a - b, 4)
        return {
            "sharpe": d(m.sharpe, baseline.sharpe),
            "total_return": d((m.extra or {}).get("total_return"), (baseline.extra or {}).get("total_return")),
            "max_drawdown": d(m.max_drawdown, baseline.max_drawdown),
            "trades": d(m.trades, baseline.trades),
        }

    def _dry_run(self, queue: TaskQueue, max_rounds: int, per_round: int, cycle_id: str) -> dict:
        ex = RealBacktestExecutor(project_root=self.project_root)
        printed = []
        # idea tasks already queued; preview one auto round (mirror _generate_auto: over-generate + limit)
        preview_tasks = list(self._peek_queue(queue))
        if self.source in ("hybrid", "proposer", "ag2"):
            proposals = self.proposer.propose(max(per_round * 6, per_round))
            preview_tasks += self.adapter.from_proposer(proposals, priority=100, limit=per_round)
        for t in preview_tasks:
            cmd = ex.execute(t.proposal["scope"], dry_run=True).get("command")
            printed.append({"task_id": t.task_id, "priority": t.priority,
                            "params": t.proposal["scope"].get("params"), "command": cmd})
        print(f"[dry-run] cycle={cycle_id} would run {len(printed)} task(s):")
        for p in printed:
            print(f"  ({p['priority']}) {p['task_id']}  {p['params']}\n      {' '.join(p['command'])}")
        return {"cycle_id": cycle_id, "dry_run": True, "tasks": printed}

    @staticmethod
    def _peek_queue(queue: TaskQueue) -> list:
        # non-destructive peek
        return list(getattr(queue, "_q", []))
