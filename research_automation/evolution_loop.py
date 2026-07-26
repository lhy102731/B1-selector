"""evolution_loop.py — Automatic generation iterator (Evolution Loop).

Drives a fixed sequence of height_ratio values through the full code-experiment
pipeline: workspace → code change → backtest → metrics → lineage → champion.

Phase 3A: ONLY deterministic (no LLM, no ClaudeCodeExecutor). The height_ratio
sequence is hardcoded per spec [0.67, 0.80, 1.00, 1.20, 1.50] to validate the
evolution framework itself before introducing LLM-driven proposal generation.

All artifacts land under the given output root (inside safety.py's SAFE_WRITE_ROOTS).
Production code is never touched.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .experiment import Experiment, ExperimentStatus, Proposal, StandardMetrics
from .experiment_runner import (
    RealBacktestExecutor, DeterministicBrickCodeExecutor,
    _BRICK_SPEC, generate_experiment_task_md,
)
from .lineage import write_lineage_json, build_lineage_tree
from .report_generator import ReportGenerator
from .result_parser import BacktestResultParser
from .safety import assert_safe_path, output_root
from .workspace_manager import WorkspaceManager
from .control_plane.contracts import SideEffect
from .control_plane.sink_guard import ExecutionAuthorizationError, ExecutionInvocation, ExecutionSinkGuard
from .control_plane.stores import AuthorityReader, TaskExecutionLease


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Height-ratio sequence for controlled evolution (spec)
_HEIGHT_SEQUENCE = [0.67, 0.80, 1.00, 1.20, 1.50]


@dataclass
class EvolutionResult:
    best_experiment_id: str | None = None
    generation_count: int = 0
    lineage_tree: dict = field(default_factory=dict)
    champion_metrics: dict = field(default_factory=dict)
    generations: list[dict] = field(default_factory=list)
    summary_path: str | None = None


class EvolutionLoop:
    """Deterministic evolution loop over height_ratio values.

    Each generation:
      1. Creates a child experiment (parent → previous gen).
      2. Writes code_change (modify_constant height_ratio → value).
      3. Runs DeterministicBrickCodeExecutor → RealBacktestExecutor → ReportGenerator.
      4. Parses metrics, records lineage, selects champion (max total_return).
    """

    def __init__(self, project_root: str | Path | None = None,
                 backtest_window: dict | None = None):
        self.project_root = Path(project_root) if project_root else Path(__file__).resolve().parent.parent
        self.window = backtest_window or {"start": "2024-01-01", "end": "2024-03-31",
                                          "strategy": "BRICK", "params": {"entry_ma_source": "t0"}}
        self.ws_manager = WorkspaceManager(workspace_root=output_root() / "_evolution",
                                           project_root=self.project_root)
        self.code_exec = DeterministicBrickCodeExecutor()
        self.executor = RealBacktestExecutor(project_root=self.project_root,
                                             workspace_mode=True,
                                             workspace_manager=self.ws_manager)
        self.parser = BacktestResultParser()
        self.reporter = ReportGenerator()

    # ---- public API -------------------------------------------------------
    def run_generation(
        self,
        root_experiment: Experiment,
        max_generations: int = 5,
        *,
        lease: TaskExecutionLease | None = None,
        invocation: ExecutionInvocation | None = None,
        execution_lease: TaskExecutionLease | None = None,
        execution_invocation: ExecutionInvocation | None = None,
        authority_reader: AuthorityReader | None = None,
        repository_root: str | Path | None = None,
    ) -> EvolutionResult:
        """Run the evolution loop starting from root_experiment.

        root_experiment's generation is treated as gen-0. Subsequent children
        use the height_ratio sequence *after* the initial value.
        """
        lease = lease if lease is not None else execution_lease
        invocation = invocation if invocation is not None else execution_invocation
        evolution_root = (output_root() / "_evolution").resolve()
        try:
            permit = ExecutionSinkGuard(
                authority_reader=authority_reader or AuthorityReader(),
                repository_root=repository_root or self.project_root,
            ).authorize(lease, invocation)
            if (
                permit.operation != "EVOLUTION"
                or permit.effect is not SideEffect.RUN_RESEARCH
            ):
                raise ExecutionAuthorizationError(
                    "evolution loop requires a RUN_RESEARCH EVOLUTION intent"
                )
            if not isinstance(invocation, ExecutionInvocation) or (
                invocation.runner.module != "research_automation.evolution_loop"
                or invocation.runner.callable_name != "EvolutionLoop.run_generation"
            ):
                raise ExecutionAuthorizationError(
                    "evolution loop entry identity is invalid"
                )
            if evolution_root not in permit.resource_paths:
                raise ExecutionAuthorizationError(
                    "evolution output root is not bound by the execution intent"
                )
        except (ExecutionAuthorizationError, OSError, ValueError) as error:
            raise ExecutionAuthorizationError(
                f"EvolutionLoop authority rejected: {error}"
            ) from error

        n_gens = min(max_generations, len(_HEIGHT_SEQUENCE))
        all_exps: list[Experiment] = [root_experiment]
        gen_records: list[dict] = []
        champion_id = root_experiment.experiment_id
        champion_metrics: dict = {}
        champion_score = float("-inf")

        # Scope for backtest execution (same for all generations)
        scope = dict(self.window)

        for gen_idx in range(n_gens):
            hr = _HEIGHT_SEQUENCE[gen_idx]
            parent = all_exps[-1]
            eid = f"{root_experiment.experiment_id}-gen{gen_idx:02d}"
            gen_dir = output_root() / "_evolution" / eid
            gen_dir.mkdir(parents=True, exist_ok=True)

            print(f"\n{'='*60}")
            print(f"Generation {gen_idx}: height_ratio = {hr}")
            print(f"{'='*60}")

            # --- 1. create child experiment ---
            old_val = _HEIGHT_SEQUENCE[gen_idx - 1] if gen_idx > 0 else "2.0 / 3.0"
            exp = Experiment(
                experiment_id=eid,
                strategy=root_experiment.strategy,
                parent_experiment_id=parent.experiment_id,
                proposal=Proposal(
                    hypothesis=f"Evolution gen {gen_idx}: height_ratio {old_val} -> {hr}",
                    scope={"code_change": {"change_type": "modify_constant",
                                           "file": "strategy/brick_chart_strategy.py",
                                           "symbol": "height_ratio",
                                           "value": hr,
                                           "old_value": old_val}},
                ),
                start_time=_now_iso(),
            )
            print(f"  experiment: {eid}  parent: {exp.parent_experiment_id}")

            # --- 2. workspace ---
            ws = self.ws_manager.create_workspace(exp, _BRICK_SPEC)
            print(f"  workspace: {ws}")

            # --- 3. deterministic code change ---
            task_path = gen_dir / "experiment_task.md"
            generate_experiment_task_md(exp, gen_dir)
            cc = self.code_exec.apply(task_path, ws, experiment=exp)
            if not cc.ok:
                print(f"  FAIL: code change failed: {cc.error}")
                exp.fail(cc.error or "code change failed")
                all_exps.append(exp)
                gen_records.append(self._record(exp, gen_idx, hr, metrics=None))
                continue
            exp.changed_files = cc.changed_files
            print(f"  code_change: ok  changed={cc.changed_files}")

            # --- 4. backtest ---
            result_dir = gen_dir / "outputs"
            print(f"  backtest: running...")
            res = self.executor.execute(scope, result_dir=result_dir, workspace_path=ws)
            if not res.get("success"):
                print(f"  FAIL: backtest failed: {res.get('error')}")
                exp.fail(res.get("error") or "backtest failed")
                all_exps.append(exp)
                gen_records.append(self._record(exp, gen_idx, hr, metrics=None))
                continue

            # --- 5. parse metrics ---
            metrics = self.parser.parse(result_dir)
            exp.metrics = metrics
            exp.status = ExperimentStatus.COMPLETED
            exp.end_time = _now_iso()
            total_ret = (metrics.extra or {}).get("total_return", 0) if metrics else 0
            score = total_ret
            print(f"  metrics: trades={metrics.trades} wr={metrics.win_rate} return={total_ret} score={score:.2f}")

            # --- 6. report + lineage ---
            self.reporter.generate(exp, gen_dir)
            write_lineage_json(exp, gen_dir, candidate_pool=all_exps)

            all_exps.append(exp)
            gen_records.append(self._record(exp, gen_idx, hr, metrics=metrics))

            # --- 7. champion check ---
            if score > champion_score:
                champion_score = score
                champion_id = eid
                champion_metrics = self._metrics_dict(metrics)

            print(f"  champion so far: {champion_id} (score={champion_score:.2f})")

        # --- final outputs ---
        evolution_dir = output_root() / "_evolution"
        evolution_dir.mkdir(parents=True, exist_ok=True)

        # lineage_tree.json
        nodes = [{"id": e.experiment_id, "parent": e.parent_experiment_id}
                 for e in all_exps]
        tree = {"root": root_experiment.experiment_id, "nodes": nodes}
        tree_path = evolution_dir / "lineage_tree.json"
        assert_safe_path(tree_path)
        tree_path.write_text(json.dumps(tree, ensure_ascii=False, indent=2), encoding="utf-8")

        # champion.json
        champ = {"best_experiment_id": champion_id, "score": champion_score,
                 "metrics": champion_metrics, "generation_count": n_gens}
        champ_path = evolution_dir / "champion.json"
        assert_safe_path(champ_path)
        champ_path.write_text(json.dumps(champ, ensure_ascii=False, indent=2), encoding="utf-8")

        # evolution_summary.md
        summary_path = self._write_summary(evolution_dir, gen_records, champion_id, champion_score)
        print(f"\n  champion: {champion_id} score={champion_score:.2f}")
        print(f"  summary: {summary_path}")

        return EvolutionResult(
            best_experiment_id=champion_id,
            generation_count=n_gens,
            lineage_tree=tree,
            champion_metrics=champion_metrics,
            generations=gen_records,
            summary_path=str(summary_path),
        )

    # ---- helpers ----------------------------------------------------------
    @staticmethod
    def _record(exp: Experiment, gen: int, height_ratio: float,
                metrics) -> dict:
        m = EvolutionLoop._metrics_dict(metrics)
        return {
            "generation": gen,
            "experiment_id": exp.experiment_id,
            "parent_id": exp.parent_experiment_id,
            "height_ratio": height_ratio,
            "metrics": m,
            "score": m.get("total_return", 0),
            "status": exp.status.value,
        }

    @staticmethod
    def _metrics_dict(metrics) -> dict:
        if metrics is None:
            return {}
        return {
            "trades": getattr(metrics, "trades", None),
            "win_rate": getattr(metrics, "win_rate", None),
            "total_return": (getattr(metrics, "extra", None) or {}).get("total_return"),
            "sharpe": getattr(metrics, "sharpe", None),
            "max_drawdown": getattr(metrics, "max_drawdown", None),
        }

    def _write_summary(self, out_dir: Path, records: list[dict],
                       champion_id: str, champion_score: float) -> Path:
        lines = [
            "# Evolution Summary",
            "",
            f"- **Champion**: `{champion_id}` (score={champion_score:.2f})",
            f"- **Generations**: {len(records)}",
            "",
            "| Gen | Experiment | height_ratio | trades | wr | return | score |",
            "|---|---|---|---|---|---|---|",
        ]
        for r in records:
            m = r.get("metrics", {})
            lines.append(
                f"| {r['generation']} | {r['experiment_id']} | {r['height_ratio']} "
                f"| {m.get('trades','-')} | {m.get('win_rate','-')} "
                f"| {m.get('total_return','-')} | {r.get('score','-'):.2f} |"
            )
        lines.extend([
            "",
            "---",
            f"Generated: {_now_iso()}",
        ])
        path = out_dir / "evolution_summary.md"
        assert_safe_path(path)
        path.write_text("\n".join(lines), encoding="utf-8")
        return path
