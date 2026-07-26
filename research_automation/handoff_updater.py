"""handoff_updater.py -- Phase 8 handoff delta generator.

Updates the four rolling fields requested by the spec:
  current_best_hypothesis, current_blockers, next_experiments, latest_results
as an INCREMENTAL delta. Does not rewrite the full handoff file.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from .experiment import Experiment
from .control_plane.contracts import SideEffect
from .control_plane.sink_guard import AuthorizedPathMutation, ExecutionInvocation
from .control_plane.stores import AuthorityReader, TaskExecutionLease


class HandoffUpdater:
    def __init__(
        self,
        *,
        authority_reader: AuthorityReader | None = None,
        repository_root: str | Path | None = None,
    ) -> None:
        self.authority_reader = authority_reader
        self.repository_root = Path(repository_root or Path(__file__).resolve().parent.parent)

    def build_delta(self, experiment: Experiment) -> dict:
        m = experiment.metrics
        blockers = []
        if experiment.escalated:
            blockers = list(experiment.escalation_reasons)

        delta = {
            "handoff_delta": {
                "generated_by": experiment.experiment_id,
                "current_best_hypothesis": experiment.proposal.hypothesis or None,
                "current_blockers": blockers,
                "next_experiments": self._next_experiments(experiment),
                "latest_results": {
                    "experiment_id": experiment.experiment_id,
                    "status": experiment.status.value,
                    "sharpe": m.sharpe,
                    "cagr": m.cagr,
                    "max_drawdown": m.max_drawdown,
                    "report_path": experiment.report_path,
                },
                "note": "Incremental delta only; do_not_repeat / escalation_conditions are not auto-edited.",
            }
        }
        experiment.handoff_update = delta
        return delta

    def write_delta(
        self,
        experiment: Experiment,
        out_dir: Path,
        *,
        lease: TaskExecutionLease | None = None,
        invocation: ExecutionInvocation | None = None,
        execution_lease: TaskExecutionLease | None = None,
        execution_invocation: ExecutionInvocation | None = None,
    ) -> Path:
        from .safety import assert_safe_path
        path = assert_safe_path(out_dir / "handoff_delta.yaml")
        AuthorizedPathMutation(
            authority_reader=self.authority_reader or AuthorityReader(),
            repository_root=self.repository_root,
        ).authorize(
            lease or execution_lease,
            invocation or execution_invocation,
            operation="HANDOFF_WRITE",
            effect=SideEffect.WRITE_STAGING,
            module="research_automation.handoff_updater",
            callable_name="HandoffUpdater.write_delta",
            paths=(path,),
        )
        delta = experiment.handoff_update or self.build_delta(experiment)
        out_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(delta, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return path

    @staticmethod
    def _next_experiments(experiment: Experiment) -> list[str]:
        ref = experiment.registry_reference
        if ref.action == "modify":
            return [f"Differentiate vs {ref.matched_id} before re-testing (partial_overlap)."]
        if experiment.status.value == "COMPLETED":
            return ["Human-verify the auto result at account level, then decide promotion."]
        return []
