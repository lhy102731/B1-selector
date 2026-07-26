"""snapshot_updater.py -- Phase 7 snapshot delta generator.

Generates an INCREMENTAL snapshot_delta.yaml only. It never rewrites the full
snapshot_<strategy>.yaml. Merging the delta is an explicit downstream step.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from .experiment import Experiment
from .control_plane.contracts import SideEffect
from .control_plane.sink_guard import AuthorizedPathMutation, ExecutionInvocation
from .control_plane.stores import AuthorityReader, TaskExecutionLease


class SnapshotUpdater:
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
        delta = {
            "snapshot_delta": {
                "generated_by": experiment.experiment_id,
                "append": {
                    # appended, not overwritten
                    "active_research_add": [experiment.proposal.hypothesis] if experiment.proposal.hypothesis else [],
                },
                "candidate_changes": {
                    # proposals for human review; NOT auto-applied to champion
                    "consider_for_next_priority": experiment.proposal.hypothesis,
                    "latest_auto_result": {
                        "experiment_id": experiment.experiment_id,
                        "sharpe": m.sharpe, "cagr": m.cagr, "max_drawdown": m.max_drawdown,
                        "status": experiment.registry_update["experiment"]["status"]
                        if experiment.registry_update else None,
                    },
                },
                "note": "Incremental delta only. Champion / frozen / rejected are not auto-edited.",
            }
        }
        experiment.snapshot_update = delta
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
        path = assert_safe_path(out_dir / "snapshot_delta.yaml")
        AuthorizedPathMutation(
            authority_reader=self.authority_reader or AuthorityReader(),
            repository_root=self.repository_root,
        ).authorize(
            lease or execution_lease,
            invocation or execution_invocation,
            operation="SNAPSHOT_WRITE",
            effect=SideEffect.WRITE_STAGING,
            module="research_automation.snapshot_updater",
            callable_name="SnapshotUpdater.write_delta",
            paths=(path,),
        )
        delta = experiment.snapshot_update or self.build_delta(experiment)
        out_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(delta, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return path
