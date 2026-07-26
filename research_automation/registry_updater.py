"""registry_updater.py -- Phase 6 registry auto-classifier + entry generator.

Reuses the Research OS RegistryGate (single source of the taxonomy) to classify a
hypothesis against the strategy's registry, then generates a registry_entry.yaml
DELTA into the staging dir. It does NOT append to the real registry file
by default. Explicit project-side merge is available through ``merge_entry`` so
completed results can be absorbed without writing to the source KBase.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from ag2_research.orchestrator import MemoryRouter  # reuse Research OS memory + gate
from .experiment import Experiment, RegistryReference
from .control_plane.contracts import SideEffect
from .control_plane.sink_guard import (
    AuthorizedPathMutation,
    ExecutionInvocation,
)
from .control_plane.stores import AuthorityReader, TaskExecutionLease


class RegistryMergeError(RuntimeError):
    """Raised when an explicit registry merge would corrupt the project ledger."""


class RegistryUpdater:
    def __init__(
        self,
        strategy_id: str = "b1",
        router: MemoryRouter | None = None,
        *,
        authority_reader: AuthorityReader | None = None,
        repository_root: str | Path | None = None,
    ):
        self.router = router or MemoryRouter(strategy_id)
        self.authority_reader = authority_reader
        self.repository_root = Path(repository_root or Path(__file__).resolve().parent.parent)

    def _authorize_mutation(
        self,
        *,
        lease: TaskExecutionLease | None,
        invocation: ExecutionInvocation | None,
        effect: SideEffect,
        callable_name: str,
        path: Path,
    ) -> None:
        AuthorizedPathMutation(
            authority_reader=self.authority_reader or AuthorityReader(),
            repository_root=self.repository_root,
        ).authorize(
            lease,
            invocation,
            operation="REGISTRY_WRITE",
            effect=effect,
            module="research_automation.registry_updater",
            callable_name=callable_name,
            paths=(path,),
        )

    # ---- Phase 6 classification (pre-flight gate) -------------------------
    def classify(self, hypothesis: str) -> RegistryReference:
        v = self.router.registry_gate.classify(hypothesis)
        return RegistryReference(
            registry_status=v["registry_status"],
            matched_id=v["matched_id"],
            overlap=v["overlap"],
            action=v["action"],
        )

    def is_conflict(self, ref: RegistryReference) -> bool:
        """duplicate / failed / verified are conflicts (Research OS reject set)."""
        return ref.registry_status in ("duplicate", "failed", "verified")

    # ---- entry generation (post-backtest) ---------------------------------
    def build_entry(self, experiment: Experiment) -> dict:
        m = experiment.metrics
        status = self._status_from_metrics(experiment, m)
        next_id = self._next_id()
        entry = {
            "id": next_id,
            "title": (experiment.proposal.hypothesis or "")[:120],
            "short_result": self._short_result(experiment, m),
            "evidence_source": [
                experiment.report_path or "",
                f"git:{experiment.git_commit}",
            ],
            "status": status,
            "stage_tag": "auto_generated",
            "reopen_condition": "Reopen only with account-level, phase-aware revalidation under unchanged 口径.",
        }
        experiment.registry_update = {"experiment": entry}
        return entry

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
        path = assert_safe_path(out_dir / "registry_entry.yaml")
        self._authorize_mutation(
            lease=lease or execution_lease,
            invocation=invocation or execution_invocation,
            effect=SideEffect.WRITE_STAGING,
            callable_name="RegistryUpdater.write_delta",
            path=path,
        )
        entry = experiment.registry_update or {"experiment": self.build_entry(experiment)}
        out_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(entry, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return path

    # ---- explicit project-side merge -------------------------------------
    def merge_entry(
        self,
        entry: dict,
        registry_path: str | Path | None = None,
        *,
        lease: TaskExecutionLease | None = None,
        invocation: ExecutionInvocation | None = None,
        execution_lease: TaskExecutionLease | None = None,
        execution_invocation: ExecutionInvocation | None = None,
    ) -> Path:
        """Append a reviewed entry to registry_<strategy>_v*.yaml.

        This is intentionally separate from ``write_delta``: automation can stage
        records safely, while an explicit caller can absorb results into the
        project registry to prevent duplicate proposals. It never writes to
        ``D:\\KBase`` and rejects duplicate ids/titles.
        """
        if not isinstance(entry, dict) or not entry.get("id"):
            raise RegistryMergeError("registry entry must be a mapping with an id")
        path = Path(registry_path) if registry_path else self._registry_path()
        if not path:
            raise RegistryMergeError(f"registry file for strategy '{self.router.strategy_id}' not found")
        self._authorize_mutation(
            lease=lease or execution_lease,
            invocation=invocation or execution_invocation,
            effect=SideEffect.WRITE_PRODUCTION_CONFIG,
            callable_name="RegistryUpdater.merge_entry",
            path=path,
        )
        data = self._load_registry_file(path)
        registry = data.setdefault("registry", {})
        experiments = registry.setdefault("experiments", [])
        if not isinstance(experiments, list):
            raise RegistryMergeError("registry.experiments must be a list")

        entry_id = str(entry.get("id"))
        entry_title = str(entry.get("title") or "").strip().lower()
        for existing in experiments:
            if not isinstance(existing, dict):
                continue
            if str(existing.get("id")) == entry_id:
                raise RegistryMergeError(f"duplicate registry id '{entry_id}'")
            if entry_title and str(existing.get("title") or "").strip().lower() == entry_title:
                raise RegistryMergeError(f"duplicate registry title '{entry.get('title')}'")

        experiments.append(entry)
        path.write_text(
            yaml.safe_dump(data, allow_unicode=True, sort_keys=False, width=100),
            encoding="utf-8",
        )
        self.router.registry_entries = experiments
        self.router.registry_gate.entries = experiments
        return path

    def merge_delta(
        self,
        delta_path: str | Path,
        registry_path: str | Path | None = None,
        **authority: object,
    ) -> Path:
        payload = yaml.safe_load(Path(delta_path).read_text(encoding="utf-8")) or {}
        entry = payload.get("experiment") if isinstance(payload, dict) else None
        return self.merge_entry(entry, registry_path=registry_path, **authority)

    # ---- helpers ----------------------------------------------------------
    def _registry_path(self) -> Path | None:
        return (
            self.router._latest(f"registry_{self.router.strategy_id}_v*.yaml")
            or self.router._latest(f"registry_{self.router.strategy_id}.yaml")
        )

    @staticmethod
    def _load_registry_file(path: Path) -> dict:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise RegistryMergeError(f"registry file '{path}' must contain a mapping")
        return data

    def _status_from_metrics(self, experiment: Experiment, m) -> str:
        # Conservative auto-status: not a champion claim, just a record.
        if experiment.escalated:
            return "OPEN"
        if m.sharpe is None and m.cagr is None:
            return "OPEN"
        # crude heuristic: positive risk-adjusted -> PARTIAL pending human verify
        if (m.sharpe or 0) > 0 and (m.cagr or 0) > 0:
            return "PARTIAL"
        return "FAILED"

    def _short_result(self, experiment: Experiment, m) -> str:
        return (f"auto: sharpe={m.sharpe}, cagr={m.cagr}, mdd={m.max_drawdown}, "
                f"win={m.win_rate}, trades={m.trades}; needs human verification.")

    def _next_id(self) -> str:
        entries = self.router.registry_entries or []
        prefix = f"{self.router.strategy_id}-exp-"
        nums = []
        for e in entries:
            eid = str(e.get("id", ""))
            if eid.startswith(prefix):
                tail = eid[len(prefix):]
                if tail.isdigit():
                    nums.append(int(tail))
        nxt = (max(nums) + 1) if nums else 1
        return f"{prefix}{nxt:03d}"
