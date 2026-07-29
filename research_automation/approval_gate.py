"""approval_gate.py -- Phase 9 Human Approval Gate.

Pure-function risk checks. Returns the list of reasons the experiment must be
paused for a human. The controller turns any non-empty result into
status = ESCALATED_TO_USER.
"""
from __future__ import annotations

from dataclasses import dataclass

from .experiment import Experiment, RegistryReference
from .experiment_runner import BacktestResult


@dataclass
class ApprovalDecision:
    escalate: bool
    reasons: list[str]


class ApprovalGate:
    def __init__(self, max_changed_files: int = 5):
        self.max_changed_files = max_changed_files

    # ---- pre-implementation (registry conflict) ---------------------------
    def check_registry(self, ref: RegistryReference) -> ApprovalDecision:
        reasons = []
        if ref.registry_status in ("duplicate", "failed", "verified"):
            reasons.append(f"registry_conflict: status={ref.registry_status} (matched {ref.matched_id})")
        return ApprovalDecision(bool(reasons), reasons)

    # ---- post-code-change -------------------------------------------------
    def check_code_change(self, experiment: Experiment) -> ApprovalDecision:
        reasons = []
        if len(experiment.changed_files) > self.max_changed_files:
            reasons.append(f"changed_files={len(experiment.changed_files)} > {self.max_changed_files}")
        if experiment.deleted_files:
            reasons.append(f"deleted_files={experiment.deleted_files}")
        if experiment.schema_changes:
            reasons.append("schema_changes=true (DB/schema migration)")
        return ApprovalDecision(bool(reasons), reasons)

    # ---- post-backtest ----------------------------------------------------
    def check_backtest(self, result: BacktestResult, experiment: Experiment) -> ApprovalDecision:
        reasons = []
        if result.anomaly:
            reasons.append("backtest_anomaly flagged by executor")
        m = experiment.metrics
        # crude out-of-range guard (e.g. >50x total return or impossible win rate)
        if m.cagr is not None and m.cagr > 50:
            reasons.append(f"backtest_anomaly: implausible return {m.cagr}")
        if m.win_rate is not None and not (0 <= m.win_rate <= 1):
            reasons.append(f"backtest_anomaly: win_rate out of range {m.win_rate}")
        return ApprovalDecision(bool(reasons), reasons)

    # ---- memory conflict (snapshot/handoff vs frozen directions) ----------
    def check_memory_conflict(self, experiment: Experiment, frozen_directions: list | None) -> ApprovalDecision:
        reasons = []
        frozen = frozen_directions or []
        hypo = (experiment.proposal.hypothesis or "").lower()
        for fd in frozen:
            name = (fd.get("direction") if isinstance(fd, dict) else str(fd)) or ""
            key = name.lower().strip()
            # if the hypothesis proposes removing/weakening a frozen-as-core direction
            if key and key in hypo and any(w in hypo for w in ("remove", "disable", "drop", "weaken", "去掉", "移除", "删除")):
                reasons.append(f"memory_conflict: touches frozen direction '{name}'")
        return ApprovalDecision(bool(reasons), reasons)
