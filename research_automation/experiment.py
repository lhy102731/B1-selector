"""Phase 2/3 -- unified Experiment object and lifecycle state machine.

Pure data + transition rules. No strategy logic, no I/O side effects here.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class ExperimentStatus(str, Enum):
    # Happy path
    PROPOSED = "PROPOSED"
    APPROVED = "APPROVED"
    IMPLEMENTING = "IMPLEMENTING"
    BACKTESTING = "BACKTESTING"
    REPORTING = "REPORTING"
    REGISTRY_UPDATE = "REGISTRY_UPDATE"
    SNAPSHOT_UPDATE = "SNAPSHOT_UPDATE"
    HANDOFF_UPDATE = "HANDOFF_UPDATE"
    COMPLETED = "COMPLETED"
    # Failure paths
    FAILED = "FAILED"
    ESCALATED_TO_USER = "ESCALATED_TO_USER"
    REJECTED = "REJECTED"


# Allowed transitions (mirrors experiment_schema.yaml lifecycle.transitions).
TRANSITIONS: dict[ExperimentStatus, set[ExperimentStatus]] = {
    ExperimentStatus.PROPOSED: {ExperimentStatus.APPROVED, ExperimentStatus.REJECTED, ExperimentStatus.ESCALATED_TO_USER},
    ExperimentStatus.APPROVED: {ExperimentStatus.IMPLEMENTING, ExperimentStatus.ESCALATED_TO_USER, ExperimentStatus.FAILED},
    ExperimentStatus.IMPLEMENTING: {ExperimentStatus.BACKTESTING, ExperimentStatus.ESCALATED_TO_USER, ExperimentStatus.FAILED},
    ExperimentStatus.BACKTESTING: {ExperimentStatus.REPORTING, ExperimentStatus.ESCALATED_TO_USER, ExperimentStatus.FAILED},
    ExperimentStatus.REPORTING: {ExperimentStatus.REGISTRY_UPDATE, ExperimentStatus.FAILED},
    ExperimentStatus.REGISTRY_UPDATE: {ExperimentStatus.SNAPSHOT_UPDATE, ExperimentStatus.ESCALATED_TO_USER, ExperimentStatus.FAILED},
    ExperimentStatus.SNAPSHOT_UPDATE: {ExperimentStatus.HANDOFF_UPDATE, ExperimentStatus.FAILED},
    ExperimentStatus.HANDOFF_UPDATE: {ExperimentStatus.COMPLETED, ExperimentStatus.FAILED},
    ExperimentStatus.COMPLETED: set(),
    ExperimentStatus.FAILED: set(),
    ExperimentStatus.ESCALATED_TO_USER: set(),
    ExperimentStatus.REJECTED: set(),
}

TERMINAL_STATES = {
    ExperimentStatus.COMPLETED,
    ExperimentStatus.FAILED,
    ExperimentStatus.ESCALATED_TO_USER,
    ExperimentStatus.REJECTED,
}


class LifecycleError(Exception):
    """Raised on an illegal state transition."""


@dataclass
class Proposal:
    hypothesis: str = ""
    alpha_source: str = ""
    scope: str = ""
    success_criteria: str = ""


@dataclass
class RegistryReference:
    registry_status: str | None = None
    matched_id: str | None = None
    overlap: float | None = None
    action: str | None = None


@dataclass
class StandardMetrics:
    sharpe: float | None = None
    cagr: float | None = None
    win_rate: float | None = None
    max_drawdown: float | None = None
    ndcg: float | None = None
    ic: float | None = None
    rank_ic: float | None = None
    turnover: float | None = None
    trades: int | None = None
    extra: dict = field(default_factory=dict)
    source: str = "none"


@dataclass
class Experiment:
    """Phase 2 -- the single object that flows through the automation loop."""

    experiment_id: str
    strategy: str
    parent_experiment_id: str | None = None
    proposal: Proposal = field(default_factory=Proposal)
    registry_reference: RegistryReference = field(default_factory=RegistryReference)
    changed_files: list[str] = field(default_factory=list)
    deleted_files: list[str] = field(default_factory=list)
    schema_changes: bool = False
    git_commit: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    status: ExperimentStatus = ExperimentStatus.PROPOSED
    metrics: StandardMetrics = field(default_factory=StandardMetrics)
    report_path: str | None = None
    registry_update: dict | None = None
    snapshot_update: dict | None = None
    handoff_update: dict | None = None
    escalated: bool = False
    escalation_reasons: list[str] = field(default_factory=list)
    logs: list[str] = field(default_factory=list)

    # ---- lifecycle helpers ------------------------------------------------
    def can_transition(self, to: ExperimentStatus) -> bool:
        return to in TRANSITIONS.get(self.status, set())

    def transition(self, to: ExperimentStatus, note: str = "") -> "Experiment":
        if not self.can_transition(to):
            raise LifecycleError(f"illegal transition {self.status} -> {to}")
        self.log(f"{self.status.value} -> {to.value}" + (f" ({note})" if note else ""))
        self.status = to
        return self

    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATES

    def escalate(self, reasons: list[str]) -> "Experiment":
        self.escalated = True
        self.escalation_reasons.extend(reasons)
        # ESCALATED_TO_USER is reachable from several states; force it if allowed.
        if ExperimentStatus.ESCALATED_TO_USER in TRANSITIONS.get(self.status, set()):
            self.transition(ExperimentStatus.ESCALATED_TO_USER, note="; ".join(reasons))
        else:
            self.status = ExperimentStatus.ESCALATED_TO_USER
            self.log("forced ESCALATED_TO_USER: " + "; ".join(reasons))
        return self

    def reject(self, reason: str) -> "Experiment":
        self.status = ExperimentStatus.REJECTED
        self.log(f"REJECTED: {reason}")
        return self

    def fail(self, reason: str) -> "Experiment":
        self.status = ExperimentStatus.FAILED
        self.log(f"FAILED: {reason}")
        return self

    def log(self, msg: str) -> None:
        self.logs.append(msg)

    # ---- serialization ----------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)
