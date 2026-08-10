"""C1 real-LLM dry-run driver (V3.4.2 Rollout C1).

The dry run validates model roster reachability, per-model usage metering,
data-free context assembly and budget accounting. It never commits scientific
Learning, never starts a Campaign, never touches Final Holdout, and never writes
production Authority/Operational stores. All calls use harmless ping prompts.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping, Sequence

from research_automation.control_plane.contracts import canonical_json
from research_automation.control_plane.rollout_c1_context import (
    DryRunContext,
    build_dry_run_context,
    verify_context,
)
from research_automation.control_plane.rollout_c1_report import (
    build_dry_run_report,
    serialize_report,
)
from research_automation.control_plane.rollout_c1_usage import (
    BudgetVerdict,
    DryRunBudget,
    UsageLedger,
    UsageRecord,
)
from research_automation.control_plane.rollout_providers import (
    ProviderCallResult,
    call_llm,
)

DRY_RUN_ATTEMPT_ID = "c1-attempt-001"
PLAN_VERSION = "V3.4.2-P0R2"
DEFAULT_MODELS = (
    "doubao-seed-2.0-pro",
    "glm-5.2",
    "kimi-k2.7-code",
    "minimax-m3",
    "deepseek-chat",
)

ProviderOverride = Callable[[str, str], ProviderCallResult]


@dataclass(frozen=True)
class DryRunOutcome:
    """Canonical outcome of one C1 dry run."""

    attempt_id: str
    plan_version: str
    models: tuple[str, ...]
    started_at: str
    completed_at: str
    usage_records: tuple[UsageRecord, ...]
    roster_verified: bool
    usage_verified: bool
    context_verified: bool
    budget_verified: bool
    budget_detail: str
    no_learning_commit: bool
    no_real_campaign_or_holdout: bool
    failures: tuple[str, ...]
    pass_: bool
    final_state_digest: str

    def to_payload(self) -> dict:
        raise NotImplementedError("C1 driver slice pending implementation")


def run_c1_dry_run(
    *,
    models: Sequence[str] = DEFAULT_MODELS,
    cycles: int = 1,
    max_tokens: int = 16,
    timeout_ms: int = 30_000,
    budget: DryRunBudget | None = None,
    provider_override: ProviderOverride | None = None,
    now: datetime | None = None,
) -> DryRunOutcome:
    """Run one C1 dry run and return the canonical outcome."""
    raise NotImplementedError("C1 driver slice pending implementation")


def serialize_outcome(outcome: DryRunOutcome) -> str:
    """Return the canonical JSON serialization of a dry-run outcome."""
    raise NotImplementedError("C1 driver slice pending implementation")
