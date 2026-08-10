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


def _iso_z(dt: datetime) -> str:
    """Return a datetime as an ISO-8601 string with Z suffix."""
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


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
        """Return the canonical payload dict with exactly the specified keys."""
        return {
            "attempt_id": self.attempt_id,
            "plan_version": self.plan_version,
            "models": list(self.models),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "usage_records": [
                {
                    "model": r.model,
                    "status": r.status,
                    "input_tokens": r.input_tokens,
                    "output_tokens": r.output_tokens,
                    "total_tokens": r.total_tokens,
                }
                for r in self.usage_records
            ],
            "roster_verified": self.roster_verified,
            "usage_verified": self.usage_verified,
            "context_verified": self.context_verified,
            "budget_verified": self.budget_verified,
            "budget_detail": self.budget_detail,
            "no_learning_commit": self.no_learning_commit,
            "no_real_campaign_or_holdout": self.no_real_campaign_or_holdout,
            "failures": list(self.failures),
            "pass": self.pass_,
            "final_state_digest": self.final_state_digest,
        }


def _deduplicate_preserving_order(models: Sequence[str]) -> list[str]:
    """Remove duplicates while preserving first-seen order."""
    seen: set[str] = set()
    result: list[str] = []
    for m in models:
        if m not in seen:
            seen.add(m)
            result.append(m)
    return result


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
    # --- Validation -------------------------------------------------------
    if cycles < 1:
        raise ValueError("cycles must be >= 1")
    if max_tokens <= 0:
        raise ValueError("max_tokens must be > 0")

    # --- Timestamp --------------------------------------------------------
    ts = now or datetime.now(timezone.utc)
    started_at = _iso_z(ts)

    # --- Deduplicate models -----------------------------------------------
    unique_models = _deduplicate_preserving_order(models)

    # --- Run every model × every cycle ------------------------------------
    ledger = UsageLedger()
    context_verified = True

    for cycle_index in range(1, cycles + 1):
        for model in unique_models:
            # Context assembly + verification
            ctx = build_dry_run_context(model, cycle_index)
            ctx_ok = verify_context(
                ctx, expected_model=model, expected_cycle=cycle_index
            )
            if not ctx_ok:
                context_verified = False

            # Provider call
            if provider_override is not None:
                result = provider_override(model, ctx.prompt)
            else:
                result = call_llm(
                    model,
                    ctx.prompt,
                    max_tokens=max_tokens,
                    timeout_ms=timeout_ms,
                )

            # Record usage
            record = UsageRecord(
                model=result.model,
                status=result.status,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                total_tokens=result.total_tokens,
            )
            ledger.record(record)

    # --- Verdicts ---------------------------------------------------------
    verdict = ledger.verify_budget(budget or DryRunBudget())
    all_records = ledger.records()

    roster_verified = tuple(unique_models) == DEFAULT_MODELS

    usage_verified = (
        all(r.status == "ok" for r in all_records)
        and ledger.total_tokens() == sum(r.total_tokens for r in all_records)
    )

    failures = [r.model for r in all_records if r.status != "ok"]

    pass_ = (
        roster_verified
        and usage_verified
        and context_verified
        and verdict.passed
        and not failures
    )

    # --- Build outcome (without digest first) -----------------------------
    completed_at = _iso_z(ts)

    outcome = DryRunOutcome(
        attempt_id=DRY_RUN_ATTEMPT_ID,
        plan_version=PLAN_VERSION,
        models=tuple(unique_models),
        started_at=started_at,
        completed_at=completed_at,
        usage_records=tuple(all_records),
        roster_verified=roster_verified,
        usage_verified=usage_verified,
        context_verified=context_verified,
        budget_verified=verdict.passed,
        budget_detail=verdict.detail,
        no_learning_commit=True,
        no_real_campaign_or_holdout=True,
        failures=tuple(failures),
        pass_=pass_,
        final_state_digest="",  # placeholder — replaced below
    )

    # --- Compute stable digest --------------------------------------------
    payload_without_digest = outcome.to_payload()
    payload_without_digest.pop("final_state_digest", None)
    digest_input = (
        b"control_plane.c1_dry_run.v1\0"
        + canonical_json(payload_without_digest).encode("utf-8")
    )
    digest = hashlib.sha256(digest_input).hexdigest()

    # Return the outcome with the correct digest
    return DryRunOutcome(
        attempt_id=DRY_RUN_ATTEMPT_ID,
        plan_version=PLAN_VERSION,
        models=tuple(unique_models),
        started_at=started_at,
        completed_at=completed_at,
        usage_records=tuple(all_records),
        roster_verified=roster_verified,
        usage_verified=usage_verified,
        context_verified=context_verified,
        budget_verified=verdict.passed,
        budget_detail=verdict.detail,
        no_learning_commit=True,
        no_real_campaign_or_holdout=True,
        failures=tuple(failures),
        pass_=pass_,
        final_state_digest=digest,
    )


def serialize_outcome(outcome: DryRunOutcome) -> str:
    """Return the canonical JSON serialization of a dry-run outcome."""
    return serialize_report(build_dry_run_report(outcome.to_payload()))
