"""C1 dry-run usage ledger and budget verdict."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class UsageRecord:
    model: str
    status: str
    input_tokens: int
    output_tokens: int
    total_tokens: int


@dataclass(frozen=True)
class DryRunBudget:
    currency: str = "USD"
    max_total_tokens: int = 4096
    max_tokens_per_model: int = 2048
    max_total_cost_usd: str = "1.00"


@dataclass(frozen=True)
class BudgetVerdict:
    passed: bool
    detail: str


@dataclass
class UsageLedger:
    _records: list[UsageRecord] = field(default_factory=list)

    def record(self, record: UsageRecord) -> None:
        """Append a usage record. Rejects None and negative token counts."""
        if record is None:
            raise ValueError("UsageRecord must not be None")
        if record.input_tokens < 0:
            raise ValueError(
                f"input_tokens must be non-negative, got {record.input_tokens}"
            )
        if record.output_tokens < 0:
            raise ValueError(
                f"output_tokens must be non-negative, got {record.output_tokens}"
            )
        if record.total_tokens < 0:
            raise ValueError(
                f"total_tokens must be non-negative, got {record.total_tokens}"
            )
        self._records.append(record)

    def records(self) -> list[UsageRecord]:
        """Return a copy of the records in insertion order."""
        return list(self._records)

    def total_input_tokens(self) -> int:
        return sum(r.input_tokens for r in self._records)

    def total_output_tokens(self) -> int:
        return sum(r.output_tokens for r in self._records)

    def total_tokens(self) -> int:
        return sum(r.total_tokens for r in self._records)

    def verify_budget(self, budget: DryRunBudget) -> BudgetVerdict:
        """Return BudgetVerdict(passed, detail) for the ledger against budget.

        Fails when any record's total_tokens exceeds max_tokens_per_model,
        or when the ledger grand total exceeds max_total_tokens.
        """
        over_per_model = [
            r.model for r in self._records
            if r.total_tokens > budget.max_tokens_per_model
        ]
        grand_total = self.total_tokens()
        over_total = grand_total > budget.max_total_tokens

        if not over_per_model and not over_total:
            detail = (
                f"budget ok: total={grand_total}/{budget.max_total_tokens} "
                f"max_per_model={budget.max_tokens_per_model} "
                f"currency={budget.currency}"
            )
            return BudgetVerdict(passed=True, detail=detail)

        reasons: list[str] = []
        if over_per_model:
            reasons.append(
                f"per-model limit exceeded by: {', '.join(over_per_model)}"
                f" (max {budget.max_tokens_per_model})"
            )
        if over_total:
            reasons.append(
                f"total tokens {grand_total} exceeds max_total_tokens "
                f"{budget.max_total_tokens}"
            )
        detail = f"budget failed ({budget.currency}): " + "; ".join(reasons)
        return BudgetVerdict(passed=False, detail=detail)
