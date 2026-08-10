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
        raise NotImplementedError("C1 usage slice pending implementation")

    def records(self) -> list[UsageRecord]:
        raise NotImplementedError("C1 usage slice pending implementation")

    def total_input_tokens(self) -> int:
        raise NotImplementedError("C1 usage slice pending implementation")

    def total_output_tokens(self) -> int:
        raise NotImplementedError("C1 usage slice pending implementation")

    def total_tokens(self) -> int:
        raise NotImplementedError("C1 usage slice pending implementation")

    def verify_budget(self, budget: DryRunBudget) -> BudgetVerdict:
        raise NotImplementedError("C1 usage slice pending implementation")
