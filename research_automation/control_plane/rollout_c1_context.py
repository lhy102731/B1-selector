"""C1 data-free dry-run context assembly and token estimation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DryRunContext:
    model: str
    cycle_index: int
    prompt: str
    prompt_chars: int
    prompt_tokens_estimate: int


def build_dry_run_context(model: str, cycle_index: int = 1) -> DryRunContext:
    raise NotImplementedError("C1 context slice pending implementation")


def estimate_tokens(text: str) -> int:
    raise NotImplementedError("C1 context slice pending implementation")


def verify_context(
    context: DryRunContext,
    *,
    expected_model: str,
    expected_cycle: int = 1,
) -> bool:
    raise NotImplementedError("C1 context slice pending implementation")


def is_data_free_prompt(prompt: str) -> bool:
    raise NotImplementedError("C1 context slice pending implementation")
