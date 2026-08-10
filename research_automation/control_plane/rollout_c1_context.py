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
    prompt = (
        "Control-plane C1 dry run cycle "
        + str(cycle_index)
        + " for model "
        + model
        + ". Reply with exactly: DRY_RUN_OK"
    )
    return DryRunContext(
        model=model,
        cycle_index=cycle_index,
        prompt=prompt,
        prompt_chars=len(prompt),
        prompt_tokens_estimate=estimate_tokens(prompt),
    )


def estimate_tokens(text: str) -> int:
    if not text:
        raise ValueError("text must be non-empty")
    return max(1, (len(text) + 3) // 4)


def verify_context(
    context: DryRunContext,
    *,
    expected_model: str,
    expected_cycle: int = 1,
) -> bool:
    return (
        context.model == expected_model
        and context.cycle_index == expected_cycle
        and is_data_free_prompt(context.prompt)
        and estimate_tokens(context.prompt) == context.prompt_tokens_estimate
    )


def is_data_free_prompt(prompt: str) -> bool:
    if not prompt:
        return False
    if "DRY_RUN" not in prompt:
        return False
    forbidden = ("kbase", ".csv", ".parquet", "strategy", "stock", "data/")
    lowered = prompt.lower()
    return not any(substring in lowered for substring in forbidden)
