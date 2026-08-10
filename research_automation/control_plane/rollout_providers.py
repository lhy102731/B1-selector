"""C1 LLM provider adapters (relay + direct) with usage parsing.

The relay exposes an Anthropic Messages compatible endpoint at
http://127.0.0.1:18080 which routes Volcano Coding Plan models. DeepSeek is
called directly through its OpenAI-compatible API. Every failure is classified
into a status string and never raises into the C1 driver.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

RELAY_BASE = "http://127.0.0.1:18080"
DEEPSEEK_BASE = "https://api.deepseek.com"


@dataclass(frozen=True)
class ProviderSpec:
    model: str
    provider: str  # "volcano_relay" | "deepseek_direct"
    base_url: str
    api_key_env: str


REGISTRY: Mapping[str, ProviderSpec] = {
    "doubao-seed-2.0-pro": ProviderSpec(
        "doubao-seed-2.0-pro", "volcano_relay", RELAY_BASE, "AG2_DouBao_API_KEY"
    ),
    "glm-5.2": ProviderSpec("glm-5.2", "volcano_relay", RELAY_BASE, "AG2_ZHIPU_API_KEY"),
    "kimi-k2.7-code": ProviderSpec(
        "kimi-k2.7-code", "volcano_relay", RELAY_BASE, "AG2_Kimi_API_KEY"
    ),
    "minimax-m3": ProviderSpec(
        "minimax-m3", "volcano_relay", RELAY_BASE, "AG2_Minimax_API_KEY"
    ),
    "deepseek-chat": ProviderSpec(
        "deepseek-chat", "deepseek_direct", DEEPSEEK_BASE, "AG2_DEEPSEEK2_API_KEY"
    ),
}


@dataclass(frozen=True)
class ProviderCallResult:
    model: str
    status: str  # ok | http_429 | http_error | timeout | malformed_json | error
    text: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    wall_time_ms: int
    detail: str


def provider_spec(model: str) -> ProviderSpec:
    raise NotImplementedError("C1 provider slice pending implementation")


def call_llm(
    model: str,
    prompt: str,
    *,
    max_tokens: int = 16,
    timeout_ms: int = 30_000,
    base_url: str | None = None,
    api_key: str | None = None,
) -> ProviderCallResult:
    raise NotImplementedError("C1 provider slice pending implementation")


def parse_anthropic_usage(payload: Mapping) -> tuple[int, int]:
    raise NotImplementedError("C1 provider slice pending implementation")


def parse_openai_usage(payload: Mapping) -> tuple[int, int]:
    raise NotImplementedError("C1 provider slice pending implementation")


def ping_llm(model: str, *, timeout_ms: int = 25_000, max_tokens: int = 4) -> ProviderCallResult:
    """Lightweight read-only availability probe for one model."""
    raise NotImplementedError("C1 provider slice pending implementation")
