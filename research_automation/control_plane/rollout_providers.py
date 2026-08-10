"""C1 LLM provider adapters (relay + direct) with usage parsing.

The relay exposes an Anthropic Messages compatible endpoint at
http://127.0.0.1:18080 which routes Volcano Coding Plan models. DeepSeek is
called directly through its OpenAI-compatible API. Every failure is classified
into a status string and never raises into the C1 driver.
"""

from __future__ import annotations

import json
import os
import socket
import time
import urllib.error
import urllib.request
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
    try:
        return REGISTRY[model]
    except KeyError:
        raise ValueError(f"unknown model: {model!r}") from None


def call_llm(
    model: str,
    prompt: str,
    *,
    max_tokens: int = 16,
    timeout_ms: int = 30_000,
    base_url: str | None = None,
    api_key: str | None = None,
) -> ProviderCallResult:
    started = time.monotonic()

    def result(status: str, text: str = "", **tokens: int) -> ProviderCallResult:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        return ProviderCallResult(
            model=model,
            status=status,
            text=text,
            input_tokens=tokens.get("input_tokens", 0),
            output_tokens=tokens.get("output_tokens", 0),
            total_tokens=tokens.get("input_tokens", 0) + tokens.get("output_tokens", 0),
            wall_time_ms=elapsed_ms,
            detail=tokens.get("detail", ""),
        )

    try:
        spec = provider_spec(model)
    except ValueError:
        raise
    base = base_url or spec.base_url
    key = api_key or os.environ.get(spec.api_key_env, "")
    if not key and spec.provider == "volcano_relay":
        key = "relay-token"

    try:
        if spec.provider == "volcano_relay":
            url = base.rstrip("/") + "/v1/messages"
            headers = {
                "content-type": "application/json",
                "authorization": "Bearer " + key,
            }
            body = {
                "model": model,
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
            }
        else:
            url = base.rstrip("/") + "/chat/completions"
            headers = {
                "content-type": "application/json",
                "authorization": "Bearer " + key,
            }
            body = {
                "model": model,
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
            }
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout_ms / 1000.0) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if spec.provider == "volcano_relay":
            text = payload["content"][0]["text"]
            input_tokens, output_tokens = parse_anthropic_usage(payload)
        else:
            text = payload["choices"][0]["message"]["content"]
            input_tokens, output_tokens = parse_openai_usage(payload)
        return result(
            "ok",
            text,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
    except urllib.error.HTTPError as exc:
        status = "http_429" if exc.code == 429 else "http_error"
        detail = f"HTTP {exc.code} {exc.reason}"
        return result(status, detail=detail)
    except (socket.timeout, TimeoutError):
        detail = f"request timed out after {timeout_ms}ms"
        return result("timeout", detail=detail)
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, (socket.timeout, TimeoutError)):
            detail = f"request timed out after {timeout_ms}ms"
            return result("timeout", detail=detail)
        detail = f"url error: {exc.reason}"
        return result("error", detail=detail)
    except json.JSONDecodeError as exc:
        detail = f"malformed json response: {exc}"
        return result("malformed_json", detail=detail)
    except Exception as exc:  # noqa: BLE001 - never raise into the C1 driver
        detail = f"{type(exc).__name__}: {exc}"
        return result("error", detail=detail)


def parse_anthropic_usage(payload: Mapping) -> tuple[int, int]:
    usage = payload.get("usage") or {}
    return usage.get("input_tokens", 0), usage.get("output_tokens", 0)


def parse_openai_usage(payload: Mapping) -> tuple[int, int]:
    usage = payload.get("usage") or {}
    return usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0)


def ping_llm(model: str, *, timeout_ms: int = 25_000, max_tokens: int = 4) -> ProviderCallResult:
    """Lightweight read-only availability probe for one model."""
    return call_llm(model, "ping", max_tokens=max_tokens, timeout_ms=timeout_ms)
