"""C1 LLM provider adapter tests (RED at skeleton, GREEN after implementation).

Spins a local ThreadingHTTPServer on 127.0.0.1 with an ephemeral port and
injects ``base_url`` into ``call_llm`` so no external network endpoint is ever
touched. Every test exercises only local sockets.
"""

from __future__ import annotations

import json
import os
import threading
import time
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

from research_automation.control_plane import rollout_providers as rp
from research_automation.control_plane.rollout_providers import (
    REGISTRY,
    call_llm,
    parse_anthropic_usage,
    parse_openai_usage,
    ping_llm,
    provider_spec,
)

ANTHROPIC_OK = {
    "content": [{"type": "text", "text": "anthropic hello"}],
    "usage": {"input_tokens": 10, "output_tokens": 5},
}

OPENAI_OK = {
    "choices": [{"message": {"role": "assistant", "content": "deepseek hello"}}],
    "usage": {"prompt_tokens": 12, "completion_tokens": 7},
}


class _StubHandler(BaseHTTPRequestHandler):
    """Serves a fixed, per-server behavior and captures the last request."""

    def do_POST(self):  # noqa: N802 - http.server protocol method name
        length = int(self.headers.get("content-length", 0) or 0)
        raw = self.rfile.read(length) if length else b""
        self.server.last_request = {
            "path": self.path,
            "headers": {k.lower(): v for k, v in self.headers.items()},
            "raw_body": raw,
        }
        try:
            self.server.last_request["body"] = json.loads(raw.decode("utf-8"))
        except Exception:
            self.server.last_request["body"] = None
        behavior = self.server.behavior
        if behavior.get("sleep"):
            time.sleep(behavior["sleep"])
        payload = behavior.get("payload", b"{}")
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        elif isinstance(payload, (dict, list)):
            payload = json.dumps(payload).encode("utf-8")
        self.send_response(behavior.get("status", 200))
        self.send_header("content-type", "application/json")
        self.end_headers()
        try:
            self.wfile.write(payload)
        except OSError:
            pass

    def log_message(self, *args):  # noqa: A002 - keep test output quiet
        pass


@contextmanager
def _serving(behavior: dict):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _StubHandler)
    server.behavior = behavior
    server.last_request = None
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield base, server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


class ProviderSpecTests(unittest.TestCase):
    def test_provider_spec_returns_registry_spec_for_known_model(self) -> None:
        spec = provider_spec("doubao-seed-2.0-pro")
        self.assertEqual(spec.model, "doubao-seed-2.0-pro")
        self.assertEqual(spec.provider, "volcano_relay")
        self.assertEqual(spec.base_url, "http://127.0.0.1:18080")
        self.assertEqual(spec.api_key_env, "AG2_DouBao_API_KEY")

    def test_provider_spec_unknown_model_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            provider_spec("not-a-model")

    def test_registry_contains_approved_roster(self) -> None:
        self.assertEqual(
            set(REGISTRY),
            {"doubao-seed-2.0-pro", "glm-5.2", "kimi-k2.7-code", "minimax-m3", "deepseek-chat"},
        )


class UsageParseTests(unittest.TestCase):
    def test_parse_anthropic_usage_returns_input_output(self) -> None:
        payload = {"usage": {"input_tokens": 10, "output_tokens": 5}}
        self.assertEqual(parse_anthropic_usage(payload), (10, 5))

    def test_parse_anthropic_usage_missing_returns_zero(self) -> None:
        self.assertEqual(parse_anthropic_usage({}), (0, 0))
        self.assertEqual(parse_anthropic_usage({"usage": None}), (0, 0))
        self.assertEqual(parse_anthropic_usage({"usage": {"output_tokens": 3}}), (0, 3))

    def test_parse_openai_usage_returns_prompt_completion(self) -> None:
        payload = {"usage": {"prompt_tokens": 12, "completion_tokens": 7}}
        self.assertEqual(parse_openai_usage(payload), (12, 7))

    def test_parse_openai_usage_missing_returns_zero(self) -> None:
        self.assertEqual(parse_openai_usage({}), (0, 0))
        self.assertEqual(parse_openai_usage({"usage": None}), (0, 0))
        self.assertEqual(parse_openai_usage({"usage": {"completion_tokens": 4}}), (0, 4))


class CallLlmTests(unittest.TestCase):
    def test_anthropic_ok_with_usage(self) -> None:
        with _serving({"payload": ANTHROPIC_OK}) as (base, server):
            result = call_llm(
                "doubao-seed-2.0-pro",
                "ping",
                max_tokens=16,
                timeout_ms=2000,
                base_url=base,
                api_key="test-key",
            )
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.text, "anthropic hello")
        self.assertEqual(result.input_tokens, 10)
        self.assertEqual(result.output_tokens, 5)
        self.assertEqual(result.total_tokens, 15)
        self.assertEqual(result.model, "doubao-seed-2.0-pro")
        self.assertGreaterEqual(result.wall_time_ms, 0)
        req = server.last_request
        self.assertEqual(req["path"], "/v1/messages")
        self.assertEqual(req["headers"]["authorization"], "Bearer test-key")
        self.assertEqual(req["headers"]["content-type"], "application/json")
        self.assertEqual(req["body"]["model"], "doubao-seed-2.0-pro")
        self.assertEqual(req["body"]["max_tokens"], 16)
        self.assertEqual(req["body"]["messages"], [{"role": "user", "content": "ping"}])
        self.assertIs(req["body"]["stream"], False)

    def test_openai_ok_with_usage(self) -> None:
        with _serving({"payload": OPENAI_OK}) as (base, server):
            result = call_llm(
                "deepseek-chat",
                "ping",
                timeout_ms=2000,
                base_url=base,
                api_key="ds-key",
            )
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.text, "deepseek hello")
        self.assertEqual(result.input_tokens, 12)
        self.assertEqual(result.output_tokens, 7)
        self.assertEqual(result.total_tokens, 19)
        req = server.last_request
        self.assertEqual(req["path"], "/chat/completions")
        self.assertEqual(req["body"]["model"], "deepseek-chat")
        self.assertEqual(req["body"]["max_tokens"], 16)
        self.assertEqual(req["body"]["messages"], [{"role": "user", "content": "ping"}])
        self.assertNotIn("stream", req["body"])

    def test_http_429_classified(self) -> None:
        with _serving({"status": 429, "payload": "quota exceeded"}) as (base, server):
            result = call_llm("glm-5.2", "ping", timeout_ms=2000, base_url=base)
        self.assertEqual(result.status, "http_429")
        self.assertIn("429", result.detail)
        self.assertEqual(result.text, "")
        self.assertEqual(result.input_tokens, 0)
        self.assertEqual(result.output_tokens, 0)

    def test_http_500_classified_http_error(self) -> None:
        with _serving({"status": 500, "payload": "boom"}) as (base, server):
            result = call_llm("glm-5.2", "ping", timeout_ms=2000, base_url=base)
        self.assertEqual(result.status, "http_error")
        self.assertIn("500", result.detail)
        self.assertEqual(result.text, "")

    def test_malformed_json_classified(self) -> None:
        with _serving({"payload": "this is not json"}) as (base, server):
            result = call_llm("glm-5.2", "ping", timeout_ms=2000, base_url=base)
        self.assertEqual(result.status, "malformed_json")
        self.assertEqual(result.text, "")
        self.assertEqual(result.input_tokens, 0)

    def test_timeout_classified(self) -> None:
        with _serving({"sleep": 0.4, "payload": ANTHROPIC_OK}) as (base, server):
            result = call_llm("glm-5.2", "ping", timeout_ms=50, base_url=base)
        self.assertEqual(result.status, "timeout")
        self.assertEqual(result.text, "")

    def test_call_llm_unknown_model_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            call_llm("nope-model", "ping", timeout_ms=2000)

    def test_volcano_relay_default_api_key_is_relay_token(self) -> None:
        with mock.patch.dict(os.environ, {"AG2_DouBao_API_KEY": ""}):
            with _serving({"payload": ANTHROPIC_OK}) as (base, server):
                result = call_llm("doubao-seed-2.0-pro", "ping", timeout_ms=2000, base_url=base)
        self.assertEqual(result.status, "ok")
        self.assertEqual(server.last_request["headers"]["authorization"], "Bearer relay-token")


class PingTests(unittest.TestCase):
    def test_ping_llm_returns_ok_with_small_max_tokens(self) -> None:
        with _serving({"payload": ANTHROPIC_OK}) as (base, server):
            spec = rp.ProviderSpec(
                "doubao-seed-2.0-pro", "volcano_relay", base, "AG2_DouBao_API_KEY"
            )
            with mock.patch.dict(rp.REGISTRY, {"doubao-seed-2.0-pro": spec}):
                result = ping_llm("doubao-seed-2.0-pro", timeout_ms=2000)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.model, "doubao-seed-2.0-pro")
        req = server.last_request
        self.assertEqual(req["body"]["max_tokens"], 4)
        self.assertEqual(req["body"]["messages"], [{"role": "user", "content": "ping"}])


if __name__ == "__main__":
    unittest.main()
