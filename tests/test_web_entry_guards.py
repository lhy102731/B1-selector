from __future__ import annotations

import queue
import unittest
from unittest.mock import patch

from research_automation.control_plane.web_guard import (
    WebAuthorizationError,
    require_loopback_host,
)


class WebEntryGuardTests(unittest.TestCase):
    def test_loopback_binding_is_required(self) -> None:
        self.assertEqual("127.0.0.1", require_loopback_host("127.0.0.1"))
        self.assertEqual("localhost", require_loopback_host("LOCALHOST"))
        self.assertEqual("::1", require_loopback_host("::1"))
        for host in ("0.0.0.0", "::", "192.168.1.8", ""):
            with self.subTest(host=host):
                with self.assertRaises(WebAuthorizationError):
                    require_loopback_host(host)

    def test_strategy_config_post_is_denied_before_write_or_reload(self) -> None:
        import apps.web_server as web_server

        client = web_server.app.test_client()
        with (
            patch("builtins.open", side_effect=AssertionError("config write attempted")),
            patch.object(
                web_server,
                "get_registry",
                side_effect=AssertionError("registry reload attempted"),
            ),
        ):
            response = client.post("/api/config", json={"B1": {"j": 29}})

        self.assertEqual(403, response.status_code)
        self.assertEqual("WEB_AUTH_REQUIRED", response.get_json()["code"])

    def test_web_server_rejects_wildcard_before_flask_run(self) -> None:
        import apps.web_server as web_server

        with patch.object(web_server.app, "run") as run:
            with self.assertRaises(WebAuthorizationError):
                web_server.run_web_server(host="0.0.0.0", debug=True)
        run.assert_not_called()

    def test_roundtable_start_is_denied_before_thread_creation(self) -> None:
        import apps.web_roundtable as web_roundtable

        web_roundtable.DISCUSSIONS.clear()
        client = web_roundtable.app.test_client()
        with patch.object(
            web_roundtable.threading,
            "Thread",
            side_effect=AssertionError("thread created"),
        ):
            response = client.post(
                "/api/start",
                json={
                    "topic": "must not start",
                    "participants": [{"profile": "test", "label": "test"}],
                },
            )

        self.assertEqual(403, response.status_code)
        self.assertEqual({}, web_roundtable.DISCUSSIONS)

    def test_direct_roundtable_worker_fails_before_llm_construction(self) -> None:
        import apps.web_roundtable as web_roundtable

        messages: queue.Queue = queue.Queue()
        with patch.object(
            web_roundtable.autogen,
            "AssistantAgent",
            side_effect=AssertionError("LLM agent constructed"),
        ):
            web_roundtable._stream_roundtable(
                "must not start",
                [{"profile": "test", "label": "test"}],
                messages,
            )

        self.assertEqual("error", messages.get_nowait()["type"])
        self.assertEqual("done", messages.get_nowait()["type"])


if __name__ == "__main__":
    unittest.main()
