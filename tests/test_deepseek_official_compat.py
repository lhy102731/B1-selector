from __future__ import annotations

from types import SimpleNamespace
import unittest

import autogen

from ag2_research.deepseek_compat import (
    DeepSeekOfficialAssistantAgent,
    create_profiled_assistant_agent,
)


class _Message:
    reasoning_content = "private reasoning"

    def model_dump(self):
        return {
            "role": "assistant",
            "content": "done",
            "reasoning_content": self.reasoning_content,
            "tool_calls": None,
            "function_call": None,
        }


class _Client:
    def __init__(self):
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(choices=[SimpleNamespace(message=_Message())])


class DeepSeekOfficialCompatibilityTests(unittest.TestCase):
    def test_official_profile_uses_compatibility_agent(self):
        agent = create_profiled_assistant_agent(
            "deepseekv4",
            name="deepseek_test",
            system_message="test",
            llm_config={
                "config_list": [{
                    "model": "deepseek-v4-pro",
                    "api_key": "test-key",
                    "base_url": "https://api.deepseek.com",
                }]
            },
            code_execution_config=False,
        )
        self.assertIsInstance(agent, DeepSeekOfficialAssistantAgent)

    def test_other_endpoint_keeps_standard_agent(self):
        agent = create_profiled_assistant_agent(
            "deepseekv4",
            name="deepseek_test",
            system_message="test",
            llm_config={
                "config_list": [{
                    "model": "deepseek-v4-pro",
                    "api_key": "test-key",
                    "base_url": "https://example.invalid/v1",
                }]
            },
            code_execution_config=False,
        )
        self.assertNotIsInstance(agent, DeepSeekOfficialAssistantAgent)

    def test_response_extraction_preserves_reasoning_content(self):
        agent = DeepSeekOfficialAssistantAgent(
            name="deepseek_test",
            llm_config=False,
            code_execution_config=False,
        )
        client = _Client()
        reply = agent._generate_oai_reply_from_client(
            client,
            [{"role": "user", "content": "test"}],
            cache=None,
        )
        self.assertEqual("private reasoning", reply["reasoning_content"])
        self.assertEqual("done", reply["content"])

    def test_agent_history_preserves_reasoning_content(self):
        agent = DeepSeekOfficialAssistantAgent(
            name="deepseek_test",
            llm_config=False,
            code_execution_config=False,
        )
        peer = autogen.ConversableAgent("peer", llm_config=False)
        message = {
            "role": "assistant",
            "content": "calling",
            "reasoning_content": "tool reasoning",
            "tool_calls": [{
                "id": "call-1",
                "type": "function",
                "function": {"name": "probe", "arguments": "{}"},
            }],
        }
        self.assertTrue(agent._append_oai_message(message, peer))
        self.assertEqual(
            "tool reasoning",
            agent._oai_messages[peer][-1]["reasoning_content"],
        )


if __name__ == "__main__":
    unittest.main()
