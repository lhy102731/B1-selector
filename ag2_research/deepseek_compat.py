"""AG2 compatibility for DeepSeek Official thinking-mode conversations."""
from __future__ import annotations

from typing import Any

import autogen


def _official_deepseek_profile(profile_name: str, llm_config: dict[str, Any]) -> bool:
    entries = llm_config.get("config_list") if isinstance(llm_config, dict) else None
    entry = entries[0] if isinstance(entries, list) and entries else {}
    base_url = str(entry.get("base_url") or "").rstrip("/").lower()
    return profile_name == "deepseekv4" and base_url == "https://api.deepseek.com"


class DeepSeekOfficialAssistantAgent(autogen.AssistantAgent):
    """Preserve reasoning_content required by DeepSeek tool-call continuations."""

    def _generate_oai_reply_from_client(
        self,
        llm_client: Any,
        messages: list[dict[str, Any]],
        cache: Any,
        **kwargs: Any,
    ) -> str | dict[str, Any] | None:
        all_messages: list[dict[str, Any]] = []
        for message in messages:
            tool_responses = message.get("tool_responses", [])
            if tool_responses:
                all_messages.extend(tool_responses)
                if message.get("role") != "tool":
                    all_messages.append({key: value for key, value in message.items() if key != "tool_responses"})
            else:
                all_messages.append(message)

        response = llm_client.create(
            context=messages[-1].pop("context", None),
            messages=all_messages,
            cache=cache,
            agent=self,
            **kwargs,
        )
        choices = getattr(response, "choices", None)
        raw_message = getattr(choices[0], "message", None) if choices else None
        if raw_message is None:
            extracted = llm_client.extract_text_or_completion_object(response)[0]
            return extracted

        dump = getattr(raw_message, "model_dump", None)
        if callable(dump):
            extracted = dump()
        elif isinstance(raw_message, dict):
            extracted = dict(raw_message)
        else:
            extracted = dict(raw_message)

        reasoning = getattr(raw_message, "reasoning_content", None)
        if reasoning is None:
            reasoning = extracted.get("reasoning_content")
        if reasoning is not None:
            extracted["reasoning_content"] = reasoning

        function_call = extracted.get("function_call")
        if isinstance(function_call, dict) and function_call.get("name"):
            function_call["name"] = self._normalize_name(function_call["name"])
        for tool_call in extracted.get("tool_calls") or []:
            function = tool_call.get("function") if isinstance(tool_call, dict) else None
            if isinstance(function, dict) and function.get("name"):
                function["name"] = self._normalize_name(function["name"])
        return extracted

    def _append_oai_message(
        self,
        message: dict[str, Any] | str,
        conversation_id: autogen.Agent,
        role: str = "assistant",
        name: str | None = None,
    ) -> bool:
        appended = super()._append_oai_message(message, conversation_id, role=role, name=name)
        if appended and isinstance(message, dict) and message.get("reasoning_content") is not None:
            self._oai_messages[conversation_id][-1]["reasoning_content"] = message["reasoning_content"]
        return appended


def create_profiled_assistant_agent(
    profile_name: str,
    *,
    llm_config: dict[str, Any],
    **kwargs: Any,
) -> autogen.AssistantAgent:
    agent_type = (
        DeepSeekOfficialAssistantAgent
        if _official_deepseek_profile(profile_name, llm_config)
        else autogen.AssistantAgent
    )
    return agent_type(llm_config=llm_config, **kwargs)
