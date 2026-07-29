"""Agent factory — creates AG2 AssistantAgent instances from config templates."""
from __future__ import annotations

import autogen

from .config import ResearchConfig
from .deepseek_compat import create_profiled_assistant_agent
from .tools import get_tools_for_agent


def create_agents(
    config: ResearchConfig,
    agent_ids: list[str],
    llm_config: dict | None = None,
    research_context: str = "",
) -> dict[str, autogen.AssistantAgent]:
    """Create AG2 agents from config templates.

    Args:
        config: ResearchConfig instance with loaded agent templates.
        agent_ids: List of agent template IDs to instantiate (e.g. ['alpha_researcher', 'risk_manager']).
        llm_config: AG2 llm_config dict. If None, each agent picks its own profile
                    via config.get_agent_llm_config(agent_id) (v4.0). If provided,
                    forces all agents to this config (legacy override path).
        research_context: Research context injected into each agent's system_message.
                          Use {research_context} placeholder in config templates.

    Returns:
        Dict mapping agent name -> AssistantAgent instance.
    """
    forced_llm_config = llm_config  # None -> per-agent profile dispatch

    agents: dict[str, autogen.AssistantAgent] = {}
    for agent_id in agent_ids:
        template = config.get_agent(agent_id)
        if template is None:
            print(f"Warning: agent template '{agent_id}' not found in config, skipping")
            continue

        system_message = template["system_message"]
        if research_context:
            system_message = system_message.replace("{research_context}", research_context)

        tool_names = template.get("tools", [])

        # v4.0: per-agent LLM routing. Caller-supplied llm_config wins for
        # backward compatibility; otherwise each agent uses its declared profile.
        per_agent_llm = (
            forced_llm_config
            if forced_llm_config is not None
            else config.get_agent_llm_config(agent_id)
        )

        profile_name = template.get("profile") or config.default_profile
        agent = create_profiled_assistant_agent(
            profile_name,
            name=template["name"],
            system_message=system_message.strip(),
            llm_config=per_agent_llm,
            code_execution_config=False,
        )

        if tool_names:
            for tool_func in get_tools_for_agent(tool_names):
                agent.register_for_llm()(tool_func)
                agent.register_for_execution()(tool_func)

        agents[template["name"]] = agent

    return agents


def create_user_proxy(name: str = "User") -> autogen.UserProxyAgent:
    """Create a UserProxyAgent for human-in-the-loop interaction."""
    return autogen.UserProxyAgent(
        name=name,
        human_input_mode="TERMINATE",
        max_consecutive_auto_reply=0,
        code_execution_config=False,
    )
