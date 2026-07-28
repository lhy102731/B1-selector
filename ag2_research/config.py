"""Legacy AG2 configuration adapter backed by typed project settings."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from research_automation.foundations.settings import (
    ProjectSettings,
    load_project_settings,
)


class ResearchConfig:
    """Preserve the legacy read API while delegating ownership to ProjectSettings."""

    def __init__(
        self,
        config_path: str | Path | None = None,
        *,
        env_file: str | Path | None = None,
        environ: Mapping[str, str] | None = None,
        overrides: Mapping[str, str] | None = None,
    ) -> None:
        if config_path is None:
            config_path = Path(__file__).parent / "config.yaml"
        self.config_path = Path(config_path)
        self._settings: ProjectSettings
        self.load(
            env_file=env_file,
            environ=environ,
            overrides=overrides,
        )

    def load(
        self,
        *,
        env_file: str | Path | None = None,
        environ: Mapping[str, str] | None = None,
        overrides: Mapping[str, str] | None = None,
    ) -> None:
        """Atomically replace the typed owner after a complete successful load."""
        candidate = load_project_settings(
            self.config_path,
            env_file=env_file,
            environ=environ,
            overrides=overrides,
        )
        self._settings = candidate

    @property
    def _raw(self) -> dict[str, Any]:
        """Return a fresh unresolved document that cannot contain credential values."""
        return self._settings.unresolved_document()

    @property
    def default_profile(self) -> str:
        return self._settings.default_profile

    @property
    def profiles(self) -> dict[str, dict[str, Any]]:
        llm = self._raw.get("llm", {})
        if not isinstance(llm, dict):
            return {}
        profiles = llm.get("profiles", {})
        return profiles if isinstance(profiles, dict) else {}

    def list_profiles(self) -> list[str]:
        return list(self.profiles)

    def get_llm_config(
        self,
        profile: str | None = None,
        model: str | None = None,
    ) -> dict[str, Any]:
        """Return one AG2-compatible config, unsealing the key only at invocation."""
        name = profile or self.default_profile
        available = self.list_profiles()
        if name not in available:
            raise KeyError(
                f"LLM profile '{name}' not found. Available: {available}"
            )
        invocation = self._settings.require_invocation_profile(
            name,
            model_override=model,
        )
        entry: dict[str, Any] = {
            "model": invocation.model,
            "api_key": invocation.api_key.get_secret_value(),
            "base_url": invocation.base_url,
            "max_retries": invocation.max_retries,
        }
        if invocation.extra_params is not None:
            entry["extra_body"] = copy.deepcopy(invocation.extra_params)
        llm_config: dict[str, Any] = {
            "config_list": [entry],
            "temperature": invocation.temperature,
            "timeout": invocation.timeout,
        }
        if invocation.max_tokens is not None:
            llm_config["max_tokens"] = invocation.max_tokens
        return llm_config

    @property
    def llm_config(self) -> dict[str, Any]:
        return self.get_llm_config()

    @property
    def agents(self) -> dict[str, dict[str, Any]]:
        agents = self._raw.get("agents", {})
        return agents if isinstance(agents, dict) else {}

    def get_agent(self, agent_id: str) -> dict[str, Any] | None:
        return self.agents.get(agent_id)

    def get_agent_llm_config(self, agent_id: str) -> dict[str, Any]:
        template = self.get_agent(agent_id) or {}
        profile = template.get("profile") or self.default_profile
        return self.get_llm_config(profile=profile)

    def list_agents(self) -> list[dict[str, str]]:
        return [
            {
                "id": agent_id,
                "name": template["name"],
                "description": template["description"],
            }
            for agent_id, template in self.agents.items()
        ]

    @property
    def workflows(self) -> dict[str, dict[str, Any]]:
        workflows = self._raw.get("workflows", {})
        return workflows if isinstance(workflows, dict) else {}

    def get_workflow(self, workflow_id: str) -> dict[str, Any] | None:
        return self.workflows.get(workflow_id)

    def list_workflows(self) -> list[dict[str, str]]:
        return [
            {
                "id": workflow_id,
                "description": workflow["description"],
            }
            for workflow_id, workflow in self.workflows.items()
        ]


__all__ = ["ResearchConfig"]
