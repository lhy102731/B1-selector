"""Side-effect-free typed settings for research automation."""

from __future__ import annotations

import copy
import hashlib
from io import StringIO
import math
import os
import re
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from urllib.parse import urlsplit

import yaml
from dotenv import dotenv_values
from dotenv.parser import parse_stream
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from research_automation.control_plane.contracts import canonical_json


_ENV_REFERENCE = re.compile(
    r"^\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>.*))?\}$"
)
_SECRET_ENV_REFERENCE = re.compile(
    r"^\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}$"
)
_PROFILE_FIELDS = frozenset(
    {
        "api_key",
        "base_url",
        "model",
        "temperature",
        "timeout",
        "max_retries",
        "max_tokens",
        "extra_params",
        "provider_id",
        "transport",
        "retry_policy_ref",
        "tokenizer_ref",
        "pricing_ref",
    }
)
_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "llm",
        "control_layer",
        "agents",
        "workflows",
        "roundtable",
    }
)
_LLM_FIELDS = frozenset({"default", "profiles", "usage_targets"})
_PROFILE_REFERENCE_FIELDS = frozenset(
    {
        "api_key",
        "base_url",
        "model",
        "temperature",
        "timeout",
        "max_retries",
        "max_tokens",
        "extra_params",
        "provider_id",
        "transport",
        "retry_policy_ref",
        "tokenizer_ref",
        "pricing_ref",
    }
)
_SECRET_FIELD_EXACT = frozenset(
    {
        "api_key",
        "apikey",
        "secret",
        "secret_key",
        "password",
        "passwd",
        "credential",
        "credentials",
        "token",
        "access_token",
        "refresh_token",
        "auth_token",
        "private_key",
        "client_secret",
    }
)


class ProjectSettingsError(ValueError):
    """Raised when project settings cannot be loaded safely."""


class MissingInvocationSettingError(ProjectSettingsError):
    """Raised before client construction when invocation settings are absent."""


def _environment_name(name: str) -> str:
    return name.upper() if os.name == "nt" else name


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeyLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[object, object]:
    loader.flatten_mapping(node)
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as error:
            raise ProjectSettingsError(
                "project settings YAML mapping keys are invalid"
            ) from error
        if duplicate:
            raise ProjectSettingsError(
                "project settings YAML contains a duplicate key"
            )
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


class InvocationProfile(BaseModel):
    """Complete profile that is eligible to construct one provider client."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    profile_id: str
    provider_id: str
    transport: str
    retry_policy_ref: str
    tokenizer_ref: str
    pricing_ref: str
    model: str
    api_key: SecretStr
    base_url: str
    temperature: float
    timeout: int = Field(gt=0)
    max_retries: int = Field(ge=0)
    max_tokens: int | None
    extra_params: dict[str, object] | None


class _ResolvedProfile(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    profile_id: str
    credential_env: str
    provider_id: str
    transport: str
    retry_policy_ref: str
    tokenizer_ref: str
    pricing_ref: str
    model: str | None
    api_key: SecretStr | None
    base_url: str | None
    temperature: float
    timeout: int = Field(gt=0)
    max_retries: int = Field(ge=0)
    max_tokens: int | None
    extra_params: dict[str, object] | None


class ProjectSettings:
    """Immutable owner of resolved profiles; inspection may tolerate missing secrets."""

    __slots__ = ("_default_profile", "_document", "_profiles")

    def __init__(
        self,
        *,
        default_profile: str,
        document: Mapping[str, object],
        profiles: Mapping[str, _ResolvedProfile],
    ) -> None:
        if default_profile not in profiles:
            raise ProjectSettingsError("default profile is not defined")
        self._default_profile = default_profile
        self._document = copy.deepcopy(dict(document))
        self._profiles = MappingProxyType(dict(profiles))

    @property
    def default_profile(self) -> str:
        return self._default_profile

    def require_invocation_profile(
        self,
        profile_id: str,
        model_override: str | None = None,
    ) -> InvocationProfile:
        try:
            profile = self._profiles[profile_id]
        except KeyError as error:
            raise KeyError(f"unknown LLM profile: {profile_id}") from error
        if profile.api_key is None:
            raise MissingInvocationSettingError("MISSING_CREDENTIAL")
        if model_override is not None and (
            not isinstance(model_override, str)
            or not model_override
            or model_override != model_override.strip()
        ):
            raise ProjectSettingsError("model override is invalid")
        model = model_override or profile.model
        if profile.base_url is None or model is None:
            raise MissingInvocationSettingError("MISSING_REQUIRED_SETTING")
        return InvocationProfile(
            profile_id=profile.profile_id,
            provider_id=profile.provider_id,
            transport=profile.transport,
            retry_policy_ref=profile.retry_policy_ref,
            tokenizer_ref=profile.tokenizer_ref,
            pricing_ref=profile.pricing_ref,
            model=model,
            api_key=profile.api_key,
            base_url=profile.base_url,
            temperature=profile.temperature,
            timeout=profile.timeout,
            max_retries=profile.max_retries,
            max_tokens=profile.max_tokens,
            extra_params=copy.deepcopy(profile.extra_params),
        )

    @staticmethod
    def _public_profile(profile: _ResolvedProfile) -> dict[str, object]:
        return {
            "profile_id": profile.profile_id,
            "credential_env": profile.credential_env,
            "provider_id": profile.provider_id,
            "transport": profile.transport,
            "retry_policy_ref": profile.retry_policy_ref,
            "tokenizer_ref": profile.tokenizer_ref,
            "pricing_ref": profile.pricing_ref,
            "model": profile.model,
            "base_url": profile.base_url,
            "temperature": profile.temperature,
            "timeout": profile.timeout,
            "max_retries": profile.max_retries,
            "max_tokens": profile.max_tokens,
            "extra_params": copy.deepcopy(profile.extra_params),
        }

    def public_manifest(self) -> dict[str, object]:
        """Return the deterministic, credential-free settings identity payload."""
        llm = self._document.get("llm", {})
        return {
            "schema_version": "control_plane.project_settings_public.v1",
            "default_profile": self._default_profile,
            "profiles": {
                profile_id: self._public_profile(profile)
                for profile_id, profile in self._profiles.items()
            },
            "usage_targets": copy.deepcopy(
                llm.get("usage_targets", {}) if isinstance(llm, dict) else {}
            ),
            "control_layer": copy.deepcopy(
                self._document.get("control_layer", {})
            ),
            "agents": copy.deepcopy(self._document.get("agents", {})),
            "workflows": copy.deepcopy(self._document.get("workflows", {})),
            "roundtable": copy.deepcopy(self._document.get("roundtable", {})),
        }

    def unresolved_document(self) -> dict[str, object]:
        """Return a fresh YAML-shaped view containing references, never credentials."""
        return copy.deepcopy(self._document)

    @property
    def public_identity_sha256(self) -> str:
        payload = canonical_json(self.public_manifest()).encode("utf-8")
        return hashlib.sha256(
            b"control_plane.project_settings_public.v1\0" + payload
        ).hexdigest()

    def inspect(self) -> dict[str, object]:
        """Return a fresh allowlisted view that never contains credential bytes."""
        return {
            "schema_version": "control_plane.project_settings_inspection.v1",
            "default_profile": self._default_profile,
            "profiles": {
                profile_id: {
                    **self._public_profile(profile),
                    "credential_status": (
                        "AVAILABLE" if profile.api_key is not None else "MISSING"
                    ),
                }
                for profile_id, profile in self._profiles.items()
            },
        }


def _resolve_reference(value: object, sources: Mapping[str, str]) -> object:
    if not isinstance(value, str):
        return value
    match = _ENV_REFERENCE.fullmatch(value)
    if match is None:
        return value
    name = match.group("name")
    normalized_name = _environment_name(name)
    if normalized_name in sources:
        return sources[normalized_name]
    return match.group("default")


def _resolve_nested(value: object, sources: Mapping[str, str]) -> object:
    if isinstance(value, dict):
        return {key: _resolve_nested(child, sources) for key, child in value.items()}
    if isinstance(value, list):
        return [_resolve_nested(child, sources) for child in value]
    return _resolve_reference(value, sources)


def _resolve_numeric(
    value: object,
    sources: Mapping[str, str],
    *,
    integer: bool,
) -> int | float:
    resolved = _resolve_reference(value, sources)
    reference_value = isinstance(value, str) and _ENV_REFERENCE.fullmatch(value)
    if integer:
        if type(resolved) is int:
            return resolved
        if (
            reference_value
            and isinstance(resolved, str)
            and resolved == resolved.strip()
            and re.fullmatch(r"[+-]?[0-9]+", resolved)
        ):
            try:
                return int(resolved)
            except ValueError:
                pass
    else:
        if type(resolved) in {int, float} and math.isfinite(float(resolved)):
            return float(resolved)
        if reference_value and isinstance(resolved, str) and resolved == resolved.strip():
            try:
                converted = float(resolved)
            except ValueError:
                converted = float("nan")
            if math.isfinite(converted):
                return converted
    raise ProjectSettingsError("profile values failed strict validation")


def _referenced_environment_names(value: object) -> set[str]:
    if isinstance(value, dict):
        result: set[str] = set()
        for child in value.values():
            result.update(_referenced_environment_names(child))
        return result
    if isinstance(value, list):
        result = set()
        for child in value:
            result.update(_referenced_environment_names(child))
        return result
    if isinstance(value, str):
        match = _ENV_REFERENCE.fullmatch(value)
        return (
            {_environment_name(match.group("name"))}
            if match is not None
            else set()
        )
    return set()


def _validate_credential_reference_scope(
    value: object,
    credential_environment_names: frozenset[str],
    *,
    path: tuple[object, ...] = (),
    active_containers: set[int] | None = None,
) -> None:
    """Keep credential references confined to profile.api_key fields."""
    if isinstance(value, str):
        match = _ENV_REFERENCE.fullmatch(value)
        if (
            match is not None
            and _environment_name(match.group("name"))
            in credential_environment_names
        ):
            raise ProjectSettingsError(
                "credential environment reference is outside profile api_key"
            )
        return
    if not isinstance(value, (dict, list)):
        return
    active = active_containers if active_containers is not None else set()
    container_id = id(value)
    if container_id in active:
        raise ProjectSettingsError("project settings document is cyclic")
    active.add(container_id)
    try:
        if isinstance(value, dict):
            for key, child in value.items():
                if path == () and key == "llm" and isinstance(child, dict):
                    for llm_key, llm_child in child.items():
                        if llm_key != "profiles":
                            _validate_credential_reference_scope(
                                llm_child,
                                credential_environment_names,
                                path=("llm", llm_key),
                                active_containers=active,
                            )
                    continue
                is_profile_credential = (
                    len(path) == 3
                    and path[0] == "llm"
                    and path[1] == "profiles"
                    and key == "api_key"
                )
                if not is_profile_credential:
                    _validate_credential_reference_scope(
                        child,
                        credential_environment_names,
                        path=path + (key,),
                        active_containers=active,
                    )
        else:
            for index, child in enumerate(value):
                _validate_credential_reference_scope(
                    child,
                    credential_environment_names,
                    path=path + (index,),
                    active_containers=active,
                )
    finally:
        active.remove(container_id)


def _validated_environment_source(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping) or any(
        not isinstance(key, str) or not isinstance(item, str)
        for key, item in value.items()
    ):
        raise ProjectSettingsError("environment source is invalid")
    result: dict[str, str] = {}
    for key, item in value.items():
        normalized_key = _environment_name(key)
        if normalized_key in result:
            raise ProjectSettingsError("environment source is invalid")
        result[normalized_key] = item
    return result


def _validate_public_json_value(
    value: object,
    *,
    active_containers: set[int] | None = None,
) -> None:
    if isinstance(value, (dict, list)):
        active = active_containers if active_containers is not None else set()
        container_id = id(value)
        if container_id in active:
            raise ProjectSettingsError(
                "project settings public configuration is invalid"
            )
        active.add(container_id)
        try:
            if isinstance(value, dict):
                if any(not isinstance(key, str) for key in value):
                    raise ProjectSettingsError(
                        "project settings public configuration is invalid"
                    )
                children = value.values()
            else:
                children = value
            for child in children:
                _validate_public_json_value(
                    child,
                    active_containers=active,
                )
        finally:
            active.remove(container_id)
        return
    if value is None or type(value) in {bool, int, str}:
        return
    if type(value) is float and math.isfinite(value):
        return
    raise ProjectSettingsError("project settings public configuration is invalid")


def _valid_base_url(value: str) -> bool:
    try:
        parsed_url = urlsplit(value)
        parsed_url.port
    except ValueError:
        return False
    return bool(
        parsed_url.scheme in {"http", "https"}
        and parsed_url.netloc
        and parsed_url.hostname
        and parsed_url.username is None
        and parsed_url.password is None
        and not parsed_url.query
        and not parsed_url.fragment
        and not any(character.isspace() for character in value)
    )


def _normalized_field_name(value: str) -> str:
    return re.sub(r"[-\s]+", "_", value).lower()


def _is_secret_field_name(value: str) -> bool:
    normalized = _normalized_field_name(value)
    return normalized in _SECRET_FIELD_EXACT or normalized.endswith(
        ("_api_key", "_secret", "_password", "_credential", "_token", "_private_key")
    )


def _validate_secret_field_placement(
    value: object,
    *,
    path: tuple[object, ...] = (),
    active_containers: set[int] | None = None,
) -> None:
    """Reject secret-like fields outside the one typed credential seam.

    A recursive public document is otherwise allowed to contain arbitrary
    project metadata.  Secret-shaped keys are the explicit exception: keeping
    them out of YAML-shaped inspection and public identity prevents a future
    adapter from accidentally treating an untyped value as safe metadata.
    """
    if not isinstance(value, (dict, list)):
        return
    active = active_containers if active_containers is not None else set()
    container_id = id(value)
    if container_id in active:
        raise ProjectSettingsError("project settings document is cyclic")
    active.add(container_id)
    try:
        if isinstance(value, dict):
            for key, child in value.items():
                if isinstance(key, str) and _is_secret_field_name(key):
                    is_profile_credential = (
                        len(path) == 3
                        and path[0] == "llm"
                        and path[1] == "profiles"
                        and key == "api_key"
                    )
                    if not is_profile_credential:
                        raise ProjectSettingsError(
                            "secret-like settings must use a profile api_key reference"
                        )
                _validate_secret_field_placement(
                    child,
                    path=path + (key,),
                    active_containers=active,
                )
        else:
            for index, child in enumerate(value):
                _validate_secret_field_placement(
                    child,
                    path=path + (index,),
                    active_containers=active,
                )
    finally:
        active.remove(container_id)


def _resolved_profile(
    profile_id: str,
    raw: Mapping[str, object],
    sources: Mapping[str, str],
    credential_environment_names: frozenset[str],
) -> _ResolvedProfile:
    if set(raw) - _PROFILE_FIELDS:
        raise ProjectSettingsError("profile contains unknown profile fields")
    key_reference = raw.get("api_key")
    key_match = (
        _SECRET_ENV_REFERENCE.fullmatch(key_reference)
        if isinstance(key_reference, str)
        else None
    )
    if key_match is None:
        raise ProjectSettingsError(
            "profile credential reference must be an exact environment reference"
        )
    public_profile_fields = {
        field_name: value
        for field_name, value in raw.items()
        if field_name != "api_key"
    }
    if (
        _referenced_environment_names(public_profile_fields)
        & credential_environment_names
    ):
        raise ProjectSettingsError(
            "credential environment reference cannot be reused in public profile fields"
        )
    key_value = sources.get(_environment_name(key_match.group("name")))
    if key_value is not None and not isinstance(key_value, str):
        raise ProjectSettingsError("profile values failed strict validation")
    api_key = (
        SecretStr(key_value)
        if (
            isinstance(key_value, str)
            and key_value
            and key_value == key_value.strip()
        )
        else None
    )
    model = _resolve_reference(raw.get("model", "gpt-4o"), sources)
    base_url = _resolve_reference(
        raw.get("base_url", "https://api.openai.com/v1"),
        sources,
    )
    extra = _resolve_nested(raw.get("extra_params"), sources)
    if extra is not None:
        _validate_public_json_value(extra)
    metadata = {
        field_name: _resolve_reference(raw.get(field_name, "UNSET"), sources)
        for field_name in (
            "provider_id",
            "transport",
            "retry_policy_ref",
            "tokenizer_ref",
            "pricing_ref",
        )
    }
    temperature = _resolve_numeric(
        raw.get("temperature", 0.7),
        sources,
        integer=False,
    )
    timeout = _resolve_numeric(
        raw.get("timeout", 120),
        sources,
        integer=True,
    )
    max_retries = _resolve_numeric(
        raw.get("max_retries", 6),
        sources,
        integer=True,
    )
    max_tokens_raw = raw.get("max_tokens")
    max_tokens = (
        None
        if max_tokens_raw is None
        else _resolve_numeric(
            max_tokens_raw,
            sources,
            integer=True,
        )
    )
    if (
        type(timeout) is not int
        or timeout <= 0
        or type(max_retries) is not int
        or max_retries < 0
        or (
            max_tokens is not None
            and (type(max_tokens) is not int or max_tokens <= 0)
        )
        or (
            model is not None
            and (
                not isinstance(model, str)
                or not model
                or model != model.strip()
            )
        )
        or (
            base_url is not None
            and (
                not isinstance(base_url, str)
                or not base_url
                or base_url != base_url.strip()
            )
        )
        or (extra is not None and not isinstance(extra, dict))
        or any(
            not isinstance(value, str) or not value or value != value.strip()
            for value in metadata.values()
        )
    ):
        raise ProjectSettingsError("profile values failed strict validation")
    if isinstance(base_url, str) and base_url and not _valid_base_url(base_url):
        raise ProjectSettingsError("profile values failed strict validation")
    return _ResolvedProfile(
        profile_id=profile_id,
        credential_env=(
            key_match.group("name") if key_match is not None else "UNDECLARED"
        ),
        provider_id=metadata["provider_id"],
        transport=metadata["transport"],
        retry_policy_ref=metadata["retry_policy_ref"],
        tokenizer_ref=metadata["tokenizer_ref"],
        pricing_ref=metadata["pricing_ref"],
        model=model if isinstance(model, str) and model else None,
        api_key=api_key,
        base_url=base_url if isinstance(base_url, str) and base_url else None,
        temperature=float(temperature),
        timeout=timeout,
        max_retries=max_retries,
        max_tokens=max_tokens,
        extra_params=extra if isinstance(extra, dict) else None,
    )


def _validate_roundtable_reference_integrity(
    roundtable: object,
    profile_ids: set[str],
) -> None:
    if not isinstance(roundtable, dict):
        raise ProjectSettingsError("project settings reference integrity failed")
    coordinator = roundtable.get("coordinator")
    if coordinator is not None and (
        not isinstance(coordinator, str) or coordinator not in profile_ids
    ):
        raise ProjectSettingsError("project settings reference integrity failed")
    participants = roundtable.get("participants", [])
    if not isinstance(participants, list):
        raise ProjectSettingsError("project settings reference integrity failed")
    labels: list[str] = []
    participant_profiles: list[str] = []
    for participant in participants:
        if not isinstance(participant, dict):
            raise ProjectSettingsError("project settings reference integrity failed")
        participant_profile = participant.get("profile")
        label = participant.get("label")
        if (
            not isinstance(participant_profile, str)
            or participant_profile not in profile_ids
            or not isinstance(label, str)
            or not label
        ):
            raise ProjectSettingsError("project settings reference integrity failed")
        labels.append(label)
        participant_profiles.append(participant_profile)
    if len(labels) != len(set(labels)):
        raise ProjectSettingsError("project settings reference integrity failed")
    if coordinator is not None and coordinator not in participant_profiles:
        raise ProjectSettingsError("project settings reference integrity failed")


def _validate_reference_integrity(
    raw: Mapping[str, object],
    profile_ids: set[str],
) -> None:
    agents = raw.get("agents", {})
    workflows = raw.get("workflows", {})
    roundtable = raw.get("roundtable", {})
    if not isinstance(agents, dict) or not isinstance(workflows, dict):
        raise ProjectSettingsError("project settings reference integrity failed")
    agent_ids = set(agents)
    workflow_ids = set(workflows)
    for agent_id, agent in agents.items():
        if (
            not isinstance(agent_id, str)
            or not agent_id
            or not isinstance(agent, dict)
            or any(
                not isinstance(agent.get(field_name), str)
                or not agent[field_name].strip()
                for field_name in ("name", "description")
            )
        ):
            raise ProjectSettingsError("project settings reference integrity failed")
        profile_id = agent.get("profile")
        if profile_id is not None and (
            not isinstance(profile_id, str) or profile_id not in profile_ids
        ):
            raise ProjectSettingsError("project settings reference integrity failed")
    for workflow_id, workflow in workflows.items():
        if (
            not isinstance(workflow_id, str)
            or not workflow_id
            or not isinstance(workflow, dict)
            or not isinstance(workflow.get("description"), str)
            or not workflow["description"].strip()
        ):
            raise ProjectSettingsError("project settings reference integrity failed")
        for field_name in ("agents", "pipeline_order"):
            references = workflow.get(field_name)
            if references is not None and (
                not isinstance(references, list)
                or any(
                    not isinstance(reference, str) or reference not in agent_ids
                    for reference in references
                )
            ):
                raise ProjectSettingsError(
                    "project settings reference integrity failed"
                )
        coordinator = workflow.get("coordinator")
        if coordinator is not None and (
            not isinstance(coordinator, str) or coordinator not in agent_ids
        ):
            raise ProjectSettingsError("project settings reference integrity failed")
        declared_agents = workflow.get("agents")
        if isinstance(declared_agents, list):
            local_agent_ids = set(declared_agents)
            pipeline_order = workflow.get("pipeline_order")
            if isinstance(pipeline_order, list) and any(
                reference not in local_agent_ids for reference in pipeline_order
            ):
                raise ProjectSettingsError(
                    "project settings reference integrity failed"
                )
            if coordinator is not None and coordinator not in local_agent_ids:
                raise ProjectSettingsError(
                    "project settings reference integrity failed"
                )
        for field_name in (
            "source_brief_workflow",
            "factor_workflow",
            "sequential_workflow",
        ):
            reference = workflow.get(field_name)
            if reference is not None and (
                not isinstance(reference, str) or reference not in workflow_ids
            ):
                raise ProjectSettingsError(
                    "project settings reference integrity failed"
                )
        nested_roundtable = workflow.get("roundtable")
        if nested_roundtable is not None:
            _validate_roundtable_reference_integrity(
                nested_roundtable,
                profile_ids,
            )
    _validate_roundtable_reference_integrity(roundtable, profile_ids)


def load_project_settings(
    yaml_path: str | Path,
    *,
    env_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
    overrides: Mapping[str, str] | None = None,
) -> ProjectSettings:
    """Load one explicitly selected YAML file without changing process globals."""
    path = Path(yaml_path)
    yaml_error: str | None = None
    try:
        raw = yaml.load(
            path.read_text(encoding="utf-8"),
            Loader=_UniqueKeyLoader,
        )
    except ProjectSettingsError as error:
        yaml_error = str(error)
    except (OSError, UnicodeError, yaml.YAMLError):
        yaml_error = "project settings YAML is invalid"
    if yaml_error is not None:
        raise ProjectSettingsError(yaml_error)
    if not isinstance(raw, dict) or not isinstance(raw.get("llm"), dict):
        raise ProjectSettingsError("project settings root is invalid")
    _validate_secret_field_placement(raw)
    if set(raw) - _TOP_LEVEL_FIELDS:
        raise ProjectSettingsError("project settings document contract is invalid")
    schema_version = raw.get("schema_version")
    if schema_version not in (None, "control_plane.project_settings.v1"):
        raise ProjectSettingsError("unsupported project settings schema version")
    llm = raw["llm"]
    if set(llm) - _LLM_FIELDS:
        raise ProjectSettingsError("project settings document contract is invalid")
    profiles_raw = llm.get("profiles")
    if not isinstance(profiles_raw, dict) or not profiles_raw:
        raise ProjectSettingsError("project settings profiles are invalid")
    credential_environment_names: set[str] = set()
    consumed_environment_names: set[str] = set()
    for profile_raw in profiles_raw.values():
        if not isinstance(profile_raw, dict):
            raise ProjectSettingsError("project settings profiles are invalid")
        key_reference = profile_raw.get("api_key")
        key_match = (
            _SECRET_ENV_REFERENCE.fullmatch(key_reference)
            if isinstance(key_reference, str)
            else None
        )
        if key_match is None:
            raise ProjectSettingsError(
                "profile credential reference must be an exact environment reference"
            )
        credential_environment_names.add(
            _environment_name(key_match.group("name"))
        )
        consumed_environment_names.update(
            _referenced_environment_names(
                {
                    field_name: value
                    for field_name, value in profile_raw.items()
                    if field_name in _PROFILE_REFERENCE_FIELDS
                }
            )
        )
    credential_names = frozenset(credential_environment_names)
    _validate_credential_reference_scope(raw, credential_names)
    _validate_reference_integrity(raw, set(profiles_raw))
    sources: dict[str, str] = {}
    if env_file is not None:
        selected: Mapping[str, str | None] | None
        selected_path = Path(env_file)
        if not selected_path.is_file():
            raise ProjectSettingsError("selected env file is invalid")
        try:
            env_text = selected_path.read_text(encoding="utf-8")
            if env_text.startswith("\ufeff"):
                raise ProjectSettingsError("selected env file is invalid")
            bindings = tuple(parse_stream(StringIO(env_text)))
            binding_names: set[str] = set()
            for binding in bindings:
                if binding.error:
                    raise ProjectSettingsError("selected env file is invalid")
                if binding.key is not None:
                    normalized_name = _environment_name(binding.key)
                    if normalized_name in binding_names:
                        raise ProjectSettingsError("selected env file is invalid")
                    binding_names.add(normalized_name)
            selected = dotenv_values(
                stream=StringIO(env_text),
                encoding="utf-8",
                interpolate=False,
            )
        except ProjectSettingsError:
            raise
        except (OSError, UnicodeError, ValueError):
            selected = None
        if selected is None:
            raise ProjectSettingsError("selected env file is invalid")
        sources.update(_validated_environment_source(selected))
    sources.update(
        _validated_environment_source(os.environ if environ is None else environ)
    )
    explicit_overrides = _validated_environment_source(
        {} if overrides is None else overrides
    )
    unknown_overrides = set(explicit_overrides) - consumed_environment_names
    if unknown_overrides:
        raise ProjectSettingsError("explicit settings contain an unknown override")
    sources.update(explicit_overrides)
    profiles: dict[str, _ResolvedProfile] = {}
    for profile_id, profile_raw in profiles_raw.items():
        if not isinstance(profile_id, str) or not isinstance(profile_raw, dict):
            raise ProjectSettingsError("project settings profiles are invalid")
        profiles[profile_id] = _resolved_profile(
            profile_id,
            profile_raw,
            sources,
            credential_names,
        )
    default_profile = llm.get("default", "openai")
    if not isinstance(default_profile, str):
        raise ProjectSettingsError("default profile is invalid")
    settings = ProjectSettings(
        default_profile=default_profile,
        document=raw,
        profiles=profiles,
    )
    _validate_public_json_value(settings.public_manifest())
    return settings


__all__ = [
    "InvocationProfile",
    "MissingInvocationSettingError",
    "ProjectSettings",
    "ProjectSettingsError",
    "load_project_settings",
]
