"""P0 contracts for the read-only KBase discovery boundary."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


CONTRACTS_DIR = Path(__file__).with_name("contracts")
FORBIDDEN_DERIVATION_FIELDS = frozenset(
    {
        "hypothesis",
        "hypotheses",
        "mechanism",
        "factor",
        "factors",
        "factor_spec",
        "proxy",
        "proxies",
        "formula",
        "parameter",
        "parameters",
        "parameter_space",
        "research_queue",
        "v5_mapping",
    }
)
FORBIDDEN_HANDOFF_QUESTION_PATTERNS = (
    re.compile(r"\b(?:factor|feature|proxy|formula|parameter|ranker|backtest)\b", re.IGNORECASE),
    re.compile(r"因子|特征(?:输入|工程|体系|排序|变量)|作为.{0,24}(?:输入|特征|因子|过滤|排序|控制)"),
    re.compile(r"是否意味着.{0,32}应(?:该)?|应(?:该)?将.{0,32}(?:分层|加入|引入|用于)"),
)


class ContractValidationError(ValueError):
    """Raised when a KBase boundary object is malformed or contains derivation fields."""


def load_contract_schema(name: str) -> dict[str, Any]:
    path = CONTRACTS_DIR / f"{name}.schema.json"
    if not path.is_file():
        raise KeyError(f"unknown KBase contract: {name}")
    return json.loads(path.read_text(encoding="utf-8"))


def _find_forbidden_keys(value: Any, location: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key).lower()
            child_location = f"{location}.{key}"
            if key_text in FORBIDDEN_DERIVATION_FIELDS:
                found.append(child_location)
            found.extend(_find_forbidden_keys(child, child_location))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_find_forbidden_keys(child, f"{location}[{index}]"))
    return found


def _validate(name: str, payload: dict[str, Any], *, source_only: bool) -> None:
    if source_only:
        forbidden = _find_forbidden_keys(payload)
        if forbidden:
            raise ContractValidationError(
                "project-derivation fields are forbidden in KBase source contracts: "
                + ", ".join(forbidden)
            )

    validator = Draft202012Validator(load_contract_schema(name))
    errors = sorted(validator.iter_errors(payload), key=lambda item: list(item.path))
    if errors:
        details = []
        for error in errors[:8]:
            path = ".".join(str(part) for part in error.path) or "$"
            details.append(f"{path}: {error.message}")
        raise ContractValidationError("; ".join(details))


def validate_catalog_entry(payload: dict[str, Any]) -> None:
    """Validate a discovery record without allowing project research fields."""
    _validate("catalog_entry", payload, source_only=True)


def validate_source_brief(payload: dict[str, Any]) -> None:
    """Validate the Source Librarian handoff and enforce the inference boundary."""
    _validate("source_brief", payload, source_only=True)


def validate_source_brief_semantics(
    payload: dict[str, Any],
    *,
    known_source_ids: set[str] | None = None,
    catalog_version: str | None = None,
    evidence_refs_by_source: dict[str, set[str]] | None = None,
) -> None:
    """Validate cross-field references after the JSON Schema boundary passes."""
    validate_source_brief(payload)
    polluted_questions = [
        str(question)
        for question in payload.get("handoff_questions", [])
        if any(pattern.search(str(question)) for pattern in FORBIDDEN_HANDOFF_QUESTION_PATTERNS)
    ]
    if polluted_questions:
        raise ContractValidationError(
            "handoff_questions contain project-derivation language; ask only about source "
            "ambiguity, conditions, disagreements, or missing evidence: "
            + " | ".join(polluted_questions)
        )
    consulted = [str(item["source_id"]) for item in payload["sources_consulted"]]
    if len(consulted) != len(set(consulted)):
        raise ContractValidationError("sources_consulted contains duplicate source_id values")
    observed = {str(item["source_id"]) for item in payload["source_observations"]}
    unknown_observations = sorted(observed - set(consulted))
    if unknown_observations:
        raise ContractValidationError(
            "source_observations reference sources that were not consulted: "
            + ", ".join(unknown_observations)
        )
    missing_observations = sorted(set(consulted) - observed)
    if missing_observations:
        raise ContractValidationError(
            "consulted sources lack source_observations: "
            + ", ".join(missing_observations)
        )
    if known_source_ids is not None:
        unknown_sources = sorted(set(consulted) - known_source_ids)
        if unknown_sources:
            raise ContractValidationError(
                "source brief contains unknown catalog source ids: "
                + ", ".join(unknown_sources)
            )
    if catalog_version is not None and payload["catalog_version"] != catalog_version:
        raise ContractValidationError(
            f"catalog_version mismatch: {payload['catalog_version']} != {catalog_version}"
        )
    if evidence_refs_by_source is not None:
        invalid_refs = []
        for source in payload["sources_consulted"]:
            source_id = str(source["source_id"])
            allowed = evidence_refs_by_source.get(source_id, set())
            for evidence_ref in source["evidence_refs"]:
                if str(evidence_ref) not in allowed:
                    invalid_refs.append(str(evidence_ref))
        if invalid_refs:
            raise ContractValidationError(
                "source brief contains untraceable evidence refs: " + ", ".join(invalid_refs)
            )


def validate_usage_event(payload: dict[str, Any]) -> None:
    """Validate metadata-only telemetry; source text is not an allowed field."""
    _validate("usage_event", payload, source_only=True)
