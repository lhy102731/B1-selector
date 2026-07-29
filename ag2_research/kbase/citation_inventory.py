"""Deterministic citation inventory for Source Librarian tool sessions."""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any

from .repository import KBaseRepository


INVENTORY_SCHEMA_VERSION = "ag2.kbase_citation_inventory.v1"
KBASE_TOOL_NAMES = frozenset(
    {"kbase_overview", "kbase_browse", "kbase_search", "kbase_open", "kbase_trace"}
)
STATEMENT_FIELDS = (
    "methods",
    "claims",
    "risks",
    "contradictions",
    "definitions",
    "examples",
)
_SAFE_ARGUMENTS = frozenset(
    {
        "query",
        "people",
        "family_id",
        "topics",
        "source_type",
        "date_from",
        "date_to",
        "voice_role",
        "review_status",
        "scope",
        "max_results",
        "cursor",
        "source_id",
        "path",
        "layer",
        "max_chars",
        "node_id",
        "relation",
        "page_size",
        "top_n",
    }
)


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _decode_payload(content: Any) -> dict[str, Any] | None:
    if isinstance(content, dict):
        return content
    if isinstance(content, list):
        text_parts = [
            str(item.get("text") or item.get("content") or "") if isinstance(item, dict) else str(item)
            for item in content
        ]
        content = "".join(text_parts)
    if not isinstance(content, str) or not content.strip():
        return None
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def summarize_tool_exchange(
    *,
    sequence: int,
    tool_call_id: str | None,
    tool_name: str,
    arguments: dict[str, Any] | None,
    content: Any,
) -> dict[str, Any] | None:
    """Reduce one KBase tool exchange to metadata without copying source text."""
    if tool_name not in KBASE_TOOL_NAMES:
        return None
    arguments = arguments if isinstance(arguments, dict) else {}
    safe_arguments = {
        str(key): value for key, value in arguments.items() if str(key) in _SAFE_ARGUMENTS
    }
    payload = _decode_payload(content)
    source_ids: list[str] = []
    catalog_version = None
    status = "invalid_json"
    title = None
    layer = safe_arguments.get("layer")
    if payload is not None:
        catalog_version = payload.get("catalog_version")
        status = "error" if payload.get("error") else "ok"
        layer = payload.get("layer") or layer
        title = payload.get("title")
        if payload.get("source_id"):
            source_ids.append(str(payload["source_id"]))
        for item in payload.get("results", []) if isinstance(payload.get("results"), list) else []:
            if isinstance(item, dict) and item.get("source_id"):
                source_ids.append(str(item["source_id"]))
    rendered_content = content if isinstance(content, str) else _stable_json(content)
    return {
        "sequence": int(sequence),
        "tool_call_id": str(tool_call_id) if tool_call_id else None,
        "tool": tool_name,
        "arguments": safe_arguments,
        "status": status,
        "catalog_version": str(catalog_version) if catalog_version else None,
        "source_ids": list(dict.fromkeys(source_ids)),
        "opened_layer": str(layer) if tool_name == "kbase_open" and layer else None,
        "title": str(title) if title else None,
        "result_sha256": hashlib.sha256(str(rendered_content).encode("utf-8")).hexdigest(),
    }


def _entry_evidence_refs(
    repo: KBaseRepository,
    entry: dict[str, Any],
    opened_layers: set[str],
) -> list[str]:
    source_id = str(entry["source_id"])
    refs: set[str] = set()
    if entry.get("summary") or "summary" in (entry.get("available_layers") or []):
        refs.add(f"{source_id}#summary")
    if opened_layers & {"statements", "evidence"}:
        packet = repo.read_packet(entry)
        record = packet.get("record") if isinstance(packet.get("record"), dict) else {}
        for field in STATEMENT_FIELDS:
            values = record.get(field, []) if isinstance(record.get(field), list) else []
            if values:
                refs.add(f"{source_id}#{field}")
            refs.update(f"{source_id}#{field}[{index}]" for index in range(len(values)))
    if "raw" in opened_layers:
        if entry.get("paths", {}).get("raw"):
            refs.add(f"{source_id}#raw")
        elif entry.get("paths", {}).get("wiki"):
            refs.add(f"{source_id}#wiki")
    return sorted(refs)


def build_citation_inventory(
    tool_audit: list[dict[str, Any]],
    *,
    repository: KBaseRepository | None = None,
) -> dict[str, Any]:
    """Build the exact source IDs and evidence refs eligible for a source brief."""
    successful = [event for event in tool_audit if event.get("status") == "ok"]
    seen_source_ids = sorted(
        {
            str(source_id)
            for event in successful
            for source_id in event.get("source_ids", [])
            if source_id
        }
    )
    opened_layers: dict[str, set[str]] = defaultdict(set)
    traced_source_ids: set[str] = set()
    for event in successful:
        if event.get("tool") == "kbase_open" and event.get("opened_layer"):
            for source_id in event.get("source_ids", []):
                opened_layers[str(source_id)].add(str(event["opened_layer"]))
        elif event.get("tool") == "kbase_trace":
            traced_source_ids.update(str(value) for value in event.get("source_ids", []))

    repository_error = None
    repo = repository
    if repo is None:
        try:
            repo = KBaseRepository()
        except Exception as error:  # The release gate reports the concrete failure upstream.
            repository_error = f"{type(error).__name__}: {error}"

    eligible_sources: list[dict[str, Any]] = []
    if repo is not None:
        for source_id in sorted(opened_layers):
            entry = repo.get(source_id)
            if not entry:
                continue
            eligible_sources.append(
                {
                    "source_id": source_id,
                    "title": entry.get("title"),
                    "voice_role": entry.get("voice_role") or "unknown",
                    "date": entry.get("date_start"),
                    "reliability": entry.get("reliability") or "unverified",
                    "review_status": entry.get("review_status") or "review_required",
                    "warnings": list(entry.get("warnings") or []),
                    "opened_layers": sorted(opened_layers[source_id]),
                    "evidence_refs": _entry_evidence_refs(repo, entry, opened_layers[source_id]),
                }
            )

    versions = sorted(
        {str(event["catalog_version"]) for event in successful if event.get("catalog_version")}
    )
    repo_version = str(repo.manifest.get("catalog_version")) if repo is not None else None
    status = "READY"
    if repository_error or not eligible_sources:
        status = "INCOMPLETE"
    if len(versions) > 1 or (versions and repo_version and versions != [repo_version]):
        status = "INCONSISTENT"

    inventory: dict[str, Any] = {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "status": status,
        "catalog_version": repo_version or (versions[0] if len(versions) == 1 else None),
        "observed_catalog_versions": versions,
        "eligible_sources": eligible_sources,
        "eligible_source_ids": [item["source_id"] for item in eligible_sources],
        "seen_source_ids": seen_source_ids,
        "traced_source_ids": sorted(traced_source_ids),
        "tool_calls": [dict(event) for event in tool_audit],
        "repository_error": repository_error,
    }
    fingerprint_input = {key: value for key, value in inventory.items() if key != "tool_calls"}
    fingerprint_input["tool_result_hashes"] = [
        event.get("result_sha256") for event in tool_audit if event.get("result_sha256")
    ]
    inventory["inventory_fingerprint"] = hashlib.sha256(
        _stable_json(fingerprint_input).encode("utf-8")
    ).hexdigest()
    return inventory


def citation_inventory_issues(
    source_brief: dict[str, Any],
    inventory: dict[str, Any],
) -> list[dict[str, Any]]:
    """Return deterministic, repairable mismatches between a brief and its tool trace."""
    issues: list[dict[str, Any]] = []
    expected_version = str(inventory.get("catalog_version") or "")
    actual_version = str(source_brief.get("catalog_version") or "")
    if expected_version and actual_version != expected_version:
        issues.append(
            {
                "code": "catalog_version_mismatch",
                "expected": expected_version,
                "actual": actual_version,
            }
        )

    seen = set(map(str, inventory.get("seen_source_ids", [])))
    eligible = {
        str(item.get("source_id")): item
        for item in inventory.get("eligible_sources", [])
        if isinstance(item, dict) and item.get("source_id")
    }
    consulted = {
        str(item.get("source_id"))
        for item in source_brief.get("sources_consulted", [])
        if isinstance(item, dict) and item.get("source_id")
    }
    not_returned = sorted(consulted - seen)
    if not_returned:
        issues.append({"code": "source_id_not_returned_by_tools", "source_ids": not_returned})
    not_opened = sorted((consulted & seen) - set(eligible))
    if not_opened:
        issues.append({"code": "source_id_not_opened", "source_ids": not_opened})
    traced = set(map(str, inventory.get("traced_source_ids", [])))
    not_traced = sorted((consulted & set(eligible)) - traced)
    if not_traced:
        issues.append({"code": "source_id_not_traced", "source_ids": not_traced})

    observed = {
        str(item.get("source_id"))
        for item in source_brief.get("source_observations", [])
        if isinstance(item, dict) and item.get("source_id")
    }
    missing_observations = sorted(consulted - observed)
    if missing_observations:
        issues.append({
            "code": "consulted_source_without_observation",
            "source_ids": missing_observations,
        })
    if not source_brief.get("disagreements_and_limits") and not source_brief.get("missing_evidence"):
        issues.append({"code": "source_limit_analysis_missing"})
    if not source_brief.get("handoff_questions"):
        issues.append({"code": "handoff_questions_missing"})

    invalid_refs: list[str] = []
    for source in source_brief.get("sources_consulted", []):
        if not isinstance(source, dict):
            continue
        source_id = str(source.get("source_id") or "")
        if source_id not in eligible:
            continue
        allowed = set(map(str, eligible[source_id].get("evidence_refs", [])))
        invalid_refs.extend(
            str(ref) for ref in source.get("evidence_refs", []) if str(ref) not in allowed
        )
    if invalid_refs:
        issues.append({"code": "evidence_ref_not_exposed", "evidence_refs": sorted(set(invalid_refs))})
    return issues


def revision_citation_context(
    inventory: dict[str, Any],
    issues: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return the bounded inventory supplied to the model for a cited revision."""
    return {
        "schema_version": inventory.get("schema_version"),
        "inventory_fingerprint": inventory.get("inventory_fingerprint"),
        "catalog_version": inventory.get("catalog_version"),
        "issues": issues,
        "eligible_sources": inventory.get("eligible_sources", []),
        "revision_rules": [
            "Copy source_id and evidence_refs exactly from eligible_sources.",
            "Do not infer, splice, complete, or replace a source_id.",
            "A source absent from eligible_sources must be opened with kbase_open before use.",
        "Every source used in the brief must also pass kbase_trace.",
        "Return a complete replacement source_brief, not a patch.",
        "Keep the replacement concise enough to finish. If the prior draft was truncated, "
        "shorten observations or use fewer eligible sources instead of omitting required fields.",
    ],
}
