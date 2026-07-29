"""AG2-facing progressive-disclosure tools over the published KBase catalog."""
from __future__ import annotations

import json
import functools
import os
import time
from pathlib import Path
from typing import Any

from .adapters import STATEMENT_FIELDS, normalize_statement_item, statement_text
from .hybrid_ranking import is_navigation_query, lexical_rank_with_anchors
from .ranking import matches_filters
from .repository import CatalogUnavailableError, DEFAULT_VAULT, KBaseRepository
from .semantic_client import (
    SemanticUnavailableError,
    merge_semantic_results,
    request_semantic_search,
)


TOOL_CONTRACT_VERSION = "1.0"


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2)


def _repo(vault_path: str | None) -> KBaseRepository:
    return KBaseRepository(vault_path)


def _compact(entry: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "source_id", "object_type", "title", "people", "family_id", "voice_role",
        "source_type", "date_start", "date_end", "topics", "summary", "reliability",
        "review_status", "available_layers", "warnings", "paths", "_score", "_match_reasons", "_semantic",
    )
    return {key: entry[key] for key in keys if key in entry}


def kbase_overview(*, vault_path: str | None = None, top_n: int = 20) -> str:
    """Return the small, stable entry point for progressive KBase browsing."""
    try:
        repo = _repo(vault_path)
    except CatalogUnavailableError as error:
        return _json({"error": str(error), "fallback": "legacy_book_index"})
    top_n = max(5, min(int(top_n), 50))
    facets = repo.facets
    content_maps = sorted(
        (_compact(entry) for entry in repo.entries() if entry.get("object_type") == "map"),
        key=lambda entry: (str(entry.get("title") or ""), str(entry.get("source_id") or "")),
    )
    return _json({
        "tool_contract_version": TOOL_CONTRACT_VERSION,
        "catalog_version": repo.manifest.get("catalog_version"),
        "generated_at": repo.manifest.get("generated_at"),
        "release": repo.release_dir.name,
        "counts": repo.manifest.get("counts", {}),
        "top_people": list(facets.get("people", {}).items())[:top_n],
        "top_families": list(facets.get("family_id", {}).items())[:top_n],
        "top_dates": list(facets.get("date_start", {}).items())[:top_n],
        "top_topics": list(facets.get("topics", {}).items())[:top_n],
        "content_maps": content_maps,
        "filter_fields": ["people", "family_id", "topics", "source_type", "date_from", "date_to", "voice_role", "review_status"],
        "protocol": [
            "browse root with relation=maps or relation=families before searching",
            "search only when browsing is insufficient",
            "open summary/statements before evidence/raw",
            "KBase returns source material only; project inference remains in AG2",
        ],
    })


def kbase_browse(
    node_id: str = "root",
    *,
    relation: str = "children",
    filters: dict[str, Any] | None = None,
    cursor: int = 0,
    page_size: int = 20,
    vault_path: str | None = None,
) -> str:
    """Browse maps, families, dates, and source relationships without full-text search."""
    try:
        repo = _repo(vault_path)
    except CatalogUnavailableError as error:
        return _json({"error": str(error)})
    filters = filters or {}
    entries = list(repo.entries())
    node = repo.get(node_id)
    if node_id == "root":
        if relation == "maps":
            candidates = [entry for entry in entries if entry["object_type"] == "map"]
        elif relation == "families":
            candidates = [entry for entry in entries if entry["object_type"] == "family"]
        elif relation == "children":
            candidates = [entry for entry in entries if entry["object_type"] in {"map", "family"}]
        else:
            return _json({
                "error": "root relation must be children, maps, or families",
                "node_id": node_id,
                "relation": relation,
            })
    elif node and node["object_type"] == "family":
        candidates = [entry for entry in entries if node_id in entry.get("parent_ids", []) or entry.get("family_id") == node_id]
        candidates = [entry for entry in candidates if entry["source_id"] != node_id]
    elif node:
        if relation == "parents":
            candidates = [repo.get(parent) for parent in node.get("parent_ids", [])]
            candidates = [entry for entry in candidates if entry]
        else:
            family = node.get("family_id")
            candidates = [entry for entry in entries if family and entry.get("family_id") == family and entry["source_id"] != node_id]
    elif node_id.startswith("date:"):
        date = node_id.split(":", 1)[1]
        candidates = [entry for entry in entries if entry.get("date_start") == date]
    else:
        return _json({"error": f"unknown node_id: {node_id}"})
    candidates = [entry for entry in candidates if matches_filters(entry, filters)]
    candidates.sort(key=lambda item: (item.get("date_start") or "", item["title"], item["source_id"]))
    cursor = max(0, int(cursor)); page_size = max(1, min(int(page_size), 100))
    page = candidates[cursor:cursor + page_size]
    next_cursor = cursor + len(page) if cursor + len(page) < len(candidates) else None
    return _json({
        "tool_contract_version": TOOL_CONTRACT_VERSION,
        "catalog_version": repo.manifest.get("catalog_version"),
        "node_id": node_id,
        "relation": relation,
        "total": len(candidates),
        "cursor": cursor,
        "next_cursor": next_cursor,
        "results": [_compact(entry) for entry in page],
    })


def kbase_search(
    query: str,
    *,
    people: list[str] | str | None = None,
    family_id: list[str] | str | None = None,
    topics: list[str] | str | None = None,
    source_type: list[str] | str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    voice_role: list[str] | str | None = None,
    review_status: list[str] | str | None = None,
    scope: str = "sources",
    max_results: int = 5,
    cursor: int = 0,
    vault_path: str | None = None,
) -> str:
    """Search catalog metadata and conservative summaries with explainable ranking."""
    if not str(query).strip():
        return _json({"error": "query must not be empty"})
    if scope not in {"sources", "all"}:
        return _json({"error": "scope must be sources or all"})
    try:
        repo = _repo(vault_path)
    except CatalogUnavailableError as error:
        return _json({"error": str(error)})
    allowed_types = {"source_packet", "family", "source_note", "book", "video", "map"}
    entries = [entry for entry in repo.entries() if scope == "all" or entry["object_type"] in allowed_types]
    filters = {
        "people": people,
        "family_id": family_id,
        "topics": topics,
        "source_type": source_type,
        "date_from": date_from,
        "date_to": date_to,
        "voice_role": voice_role,
        "review_status": review_status,
    }
    max_results = max(1, min(int(max_results), 50)); cursor = max(0, int(cursor))
    lexical_limit = max(100, cursor + max_results + 20)
    lexical_ranked, protected_ids = lexical_rank_with_anchors(
        entries, query, filters=filters, limit=lexical_limit,
    )
    ranked = lexical_ranked
    search_backend: dict[str, Any] = {
        "mode": "lexical",
        "semantic_status": "not_requested",
        "models": None,
    }
    broad_terms = len(str(query).strip()) <= 2
    navigation_query = is_navigation_query(query)
    filtered_entries = [entry for entry in entries if matches_filters(entry, filters)]
    if navigation_query:
        search_backend["semantic_status"] = "skipped_navigation"
    elif lexical_ranked and not broad_terms:
        try:
            semantic_result, semantic_meta = request_semantic_search(
                repo.vault,
                catalog_version=str(repo.manifest.get("catalog_version") or ""),
                query=str(query),
                lexical_ids=[str(entry["source_id"]) for entry in lexical_ranked],
                allowed_ids=[str(entry["source_id"]) for entry in filtered_entries],
                candidate_limit=64,
                result_limit=100,
            )
            ranked = merge_semantic_results(
                lexical_ranked=lexical_ranked,
                entries_by_id={str(entry["source_id"]): entry for entry in filtered_entries},
                semantic_result=semantic_result,
                protected_ids=protected_ids,
                limit=lexical_limit,
            )
            search_backend = {
                "mode": "hybrid",
                "semantic_status": "completed",
                "models": {"embedding": "bge-m3", "reranker": "bge-reranker-v2-m3"},
                **semantic_meta,
            }
        except (SemanticUnavailableError, OSError, ValueError, RuntimeError) as error:
            ranked = lexical_ranked
            search_backend = {
                "mode": "lexical_fallback",
                "semantic_status": "unavailable",
                "failure_class": type(error).__name__,
                "models": None,
            }
    page = ranked[cursor:cursor + max_results]
    return _json({
        "tool_contract_version": TOOL_CONTRACT_VERSION,
        "catalog_version": repo.manifest.get("catalog_version"),
        "query": query,
        "scope": scope,
        "filters": {key: value for key, value in filters.items() if value not in (None, "", [])},
        "result_count": len(page),
        "cursor": cursor,
        "next_cursor": cursor + len(page) if len(ranked) > cursor + len(page) else None,
        "requires_refinement": broad_terms,
        "search_backend": search_backend,
        "results": [_compact(entry) for entry in page],
        "guidance": "Open summary/statements first; request evidence/raw only when needed.",
    })


def _slice_text(text: str, cursor: int, max_chars: int) -> tuple[str, int | None]:
    cursor = max(0, int(cursor)); max_chars = max(500, min(int(max_chars), 30000))
    chunk = text[cursor:cursor + max_chars]
    return chunk, cursor + len(chunk) if cursor + len(chunk) < len(text) else None


def _has_packet_statements(record: dict[str, Any]) -> bool:
    return any(
        isinstance(record.get(field), list)
        and any(isinstance(item, dict) and statement_text(item, field) for item in record.get(field, []))
        for field in STATEMENT_FIELDS
    )


def _with_distilled_statements(record: dict[str, Any], candidate_record: dict[str, Any]) -> dict[str, Any]:
    merged = {**record, **candidate_record}
    for field in STATEMENT_FIELDS:
        merged[field] = candidate_record.get(field, []) if isinstance(candidate_record.get(field), list) else []
    return merged


def kbase_open(
    source_id: str | None = None,
    *,
    path: str | None = None,
    layer: str = "summary",
    cursor: int = 0,
    max_chars: int = 8000,
    vault_path: str | None = None,
) -> str:
    """Open exactly one requested disclosure layer with a bounded response."""
    if layer not in {"summary", "statements", "evidence", "raw", "visual"}:
        return _json({"error": "layer must be summary, statements, evidence, raw, or visual"})
    try:
        repo = _repo(vault_path)
    except CatalogUnavailableError as error:
        return _json({"error": str(error)})
    entry = repo.get(source_id) if source_id else repo.entry_for_path(path or "")
    if not entry:
        return _json({"error": "source not found", "source_id": source_id, "path": path})
    if layer not in entry.get("available_layers", []) and layer != "summary":
        return _json({"error": f"layer unavailable: {layer}", "source_id": entry["source_id"], "available_layers": entry.get("available_layers", [])})

    if layer == "summary":
        content: Any = _compact(entry)
    elif entry["object_type"] == "source_packet":
        packet = repo.read_packet(entry)
        record = packet.get("record") if isinstance(packet.get("record"), dict) else {}
        distilled = repo.read_distilled_candidate(entry)
        if distilled:
            candidate_record = distilled.get("record") if isinstance(distilled.get("record"), dict) else {}
            if not _has_packet_statements(record):
                record = _with_distilled_statements(record, candidate_record)
        fields = STATEMENT_FIELDS
        if layer == "statements":
            content = {key: [
                {k: v for k, v in normalize_statement_item(item, key).items()
                 if k not in {"evidence_quote"}}
                for item in record.get(key, []) if isinstance(item, dict)
            ] for key in fields if isinstance(record.get(key), list)}
        elif layer == "evidence":
            content = {key: [normalize_statement_item(item, key) if isinstance(item, dict) else item
                             for item in record.get(key, [])]
                       for key in fields if isinstance(record.get(key), list)}
        elif layer == "visual":
            content = {"visual_gaps": record.get("visual_gaps", []), "extraction_layers": packet.get("extraction_layers", [])}
        else:
            raw_path = entry.get("paths", {}).get("raw")
            file_path = repo.safe_path(raw_path) if raw_path else None
            if not file_path or not file_path.is_file():
                return _json({"error": "raw object unavailable", "raw_path": raw_path})
            if file_path.suffix.lower() not in {".md", ".txt", ".json", ".csv", ".yaml", ".yml"}:
                return _json({"source_id": entry["source_id"], "layer": "raw", "raw_path": raw_path, "binary": True, "guidance": "Use the packet evidence layer or a modality-specific reader."})
            try:
                content = file_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                content = file_path.read_text(encoding="gbk", errors="replace")
    else:
        wiki_path = entry.get("paths", {}).get("wiki")
        file_path = repo.safe_path(wiki_path) if wiki_path else None
        if not file_path or not file_path.is_file():
            return _json({"error": "wiki object unavailable", "path": wiki_path})
        content = file_path.read_text(encoding="utf-8", errors="replace")

    rendered = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False, indent=2)
    chunk, next_cursor = _slice_text(rendered, cursor, max_chars)
    return _json({
        "tool_contract_version": TOOL_CONTRACT_VERSION,
        "catalog_version": repo.manifest.get("catalog_version"),
        "source_id": entry["source_id"],
        "title": entry["title"],
        "layer": layer,
        "cursor": max(0, int(cursor)),
        "next_cursor": next_cursor,
        "truncated": next_cursor is not None,
        "content": chunk,
        "provenance": entry.get("paths", {}),
    })


def kbase_trace(source_id: str, *, vault_path: str | None = None) -> str:
    """Return the source-to-family-to-evidence provenance chain without inference."""
    try:
        repo = _repo(vault_path)
    except CatalogUnavailableError as error:
        return _json({"error": str(error)})
    entry = repo.get(source_id)
    if not entry:
        return _json({"error": f"source not found: {source_id}"})
    packet = repo.read_packet(entry) if entry["object_type"] == "source_packet" else {}
    distilled = repo.read_distilled_candidate(entry) if entry["object_type"] == "source_packet" else {}
    record = packet.get("record") if isinstance(packet.get("record"), dict) else {}
    if distilled:
        candidate_record = distilled.get("record") if isinstance(distilled.get("record"), dict) else {}
        if not _has_packet_statements(record):
            record = _with_distilled_statements(record, candidate_record)
    anchors = []
    for field in ("methods", "claims", "risks", "contradictions", "definitions", "examples"):
        for index, item in enumerate(record.get(field, []) if isinstance(record.get(field), list) else []):
            if isinstance(item, dict):
                anchors.append({"ref": f"{field}[{index}]", "anchor": item.get("evidence_anchor"), "has_quote": bool(item.get("evidence_quote"))})
    return _json({
        "tool_contract_version": TOOL_CONTRACT_VERSION,
        "catalog_version": repo.manifest.get("catalog_version"),
        "source_id": source_id,
        "family_id": entry.get("family_id"),
        "parent_ids": entry.get("parent_ids", []),
        "paths": entry.get("paths", {}),
        "available_layers": entry.get("available_layers", []),
        "evidence_anchors": anchors,
        "warnings": entry.get("warnings", []),
    })


def _instrument(event_type: str, function):
    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        started = time.perf_counter()
        rendered = function(*args, **kwargs)
        if os.environ.get("KBASE_TELEMETRY_DISABLED") == "1":
            return rendered
        vault_path = kwargs.get("vault_path")
        # Temporary/test vaults remain side-effect free. Production KBase usage
        # writes metadata only to the project data directory, never to raw/.
        try:
            repo = _repo(vault_path)
            if repo.vault != DEFAULT_VAULT.resolve():
                return rendered
            payload = json.loads(rendered)
            results = payload.get("results", []) if isinstance(payload, dict) else []
            source_ids = [str(item.get("source_id")) for item in results if isinstance(item, dict) and item.get("source_id")]
            if isinstance(payload, dict) and payload.get("source_id"):
                source_ids.append(str(payload["source_id"]))
            outcome = "error" if isinstance(payload, dict) and payload.get("error") else "ok"
            if event_type == "search" and not results and outcome == "ok":
                outcome = "no_result"
            if isinstance(payload, dict) and payload.get("requires_refinement"):
                outcome = "refine"
            from .telemetry import new_event, record_usage
            record_usage(new_event(
                event_type=event_type,
                tool=function.__name__,
                catalog_version=payload.get("catalog_version") if isinstance(payload, dict) else None,
                latency_ms=(time.perf_counter() - started) * 1000,
                query=(args[0] if args and event_type == "search" else kwargs.get("query")),
                filters=(payload.get("filters", {}) if isinstance(payload, dict) else {}),
                source_ids=list(dict.fromkeys(source_ids)),
                layer=(payload.get("layer") if isinstance(payload, dict) else None),
                result_count=(payload.get("result_count") if isinstance(payload, dict) else None),
                outcome=outcome,
            ))
        except Exception:
            # Telemetry must never break a read path.
            pass
        return rendered
    return wrapped


kbase_overview = _instrument("overview", kbase_overview)
kbase_browse = _instrument("browse", kbase_browse)
kbase_search = _instrument("search", kbase_search)
kbase_open = _instrument("open", kbase_open)
kbase_trace = _instrument("trace", kbase_trace)
