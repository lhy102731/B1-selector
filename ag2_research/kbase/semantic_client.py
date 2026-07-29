"""Fail-open client and deterministic merge policy for KBase semantic search."""
from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Iterable


RUNTIME_RELATIVE = Path("wiki/outputs/runtime/ag2-kbase-semantic")
SERVICE_SCHEMA = "kbase.semantic_service.v1"


class SemanticUnavailableError(RuntimeError):
    pass


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    payload = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    with temporary.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def semantic_health(vault: Path, *, catalog_version: str, max_age_seconds: float = 15.0) -> dict[str, Any]:
    path = vault / RUNTIME_RELATIVE / "health.json"
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise SemanticUnavailableError("semantic service health is unavailable")
    if not isinstance(document, dict) or document.get("schema_version") != SERVICE_SCHEMA:
        raise SemanticUnavailableError("semantic service health schema mismatch")
    if document.get("status") != "READY":
        raise SemanticUnavailableError("semantic service is not ready")
    heartbeat = document.get("heartbeat_epoch")
    if not isinstance(heartbeat, (int, float)) or time.time() - float(heartbeat) > max_age_seconds:
        raise SemanticUnavailableError("semantic service heartbeat is stale")
    if str(document.get("catalog_version") or "") != str(catalog_version):
        raise SemanticUnavailableError("semantic index is not bound to the active catalog")
    if document.get("models") != {"embedding": "bge-m3", "reranker": "bge-reranker-v2-m3"}:
        raise SemanticUnavailableError("semantic service model pair mismatch")
    return document


def request_semantic_search(
    vault: Path,
    *,
    catalog_version: str,
    query: str,
    lexical_ids: Iterable[str],
    allowed_ids: Iterable[str],
    candidate_limit: int = 64,
    result_limit: int = 100,
    timeout_seconds: float = 5.0,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Request one warm local-GPU search through the offline file queue."""
    health = semantic_health(vault, catalog_version=catalog_version)
    runtime = vault / RUNTIME_RELATIVE
    request_id = uuid.uuid4().hex
    request_path = runtime / "requests" / f"{request_id}.json"
    response_path = runtime / "responses" / f"{request_id}.json"
    payload = {
        "schema_version": SERVICE_SCHEMA,
        "request_id": request_id,
        "query": str(query),
        "lexical_ids": list(dict.fromkeys(str(value) for value in lexical_ids)),
        "allowed_ids": list(dict.fromkeys(str(value) for value in allowed_ids)),
        "candidate_limit": int(candidate_limit),
        "result_limit": int(result_limit),
        "catalog_version": str(catalog_version),
    }
    _atomic_json(request_path, payload)
    deadline = time.monotonic() + max(0.1, float(timeout_seconds))
    try:
        while time.monotonic() < deadline:
            try:
                response = json.loads(response_path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                time.sleep(0.02)
                continue
            except (OSError, UnicodeError, json.JSONDecodeError) as error:
                raise SemanticUnavailableError(f"semantic response unreadable: {error}") from error
            if not isinstance(response, dict) or response.get("request_id") != request_id:
                raise SemanticUnavailableError("semantic response identity mismatch")
            if response.get("status") != "COMPLETED" or not isinstance(response.get("result"), dict):
                raise SemanticUnavailableError(str(response.get("error") or "semantic request failed"))
            result = response["result"]
            if str(result.get("catalog_version") or "") != str(catalog_version):
                raise SemanticUnavailableError("semantic response catalog binding mismatch")
            return result, {
                "service_instance_id": health.get("instance_id"),
                "timing_seconds": response.get("timing_seconds"),
                "index_source_fingerprint": result.get("index_source_fingerprint"),
            }
        raise SemanticUnavailableError("semantic request timed out")
    finally:
        request_path.unlink(missing_ok=True)
        response_path.unlink(missing_ok=True)


def _diversify(
    ordered: list[dict[str, Any]], *, limit: int, protected_ids: set[str], family_limit: int = 2,
) -> list[dict[str, Any]]:
    chosen: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    family_counts: dict[str, int] = {}
    for entry in ordered:
        source_id = str(entry.get("source_id") or "")
        family = str(entry.get("family_id") or source_id)
        if source_id in protected_ids or family_counts.get(family, 0) < family_limit:
            chosen.append(entry)
            family_counts[family] = family_counts.get(family, 0) + 1
        else:
            deferred.append(entry)
        if len(chosen) >= limit:
            return chosen[:limit]
    if len(chosen) < limit:
        chosen.extend(deferred[: limit - len(chosen)])
    return chosen[:limit]


def merge_semantic_results(
    *,
    lexical_ranked: list[dict[str, Any]],
    entries_by_id: dict[str, dict[str, Any]],
    semantic_result: dict[str, Any],
    protected_ids: Iterable[str] = (),
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Merge BGE results while preserving deterministic identity/date anchors.

    A lexical anchor is required by design.  This prevents a dense model from
    inventing results for out-of-domain or gibberish queries and keeps semantic
    retrieval in its approved "candidate supplement" role.
    """
    if not lexical_ranked:
        return []
    protected_order = list(dict.fromkeys(str(value) for value in protected_ids))
    protected = set(protected_order)
    lexical_by_id = {str(entry["source_id"]): entry for entry in lexical_ranked}
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()

    def append_entry(source_id: str, semantic_row: dict[str, Any] | None = None) -> None:
        if source_id in seen:
            return
        base = lexical_by_id.get(source_id) or entries_by_id.get(source_id)
        if base is None:
            return
        entry = dict(base)
        if semantic_row is not None:
            entry["_score"] = float(semantic_row["reranker_score"])
            reasons = list(entry.get("_match_reasons") or [])
            reasons.extend([
                "semantic:bge-m3",
                "reranker:bge-reranker-v2-m3",
                f"semantic_object:{semantic_row.get('object_id')}",
            ])
            entry["_match_reasons"] = list(dict.fromkeys(reasons))[:12]
            entry["_semantic"] = {
                "object_id": semantic_row.get("object_id"),
                "document_type": semantic_row.get("document_type"),
                "dense_rank": semantic_row.get("dense_rank"),
                "lexical_rank": semantic_row.get("lexical_rank"),
                "rrf_score": semantic_row.get("rrf_score"),
                "reranker_score": semantic_row.get("reranker_score"),
            }
        ordered.append(entry)
        seen.add(source_id)

    semantic_rows = semantic_result.get("results") if isinstance(semantic_result, dict) else []
    semantic_by_id = {
        str(row.get("source_id")): row
        for row in semantic_rows or []
        if isinstance(row, dict) and str(row.get("source_id") or "") in entries_by_id
    }
    for source_id in protected_order:
        append_entry(source_id, semantic_by_id.get(source_id))
    for row in semantic_rows or []:
        if isinstance(row, dict):
            append_entry(str(row.get("source_id") or ""), row)
    for entry in lexical_ranked:
        append_entry(str(entry["source_id"]), semantic_by_id.get(str(entry["source_id"])))
    return _diversify(ordered, limit=max(1, int(limit)), protected_ids=protected)
