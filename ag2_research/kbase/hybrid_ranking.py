"""Lexical anchor extraction for the progressive KBase hybrid search path."""
from __future__ import annotations

import re
from typing import Any, Iterable

from .ranking import rank_entries


_DATE = re.compile(r"20\d{2}[-./]\d{1,2}[-./]\d{1,2}")
_COMPARISON_PREFIX = re.compile(r"^\s*(?:对比|比较)\s*", flags=re.I)
_COMPARISON_SEPARATOR = re.compile(r"\s*(?:与|\bvs\.?\b)\s*", flags=re.I)
_EXPLICIT_VS = re.compile(r"\bvs\.?\b", flags=re.I)
_NAVIGATION_PREFIX = re.compile(r"^\s*(?:浏览|导航|browse\b|navigate\b)", flags=re.I)
_NAVIGATION_COMMAND = re.compile(r"^\s*(?:打开|查看|列出|展示)\s*", flags=re.I)
_NAVIGATION_NOUN = re.compile(r"(?:家族|索引|目录|地图|来源|资料|课程|节点|条目)")


def is_navigation_query(query: str) -> bool:
    """Return whether *query* is a catalog-navigation command.

    Navigation is intentionally lexical-only: maps, metadata, exact names, and
    provenance remain authoritative.  Callers can use this predicate to avoid
    making a semantic request; protecting the entire lexical result below also
    keeps the final order lexical if an older caller still requests semantics.
    """
    text = str(query).strip()
    if not text:
        return False
    if _NAVIGATION_PREFIX.match(text):
        return True
    return bool(_NAVIGATION_COMMAND.match(text) and _NAVIGATION_NOUN.search(text))


def comparison_parts(query: str) -> list[str]:
    text = str(query).strip()
    if not text or is_navigation_query(text):
        return []
    prefix = _COMPARISON_PREFIX.match(text)
    if prefix:
        text = text[prefix.end():]
    elif not _EXPLICIT_VS.search(text):
        return []
    return [
        part.strip(" \t\r\n，。、“”‘’《》()（）")
        for part in _COMPARISON_SEPARATOR.split(text)
        if part.strip(" \t\r\n，。、“”‘’《》()（）")
    ]


def lexical_rank_with_anchors(
    entries: Iterable[dict[str, Any]],
    query: str,
    *,
    filters: dict[str, Any] | None = None,
    limit: int = 100,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Return existing lexical order plus identities reranking cannot demote."""
    materialized = list(entries)
    filters = filters or {}
    if is_navigation_query(query):
        ranked = rank_entries(materialized, query, filters=filters, limit=limit)
        # This makes the merge result exactly lexical even for callers that
        # have not yet adopted ``is_navigation_query`` as a semantic bypass.
        return ranked, [str(entry["source_id"]) for entry in ranked]

    parts = comparison_parts(query)
    protected: list[str] = []
    if len(parts) >= 2:
        groups = [rank_entries(materialized, part, filters=filters, limit=limit) for part in parts]
        ranked: list[dict[str, Any]] = []
        seen: set[str] = set()
        # Preserve two lexical candidates per comparison branch, interleaved
        # so every branch remains visible before semantic supplementation.
        for index in range(2):
            for group in groups:
                if index >= len(group):
                    continue
                source_id = str(group[index]["source_id"])
                if source_id not in protected:
                    protected.append(source_id)
        for index in range(max((len(group) for group in groups), default=0)):
            for group in groups:
                if index < len(group):
                    source_id = str(group[index]["source_id"])
                    if source_id not in seen:
                        ranked.append(group[index])
                        seen.add(source_id)
        for entry in rank_entries(materialized, query, filters=filters, limit=limit):
            source_id = str(entry["source_id"])
            if source_id not in seen:
                ranked.append(entry)
                seen.add(source_id)
    else:
        ranked = rank_entries(materialized, query, filters=filters, limit=limit)
        # ``rank_entries`` already rejects weak/no-coverage matches.  Keep its
        # first two accepted anchors stable, then let BGE reorder/supplement
        # everything below them using scores that are meaningful only within
        # this query.
        protected.extend(str(entry["source_id"]) for entry in ranked[:2])

    has_date = bool(_DATE.search(str(query)))
    for entry in ranked:
        reasons = [str(reason) for reason in entry.get("_match_reasons") or []]
        identity = any(reason in {"exact_source_id", "full_query_in_title"} for reason in reasons)
        dated_identity = has_date and "exact_date" in reasons and any(
            reason.startswith(("title:", "alias:", "people:", "title_value:", "person_value:"))
            for reason in reasons
        )
        if identity or dated_identity:
            source_id = str(entry["source_id"])
            if source_id not in protected:
                protected.append(source_id)
    ranked = ranked[:limit]
    protected_set = set(protected)
    protected = [str(entry["source_id"]) for entry in ranked if str(entry["source_id"]) in protected_set]
    return ranked, protected
