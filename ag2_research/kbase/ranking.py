"""Deterministic, explainable lexical ranking for KBase catalog entries."""
from __future__ import annotations

import re
from collections import Counter
from typing import Any, Iterable


DATE_RE = re.compile(r"20\d{2}[-./]\d{1,2}[-./]\d{1,2}")


def normalize(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "").lower()).strip()


def query_terms(query: str) -> list[str]:
    text = normalize(query)
    chunks = re.findall(r"[a-z0-9]+(?:[-./][a-z0-9]+)*|[\u4e00-\u9fff]+", text)
    return list(dict.fromkeys(term for term in chunks if len(term) >= 2))


def _contains(haystack: str, needle: str) -> bool:
    return bool(needle and needle in haystack)


def _query_coverage(query: str, needles: list[str]) -> float:
    compact = re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", query)
    if not compact:
        return 0.0
    covered = [False] * len(compact)
    for needle in needles:
        needle = re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", needle)
        if len(needle) < 2:
            continue
        start = 0
        while True:
            index = compact.find(needle, start)
            if index < 0:
                break
            for pos in range(index, min(index + len(needle), len(covered))):
                covered[pos] = True
            start = index + 1
    return sum(covered) / len(covered)


def score_entry(entry: dict[str, Any], query: str) -> tuple[int, list[str], float]:
    q = normalize(query)
    terms = query_terms(query)
    source_id = normalize(entry.get("source_id"))
    title = normalize(entry.get("title"))
    aliases = normalize(" ".join(entry.get("aliases", [])))
    people = normalize(" ".join(entry.get("people", [])))
    family = normalize(entry.get("family_id"))
    topics = normalize(" ".join(entry.get("topics", [])))
    summary = normalize(entry.get("summary"))
    date_text = normalize(f"{entry.get('date_start') or ''} {entry.get('date_end') or ''}")
    score = 0
    reasons: list[str] = []
    matched_needles: list[str] = []

    if q == source_id:
        score += 2000
        reasons.append("exact_source_id")
        matched_needles.append(q)
    dates = [item.replace("/", "-").replace(".", "-") for item in DATE_RE.findall(q)]
    for date in dates:
        if date in date_text:
            score += 400
            reasons.append("exact_date")
            matched_needles.append(date)
    if q and q in title:
        score += 500
        reasons.append("full_query_in_title")
        matched_needles.append(q)

    field_weights = (
        (title, 80, "title"),
        (aliases, 60, "alias"),
        (people, 70, "people"),
        (family, 45, "family"),
        (topics, 35, "topic"),
        (summary, 8, "summary"),
    )
    for term in terms:
        for haystack, weight, reason in field_weights:
            if _contains(haystack, term):
                score += weight + min(haystack.count(term), 3)
                reasons.append(f"{reason}:{term}")
                matched_needles.append(term)
    # Natural Chinese questions often do not contain spaces. Match only complete
    # catalog values (people/topics), never arbitrary character n-grams.
    for label, values, weight in (
        ("title_value", [entry.get("title", ""), *entry.get("aliases", [])], 160),
        ("person_value", entry.get("people", []), 120),
        ("topic_value", entry.get("topics", []), 55),
    ):
        for value in values:
            value = normalize(value)
            if len(value) >= 2 and value in q:
                score += weight
                reasons.append(f"{label}:{value}")
                matched_needles.append(value)
    if terms:
        matched = sum(any(_contains(field[0], term) for field in field_weights) for term in terms)
        score += int(100 * matched / len(terms))
    coverage = _query_coverage(q, matched_needles)
    score += int(coverage * 300)
    reasons.append(f"query_coverage:{coverage:.2f}")

    # Relevance remains primary, but equally relevant reviewed sources should
    # outrank low-confidence material that still needs extraction review.
    reliability_adjustment = {
        "high": 25, "medium": 10, "low": -10, "unverified": -15,
    }.get(normalize(entry.get("reliability")), 0)
    review_adjustment = {
        "reviewed": 30, "source_only": 10, "review_required": -15,
    }.get(normalize(entry.get("review_status")), 0)
    warning_adjustment = -min(20, 2 * len(entry.get("warnings", []) or []))
    quality_adjustment = reliability_adjustment + review_adjustment + warning_adjustment
    score += quality_adjustment
    reasons.append(f"quality_adjustment:{quality_adjustment}")
    return score, list(dict.fromkeys(reasons)), coverage


def matches_filters(entry: dict[str, Any], filters: dict[str, Any]) -> bool:
    list_fields = {"people", "topics", "available_layers"}
    for key, expected in filters.items():
        if expected in (None, "", []):
            continue
        values = expected if isinstance(expected, list) else [expected]
        wanted = {normalize(item) for item in values}
        if key in list_fields:
            actual = {normalize(item) for item in entry.get(key, [])}
            if not any(any(w in a or a in w for a in actual) for w in wanted):
                return False
        elif key == "date_from":
            if not entry.get("date_end") or str(entry["date_end"]) < str(expected):
                return False
        elif key == "date_to":
            if not entry.get("date_start") or str(entry["date_start"]) > str(expected):
                return False
        else:
            actual = normalize(entry.get(key))
            if not any(w == actual or w in actual for w in wanted):
                return False
    return True


def rank_entries(
    entries: Iterable[dict[str, Any]],
    query: str,
    *,
    filters: dict[str, Any] | None = None,
    limit: int = 20,
    diversify: bool = True,
) -> list[dict[str, Any]]:
    filters = filters or {}
    scored = []
    for entry in entries:
        if not matches_filters(entry, filters):
            continue
        score, reasons, coverage = score_entry(entry, query)
        compact_query = re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", normalize(query))
        has_identity_match = any(
            reason.startswith(("title_value:", "person_value:")) for reason in reasons
        )
        minimum_coverage = 0.15 if has_identity_match else (0.30 if len(compact_query) >= 6 else 0.0)
        if score >= 80 and coverage >= minimum_coverage:
            scored.append((score, entry, reasons))
    scored.sort(key=lambda item: (-item[0], item[1]["source_id"]))
    if not diversify:
        chosen = scored[:limit]
    else:
        chosen = []
        deferred = []
        family_counts: Counter[str] = Counter()
        for item in scored:
            family = str(item[1].get("family_id") or item[1]["source_id"])
            if family_counts[family] < 2:
                chosen.append(item)
                family_counts[family] += 1
            else:
                deferred.append(item)
            if len(chosen) >= limit:
                break
        if len(chosen) < limit:
            chosen.extend(deferred[: limit - len(chosen)])
    return [dict(entry, _score=score, _match_reasons=reasons[:12]) for score, entry, reasons in chosen]
