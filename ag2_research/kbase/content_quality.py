"""Fail-closed quality gate for OCR/ASR content repair candidates.

This module validates candidate output only.  It never writes source packets,
raw objects, or the published catalog.
"""
from __future__ import annotations

import re
from typing import Any

from .adapters import STATEMENT_FIELDS, statement_text


ANCHOR_RE = re.compile(
    r"(?:\b(?:page|p|line|lines|l|timestamp|time)\s*[:#.]?\s*\d+|"
    r"\[(?:p(?:age)?|l(?:ine)?|t(?:ime)?)?\s*\d+(?::\d{2}){0,2}(?:[-~]\d+(?::\d{2}){0,2})?\]|"
    r"\b\d{1,2}:\d{2}(?::\d{2})?(?:\.\d+)?\b|"
    r"第\s*\d+\s*页|第\s*\d+\s*行)",
    re.IGNORECASE,
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _has_resolvable_evidence(item: dict[str, Any]) -> bool:
    quote = _text(item.get("evidence_quote"))
    anchor = _text(item.get("evidence_anchor"))
    # A verbatim quote is independently traceable by search; an anchor must be
    # recognisably page/line/time based, rather than an opaque label.
    return bool(quote or ANCHOR_RE.search(anchor))


def validate_extraction_candidate(
    candidate: dict[str, Any], *, minimum_confidence: float = 0.80,
) -> dict[str, Any]:
    """Return a deterministic decision: accept, review, or reject.

    ``reject`` means structural/provenance failure. ``review`` means the
    content is structurally valid but extraction confidence is below policy.
    """
    errors: list[str] = []
    record = candidate.get("record")
    if not isinstance(record, dict):
        return {"decision": "reject", "errors": ["record must be an object"], "warnings": []}

    source_id = _text(candidate.get("source_id") or candidate.get("sha256"))
    raw_path = _text(candidate.get("raw_path") or candidate.get("original_path"))
    if not source_id:
        errors.append("source_id is required")
    if not raw_path:
        errors.append("raw_path provenance is required")

    summary = _text(record.get("summary"))
    accepted = 0
    confidences: list[float] = []
    for field in STATEMENT_FIELDS:
        values = record.get(field, [])
        if values is None:
            continue
        if not isinstance(values, list):
            errors.append(f"{field} must be a list")
            continue
        for index, item in enumerate(values):
            if not isinstance(item, dict):
                errors.append(f"{field}[{index}] must be an object")
                continue
            text = statement_text(item, field).strip()
            if not text:
                errors.append(f"{field}[{index}] has no explicit statement")
                continue
            if summary and text == summary:
                errors.append(f"{field}[{index}] duplicates summary")
            if not _has_resolvable_evidence(item):
                errors.append(f"{field}[{index}] lacks page/line/timestamp anchor or quote")
            confidence = item.get("confidence", candidate.get("confidence"))
            try:
                confidence_value = float(confidence)
            except (TypeError, ValueError):
                errors.append(f"{field}[{index}] has no numeric confidence")
            else:
                if not 0 <= confidence_value <= 1:
                    errors.append(f"{field}[{index}] confidence is outside 0..1")
                else:
                    confidences.append(confidence_value)
            accepted += 1

    if not accepted:
        errors.append("at least one explicit non-summary statement is required")
    if errors:
        return {"decision": "reject", "errors": errors, "warnings": []}
    low = [value for value in confidences if value < minimum_confidence]
    if low:
        return {"decision": "review", "errors": [],
                "warnings": ["low confidence requires human or non-GPT model review"],
                "minimum_confidence": min(low)}
    return {"decision": "accept", "errors": [], "warnings": [],
            "minimum_confidence": min(confidences)}
