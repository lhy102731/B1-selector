"""Candidate-only repair for legacy packet statement aliases.

Raw objects, source packets, and the published catalog are never modified.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from collections import Counter

from .adapters import STATEMENT_FIELDS, normalize_statement_item, statement_text
from .repository import KBaseRepository


DEFAULT_CANDIDATE_ROOT = Path("wiki/outputs/candidates/ag2-kbase/content-layer-repair")


def _legacy_layers(record: dict[str, Any]) -> tuple[bool, bool]:
    items = [item for field in STATEMENT_FIELDS
             for item in (record.get(field, []) if isinstance(record.get(field), list) else [])
             if isinstance(item, dict)]
    statements = any(str(item.get("text") or item.get("claim") or item.get("description") or "").strip()
                     for item in items)
    evidence = any(item.get("evidence_anchor") or item.get("evidence_quote") for item in items)
    return statements, evidence


def _canonical_layers(record: dict[str, Any]) -> tuple[bool, bool]:
    pairs = [(field, item) for field in STATEMENT_FIELDS
             for item in (record.get(field, []) if isinstance(record.get(field), list) else [])
             if isinstance(item, dict)]
    statements = any(statement_text(item, field) for field, item in pairs)
    evidence = any(statement_text(item, field) and
                   (item.get("evidence_anchor") or item.get("evidence_quote"))
                   for field, item in pairs)
    return statements, evidence


def _extraction_route(packet: dict[str, Any], record: dict[str, Any]) -> str:
    modality = json.dumps({
        "source_type": record.get("source_type"),
        "extraction_layers": packet.get("extraction_layers"),
    }, ensure_ascii=False).lower()
    haystack = json.dumps({
        "source_type": record.get("source_type"),
        "review_flags": record.get("review_flags"),
        "visual_gaps": record.get("visual_gaps"),
        "extraction_layers": packet.get("extraction_layers"),
        "extraction_status": packet.get("extraction_status"),
    }, ensure_ascii=False).lower()
    if any(marker in modality for marker in ("audio", "video", "asr", "音频", "视频")):
        return "requires_asr"
    if any(marker in haystack for marker in (
        "ocr", "image", "visual", "pdf", "office", "document", ".doc",
        "图片", "图像", "正文缺失",
    )):
        return "requires_ocr_or_visual_extraction"
    return "requires_manual_review"


def _normalized_candidate(packet: dict[str, Any]) -> tuple[dict[str, Any], int]:
    candidate = deepcopy(packet)
    record = candidate.get("record") if isinstance(candidate.get("record"), dict) else {}
    changed = 0
    for field in STATEMENT_FIELDS:
        values = record.get(field)
        if not isinstance(values, list):
            continue
        normalized = []
        for item in values:
            if not isinstance(item, dict):
                normalized.append(item)
                continue
            updated = normalize_statement_item(item, field)
            changed += int(updated != item)
            normalized.append(updated)
        record[field] = normalized
    return candidate, changed


def generate_content_repair_candidates(
    *, vault_path: str | Path | None = None, output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Classify published gaps and write mechanically derived candidates."""
    repo = KBaseRepository(vault_path)
    root = (repo.vault / (output_root or DEFAULT_CANDIDATE_ROOT)).resolve()
    root.relative_to(repo.vault)
    root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    counts = {
        "legacy_missing_statements": 0, "legacy_missing_evidence": 0,
        "mechanically_recoverable": 0, "requires_ocr_or_visual_extraction": 0,
        "requires_asr": 0, "requires_manual_review": 0,
    }

    for entry in sorted(repo.entries(), key=lambda value: str(value.get("source_id"))):
        if entry.get("object_type") != "source_packet":
            continue
        packet = repo.read_packet(entry)
        record = packet.get("record") if isinstance(packet.get("record"), dict) else {}
        old_statements, old_evidence = _legacy_layers(record)
        if old_statements and old_evidence:
            continue
        counts["legacy_missing_statements"] += int(not old_statements)
        counts["legacy_missing_evidence"] += int(not old_evidence)
        new_statements, new_evidence = _canonical_layers(record)
        row = {
            "source_id": str(entry["source_id"]),
            "packet_path": entry.get("paths", {}).get("packet"),
            "legacy_missing": [name for name, present in (
                ("statements", old_statements), ("evidence", old_evidence)) if not present],
            "canonical_layers": {"statements": new_statements, "evidence": new_evidence},
        }
        if not old_statements and new_statements:
            candidate, changed = _normalized_candidate(packet)
            candidate_path = root / f"{entry['source_id']}.json"
            candidate_path.write_text(json.dumps(candidate, ensure_ascii=False, indent=2), encoding="utf-8")
            row.update({"classification": "mechanically_recoverable", "normalized_items": changed,
                        "candidate_path": str(candidate_path.relative_to(repo.vault)).replace("\\", "/")})
            counts["mechanically_recoverable"] += 1
        else:
            route = _extraction_route(packet, record)
            row["classification"] = route
            counts[route] += 1
        rows.append(row)

    report = {
        "report_schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "catalog_version": repo.manifest.get("catalog_version"),
        "policy": {
            "candidate_only": True, "published_catalog_modified": False,
            "raw_modified": False, "summary_used_as_evidence": False,
            "evidence_rule": "explicit statement plus existing evidence_anchor or evidence_quote",
        },
        "counts": counts,
        "items": rows,
    }
    report_path = root / "content-layer-gap-report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report["report_path"] = str(report_path.relative_to(repo.vault)).replace("\\", "/")
    return report


def _size_bucket(size: int | None) -> str:
    if size is None:
        return "unknown"
    if size < 10 * 1024 * 1024:
        return "small_lt_10mb"
    if size < 100 * 1024 * 1024:
        return "medium_10_100mb"
    if size < 1024 * 1024 * 1024:
        return "large_100mb_1gb"
    return "xlarge_ge_1gb"


def _recommended_extractor(route: str, suffix: str) -> dict[str, Any]:
    if route == "requires_asr":
        return {
            "method": "asr_with_timestamps",
            "steps": ["extract_audio_track", "speech_to_text_with_timestamps",
                      "preserve_speaker_or_segment_boundaries", "distill_only_from_transcript"],
        }
    if suffix in {".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx"}:
        method = "office_native_text_and_embedded_visual_extraction_then_ocr"
    elif suffix == ".pdf":
        method = "pdf_text_layout_then_page_ocr_and_visual_review"
    elif suffix in {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}:
        method = "image_ocr_with_visual_review"
    else:
        method = "local_text_extraction_then_ocr_or_visual_fallback"
    return {
        "method": method,
        "steps": ["extract_existing_text_and_layout", "ocr_unreadable_or_image_regions",
                  "preserve_page_or_line_anchors", "distill_only_from_extracted_content"],
    }


def generate_reextraction_queue(
    *, vault_path: str | Path | None = None, output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Write a deterministic queue for gaps that aliases cannot recover."""
    repo = KBaseRepository(vault_path)
    root = (repo.vault / (output_root or DEFAULT_CANDIDATE_ROOT)).resolve()
    root.relative_to(repo.vault)
    root.mkdir(parents=True, exist_ok=True)
    tasks: list[dict[str, Any]] = []

    for entry in sorted(repo.entries(), key=lambda value: str(value.get("source_id"))):
        if entry.get("object_type") != "source_packet":
            continue
        packet = repo.read_packet(entry)
        record = packet.get("record") if isinstance(packet.get("record"), dict) else {}
        statements, evidence = _canonical_layers(record)
        if statements and evidence:
            continue
        # This queue is deliberately limited to content gaps; navigation/raw-only
        # gaps belong to their own maintenance queue.
        if statements:
            continue
        route = _extraction_route(packet, record)
        raw_path = entry.get("paths", {}).get("raw")
        raw_file = None
        if raw_path:
            try:
                candidate = repo.safe_path(raw_path)
                raw_file = candidate if candidate.is_file() else None
            except ValueError:
                raw_file = None
        suffix = raw_file.suffix.lower() if raw_file else Path(str(raw_path or "")).suffix.lower()
        size = raw_file.stat().st_size if raw_file else None
        priority = "P0_READY" if raw_file else "BLOCKED_RAW_UNAVAILABLE"
        tasks.append({
            "queue_id": f"content-reextract:{entry['source_id']}",
            "source_id": str(entry["source_id"]),
            "packet_path": entry.get("paths", {}).get("packet"),
            "raw_path": raw_path,
            "source_type": record.get("source_type") or entry.get("source_type") or "unknown",
            "raw_format": suffix or "unknown",
            "raw_size_bytes": size,
            "size_bucket": _size_bucket(size),
            "priority": priority,
            "gaps": ["statements", "evidence"],
            "route": route,
            "suggested_extraction": _recommended_extractor(route, suffix),
            "validation_requirements": [
                "at least one explicit non-summary statement in methods/claims/risks/contradictions/definitions/examples",
                "each accepted evidence item retains an existing or newly extracted page/line/timestamp anchor or verbatim quote",
                "anchors resolve to the extracted artifact and preserve raw_path provenance",
                "summary must never be promoted to a statement or used as evidence",
                "source_id and immutable raw object remain unchanged",
                "empty, metadata-only, or image-placeholder extraction remains unresolved",
            ],
            "downstream_policy": "Volcano-model distillation is allowed only after local extraction passes validation",
        })

    def count(key: str) -> dict[str, int]:
        return dict(sorted(Counter(str(task[key]) for task in tasks).items()))

    queue = {
        "queue_schema_version": 1,
        "catalog_version": repo.manifest.get("catalog_version"),
        "policy": {
            "candidate_only": True, "published_catalog_modified": False,
            "raw_modified": False, "external_models_called": False,
            "summary_as_evidence_forbidden": True,
        },
        "statistics": {
            "total": len(tasks),
            "by_route": count("route"),
            "by_format": count("raw_format"),
            "by_size_bucket": count("size_bucket"),
            "by_priority": count("priority"),
            "total_ready_bytes": sum(task["raw_size_bytes"] or 0 for task in tasks
                                     if task["priority"] == "P0_READY"),
        },
        "tasks": tasks,
    }
    output = root / "content-reextraction-queue.json"
    rendered = json.dumps(queue, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    temporary = output.with_suffix(".json.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(output)
    queue["queue_path"] = str(output.relative_to(repo.vault)).replace("\\", "/")
    return queue
