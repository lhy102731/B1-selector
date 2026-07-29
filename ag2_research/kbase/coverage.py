"""Audit progressive-disclosure coverage of the published KBase catalog.

This deliberately audits catalogued source packets only.  It does not walk the
raw vault: coverage here means "can an agent navigate from Wiki metadata to the
published source and its evidence/raw layers", not "every disk file is indexed".
"""
from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any

from .repository import KBaseRepository
from .adapters import STATEMENT_FIELDS, statement_text


REQUIRED_LAYERS = ("summary", "statements", "evidence", "raw")
NON_ACTIONABLE_CONTENT_STATUSES = {
    "manual_review_no_useful_content",
    "manual_review_source_only_no_statements",
    "manual_review_no_supported_items",
}
CONTENT_STATUS_WARNINGS = NON_ACTIONABLE_CONTENT_STATUSES | {
    "manual_review_needs_gpu_ocr_or_visual_reextraction",
}
SEMANTIC_LAYER_REASONS = {"missing_statements", "missing_evidence"}


def _priority(reasons: list[str]) -> str | None:
    if any(reason in reasons for reason in (
        "not_navigable", "missing_evidence", "missing_raw", "packet_unreadable", "raw_file_missing"
    )):
        return "P0"
    if any(reason in reasons for reason in ("missing_summary", "missing_statements", "packet_path_missing")):
        return "P1"
    if reasons:
        return "P2"
    return None


def _manual_content_status(entry: dict[str, Any]) -> str | None:
    warnings = {str(value) for value in entry.get("warnings", [])}
    matches = sorted(warnings & CONTENT_STATUS_WARNINGS)
    return matches[0] if matches else None


def _has_ancestor_type(start_ids: list[str], nodes: dict[str, dict[str, Any]], object_type: str) -> bool:
    pending = list(start_ids)
    seen: set[str] = set()
    while pending:
        node_id = pending.pop()
        if node_id in seen:
            continue
        seen.add(node_id)
        node = nodes.get(node_id)
        if not node:
            continue
        if node.get("object_type") == object_type:
            return True
        pending.extend(str(value) for value in node.get("parent_ids", []))
    return False


def _verify_layers(repo: KBaseRepository, entry: dict[str, Any]) -> tuple[dict[str, bool], list[str]]:
    reasons: list[str] = []
    packet_path = entry.get("paths", {}).get("packet")
    packet: dict[str, Any] = {}
    if not packet_path:
        reasons.append("packet_path_missing")
    else:
        try:
            value = json.loads(repo.safe_path(packet_path).read_text(encoding="utf-8"))
            if isinstance(value, dict):
                packet = value
            else:
                reasons.append("packet_unreadable")
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
            reasons.append("packet_unreadable")
    record = packet.get("record") if isinstance(packet.get("record"), dict) else {}
    distilled = repo.read_distilled_candidate(entry)
    if distilled:
        candidate_record = distilled.get("record") if isinstance(distilled.get("record"), dict) else {}
        has_packet_statements = any(
            isinstance(record.get(field), list)
            and any(isinstance(item, dict) and statement_text(item, field) for item in record.get(field, []))
            for field in STATEMENT_FIELDS
        )
        if not has_packet_statements:
            record = {**record, **candidate_record}
    statement_items: list[dict[str, Any]] = []
    statement_pairs: list[tuple[str, dict[str, Any]]] = []
    for key in STATEMENT_FIELDS:
        values = record.get(key)
        if isinstance(values, list):
            valid = [value for value in values if isinstance(value, dict) and value]
            statement_items.extend(valid)
            statement_pairs.extend((key, value) for value in valid)
    summary = bool(str(record.get("summary") or "").strip())
    statements = any(statement_text(item, field) for field, item in statement_pairs)
    evidence = any(statement_text(item, field) and (item.get("evidence_anchor") or item.get("evidence_quote"))
                   for field, item in statement_pairs)
    raw_path = entry.get("paths", {}).get("raw")
    raw = False
    if raw_path:
        try:
            raw = repo.safe_path(raw_path).is_file()
        except ValueError:
            raw = False
        if not raw:
            reasons.append("raw_file_missing")
    return {"summary": summary, "statements": statements, "evidence": evidence, "raw": raw}, reasons


def build_navigation_coverage(
    *, vault_path: str | Path | None = None, release_dir: str | Path | None = None
) -> dict[str, Any]:
    """Return per-packet navigation/layer coverage and a deterministic gap queue."""
    repo = KBaseRepository(vault_path, release_dir=release_dir)
    entries = list(repo.entries())
    nodes = {str(entry["source_id"]): entry for entry in entries}
    packets = [entry for entry in entries if entry.get("object_type") == "source_packet"]
    packet_rows: list[dict[str, Any]] = []
    gap_queue: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()

    for entry in packets:
        source_id = str(entry["source_id"])
        parents = [str(value) for value in entry.get("parent_ids", [])]
        family_id = str(entry.get("family_id") or "")
        family_entry = bool(family_id and nodes.get(family_id, {}).get("object_type") == "family")
        ancestor_starts = parents + ([family_id] if family_id else [])
        map_entry = _has_ancestor_type(ancestor_starts, nodes, "map")
        date_value = str(entry.get("date_start") or "")
        overview_dates = repo.facets.get("date_start", {})
        date_overview_exposed = bool(date_value and date_value in overview_dates)
        dated_entry = bool(date_value)  # Direct kbase_browse("date:<date>") is always supported.
        navigation = {
            "map": map_entry,
            "family": family_entry,
            "date": dated_entry,
        }
        layers = set(entry.get("available_layers", []))
        declared_layers = {layer: layer in layers for layer in REQUIRED_LAYERS}
        verified_layers, verification_reasons = _verify_layers(repo, entry)
        reasons: list[str] = list(verification_reasons)
        if not any(navigation.values()):
            reasons.append("not_navigable")
        reasons.extend(f"missing_{layer}" for layer, present in verified_layers.items() if not present)
        unresolved_parents = [value for value in parents if value not in nodes]
        if unresolved_parents:
            reasons.append("unresolved_parent")
        manual_status = _manual_content_status(entry)
        semantic_only_gap = bool(reasons) and all(reason in SEMANTIC_LAYER_REASONS for reason in reasons)
        priority = None if (
            manual_status in NON_ACTIONABLE_CONTENT_STATUSES and semantic_only_gap
        ) else _priority(reasons)
        visible_reasons = reasons + ([manual_status] if manual_status else [])
        reason_counts.update(visible_reasons)
        row = {
            "source_id": source_id,
            "title": entry.get("title", ""),
            "navigation": navigation,
            "date_navigation": {
                "overview_facet_exposed": date_overview_exposed,
                "browse_node": f"date:{date_value}" if date_value else None,
                "note": "A date is reachable only through an overview date facet or an explicit date:<date> browse node.",
            },
            "navigable": any(navigation.values()),
            "declared_layers": declared_layers,
            "verified_layers": verified_layers,
            "layers": verified_layers,
            "packet_path": entry.get("paths", {}).get("packet"),
            "raw_path": entry.get("paths", {}).get("raw"),
            "unresolved_parents": unresolved_parents,
            "reasons": visible_reasons,
            "content_gap_status": manual_status,
            "priority": priority,
        }
        packet_rows.append(row)
        if priority:
            gap_queue.append({
                "priority": priority,
                "source_id": source_id,
                "title": entry.get("title", ""),
                "reasons": reasons,
                "recommended_action": _recommended_action(reasons),
            })

    order = {"P0": 0, "P1": 1, "P2": 2}
    gap_queue.sort(key=lambda item: (order[item["priority"]], item["source_id"]))
    packet_rows.sort(key=lambda item: item["source_id"])
    return {
        "catalog_version": repo.manifest.get("catalog_version"),
        "scope": {
            "policy": "published_catalog_only",
            "source_packets": len(packets),
            "raw_files_scanned": False,
            "note": "Audit Wiki-to-source navigation; raw-vault inventory is intentionally out of scope.",
        },
        "coverage": {
            "navigable_from_any_entry": sum(row["navigable"] for row in packet_rows),
            "from_map": sum(row["navigation"]["map"] for row in packet_rows),
            "from_family": sum(row["navigation"]["family"] for row in packet_rows),
            "from_date": sum(row["navigation"]["date"] for row in packet_rows),
            "has_summary": sum(row["verified_layers"]["summary"] for row in packet_rows),
            "has_statements": sum(row["verified_layers"]["statements"] for row in packet_rows),
            "has_evidence": sum(row["verified_layers"]["evidence"] for row in packet_rows),
            "traceable_to_raw": sum(row["verified_layers"]["raw"] for row in packet_rows),
        },
        "gap_reason_counts": dict(sorted(reason_counts.items())),
        "orphans": [row for row in packet_rows if not row["navigable"]],
        "gap_queue": gap_queue,
        "packets": packet_rows,
    }


def _recommended_action(reasons: list[str]) -> list[str]:
    actions = []
    if "not_navigable" in reasons:
        actions.append("assign_family_map_or_date_entry")
    for layer in REQUIRED_LAYERS:
        if f"missing_{layer}" in reasons:
            actions.append(f"build_{layer}_layer")
    if "packet_path_missing" in reasons:
        actions.append("repair_packet_path")
    if "packet_unreadable" in reasons:
        actions.append("repair_or_rebuild_packet")
    if "raw_file_missing" in reasons:
        actions.append("repair_raw_path_or_restore_source")
    if "unresolved_parent" in reasons:
        actions.append("repair_parent_link")
    return actions
