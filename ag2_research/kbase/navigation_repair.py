"""Generate deterministic, candidate-only repairs for orphaned KBase navigation."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .coverage import build_navigation_coverage
from .repository import KBaseRepository

REPAIR_GENERATOR_VERSION = 2

def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return "".join(re.findall(r"[0-9a-z\u4e00-\u9fff]+", text))


def _family_key(entry: dict[str, Any]) -> str:
    source_id = str(entry.get("source_id") or "")
    parts = source_id.split("/")
    if len(parts) >= 3 and parts[0] in {"exact_family_key", "primary_person"}:
        return parts[1]
    return str(entry.get("title") or "")


def _candidate_family(key: str, normalized: str, members: list[str], member_packet_path: str) -> dict[str, Any]:
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]
    source_id = f"candidate_family/{digest}"
    return {
        "catalog_schema_version": 1,
        "source_id": source_id,
        "object_type": "family",
        "title": key,
        "aliases": [], "people": [], "family_id": source_id,
        "voice_role": "unknown", "source_type": "source_family",
        "date_start": None, "date_end": None, "topics": [],
        "summary": f"由完全相同的规范化 family_key 确定性归组，共 {len(members)} 个来源。",
        "reliability": "unverified", "review_status": "review_required",
        "available_layers": ["summary"],
        "warnings": ["candidate_repair_not_published"], "parent_ids": [],
        "paths": {"member_packet": member_packet_path},
        "content_fingerprint": hashlib.sha256((normalized + "\n" + "\n".join(members)).encode("utf-8")).hexdigest(),
        "source_schema_version": None,
    }


def generate_navigation_repair_candidate(
    *, vault_path: str | Path, output_dir: str | Path | None = None
) -> dict[str, Any]:
    repo = KBaseRepository(vault_path)
    before = build_navigation_coverage(vault_path=repo.vault)
    entries = [dict(entry) for entry in repo.entries()]
    by_id = {str(entry["source_id"]): entry for entry in entries}
    existing: dict[str, list[str]] = defaultdict(list)
    for entry in entries:
        if entry.get("object_type") == "family":
            normalized = _normalize(_family_key(entry))
            if normalized:
                existing[normalized].append(str(entry["source_id"]))

    packet_info: list[tuple[dict[str, Any], str, str]] = []
    groups: dict[str, list[str]] = defaultdict(list)
    ambiguous: list[dict[str, Any]] = []
    for orphan in before["orphans"]:
        entry = by_id[orphan["source_id"]]
        record = repo.read_packet(entry).get("record", {})
        raw_key = str(record.get("family_key") or "").strip()
        normalized = _normalize(raw_key)
        if not normalized:
            ambiguous.append({"source_id": entry["source_id"], "reason": "missing_family_key",
                              "evidence": {"packet_path": entry.get("paths", {}).get("packet")}})
            continue
        packet_info.append((entry, raw_key, normalized))
        groups[normalized].append(str(entry["source_id"]))

    created: dict[str, dict[str, Any]] = {}
    patches: list[dict[str, Any]] = []
    for entry, raw_key, normalized in sorted(packet_info, key=lambda item: str(item[0]["source_id"])):
        candidates = sorted(set(existing.get(normalized, [])))
        if len(candidates) > 1:
            ambiguous.append({
                "source_id": entry["source_id"], "reason": "multiple_existing_families_for_exact_key",
                "family_key": raw_key, "normalized_family_key": normalized,
                "candidate_family_ids": candidates,
            })
            continue
        if candidates:
            family_id = candidates[0]
            rule = "exact_normalized_family_key_reuse"
        else:
            members = sorted(groups[normalized])
            representative = sorted({item[1] for item in packet_info if item[2] == normalized})[0]
            first_member = by_id[members[0]].get("paths", {}).get("packet")
            family = _candidate_family(representative, normalized, members, first_member)
            created[family["source_id"]] = family
            family_id = family["source_id"]
            rule = "exact_normalized_family_key_create"
        before_state = {"family_id": entry.get("family_id"), "parent_ids": list(entry.get("parent_ids", []))}
        parent_ids = list(dict.fromkeys([*before_state["parent_ids"], family_id]))
        after_state = {"family_id": family_id, "parent_ids": parent_ids}
        patches.append({
            "source_id": entry["source_id"], "before": before_state, "after": after_state,
            "rule": rule, "confidence": 1.0,
            "evidence": {"family_key": raw_key, "normalized_family_key": normalized,
                         "packet_path": entry.get("paths", {}).get("packet")},
        })

    payload_seed = json.dumps({"generator_version": REPAIR_GENERATOR_VERSION,
                               "catalog": repo.manifest.get("catalog_version"), "patches": patches,
                               "ambiguous": ambiguous}, ensure_ascii=False, sort_keys=True)
    candidate_id = "navigation-" + hashlib.sha256(payload_seed.encode("utf-8")).hexdigest()[:16]
    root = repo.vault / "wiki/outputs/manifests/ag2-kbase/candidate-repairs"
    target = Path(output_dir).resolve() if output_dir else root / candidate_id
    if (target / "summary.json").is_file():
        return json.loads((target / "summary.json").read_text(encoding="utf-8"))

    repaired_entries = []
    patch_by_id = {item["source_id"]: item for item in patches}
    for entry in entries:
        replacement = dict(entry)
        patch = patch_by_id.get(str(entry["source_id"]))
        if patch:
            replacement.update(patch["after"])
        repaired_entries.append(replacement)
    repaired_entries.extend(created.values())

    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=target.name + ".", dir=target.parent))
    try:
        manifest = dict(repo.manifest)
        manifest["catalog_version"] = candidate_id
        manifest["candidate_only"] = True
        manifest["base_catalog_version"] = repo.manifest.get("catalog_version")
        counts = Counter(str(entry.get("object_type")) for entry in repaired_entries)
        manifest["counts"] = {**manifest.get("counts", {}), "source_packets": counts["source_packet"],
                              "families": counts["family"], "entries": len(repaired_entries)}
        (staging / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        shutil.copy2(repo.release_dir / "facets.json", staging / "facets.json")
        with (staging / "catalog.jsonl").open("w", encoding="utf-8") as handle:
            for entry in repaired_entries:
                handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
        plan = {"candidate_id": candidate_id, "base_catalog_version": repo.manifest.get("catalog_version"),
                "policy": "candidate_only_no_raw_or_current_mutation", "created_families": list(created.values()),
                "patches": patches, "ambiguous": sorted(ambiguous, key=lambda item: item["source_id"])}
        (staging / "plan.json").write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
        after = build_navigation_coverage(vault_path=repo.vault, release_dir=staging)
        comparison = {"orphans_before": len(before["orphans"]), "orphans_after": len(after["orphans"]),
                      "orphans_eliminated": len(before["orphans"]) - len(after["orphans"]),
                      "high_confidence_patches": len(patches), "created_families": len(created),
                      "ambiguous": len(ambiguous)}
        (staging / "coverage-comparison.json").write_text(json.dumps(comparison, ensure_ascii=False, indent=2), encoding="utf-8")
        summary = {"candidate_id": candidate_id, "output_dir": str(target), "coverage_comparison": comparison,
                   "published": False}
        (staging / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(staging, target)
        return summary
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vault", default=r"D:\KBase")
    parser.add_argument("--output")
    args = parser.parse_args()
    print(json.dumps(generate_navigation_repair_candidate(vault_path=args.vault, output_dir=args.output),
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
