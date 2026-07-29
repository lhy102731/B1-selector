"""Evidence-only second-pass review for navigation orphans."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .coverage import build_navigation_coverage
from .repository import KBaseRepository


def _literal_family_key(entry: dict[str, Any]) -> str:
    parts = str(entry.get("source_id") or "").split("/")
    return parts[1] if len(parts) >= 3 and parts[0] == "exact_family_key" else ""


def generate_manual_orphan_candidate(*, vault_path: str | Path) -> dict[str, Any]:
    repo = KBaseRepository(vault_path)
    coverage = build_navigation_coverage(vault_path=repo.vault)
    entries = list(repo.entries())
    families = [entry for entry in entries if entry.get("object_type") == "family"]
    analyses: list[dict[str, Any]] = []
    patches: list[dict[str, Any]] = []
    for orphan in coverage["orphans"]:
        entry = repo.get(orphan["source_id"])
        packet = repo.read_packet(entry)
        record = packet.get("record", {})
        key = str(record.get("family_key") or "").strip()
        exact = [family for family in families if key and _literal_family_key(family) == key]
        conflicts = []
        for family in exact:
            members = [item for item in entries if item.get("family_id") == family["source_id"]
                       and item.get("object_type") == "source_packet"]
            conflicts.append({"family_id": family["source_id"], "members": [
                {"source_id": item["source_id"], "title": item.get("title"),
                 "packet_path": item.get("paths", {}).get("packet")} for item in members[:20]]})
        support = [{"field": "family_key", "value": key},
                   {"field": "original_path", "value": packet.get("original_path")},
                   {"field": "title", "value": entry.get("title")}]
        opposition: list[str] = []
        recommendation = None
        confidence = 0.0
        decision = "retain_for_human_review"
        if len(exact) == 1:
            recommendation = exact[0]["source_id"]
            confidence = 0.99
            decision = "candidate_patch"
            opposition.append("source_role may differ; family identity is based only on exact literal series key")
            after = {"family_id": recommendation,
                     "parent_ids": list(dict.fromkeys([*entry.get("parent_ids", []), recommendation]))}
            patches.append({"source_id": entry["source_id"],
                "before": {"family_id": entry.get("family_id"), "parent_ids": entry.get("parent_ids", [])},
                "after": after, "rule": "manual_review_exact_literal_family_key_unique",
                "confidence": confidence, "evidence": {"support": support, "opposition": opposition}})
        elif not key:
            opposition.append("family_key is missing; title/topics alone are insufficient for deterministic grouping")
        elif len(exact) > 1:
            opposition.append("the exact literal family_key maps to multiple role-specific families")
        else:
            opposition.append("no existing family has the same literal family_key; normalized similarity is ambiguous")
        analyses.append({"source_id": entry["source_id"], "title": entry.get("title"),
            "original_path": packet.get("original_path"), "people": record.get("primary_people", []),
            "topics": record.get("topics", []), "family_key": record.get("family_key"),
            "source_role": record.get("source_role"), "recommended_family": recommendation,
            "confidence": confidence, "decision": decision, "supporting_evidence": support,
            "opposing_evidence": opposition, "conflicting_families": conflicts})
    seed = json.dumps({"base": repo.manifest.get("catalog_version"), "analyses": analyses},
                      ensure_ascii=False, sort_keys=True)
    candidate_id = "manual-navigation-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    target = repo.vault / "wiki/outputs/manifests/ag2-kbase/candidate-repairs" / candidate_id
    plan = {"candidate_id": candidate_id, "base_catalog_version": repo.manifest.get("catalog_version"),
            "policy": "candidate_only_evidence_review_no_publish", "patches": patches,
            "created_families": [], "additional_entries": {}, "analyses": analyses,
            "summary": {"orphans_reviewed": len(analyses), "candidate_patches": len(patches),
                        "retained_for_human_review": len(analyses) - len(patches),
                        "predicted_orphans_after": len(analyses) - len(patches)}}
    if target.is_dir():
        return {"candidate_id": candidate_id, "output_dir": str(target), **plan["summary"], "published": False}
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=target.name + ".", dir=target.parent))
    try:
        (staging / "plan.json").write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(staging, target)
    except Exception:
        import shutil
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {"candidate_id": candidate_id, "output_dir": str(target), **plan["summary"], "published": False}


if __name__ == "__main__":
    print(json.dumps(generate_manual_orphan_candidate(vault_path=r"D:\KBase"), ensure_ascii=False, indent=2))
