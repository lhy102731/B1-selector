"""Validate and promote candidate metadata repairs into persistent overrides."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .catalog_builder import publish_catalog
from .overrides import ALLOWED_PATCH_KEYS, load_approved_overrides, override_path
from .repository import KBaseRepository
from .schemas import validate_catalog_entry


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush(); os.fsync(handle.fileno())
        os.replace(name, path)
    except Exception:
        Path(name).unlink(missing_ok=True)
        raise


def _load_candidate(candidate: Path) -> dict[str, Any]:
    plan_path = candidate / "plan.json"
    if plan_path.is_file():
        return json.loads(plan_path.read_text(encoding="utf-8"))
    report_path = candidate / "repair-report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report.get("schema_version") != 1 or not isinstance(report.get("findings"), list):
        raise ValueError("invalid trace repair-report schema")
    if int(report.get("gap_count", -1)) != len(report["findings"]):
        raise ValueError("trace repair-report gap_count mismatch")
    patches = []
    for finding in report["findings"]:
        if finding.get("status") != "recoverable" or not isinstance(finding.get("candidate_patch"), dict):
            continue
        patches.append({"source_id": finding.get("source_id"), "after": finding["candidate_patch"],
                        "rule": "trace_repair_report", "confidence": 1.0,
                        "evidence": {"reasons": finding.get("reasons", []),
                                     "raw_resolution": finding.get("raw_resolution")}})
    return {"candidate_id": candidate.name, "base_catalog_version": report.get("source_catalog_version"),
            "patches": patches, "created_families": [], "additional_entries": {}}


def _fingerprint(value: Any) -> str:
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def promote_candidates(*, vault_path: str | Path, candidate_dirs: list[str | Path]) -> dict[str, Any]:
    vault = Path(vault_path).resolve()
    plans = []
    for value in candidate_dirs:
        candidate = Path(value).resolve()
        candidate.relative_to(vault / "wiki/outputs/manifests/ag2-kbase")
        plans.append(_load_candidate(candidate))
    if not plans:
        raise ValueError("at least one candidate is required")
    base_versions = {plan.get("base_catalog_version") for plan in plans}
    if len(base_versions) != 1:
        raise ValueError("candidates must share one source catalog version")
    component_ids = [str(plan.get("candidate_id") or "") for plan in plans]
    if any(not value for value in component_ids) or len(set(component_ids)) != len(component_ids):
        raise ValueError("candidate IDs must be present and unique")
    plan = {"candidate_id": "+".join(component_ids), "component_ids": component_ids,
            "base_catalog_version": next(iter(base_versions)),
            "patches": [item for value in plans for item in value.get("patches", [])],
            "created_families": [item for value in plans for item in value.get("created_families", [])],
            "additional_entries": {key: item for value in plans
                                   for key, item in value.get("additional_entries", {}).items()}}
    return _promote_plan(vault, plan)


def promote_candidate(*, vault_path: str | Path, candidate_dir: str | Path) -> dict[str, Any]:
    return promote_candidates(vault_path=vault_path, candidate_dirs=[candidate_dir])


def _promote_plan(vault: Path, plan: dict[str, Any]) -> dict[str, Any]:
    candidate_id = str(plan.get("candidate_id") or "")
    approved = load_approved_overrides(vault)
    repo = KBaseRepository(vault)
    promoted_ids = {item.get("candidate_id") for item in approved.get("promotions", [])}
    component_ids = plan.get("component_ids", [candidate_id])
    if all(value in promoted_ids for value in component_ids):
        if repo.manifest.get("override_state") == _fingerprint(approved):
            return {"promoted": True, "idempotent": True, "candidate_id": candidate_id, "published": True}
        publication = publish_catalog(vault)
        return {"promoted": True, "idempotent": True, "candidate_id": candidate_id,
                "published": bool(publication.get("published")),
                "catalog_version": publication.get("manifest", {}).get("catalog_version")}
    if plan.get("base_catalog_version") != repo.manifest.get("catalog_version"):
        raise ValueError("candidate source catalog version does not match current")
    known = {str(entry["source_id"]): entry for entry in repo.entries()}
    additions = {str(entry["source_id"]): entry for entry in plan.get("created_families", [])}
    additions.update({str(key): value for key, value in plan.get("additional_entries", {}).items()})
    for source_id, entry in additions.items():
        if source_id != str(entry.get("source_id")):
            raise ValueError(f"additional entry ID mismatch: {source_id}")
        validate_catalog_entry(entry)
        if source_id in known:
            raise ValueError(f"additional entry already exists: {source_id}")
        for key, relative in entry.get("paths", {}).items():
            if not repo.safe_path(relative).is_file():
                raise ValueError(f"additional entry path does not exist: {source_id}:{key}:{relative}")

    candidate_patches: dict[str, dict[str, Any]] = {}
    for item in plan.get("patches", []):
        source_id = str(item.get("source_id") or "")
        if source_id not in known:
            raise ValueError(f"patch target does not exist: {source_id}")
        after = item.get("after")
        if not isinstance(after, dict) or set(after) - ALLOWED_PATCH_KEYS:
            raise ValueError(f"patch has forbidden fields: {source_id}")
        before = item.get("before", {})
        for key, expected in before.items():
            if key in ALLOWED_PATCH_KEYS and known[source_id].get(key) != expected:
                raise ValueError(f"stale before state: {source_id}:{key}")
        if "paths" in after:
            for key, relative in after["paths"].items():
                if not relative or not repo.safe_path(relative).is_file():
                    raise ValueError(f"override path does not exist: {source_id}:{key}:{relative}")
        if "available_layers" in after:
            candidate = dict(known[source_id])
            candidate["available_layers"] = after["available_layers"]
            validate_catalog_entry(candidate)
        if "warnings" in after:
            candidate = dict(known[source_id])
            candidate["warnings"] = after["warnings"]
            validate_catalog_entry(candidate)
        family_id = after.get("family_id")
        if family_id and family_id not in known and family_id not in additions:
            raise ValueError(f"unknown family target: {family_id}")
        for parent_id in after.get("parent_ids", []):
            if parent_id not in known and parent_id not in additions:
                raise ValueError(f"unknown parent target: {parent_id}")
        current = candidate_patches.setdefault(source_id, {})
        for key, value in after.items():
            if key == "paths":
                current[key] = {**current.get(key, {}), **value}
            else:
                current[key] = value

    merged = json.loads(json.dumps(approved))
    merged.setdefault("entry_patches", {})
    merged.setdefault("additional_entries", {})
    merged.setdefault("promotions", [])
    for source_id, patch in candidate_patches.items():
        current_patch = merged["entry_patches"].setdefault(source_id, {})
        for key, value in patch.items():
            if key == "paths":
                current_patch[key] = {**current_patch.get(key, {}), **value}
            else:
                current_patch[key] = value
    merged["additional_entries"].update(additions)
    for component_id in component_ids:
        merged["promotions"].append({"candidate_id": component_id,
            "base_catalog_version": plan["base_catalog_version"],
            "promoted_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "patches": len(candidate_patches), "additional_entries": len(additions)})

    target = override_path(vault)
    old_bytes = target.read_bytes() if target.is_file() else None
    _atomic_json(target, merged)
    try:
        publication = publish_catalog(vault)
        if not publication.get("published"):
            raise RuntimeError(f"catalog publication failed: {publication.get('validation')}")
    except Exception:
        if old_bytes is None:
            target.unlink(missing_ok=True)
        else:
            fd, name = tempfile.mkstemp(prefix=target.name + ".rollback.", dir=target.parent)
            with os.fdopen(fd, "wb") as handle:
                handle.write(old_bytes); handle.flush(); os.fsync(handle.fileno())
            os.replace(name, target)
        raise
    return {"promoted": True, "idempotent": False, "candidate_id": candidate_id,
            "published": True, "catalog_version": publication["manifest"]["catalog_version"],
            "patches": len(candidate_patches), "additional_entries": len(additions)}
