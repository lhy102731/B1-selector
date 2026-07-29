"""Persistent, approved metadata overrides for immutable legacy source packets."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


OVERRIDE_FILENAME = "approved-overrides.json"
ALLOWED_PATCH_KEYS = {"family_id", "parent_ids", "paths", "available_layers", "warnings"}


def override_path(vault: Path) -> Path:
    return vault / "wiki/outputs/manifests/ag2-kbase" / OVERRIDE_FILENAME


def load_approved_overrides(vault: Path) -> dict[str, Any]:
    path = override_path(vault)
    if not path.is_file():
        return {"schema_version": 1, "entry_patches": {}, "additional_entries": {}, "promotions": []}
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema_version") != 1:
        raise ValueError("unsupported override schema_version")
    if not isinstance(value.get("entry_patches", {}), dict) or not isinstance(value.get("additional_entries", {}), dict):
        raise ValueError("invalid approved override structure")
    return value


def apply_approved_overrides(entries: list[dict[str, Any]], overrides: dict[str, Any]) -> list[dict[str, Any]]:
    by_id = {str(entry["source_id"]): dict(entry) for entry in entries}
    for source_id, patch in overrides.get("entry_patches", {}).items():
        if source_id not in by_id:
            continue
        unknown = set(patch) - ALLOWED_PATCH_KEYS
        if unknown:
            raise ValueError(f"override contains forbidden fields for {source_id}: {sorted(unknown)}")
        target = by_id[source_id]
        for key, value in patch.items():
            if key == "paths":
                target["paths"] = {**target.get("paths", {}), **value}
            else:
                target[key] = value
        by_id[source_id] = target
    for source_id, entry in overrides.get("additional_entries", {}).items():
        if source_id not in by_id:
            by_id[source_id] = dict(entry)
    return list(by_id.values())
