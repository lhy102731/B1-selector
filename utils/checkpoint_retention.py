"""Shared retention policy for validated daily-update checkpoints."""

from __future__ import annotations

import json
import os
import re
import shutil
from datetime import date
from pathlib import Path


DEFAULT_CHECKPOINT_RETENTION = max(
    1,
    int(os.environ.get("DAILY_CHECKPOINT_RETENTION", "1")),
)
_REPAIR_CHECKPOINT_RE = re.compile(
    r"repair_\d{4}-\d{2}-\d{2}(?:_\d{4}-\d{2}-\d{2})?"
)


def _is_date_checkpoint(path: Path) -> bool:
    try:
        return date.fromisoformat(path.name).isoformat() == path.name
    except ValueError:
        return False


def _is_repair_checkpoint(path: Path) -> bool:
    """Recognize generated repair checkpoints without matching manual evidence."""

    return _REPAIR_CHECKPOINT_RE.fullmatch(path.name) is not None


def _checkpoint_is_protected(path: Path) -> bool:
    """Keep a checkpoint whose validation explicitly failed or is unreadable.

    Checkpoints created before validation metadata existed remain eligible for
    the one-time legacy cleanup. A present validation file is different: false
    is diagnostic evidence, while malformed metadata must fail closed.
    """

    validation_path = path / "checkpoint_validation.json"
    if not validation_path.exists():
        return False
    try:
        validation = json.loads(validation_path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return True
    return not isinstance(validation, dict) or validation.get("valid") is not True


def prune_checkpoint_history(
    repair_root: Path,
    current_checkpoint: Path,
    *,
    retention: int = DEFAULT_CHECKPOINT_RETENTION,
) -> dict[str, object]:
    """Prune generated history while preserving the current and unknown trees."""

    repair_root = Path(repair_root).resolve()
    current_checkpoint = Path(current_checkpoint).resolve()
    if current_checkpoint.parent != repair_root:
        raise ValueError("current checkpoint must be a direct child of the repair root")
    retention = max(1, int(retention))
    child_dirs = [path for path in repair_root.iterdir() if path.is_dir()]
    date_dirs = sorted(
        [path for path in child_dirs if _is_date_checkpoint(path)],
        key=lambda path: path.name,
        reverse=True,
    )
    generated_dirs = {
        path
        for path in child_dirs
        if _is_date_checkpoint(path) or _is_repair_checkpoint(path)
    }
    protected = {path for path in generated_dirs if _checkpoint_is_protected(path)}
    eligible = [path for path in date_dirs if path not in protected]
    keep = set(eligible[:retention])
    keep.add(current_checkpoint)
    candidates = [
        path
        for path in child_dirs
        if path not in keep
        and path not in protected
        and (_is_date_checkpoint(path) or _is_repair_checkpoint(path))
    ]
    stale_dir = repair_root / "stale_indicators_cache"
    if stale_dir.is_dir() and stale_dir not in keep:
        candidates.append(stale_dir)

    removed: list[dict[str, object]] = []
    for path in sorted(set(candidates), key=lambda item: item.name):
        file_count = 0
        byte_count = 0
        for item in path.rglob("*"):
            if item.is_file():
                file_count += 1
                byte_count += item.stat().st_size
        shutil.rmtree(path)
        removed.append({"path": str(path), "files": file_count, "bytes": byte_count})

    return {
        "retention": retention,
        "kept": sorted(str(path) for path in keep if path.exists()),
        "protected": sorted(str(path) for path in protected if path.exists()),
        "removed": removed,
        "removed_files": sum(int(row["files"]) for row in removed),
        "removed_bytes": sum(int(row["bytes"]) for row in removed),
    }
