"""Compile bounded, hash-bound project state before KBase discovery."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import re
from typing import Any

import yaml

from .kbase.release_bundle import inspect_semantic_release_bundle


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_yaml(path: Path | None) -> dict[str, Any]:
    if not path or not path.is_file():
        return {}
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, UnicodeError, yaml.YAMLError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_json(path: Path | None) -> Any:
    if not path or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None


def _latest(root: Path, pattern: str) -> Path | None:
    def key(path: Path) -> tuple[int, str]:
        match = re.search(r"_v(\d+)$", path.stem, re.IGNORECASE)
        return (int(match.group(1)) if match else -1, path.name)

    values = sorted(root.glob(pattern), key=key)
    return values[-1] if values else None


def _relative(path: Path, root: Path) -> str:
    return str(path.resolve().relative_to(root.resolve())).replace("\\", "/")


def _binding(path: Path | None, root: Path) -> dict[str, Any] | None:
    if not path or not path.is_file():
        return None
    return {
        "path": _relative(path, root),
        "sha256": _sha256(path),
        "bytes": path.stat().st_size,
    }


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        for key in (
            "factors", "components", "directions", "items", "experiments",
            "lessons", "entries",
        ):
            if isinstance(value.get(key), list):
                return [item for item in value[key] if isinstance(item, dict)]
    return []


def _priority_items(path: Path | None) -> list[str]:
    if not path or not path.is_file():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return []
    match = re.search(
        r"(?ims)^##\s+(?:Current Priority List|当前优先级|当前优先事项)\s*$\s*(.*?)(?=^##\s+|\Z)",
        text,
    )
    if not match:
        return []
    return [
        item.strip()
        for item in re.findall(r"(?m)^\s*\d+\.\s+(.+?)\s*$", match.group(1))
        if item.strip()
    ][:20]


def _fingerprint(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _compact_registry(experiments: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    compact = []
    for entry in experiments:
        status = str(entry.get("status") or "UNKNOWN").upper()
        status_counts[status] = status_counts.get(status, 0) + 1
        compact.append({
            key: entry.get(key)
            for key in ("id", "title", "status", "short_result", "reopen_condition")
            if entry.get(key) is not None
        })
    return {
        "experiment_count": len(experiments),
        "status_counts": status_counts,
        "experiments": compact,
    }


def compile_project_state(
    strategy_id: str,
    optimization_request: str,
    *,
    project_root: str | Path | None = None,
    vault_path: str | Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Compile the current project closure without creating research ideas."""
    strategy = str(strategy_id or "").strip().lower()
    request = str(optimization_request or "").strip()
    if not strategy:
        raise ValueError("strategy_id is required")
    if not request:
        raise ValueError("optimization_request is required")
    root = Path(project_root or Path(__file__).resolve().parent.parent).resolve()

    snapshot_path = _latest(root, f"snapshot_{strategy}.yaml")
    handoff_path = _latest(root, f"handoff_{strategy}_v*.yaml") or _latest(
        root, f"handoff_{strategy}.yaml"
    )
    registry_path = _latest(root, f"registry_{strategy}_v*.yaml") or _latest(
        root, f"registry_{strategy}.yaml"
    )
    project_path = _latest(root, f"project_{strategy}_v*.yaml") or _latest(
        root, f"project_{strategy}.yaml"
    )
    memory_path = root / f"{strategy}_memory.yaml"

    snapshot = _read_yaml(snapshot_path).get("snapshot", {})
    handoff = _read_yaml(handoff_path).get("handoff", {})
    registry_doc = _read_yaml(registry_path).get("registry", {})
    project = _read_yaml(project_path).get("project", {})
    memory = _read_yaml(memory_path)
    experiments = registry_doc.get("experiments", []) if isinstance(registry_doc, dict) else []
    experiments = [item for item in experiments if isinstance(item, dict)]

    kb_subject = "b1_v3" if strategy == "b1" else strategy
    kb_root = root / "ag2_research" / "knowledge_base" / kb_subject
    kb_manifest_path = kb_root / "manifest.yaml"
    kb_manifest = _read_yaml(kb_manifest_path)
    kb_artifacts: dict[str, Any] = {}
    for key, relative in (kb_manifest.get("artifacts") or {}).items():
        artifact_path = kb_root / str(relative)
        if artifact_path.suffix.lower() == ".json":
            value = _read_json(artifact_path)
            kb_artifacts[str(key)] = value if value is not None else []

    research_root = root / "research_state" / strategy
    summaries = sorted(research_root.glob("*research_summary_*.md")) if research_root.is_dir() else []
    latest_summary = summaries[-1] if summaries else None
    factor_files = sorted((research_root / "factor_library").glob("*.md")) \
        if (research_root / "factor_library").is_dir() else []
    recent_files = []
    if research_root.is_dir():
        candidates = [
            path for path in research_root.rglob("*")
            if path.is_file() and path.suffix.lower() in {".md", ".json", ".yaml", ".yml"}
            and path.stat().st_size <= 2 * 1024 * 1024
        ]
        recent_files = sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)[:20]

    script_names = {
        "brick": (
            "backtest_brick_v2.py", "backtest_brick_v2_research.py", "daily_select.py"
        ),
        "b1": ("backtest_optimized.py", "strategy/unified_b1_strategy.py"),
    }.get(strategy, ())
    evaluation_paths = {
        "fixed_suite": root / "ag2_research/kbase/query_regression.yaml",
        "holdout_suite": root / "ag2_research/kbase/query_holdout.yaml",
        "shadow_suite_20260723": root / "ag2_research/kbase/query_shadow_20260723.yaml",
        "shadow_result_20260723": root / "data/ag2_kbase/query-shadow-20260723-v1.json",
    }

    binding_paths = [
        snapshot_path, handoff_path, registry_path, project_path,
        memory_path if memory_path.is_file() else None,
        kb_manifest_path if kb_manifest_path.is_file() else None,
        latest_summary,
        *factor_files,
        *(kb_root / str(value) for value in (kb_manifest.get("artifacts") or {}).values()),
        *(root / name for name in script_names),
        *evaluation_paths.values(),
    ]
    bindings = [item for item in (_binding(path, root) for path in binding_paths) if item]
    bindings = sorted({item["path"]: item for item in bindings}.values(), key=lambda item: item["path"])

    packet: dict[str, Any] = {
        "schema_version": "ag2.project_state_packet.v1",
        "generated_at": generated_at or dt.datetime.now(dt.timezone.utc).isoformat(),
        "strategy_id": strategy,
        "optimization_request": request,
        "memory": {
            "snapshot": {
                "current_champion": snapshot.get("current_champion"),
                "next_priority": snapshot.get("next_priority"),
                "frozen_directions": snapshot.get("frozen_directions") or [],
                "rejected_directions": snapshot.get("rejected_directions") or [],
            },
            "handoff": {
                "active_focus": handoff.get("active_focus"),
                "do_not_repeat": handoff.get("do_not_repeat") or [],
                "escalation_conditions": handoff.get("escalation_conditions") or [],
            },
            "registry": _compact_registry(experiments),
            "research_memory": memory,
            "project": {
                "name": project.get("name"),
                "boundary": project.get("boundary"),
            },
        },
        "project_kb": {
            "subject": kb_subject,
            "kb_version": kb_manifest.get("kb_version"),
            "as_of_phase": kb_manifest.get("as_of_phase"),
            "closure_status": kb_manifest.get("closure_status"),
            "headline": kb_manifest.get("headline") or {},
            "artifacts": kb_artifacts,
        },
        "research_history": {
            "latest_summary": _binding(latest_summary, root),
            "current_priority_items": _priority_items(latest_summary),
            "factor_library": [item for item in (_binding(path, root) for path in factor_files) if item],
            "recent_artifacts": [item for item in (_binding(path, root) for path in recent_files) if item],
        },
        "script_boundary": {
            "files": [item for item in (_binding(root / name, root) for name in script_names) if item],
            "production_changes_authorized": False,
        },
        "kbase_release": inspect_semantic_release_bundle(vault_path),
        "research_validation": {
            key: _binding(path, root) for key, path in evaluation_paths.items() if path.is_file()
        },
        "artifact_bindings": bindings,
    }
    fingerprint_input = {key: value for key, value in packet.items() if key != "generated_at"}
    release = packet.get("kbase_release") or {}
    fingerprint_input["kbase_release"] = {
        key: release.get(key)
        for key in (
            "schema_version", "status", "catalog_version", "catalog_source_fingerprint",
            "semantic_source_fingerprint", "model_binding_sha256", "models",
            "documents_sha256", "vectors_sha256", "bundle_fingerprint", "issues",
        )
    }
    packet["project_state_fingerprint"] = _fingerprint(fingerprint_input)
    return packet
