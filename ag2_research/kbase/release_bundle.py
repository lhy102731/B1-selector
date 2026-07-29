"""Read-only version handshake for the published AG2-KBase semantic release."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import time
from typing import Any

from .repository import DEFAULT_VAULT


CATALOG_RELEASE = Path("wiki/outputs/manifests/ag2-kbase/current")
SEMANTIC_RELEASE = Path("wiki/outputs/manifests/ag2-kbase-semantic/current")
SEMANTIC_RUNTIME = Path("wiki/outputs/runtime/ag2-kbase-semantic")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, label: str, issues: list[str]) -> dict[str, Any]:
    if not path.is_file():
        issues.append(f"missing_{label}")
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        issues.append(f"invalid_{label}")
        return {}
    if not isinstance(value, dict):
        issues.append(f"invalid_{label}")
        return {}
    return value


def _model_id(manifest: dict[str, Any], role: str) -> str | None:
    model = (manifest.get("models") or {}).get(role)
    if isinstance(model, dict):
        return str(model.get("model_id") or "") or None
    return str(model or "") or None


def _fingerprint(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def inspect_semantic_release_bundle(
    vault_path: str | Path | None = None,
    *,
    now_epoch: float | None = None,
    max_heartbeat_age_seconds: float = 180.0,
) -> dict[str, Any]:
    """Bind catalog, semantic release, gate, and live worker identity.

    This function is intentionally read-only. It verifies the small control
    documents and the catalog payload hash, while carrying the declared large
    document/vector hashes into one stable fingerprint.
    """
    vault = Path(vault_path or DEFAULT_VAULT).resolve()
    catalog_root = vault / CATALOG_RELEASE
    semantic_root = vault / SEMANTIC_RELEASE
    runtime_root = vault / SEMANTIC_RUNTIME
    issues: list[str] = []

    catalog_manifest_path = catalog_root / "manifest.json"
    catalog_payload_path = catalog_root / "catalog.jsonl"
    semantic_manifest_path = semantic_root / "manifest.json"
    gate_path = semantic_root / "gate-report.json"
    health_path = runtime_root / "health.json"

    catalog = _read_json(catalog_manifest_path, "catalog_manifest", issues)
    semantic = _read_json(semantic_manifest_path, "semantic_manifest", issues)
    gate = _read_json(gate_path, "semantic_gate", issues)
    health = _read_json(health_path, "semantic_health", issues)

    catalog_version = str(catalog.get("catalog_version") or "")
    semantic_catalog_version = str(semantic.get("catalog_version") or "")
    semantic_fingerprint = str(semantic.get("source_fingerprint") or "")
    model_binding = str(semantic.get("model_binding_sha256") or "")
    embedding = _model_id(semantic, "embedding")
    reranker = _model_id(semantic, "reranker")

    if catalog and not catalog_version:
        issues.append("catalog_version_missing")
    if semantic:
        if semantic.get("status") != "COMPLETED":
            issues.append("semantic_release_not_completed")
        if semantic.get("promotion_status") != "current":
            issues.append("semantic_release_not_current")
        if semantic_catalog_version != catalog_version:
            issues.append("semantic_catalog_version_mismatch")
    if catalog_payload_path.is_file():
        catalog_sha = _sha256(catalog_payload_path)
        if semantic and semantic.get("catalog_sha256") != catalog_sha:
            issues.append("semantic_catalog_payload_hash_mismatch")
    else:
        catalog_sha = None
        issues.append("missing_catalog_payload")

    if gate:
        if gate.get("status") != "APPROVED":
            issues.append("semantic_gate_not_approved")
        if str(gate.get("catalog_version") or "") != catalog_version:
            issues.append("gate_catalog_version_mismatch")
        if str(gate.get("source_fingerprint") or "") != semantic_fingerprint:
            issues.append("gate_source_fingerprint_mismatch")
        if str(gate.get("model_binding_sha256") or "") != model_binding:
            issues.append("gate_model_binding_mismatch")

    if health:
        if health.get("status") != "READY":
            issues.append("semantic_runtime_not_ready")
        if str(health.get("catalog_version") or "") != catalog_version:
            issues.append("runtime_catalog_version_mismatch")
        if str(health.get("index_source_fingerprint") or "") != semantic_fingerprint:
            issues.append("runtime_source_fingerprint_mismatch")
        runtime_models = health.get("models") if isinstance(health.get("models"), dict) else {}
        if str(runtime_models.get("embedding") or "") != str(embedding or ""):
            issues.append("runtime_embedding_model_mismatch")
        if str(runtime_models.get("reranker") or "") != str(reranker or ""):
            issues.append("runtime_reranker_model_mismatch")
        heartbeat = health.get("heartbeat_epoch")
        if not isinstance(heartbeat, (int, float)):
            issues.append("runtime_heartbeat_missing")
        else:
            age = (time.time() if now_epoch is None else float(now_epoch)) - float(heartbeat)
            if age < -30 or age > max_heartbeat_age_seconds:
                issues.append("runtime_heartbeat_stale")

    semantic_files = semantic.get("files") if isinstance(semantic.get("files"), dict) else {}
    for label, key in (("semantic_documents", "documents"), ("semantic_vectors", "vectors")):
        relative = semantic_files.get(key)
        if not relative or not (semantic_root / str(relative)).is_file():
            issues.append(f"missing_{label}")

    control_hashes = {}
    for name, path in (
        ("catalog_manifest", catalog_manifest_path),
        ("semantic_manifest", semantic_manifest_path),
        ("semantic_gate", gate_path),
    ):
        if path.is_file():
            control_hashes[name] = _sha256(path)

    identity = {
        "catalog_version": catalog_version or None,
        "catalog_source_fingerprint": catalog.get("source_fingerprint"),
        "catalog_sha256": catalog_sha,
        "semantic_source_fingerprint": semantic_fingerprint or None,
        "model_binding_sha256": model_binding or None,
        "models": {"embedding": embedding, "reranker": reranker},
        "documents_sha256": (semantic.get("files") or {}).get("documents_sha256"),
        "vectors_sha256": (semantic.get("files") or {}).get("vectors_sha256"),
        "control_hashes": control_hashes,
    }
    runtime_observation = {
        "health_sha256": _sha256(health_path) if health_path.is_file() else None,
        "instance_id": health.get("instance_id"),
        "heartbeat_at": health.get("heartbeat_at"),
        "heartbeat_epoch": health.get("heartbeat_epoch"),
        "backend": health.get("backend"),
        "device_name": health.get("device_name"),
    }
    core_available = bool(catalog and semantic and gate and health and catalog_sha)
    status = "READY" if core_available and not issues else (
        "DEGRADED" if core_available else "UNAVAILABLE"
    )
    return {
        "schema_version": "ag2.kbase_semantic_release_bundle.v1",
        "status": status,
        **identity,
        "bundle_fingerprint": _fingerprint(identity),
        "runtime_observation": runtime_observation,
        "issues": sorted(set(issues)),
        "paths": {
            "catalog_release": str(CATALOG_RELEASE).replace("\\", "/"),
            "semantic_release": str(SEMANTIC_RELEASE).replace("\\", "/"),
            "runtime": str(SEMANTIC_RUNTIME).replace("\\", "/"),
        },
    }
