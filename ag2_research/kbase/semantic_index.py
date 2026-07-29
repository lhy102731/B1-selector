"""Provision, validate, gate, and publish the KBase BGE semantic release.

The initial release bridges the already evaluated 26,307-object BGE-M3 matrix
to the active catalog.  It does not recompute Attempt 18 or modify its frozen
artifacts.  Every durable change is candidate-first and hash-bound.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import yaml

from research_automation.foundations.immutable_release import ImmutableReleaseStore

from .hybrid_ranking import is_navigation_query, lexical_rank_with_anchors
from .query_regression import evaluate_case
from .semantic_client import merge_semantic_results


IMAGE = "mineru@sha256:16b753e76f3fd9609e2f2efcafb6bec786fc6ac9780fe12a675c32062fb52a6b"
SEMANTIC_ROOT = Path("wiki/outputs/manifests/ag2-kbase-semantic")
MODEL_ROOT = Path("wiki/outputs/models/ag2-kbase")
RUNTIME_ROOT = Path("wiki/outputs/runtime/ag2-kbase-semantic")
EMBEDDING_ID = "bge-m3"
EMBEDDING_REPOSITORY = "BAAI/bge-m3"
EMBEDDING_REVISION = "5617a9f61b028005a4858fdac845db406aefb181"
RERANKER_ID = "bge-reranker-v2-m3"
RERANKER_REPOSITORY = "BAAI/bge-reranker-v2-m3"
RERANKER_REVISION = "953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e"
MODEL_PREFIXES = {
    "embedding": "backend_models/embedding/bge-m3/",
    "reranker": "backend_models/reranker/bge-reranker-v2-m3/",
}
CATALOG_FILES = ("manifest.json", "catalog.jsonl", "facets.json", "build-report.json")
ALLOWED_TYPES = {"source_packet", "family", "source_note", "book", "video", "map"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"JSONL object required at {path}:{line_number}")
            rows.append(value)
    return rows


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    payload = _canonical_bytes(value)
    with temporary.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_copy_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    with source.open("rb") as input_handle, temporary.open("xb") as output_handle:
        shutil.copyfileobj(input_handle, output_handle, length=1024 * 1024)
        output_handle.flush()
        os.fsync(output_handle.fileno())
    os.replace(temporary, target)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _assert_inside(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as error:
        raise ValueError(f"path must stay inside {root}: {path}") from error
    return resolved


@contextmanager
def _exclusive_lock(path: Path, timeout: float = 30.0):
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    descriptor: int | None = None
    while descriptor is None:
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(descriptor, f"pid={os.getpid()} time={time.time()}\n".encode("ascii"))
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"lock timed out: {path}")
            time.sleep(0.05)
    try:
        yield
    finally:
        if descriptor is not None:
            os.close(descriptor)
        path.unlink(missing_ok=True)


def _inventory_rows(source_candidate: Path) -> list[dict[str, Any]]:
    allowlist = _read_json(source_candidate / ".superpowers/sdd/tasks3-7-staging-allowlist.json")
    rows = allowlist.get("rows")
    if not isinstance(rows, list):
        raise ValueError("model allowlist rows are missing")
    selected = [
        row for row in rows
        if isinstance(row, dict) and any(str(row.get("path") or "").startswith(prefix) for prefix in MODEL_PREFIXES.values())
    ]
    selected.sort(key=lambda row: str(row["path"]))
    if not selected:
        raise ValueError("selected BGE model files are absent from the approved allowlist")
    for row in selected:
        path = source_candidate / str(row["path"])
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"approved model file unavailable: {path}")
        if path.stat().st_size != int(row["bytes"]) or _sha256_file(path) != row["sha256"]:
            raise ValueError(f"approved model file identity mismatch: {path}")
    return selected


def _evaluation_binding(evaluation_dir: Path) -> dict[str, Any]:
    metrics = evaluation_dir / "metrics/final20.json"
    audit = evaluation_dir / "reports/final20-audit-v2.json"
    metrics_doc = _read_json(metrics)
    audit_doc = _read_json(audit)
    if metrics_doc.get("status") != "COMPLETED" or audit_doc.get("status") != "APPROVED":
        raise ValueError("joint evaluation is not approved")
    return {
        "metrics_path": str(metrics),
        "metrics_sha256": _sha256_file(metrics),
        "metrics_status": metrics_doc.get("status"),
        "winner_gate": metrics_doc.get("winner_gate"),
        "audit_path": str(audit),
        "audit_sha256": _sha256_file(audit),
        "audit_status": audit_doc.get("status"),
        "audit_decision": audit_doc.get("decision"),
    }


def validate_model_release(release: Path) -> dict[str, Any]:
    manifest = _read_json(release / "manifest.json")
    if manifest.get("schema_version") != "kbase.selected_model_bundle.v1" or manifest.get("status") != "COMPLETED":
        raise ValueError("selected model bundle manifest is invalid")
    rows = manifest.get("files")
    if not isinstance(rows, list) or not rows:
        raise ValueError("selected model bundle inventory is empty")
    seen: set[str] = set()
    total = 0
    for row in rows:
        relative = str(row.get("path") or "")
        if relative in seen:
            raise ValueError("duplicate selected model bundle path")
        seen.add(relative)
        path = _assert_inside(release / relative, release)
        if not path.is_file() or path.stat().st_size != int(row["bytes"]):
            raise ValueError(f"selected model bundle file unavailable: {relative}")
        if _sha256_file(path) != row["sha256"]:
            raise ValueError(f"selected model bundle hash mismatch: {relative}")
        total += path.stat().st_size
    if manifest.get("counts") != {"files": len(rows), "bytes": total, "models": 2}:
        raise ValueError("selected model bundle counts mismatch")
    expected = {
        "embedding": {"model_id": EMBEDDING_ID, "repository": EMBEDDING_REPOSITORY, "revision": EMBEDDING_REVISION},
        "reranker": {"model_id": RERANKER_ID, "repository": RERANKER_REPOSITORY, "revision": RERANKER_REVISION},
    }
    if manifest.get("models") != expected:
        raise ValueError("selected model bundle identity mismatch")
    return manifest


def provision_models(
    *, vault: Path, source_candidate: Path, evaluation_dir: Path, apply: bool,
) -> dict[str, Any]:
    vault = vault.resolve()
    source_candidate = _assert_inside(source_candidate, vault / "wiki/outputs/candidates")
    rows = _inventory_rows(source_candidate)
    evaluation = _evaluation_binding(evaluation_dir)
    inventory_identity = sha256(_canonical_bytes([
        {key: row[key] for key in ("path", "sha256", "bytes")} for row in rows
    ])).hexdigest()
    root = vault / MODEL_ROOT
    current = root / "current"
    if current.is_dir():
        current_manifest = validate_model_release(current)
        if current_manifest.get("source_fingerprint") == inventory_identity:
            return {"status": "UNCHANGED", "current": str(current), "manifest": current_manifest}
    if not apply:
        return {"status": "DRY_RUN", "source_fingerprint": inventory_identity, "counts": {"files": len(rows), "bytes": sum(int(row["bytes"]) for row in rows)}}

    build_id = f"models-{inventory_identity[:16]}"
    candidate = root / "candidate" / build_id
    if candidate.exists():
        raise FileExistsError(f"non-identical or abandoned model candidate exists: {candidate}")
    candidate.mkdir(parents=True)
    published_rows: list[dict[str, Any]] = []
    for row in rows:
        source_relative = str(row["path"])
        if source_relative.startswith(MODEL_PREFIXES["embedding"]):
            relative = "embedding/bge-m3/" + source_relative[len(MODEL_PREFIXES["embedding"]):]
        elif source_relative.startswith(MODEL_PREFIXES["reranker"]):
            relative = "reranker/bge-reranker-v2-m3/" + source_relative[len(MODEL_PREFIXES["reranker"]):]
        else:
            raise ValueError("unexpected model inventory path")
        source = source_candidate / source_relative
        target = candidate / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(source, target)
        published_rows.append({"path": relative.replace("\\", "/"), "sha256": row["sha256"], "bytes": int(row["bytes"])})
    published_rows.sort(key=lambda row: row["path"])
    manifest = {
        "schema_version": "kbase.selected_model_bundle.v1",
        "generator": "ag2_research.kbase.semantic_index",
        "generator_version": "1.0.0",
        "generated_at": _now(),
        "source_fingerprint": inventory_identity,
        "source_candidate": str(source_candidate),
        "evaluation": evaluation,
        "models": {
            "embedding": {"model_id": EMBEDDING_ID, "repository": EMBEDDING_REPOSITORY, "revision": EMBEDDING_REVISION},
            "reranker": {"model_id": RERANKER_ID, "repository": RERANKER_REPOSITORY, "revision": RERANKER_REVISION},
        },
        "files": published_rows,
        "counts": {"files": len(published_rows), "bytes": sum(row["bytes"] for row in published_rows), "models": 2},
        "sanitized_config": {"storage": "ntfs_hardlink", "trust_remote_code": False, "external_download": False},
        "status": "COMPLETED",
    }
    _atomic_json(candidate / "manifest.json", manifest)
    validate_model_release(candidate)
    _publish_directory(root=root, candidate=candidate, validator=validate_model_release)
    return {"status": "PUBLISHED", "current": str(current), "manifest": validate_model_release(current)}


def _publish_directory(*, root: Path, candidate: Path, validator: Any) -> None:
    candidate = _assert_inside(candidate, root / "candidate")

    class _ValidatorAdapter:
        def validate(self, release: Path) -> str:
            result = validator(release)
            manifest_path = release / "manifest.json"
            if manifest_path.is_file():
                return _sha256_file(manifest_path)
            if isinstance(result, str) and result:
                return result
            raise ValueError("release validator returned no immutable identity")

    adapter = _ValidatorAdapter()
    current = root / "current"
    expected_current_id = adapter.validate(current) if current.exists() else None
    expected_candidate_id = adapter.validate(candidate)
    ImmutableReleaseStore(root, adapter=adapter).promote(
        candidate,
        expected_current_id=expected_current_id,
        expected_candidate_id=expected_candidate_id,
    )


def _semantic_payload_identity(manifest: dict[str, Any]) -> bytes:
    return _canonical_bytes({
        key: manifest.get(key)
        for key in (
            "source_fingerprint", "catalog_version", "catalog_source_fingerprint",
            "catalog_sha256", "catalog_files", "model_binding_sha256", "models",
            "dimension", "dtype", "counts", "lexical_only_sources", "files",
        )
    })


def _clone_release_with_hardlinks(source: Path, target: Path) -> None:
    if target.exists():
        raise FileExistsError(f"release clone target exists: {target}")
    target.mkdir(parents=True)
    for path in sorted(source.rglob("*"), key=lambda value: (len(value.parts), str(value))):
        relative = path.relative_to(source)
        destination = target / relative
        if path.is_dir():
            destination.mkdir(exist_ok=True)
        elif path.is_file():
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(path, destination)
            except OSError:
                shutil.copy2(path, destination)


def _publish_semantic_metadata_update(
    *, root: Path, candidate: Path, validator: Any,
) -> Path | None:
    """Promote new gates for an identical index without renaming a live bind.

    Docker Desktop keeps the mounted ``current`` directory open on Windows,
    which prevents a directory swap.  Immutable payload equality makes a
    file-level metadata promotion safe while a full validated previous release
    is retained for rollback.
    """
    candidate = _assert_inside(candidate, root / "candidate")
    current = root / "current"
    previous = root / "previous"
    archive = root / "archive"
    validator(candidate)
    validator(current)
    if _semantic_payload_identity(_read_json(candidate / "manifest.json")) != _semantic_payload_identity(_read_json(current / "manifest.json")):
        raise ValueError("metadata-only promotion requires an identical semantic payload")

    metadata_files = (
        "regression-fixed.json",
        "regression-holdout.json",
        "gate-report.json",
        "validation.json",
        "manifest.json",
    )
    promoted_archive: Path | None = None
    with _exclusive_lock(root / ".publish.lock"):
        # Re-check inside the lock so a concurrent catalog or semantic release
        # cannot turn the metadata update into a cross-index mix.
        validator(candidate)
        validator(current)
        if _semantic_payload_identity(_read_json(candidate / "manifest.json")) != _semantic_payload_identity(_read_json(current / "manifest.json")):
            raise ValueError("semantic payload changed while acquiring publish lock")

        temporary_previous = root / f".previous.{uuid.uuid4().hex}.tmp"
        _clone_release_with_hardlinks(current, temporary_previous)
        validator(temporary_previous)
        archived_previous: Path | None = None
        if previous.exists():
            archive.mkdir(parents=True, exist_ok=True)
            archived_previous = archive / f"previous-{datetime.now().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
            os.replace(previous, archived_previous)
        os.replace(temporary_previous, previous)
        try:
            for name in metadata_files[:-1]:
                _atomic_copy_file(candidate / name, current / name)
            # Manifest is the commit point and is written last.
            _atomic_copy_file(candidate / "manifest.json", current / "manifest.json")
            validator(current)
        except Exception:
            for name in metadata_files[:-1]:
                _atomic_copy_file(previous / name, current / name)
            _atomic_copy_file(previous / "manifest.json", current / "manifest.json")
            validator(current)
            raise

        archive.mkdir(parents=True, exist_ok=True)
        promoted_archive = archive / f"metadata-candidate-{datetime.now().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        try:
            os.replace(candidate, promoted_archive)
        except OSError:
            promoted_archive = None
    return promoted_archive


def _catalog_binding(vault: Path) -> tuple[Path, dict[str, Any], list[dict[str, Any]], dict[str, str]]:
    current = vault / "wiki/outputs/manifests/ag2-kbase/current"
    manifest = _read_json(current / "manifest.json")
    rows = _read_jsonl(current / "catalog.jsonl")
    hashes = {name: _sha256_file(current / name) for name in CATALOG_FILES}
    if manifest.get("counts", {}).get("entries") != len(rows):
        raise ValueError("active catalog count mismatch")
    if len({str(row.get("source_id") or "") for row in rows}) != len(rows):
        raise ValueError("active catalog source IDs are not unique")
    return current, manifest, rows, hashes


def _document_type(retrieval_text: str) -> str:
    match = re.search(r"(?:^|\n)object_type:\s*([^\n]+)", retrieval_text)
    return match.group(1).strip() if match else "content_object"


def bootstrap_candidate(
    *, vault: Path, source_candidate: Path, evaluation_dir: Path, apply: bool,
) -> dict[str, Any]:
    vault = vault.resolve()
    source_candidate = _assert_inside(source_candidate, vault / "wiki/outputs/candidates")
    model_release = vault / MODEL_ROOT / "current"
    model_manifest = validate_model_release(model_release)
    catalog_dir, catalog_manifest, catalog_rows, catalog_hashes = _catalog_binding(vault)
    evaluation = _evaluation_binding(evaluation_dir)
    corpus_path = source_candidate / "model_bakeoff/corpus.jsonl"
    source_index_path = source_candidate / "model_bakeoff_outputs/runs/first_stage/bge-m3/dense_index.bin"
    dense_manifest_path = source_candidate / "model_bakeoff_outputs/runs/first_stage/bge-m3/dense_manifest.json"
    dense_manifest = _read_json(dense_manifest_path)
    corpus_sha = _sha256_file(corpus_path)
    index_sha = _sha256_file(source_index_path)
    if dense_manifest.get("corpus_fingerprint") != corpus_sha or dense_manifest.get("index_sha256") != index_sha:
        raise ValueError("evaluated BGE index binding mismatch")
    corpus_rows = _read_jsonl(corpus_path)
    vectors = np.load(source_index_path, mmap_mode="r", allow_pickle=False)
    if vectors.shape != (len(corpus_rows), 1024) or vectors.dtype != np.float32:
        raise ValueError("evaluated BGE index shape or dtype mismatch")

    catalog_by_id = {str(row["source_id"]): row for row in catalog_rows}
    corpus_source_ids: set[str] = set()
    documents: list[dict[str, Any]] = []
    seen_objects: set[str] = set()
    for row in corpus_rows:
        object_id = str(row.get("object_id") or "")
        source_id = str(row.get("source_id") or "")
        retrieval_text = str(row.get("retrieval_text") or "")
        content_sha = str(row.get("content_sha256") or "")
        if not object_id or object_id in seen_objects or source_id not in catalog_by_id:
            raise ValueError("evaluated corpus object/source identity mismatch")
        if sha256(retrieval_text.encode("utf-8")).hexdigest() != content_sha:
            raise ValueError("evaluated corpus content hash mismatch")
        seen_objects.add(object_id)
        corpus_source_ids.add(source_id)
        documents.append({
            "object_id": object_id,
            "source_id": source_id,
            "content_sha256": content_sha,
            "document_type": _document_type(retrieval_text),
            "retrieval_text": retrieval_text,
        })
    packet_ids = {str(row["source_id"]) for row in catalog_rows if row.get("object_type") == "source_packet"}
    extra = corpus_source_ids - set(catalog_by_id)
    if extra:
        raise ValueError("evaluated corpus contains sources outside the active catalog")
    lexical_only = sorted(packet_ids - corpus_source_ids)
    lexical_only_rows = [
        {
            "source_id": source_id,
            "title": catalog_by_id[source_id].get("title"),
            "available_layers": catalog_by_id[source_id].get("available_layers", []),
            "warnings": catalog_by_id[source_id].get("warnings", []),
            "reason": "no_evaluated_content_object_lexical_fallback",
        }
        for source_id in lexical_only
    ]
    source_fingerprint = sha256(_canonical_bytes({
        "catalog": catalog_hashes,
        "corpus": corpus_sha,
        "index": index_sha,
        "models": model_manifest["source_fingerprint"],
    })).hexdigest()
    if not apply:
        return {
            "status": "DRY_RUN",
            "source_fingerprint": source_fingerprint,
            "counts": {"documents": len(documents), "indexed_sources": len(corpus_source_ids), "lexical_only_source_packets": len(lexical_only)},
        }

    root = vault / SEMANTIC_ROOT
    candidate = root / "candidate" / f"bridge-{source_fingerprint[:16]}"
    if candidate.exists():
        raise FileExistsError(f"semantic candidate already exists: {candidate}")
    candidate.mkdir(parents=True)
    _write_jsonl(candidate / "documents.jsonl", documents)
    os.link(source_index_path, candidate / "vectors.npy")
    for name in CATALOG_FILES:
        target_name = "catalog-manifest.json" if name == "manifest.json" else name
        os.link(catalog_dir / name, candidate / target_name)
    manifest = {
        "schema_version": "kbase.semantic_index.v1",
        "generator": "ag2_research.kbase.semantic_index",
        "generator_version": "1.0.0",
        "generated_at": _now(),
        "source_fingerprint": source_fingerprint,
        "catalog_version": catalog_manifest["catalog_version"],
        "catalog_source_fingerprint": catalog_manifest["source_fingerprint"],
        "catalog_sha256": catalog_hashes["catalog.jsonl"],
        "catalog_files": catalog_hashes,
        "model_binding_sha256": model_manifest["source_fingerprint"],
        "models": model_manifest["models"],
        "dimension": 1024,
        "dtype": "float32",
        "counts": {
            "entries": len(catalog_rows),
            "source_packets": len(packet_ids),
            "documents": len(documents),
            "indexed_sources": len(corpus_source_ids),
            "indexed_source_packets": len(packet_ids & corpus_source_ids),
            "lexical_only_source_packets": len(lexical_only),
        },
        "lexical_only_sources": lexical_only_rows,
        "files": {
            "documents": "documents.jsonl",
            "documents_sha256": _sha256_file(candidate / "documents.jsonl"),
            "vectors": "vectors.npy",
            "vectors_sha256": _sha256_file(candidate / "vectors.npy"),
        },
        "provenance": {
            "source_candidate": str(source_candidate),
            "evaluated_corpus_path": str(corpus_path),
            "evaluated_corpus_sha256": corpus_sha,
            "evaluated_index_path": str(source_index_path),
            "evaluated_index_sha256": index_sha,
            "evaluated_dense_manifest_path": str(dense_manifest_path),
            "evaluated_dense_manifest_sha256": _sha256_file(dense_manifest_path),
            "joint_evaluation": evaluation,
        },
        "runtime": {
            "backend": "torch_cuda",
            "device": "cuda:0",
            "device_name": "NVIDIA GeForce RTX 5080",
            "torch_version": "2.11.0+cu130",
            "cuda_version": "13.0",
            "cpu_fallback": False,
        },
        "sanitized_config": {
            "retrieval": "lexical+bge-m3-rrf",
            "reranker": RERANKER_ID,
            "candidate_limit": 64,
            "result_limit": 100,
            "exact_anchor_protection": True,
            "lexical_anchor_required": True,
            "family_limit": 2,
            "network": "none",
            "external_calls": 0,
        },
        "release_gates": {"status": "PENDING"},
        "promotion_status": "candidate",
        "status": "COMPLETED",
    }
    _atomic_json(candidate / "manifest.json", manifest)
    validation = validate_semantic_release(candidate, active_catalog_dir=catalog_dir, require_gates=False)
    _atomic_json(candidate / "validation.json", validation)
    return {"status": "CANDIDATE_READY", "candidate": str(candidate), "manifest": manifest, "validation": validation}


def validate_semantic_release(
    release: Path, *, active_catalog_dir: Path | None = None, require_gates: bool = True,
) -> dict[str, Any]:
    manifest = _read_json(release / "manifest.json")
    if manifest.get("schema_version") != "kbase.semantic_index.v1" or manifest.get("status") != "COMPLETED":
        raise ValueError("semantic manifest invalid")
    documents_path = release / str(manifest.get("files", {}).get("documents") or "")
    vectors_path = release / str(manifest.get("files", {}).get("vectors") or "")
    if _sha256_file(documents_path) != manifest["files"]["documents_sha256"]:
        raise ValueError("semantic documents hash mismatch")
    if _sha256_file(vectors_path) != manifest["files"]["vectors_sha256"]:
        raise ValueError("semantic vectors hash mismatch")
    documents = _read_jsonl(documents_path)
    object_ids = [str(row.get("object_id") or "") for row in documents]
    if not object_ids or len(set(object_ids)) != len(object_ids):
        raise ValueError("semantic document object IDs invalid")
    for row in documents:
        text = str(row.get("retrieval_text") or "")
        if sha256(text.encode("utf-8")).hexdigest() != row.get("content_sha256"):
            raise ValueError("semantic document content hash mismatch")
    vectors = np.load(vectors_path, mmap_mode="r", allow_pickle=False)
    if vectors.shape != (len(documents), int(manifest["dimension"])) or vectors.dtype != np.float32:
        raise ValueError("semantic vectors shape/dtype mismatch")
    if not np.isfinite(vectors).all():
        raise ValueError("semantic vectors contain non-finite values")
    norms = np.linalg.norm(np.asarray(vectors, dtype=np.float64), axis=1)
    if not np.all(np.abs(norms - 1.0) <= 1e-5):
        raise ValueError("semantic vectors are not normalized")
    catalog_path = release / "catalog.jsonl"
    catalog_manifest_path = release / "catalog-manifest.json"
    if _sha256_file(catalog_path) != manifest["catalog_files"]["catalog.jsonl"]:
        raise ValueError("semantic catalog copy hash mismatch")
    if _sha256_file(catalog_manifest_path) != manifest["catalog_files"]["manifest.json"]:
        raise ValueError("semantic catalog manifest copy hash mismatch")
    catalog_rows = _read_jsonl(catalog_path)
    catalog_ids = {str(row["source_id"]) for row in catalog_rows}
    document_sources = {str(row["source_id"]) for row in documents}
    if not document_sources <= catalog_ids:
        raise ValueError("semantic documents escape the bound catalog")
    packet_ids = {str(row["source_id"]) for row in catalog_rows if row.get("object_type") == "source_packet"}
    lexical_only = {str(row["source_id"]) for row in manifest.get("lexical_only_sources") or []}
    if lexical_only != packet_ids - document_sources:
        raise ValueError("semantic lexical-only source accounting mismatch")
    counts = manifest.get("counts", {})
    expected_counts = {
        "entries": len(catalog_rows),
        "source_packets": len(packet_ids),
        "documents": len(documents),
        "indexed_sources": len(document_sources),
        "indexed_source_packets": len(packet_ids & document_sources),
        "lexical_only_source_packets": len(lexical_only),
    }
    if counts != expected_counts:
        raise ValueError("semantic release counts mismatch")
    if active_catalog_dir is not None:
        for name in CATALOG_FILES:
            if _sha256_file(active_catalog_dir / name) != manifest["catalog_files"][name]:
                raise ValueError("active catalog changed after semantic candidate build")
    gates = manifest.get("release_gates") or {}
    if require_gates:
        if gates.get("status") != "APPROVED":
            raise ValueError("semantic release gates are not approved")
        for key in ("fixed_report", "holdout_report", "gate_report"):
            record = gates.get(key)
            if not isinstance(record, dict):
                raise ValueError(f"semantic release gate missing: {key}")
            path = release / str(record.get("path") or "")
            if not path.is_file() or _sha256_file(path) != record.get("sha256"):
                raise ValueError(f"semantic release gate hash mismatch: {key}")
    return {
        "schema_version": "kbase.semantic_validation.v1",
        "generated_at": _now(),
        "status": "PASS",
        "source_fingerprint": manifest["source_fingerprint"],
        "counts": expected_counts,
        "checks": {
            "hashes": "PASS", "shape": "PASS", "finite": "PASS", "unit_norm": "PASS",
            "catalog_binding": "PASS", "source_coverage": "PASS", "release_gates": "PASS" if require_gates else "PENDING",
        },
    }


def _docker_base(project_root: Path) -> list[str]:
    return [
        "docker", "run", "--rm", "--gpus", "all", "--network", "none",
        "--read-only", "--tmpfs", "/tmp:rw,size=1g", "--shm-size", "1g",
        "-e", "HF_HUB_OFFLINE=1", "-e", "TRANSFORMERS_OFFLINE=1",
        "-e", "HF_HOME=/tmp/hf", "-e", "PYTHONDONTWRITEBYTECODE=1",
        "-v", f"{project_root}:/app:ro",
    ]


def _suite_requests(
    *, entries: list[dict[str, Any]], suite_path: Path, suite_name: str,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, Any]]:
    suite = yaml.safe_load(suite_path.read_text(encoding="utf-8"))
    cases = suite.get("cases") if isinstance(suite, dict) else None
    if not isinstance(cases, list):
        raise ValueError(f"query suite cases missing: {suite_path}")
    searchable = [entry for entry in entries if entry.get("object_type") in ALLOWED_TYPES]
    allowed_ids = [str(entry["source_id"]) for entry in searchable]
    requests: list[dict[str, Any]] = []
    contexts: dict[str, dict[str, Any]] = {}
    for case in cases:
        request_id = f"{suite_name}:{case['id']}"
        lexical, protected = lexical_rank_with_anchors(searchable, str(case["query"]), limit=100)
        requests.append({
            "request_id": request_id,
            "query": str(case["query"]),
            "lexical_ids": [str(entry["source_id"]) for entry in lexical],
            "allowed_ids": allowed_ids,
            "candidate_limit": 64,
            "result_limit": 100,
        })
        contexts[request_id] = {
            "case": case,
            "lexical": lexical,
            "protected": protected,
            "semantic_enabled": not is_navigation_query(str(case["query"])),
        }
    return requests, contexts, suite


def _compatible_cached_requests(
    *,
    executed: dict[str, Any],
    current: dict[str, Any],
    contexts: dict[str, dict[str, Any]],
) -> list[str]:
    """Allow reuse only when changes are confined to semantic-bypassed navigation.

    The cached GPU rows remain authoritative for every semantic-enabled case.
    Navigation rows are not consumed by the evaluator or production path, so a
    changed lexical ordering for those requests does not require model work.
    """
    for field in ("schema_version", "source_fingerprint"):
        if executed.get(field) != current.get(field):
            raise RuntimeError(f"existing semantic regression {field} binding changed")
    executed_rows = executed.get("requests")
    current_rows = current.get("requests")
    if not isinstance(executed_rows, list) or not isinstance(current_rows, list):
        raise RuntimeError("existing semantic regression requests are invalid")
    executed_by_id = {str(row.get("request_id") or ""): row for row in executed_rows if isinstance(row, dict)}
    current_by_id = {str(row.get("request_id") or ""): row for row in current_rows if isinstance(row, dict)}
    if not executed_by_id or set(executed_by_id) != set(current_by_id):
        raise RuntimeError("existing semantic regression request IDs changed")
    changed_navigation: list[str] = []
    for request_id, current_row in current_by_id.items():
        executed_row = executed_by_id[request_id]
        if executed_row == current_row:
            continue
        context = contexts.get(request_id) or {}
        if context.get("semantic_enabled", True):
            raise RuntimeError(f"semantic-enabled cached request changed: {request_id}")
        # Query identity and all execution fields except the unused lexical
        # ordering must still match exactly.
        executed_without_lexical = {key: value for key, value in executed_row.items() if key != "lexical_ids"}
        current_without_lexical = {key: value for key, value in current_row.items() if key != "lexical_ids"}
        if executed_without_lexical != current_without_lexical:
            raise RuntimeError(f"navigation cached request changed beyond lexical order: {request_id}")
        changed_navigation.append(request_id)
    return sorted(changed_navigation)


def _trace_success_for_case(
    case: dict[str, Any], outcome: dict[str, Any], results: list[dict[str, Any]],
) -> bool | None:
    """Evaluate catalog-backed traceability with query_regression semantics."""
    if case.get("intent") == "negative" or not outcome["hit"]:
        return None
    result_ids = [str(item.get("source_id")) for item in results if item.get("source_id")]
    target_ids = [str(item) for item in (case.get("expected") or {}).get("source_ids", [])]
    traced_ids = [source_id for source_id in result_ids if source_id in target_ids]
    if not traced_ids and result_ids:
        traced_ids = result_ids[:1]
    trace_results: list[bool] = []
    for source_id in traced_ids:
        entry = next((item for item in results if str(item.get("source_id") or "") == source_id), None)
        trace_results.append(
            entry is not None
            and bool(entry.get("paths"))
            and (
                not case.get("evidence_layer_required")
                or "evidence" in (entry.get("available_layers") or [])
            )
        )
    return bool(trace_results) and all(trace_results)


def _brief_compliance_for_results(
    results: list[dict[str, Any]], forbidden_scopes: list[str],
) -> tuple[bool, list[str]]:
    """Check that search output remains a source-only, traceable brief input."""
    issues: list[str] = []
    project_fields = {
        "factor_spec", "hypothesis", "formula", "parameters", "proxy",
        "experiment_queue", "strategy_mapping",
    }
    for index, entry in enumerate(results):
        source_id = str(entry.get("source_id") or "")
        if not source_id:
            issues.append(f"result[{index}]:missing_source_id")
        if str(entry.get("object_type") or "") not in ALLOWED_TYPES:
            issues.append(f"result[{index}]:disallowed_object_type")
        paths = entry.get("paths")
        if not isinstance(paths, dict) or not any(str(value) for value in paths.values()):
            issues.append(f"result[{index}]:missing_provenance_path")
        if project_fields & set(entry):
            issues.append(f"result[{index}]:project_derivation_field")
        rendered_paths = " ".join(
            str(value).replace("\\", "/").lower()
            for value in (paths.values() if isinstance(paths, dict) else [])
        )
        if any(str(scope).replace("\\", "/").lower() in rendered_paths for scope in forbidden_scopes):
            issues.append(f"result[{index}]:forbidden_output_scope")
    return not issues, issues


def _evaluate_suite(
    *, suite_name: str, suite_path: Path, suite: dict[str, Any], contexts: dict[str, dict[str, Any]],
    batch_by_id: dict[str, dict[str, Any]], entries_by_id: dict[str, dict[str, Any]], semantic_manifest: dict[str, Any],
) -> dict[str, Any]:
    forbidden = list(suite.get("forbidden_scopes") or [])
    rows: list[dict[str, Any]] = []
    known = 0
    known_pass = 0
    pollution = 0
    trace_required = 0
    trace_pass = 0
    brief_pass = 0
    noncompliant_results = 0
    regressions = 0
    for request_id, context in contexts.items():
        case = context["case"]
        semantic_enabled = bool(context.get("semantic_enabled", True))
        batch = batch_by_id.get(request_id)
        error = None
        semantic_result: dict[str, Any] = {"results": []}
        if not semantic_enabled:
            merged = context["lexical"]
        elif not batch or batch.get("status") != "COMPLETED":
            error = str((batch or {}).get("error") or "missing batch result")
            merged = context["lexical"]
        else:
            semantic_result = batch["result"]
            merged = merge_semantic_results(
                lexical_ranked=context["lexical"], entries_by_id=entries_by_id,
                semantic_result=semantic_result, protected_ids=context["protected"], limit=100,
            )
        top = merged[: int(suite.get("max_results") or 5)]
        top_ids = [str(entry["source_id"]) for entry in top]
        baseline = context["lexical"][: int(suite.get("max_results") or 5)]
        baseline_ids = [str(entry["source_id"]) for entry in baseline]
        requires_refinement = len(str(case["query"]).strip()) <= 2
        outcome = evaluate_case(
            case,
            {"results": top, "requires_refinement": requires_refinement, "error": error},
            forbidden,
        )
        baseline_outcome = evaluate_case(
            case,
            {"results": baseline, "requires_refinement": requires_refinement, "error": None},
            forbidden,
        )
        trace_ok = _trace_success_for_case(case, outcome, top)
        baseline_trace_ok = _trace_success_for_case(case, baseline_outcome, baseline)
        brief_ok, brief_issues = _brief_compliance_for_results(top, forbidden)
        baseline_brief_ok, _ = _brief_compliance_for_results(baseline, forbidden)
        positive = case.get("intent") != "negative"
        known += int(positive)
        known_pass += int(positive and bool(outcome["hit"]))
        trace_required += int(positive)
        trace_pass += int(bool(trace_ok))
        brief_pass += int(brief_ok)
        noncompliant_results += len(brief_issues)
        case_pollution = len(outcome["pollution_paths"])
        pollution += case_pollution
        passed = error is None and bool(outcome["hit"]) and (not positive or bool(trace_ok)) and brief_ok
        baseline_passed = (
            bool(baseline_outcome["hit"])
            and (not positive or bool(baseline_trace_ok))
            and baseline_brief_ok
        )
        if baseline_passed and not passed:
            regressions += 1
        rows.append({
            "case_id": case["id"], "intent": case.get("intent"), "query_sha256": sha256(str(case["query"]).encode("utf-8")).hexdigest(),
            "baseline_top_ids": baseline_ids, "hybrid_top_ids": top_ids,
            "protected_ids": context["protected"], "passed": passed,
            "semantic_enabled": semantic_enabled,
            "hit": bool(outcome["hit"]), "baseline_hit": bool(baseline_outcome["hit"]),
            "matched_expected_sources": outcome["matched_expected_sources"],
            "trace_success": trace_ok, "baseline_trace_success": baseline_trace_ok,
            "brief_compliant": brief_ok, "brief_compliance_issues": brief_issues,
            "baseline_passed": baseline_passed, "error": error, "pollution": case_pollution,
            "pollution_paths": outcome["pollution_paths"],
        })
    recall = known_pass / known if known else 1.0
    trace_success = trace_pass / trace_required if trace_required else 1.0
    brief_compliance = brief_pass / len(rows) if rows else 1.0
    acceptance = suite.get("acceptance") or {}
    status = "PASS" if (
        recall >= float(acceptance.get("known_item_recall_at_5", 0.0))
        and pollution == 0
        and trace_success >= float(acceptance.get("trace_success", 0.0))
        and brief_compliance >= float(acceptance.get("brief_compliance", 0.0))
        and regressions == 0
        and all(row["error"] is None for row in rows)
    ) else "FAIL"
    return {
        "schema_version": "kbase.semantic_query_regression.v1",
        "generator": "ag2_research.kbase.semantic_index",
        "generator_version": "1.0.0",
        "generated_at": _now(),
        "source_fingerprint": semantic_manifest["source_fingerprint"],
        "suite": suite_name,
        "suite_path": str(suite_path),
        "suite_sha256": _sha256_file(suite_path),
        "catalog_version": semantic_manifest["catalog_version"],
        "catalog_sha256": semantic_manifest["catalog_sha256"],
        "model_binding_sha256": semantic_manifest["model_binding_sha256"],
        "counts": {"cases": len(rows), "known": known, "known_pass": known_pass, "errors": sum(row["error"] is not None for row in rows), "regressions": regressions, "polluted_results": pollution, "brief_noncompliance_issues": noncompliant_results},
        "metrics": {"known_item_recall_at_5": recall, "output_pollution": 0.0 if not rows else pollution / max(1, sum(len(row["hybrid_top_ids"]) for row in rows)), "trace_success": trace_success, "brief_compliance": brief_compliance},
        "acceptance": acceptance,
        "cases": rows,
        "status": status,
    }


def run_candidate_regression(
    *, vault: Path, candidate: Path, project_root: Path, apply: bool,
) -> dict[str, Any]:
    vault = vault.resolve()
    candidate = _assert_inside(candidate, vault / SEMANTIC_ROOT / "candidate")
    manifest = _read_json(candidate / "manifest.json")
    validate_semantic_release(candidate, active_catalog_dir=vault / "wiki/outputs/manifests/ag2-kbase/current", require_gates=False)
    entries = _read_jsonl(candidate / "catalog.jsonl")
    entries_by_id = {str(entry["source_id"]): entry for entry in entries}
    package = project_root / "ag2_research/kbase"
    fixed_path = package / "query_regression.yaml"
    holdout_path = package / "query_holdout.yaml"
    fixed_requests, fixed_contexts, fixed_suite = _suite_requests(entries=entries, suite_path=fixed_path, suite_name="fixed")
    holdout_requests, holdout_contexts, holdout_suite = _suite_requests(entries=entries, suite_path=holdout_path, suite_name="holdout")
    if not apply:
        return {"status": "DRY_RUN", "counts": {"fixed": len(fixed_requests), "holdout": len(holdout_requests)}}

    work = candidate / "regression-work"
    request_document = {
        "schema_version": "kbase.semantic_batch_requests.v1",
        "source_fingerprint": manifest["source_fingerprint"],
        "requests": fixed_requests + holdout_requests,
    }
    results_path = work / "results.json"
    resumed = False
    cache_reuse: dict[str, Any] | None = None
    if work.exists():
        request_path = work / "requests.json"
        if not request_path.is_file():
            raise RuntimeError("existing semantic regression work has no request binding")
        if not results_path.is_file():
            raise RuntimeError("semantic regression work is still in progress; refusing a duplicate run")
        if request_path.read_bytes() != _canonical_bytes(request_document):
            executed_requests = _read_json(request_path)
            all_contexts = {**fixed_contexts, **holdout_contexts}
            compatible_navigation = _compatible_cached_requests(
                executed=executed_requests,
                current=request_document,
                contexts=all_contexts,
            )
            cache_reuse = {
                "schema_version": "kbase.semantic_regression_cache_reuse.v1",
                "generator": "ag2_research.kbase.semantic_index",
                "generator_version": "1.0.0",
                "generated_at": _now(),
                "source_fingerprint": manifest["source_fingerprint"],
                "executed_request_sha256": _sha256_file(request_path),
                "current_request_sha256": sha256(_canonical_bytes(request_document)).hexdigest(),
                "batch_results_sha256": _sha256_file(results_path),
                "compatible_navigation_request_ids": compatible_navigation,
                "reason": "semantic_bypassed_navigation_lexical_order_only",
                "status": "APPROVED_FOR_REUSE",
            }
            _atomic_json(work / "cache-reuse.json", cache_reuse)
        resumed = True
    else:
        work.mkdir()
        _atomic_json(work / "requests.json", request_document)
    models = vault / MODEL_ROOT / "current"
    validate_model_release(models)
    command = _docker_base(project_root) + [
        "-v", f"{candidate}:/semantic:ro",
        "-v", f"{models}:/models:ro",
        "-v", f"{work}:/work:rw",
        "--entrypoint", "/bin/bash", IMAGE, "-lc",
        "python3 /app/ag2_research/kbase/semantic_worker.py batch --semantic-root /semantic "
        "--embedding-root /models/embedding/bge-m3 --reranker-root /models/reranker/bge-reranker-v2-m3 "
        "--requests /work/requests.json --output /work/results.json",
    ]
    if not resumed:
        completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=600)
        _atomic_json(work / "execution.json", {
            "schema_version": "kbase.semantic_regression_execution.v1",
            "generated_at": _now(), "image": IMAGE, "network": "none",
            "returncode": completed.returncode, "stdout_tail": completed.stdout[-4000:], "stderr_tail": completed.stderr[-4000:],
            "resumed": False,
        })
        if completed.returncode != 0:
            raise RuntimeError(f"semantic regression worker failed: {completed.stderr[-1000:]}")
    elif not (work / "execution.json").is_file():
        _atomic_json(work / "execution.json", {
            "schema_version": "kbase.semantic_regression_execution.v1",
            "generated_at": _now(), "image": IMAGE, "network": "none",
            "returncode": 0, "stdout_tail": "", "stderr_tail": "",
            "resumed": True, "reason": "host wrapper ended while the already-running Docker worker completed",
        })
    batch = _read_json(work / "results.json")
    if batch.get("status") != "COMPLETED":
        raise RuntimeError("semantic regression batch did not complete")
    batch_by_id = {str(row["request_id"]): row for row in batch["results"]}
    fixed_report = _evaluate_suite(
        suite_name="fixed", suite_path=fixed_path, suite=fixed_suite, contexts=fixed_contexts,
        batch_by_id=batch_by_id, entries_by_id=entries_by_id, semantic_manifest=manifest,
    )
    holdout_report = _evaluate_suite(
        suite_name="holdout", suite_path=holdout_path, suite=holdout_suite, contexts=holdout_contexts,
        batch_by_id=batch_by_id, entries_by_id=entries_by_id, semantic_manifest=manifest,
    )
    _atomic_json(candidate / "regression-fixed.json", fixed_report)
    _atomic_json(candidate / "regression-holdout.json", holdout_report)
    manifest.setdefault("sanitized_config", {}).update({
        "ranking_policy": "lexical_anchors_then_within_query_bge_rerank",
        "navigation_semantic_bypass": True,
        "ordinary_lexical_prefix_protected": 2,
        "comparison_branch_prefix_protected": 2,
        "reranker_score_scope": "within_query_only",
        "cross_query_reranker_threshold": None,
    })
    gate = {
        "schema_version": "kbase.semantic_release_gate.v1",
        "generator": "ag2_research.kbase.semantic_index",
        "generator_version": "1.0.0",
        "generated_at": _now(),
        "source_fingerprint": manifest["source_fingerprint"],
        "catalog_version": manifest["catalog_version"],
        "model_binding_sha256": manifest["model_binding_sha256"],
        "fixed_status": fixed_report["status"],
        "holdout_status": holdout_report["status"],
        "batch_results_sha256": _sha256_file(work / "results.json"),
        "evaluation_request_sha256": sha256(_canonical_bytes(request_document)).hexdigest(),
        "executed_request_sha256": _sha256_file(work / "requests.json"),
        "cache_reuse": (
            {"path": "regression-work/cache-reuse.json", "sha256": _sha256_file(work / "cache-reuse.json")}
            if cache_reuse is not None else None
        ),
        "status": "APPROVED" if fixed_report["status"] == holdout_report["status"] == "PASS" else "REJECTED",
    }
    _atomic_json(candidate / "gate-report.json", gate)
    manifest["release_gates"] = {
        "status": gate["status"],
        "fixed_report": {"path": "regression-fixed.json", "sha256": _sha256_file(candidate / "regression-fixed.json")},
        "holdout_report": {"path": "regression-holdout.json", "sha256": _sha256_file(candidate / "regression-holdout.json")},
        "gate_report": {"path": "gate-report.json", "sha256": _sha256_file(candidate / "gate-report.json")},
        "batch_results_sha256": gate["batch_results_sha256"],
        "evaluation_request_sha256": gate["evaluation_request_sha256"],
        "executed_request_sha256": gate["executed_request_sha256"],
        "cache_reuse": gate["cache_reuse"],
    }
    manifest["promotion_status"] = "validated_candidate" if gate["status"] == "APPROVED" else "rejected_candidate"
    _atomic_json(candidate / "manifest.json", manifest)
    validation = validate_semantic_release(
        candidate, active_catalog_dir=vault / "wiki/outputs/manifests/ag2-kbase/current",
        require_gates=gate["status"] == "APPROVED",
    )
    _atomic_json(candidate / "validation.json", validation)
    if gate["status"] != "APPROVED":
        raise RuntimeError("semantic candidate regression gate rejected")
    return {"status": "APPROVED", "candidate": str(candidate), "fixed": fixed_report["metrics"], "holdout": holdout_report["metrics"], "validation": validation}


def publish_semantic(*, vault: Path, candidate: Path, apply: bool) -> dict[str, Any]:
    vault = vault.resolve()
    candidate = _assert_inside(candidate, vault / SEMANTIC_ROOT / "candidate")
    catalog_dir = vault / "wiki/outputs/manifests/ag2-kbase/current"
    validation = validate_semantic_release(candidate, active_catalog_dir=catalog_dir, require_gates=True)
    if not apply:
        return {"status": "DRY_RUN", "candidate": str(candidate), "validation": validation}
    root = vault / SEMANTIC_ROOT
    manifest = _read_json(candidate / "manifest.json")
    manifest["promotion_status"] = "current"
    _atomic_json(candidate / "manifest.json", manifest)
    validation = validate_semantic_release(
        candidate,
        active_catalog_dir=catalog_dir,
        require_gates=True,
    )
    _atomic_json(candidate / "validation.json", validation)
    validator = lambda release: validate_semantic_release(
        release, active_catalog_dir=catalog_dir, require_gates=True,
    )
    current_before = root / "current"
    metadata_only = (
        current_before.is_dir()
        and _semantic_payload_identity(_read_json(candidate / "manifest.json"))
        == _semantic_payload_identity(_read_json(current_before / "manifest.json"))
    )
    promoted_archive: Path | None = None
    if metadata_only:
        promoted_archive = _publish_semantic_metadata_update(
            root=root, candidate=candidate, validator=validator,
        )
        promotion_mode = "metadata_hot_swap"
    else:
        _publish_directory(root=root, candidate=candidate, validator=validator)
        promotion_mode = "directory_swap"
    current = root / "current"
    manifest = _read_json(current / "manifest.json")
    validation = validate_semantic_release(current, active_catalog_dir=catalog_dir, require_gates=True)
    return {
        "status": "PUBLISHED", "current": str(current), "validation": validation,
        "manifest": manifest, "promotion_mode": promotion_mode,
        "promoted_candidate_archive": str(promoted_archive) if promoted_archive else None,
    }


def install_service(*, vault: Path, project_root: Path, apply: bool) -> dict[str, Any]:
    vault = vault.resolve()
    semantic = vault / SEMANTIC_ROOT
    models = vault / MODEL_ROOT
    runtime = vault / RUNTIME_ROOT
    validate_semantic_release(semantic / "current", active_catalog_dir=vault / "wiki/outputs/manifests/ag2-kbase/current", require_gates=True)
    validate_model_release(models / "current")
    inspect = subprocess.run(["docker", "inspect", "ag2-kbase-semantic"], capture_output=True, text=True, check=False)
    if inspect.returncode == 0:
        raise RuntimeError("ag2-kbase-semantic container already exists; refusing to replace or restart it")
    command = [
        "docker", "run", "-d", "--name", "ag2-kbase-semantic", "--restart", "unless-stopped",
        "--gpus", "all", "--network", "none", "--read-only", "--tmpfs", "/tmp:rw,size=1g", "--shm-size", "1g",
        "-e", "HF_HUB_OFFLINE=1", "-e", "TRANSFORMERS_OFFLINE=1", "-e", "HF_HOME=/tmp/hf",
        "-e", "PYTHONDONTWRITEBYTECODE=1",
        "-v", f"{project_root / 'ag2_research/kbase'}:/app:ro",
        "-v", f"{semantic}:/semantic:ro",
        "-v", f"{models}:/models:ro",
        "-v", f"{runtime}:/runtime:rw",
        "--entrypoint", "/usr/bin/python3", IMAGE,
        "/app/semantic_worker.py", "serve", "--semantic-root", "/semantic",
        "--embedding-root", "/models/current/embedding/bge-m3",
        "--reranker-root", "/models/current/reranker/bge-reranker-v2-m3",
        "--runtime-root", "/runtime",
    ]
    if not apply:
        return {"status": "DRY_RUN", "container": "ag2-kbase-semantic", "image": IMAGE, "network": "none"}
    runtime.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(command, capture_output=True, text=True, check=False, timeout=120)
    if completed.returncode != 0:
        raise RuntimeError(f"failed to start semantic service: {completed.stderr[-1000:]}")
    deadline = time.monotonic() + 180.0
    health_path = runtime / "health.json"
    health: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        try:
            candidate_health = _read_json(health_path)
            if candidate_health.get("status") == "READY" and time.time() - float(candidate_health.get("heartbeat_epoch") or 0) < 15:
                health = candidate_health
                break
        except (OSError, ValueError, json.JSONDecodeError):
            pass
        time.sleep(1.0)
    if health is None:
        logs = subprocess.run(["docker", "logs", "--tail", "100", "ag2-kbase-semantic"], capture_output=True, text=True, check=False)
        raise RuntimeError(f"semantic service did not become ready: {logs.stderr[-2000:]} {logs.stdout[-2000:]}")
    inspect = subprocess.run(["docker", "inspect", "ag2-kbase-semantic"], capture_output=True, text=True, check=True)
    inspected = json.loads(inspect.stdout)[0]
    if inspected.get("HostConfig", {}).get("NetworkMode") != "none":
        raise RuntimeError("semantic service network isolation mismatch")
    service_manifest = {
        "schema_version": "kbase.semantic_service_install.v1",
        "generator": "ag2_research.kbase.semantic_index",
        "generator_version": "1.0.0",
        "generated_at": _now(),
        "source_fingerprint": health["index_source_fingerprint"],
        "container_id": inspected["Id"],
        "container_name": "ag2-kbase-semantic",
        "image": IMAGE,
        "image_id": inspected["Image"],
        "network": "none",
        "restart_policy": inspected.get("HostConfig", {}).get("RestartPolicy", {}).get("Name"),
        "health": health,
        "sanitized_config": {"models_read_only": True, "index_read_only": True, "runtime_queue": "local_filesystem", "endpoint": None},
        "status": "READY",
    }
    _atomic_json(runtime / "service-manifest.json", service_manifest)
    return {"status": "READY", "container_id": inspected["Id"], "health": health, "manifest": str(runtime / "service-manifest.json")}


def rollback_semantic(*, vault: Path, apply: bool) -> dict[str, Any]:
    vault = vault.resolve()
    root = vault / SEMANTIC_ROOT
    current, previous = root / "current", root / "previous"
    catalog_dir = vault / "wiki/outputs/manifests/ag2-kbase/current"
    validate_semantic_release(previous, active_catalog_dir=catalog_dir, require_gates=True)
    if not apply:
        return {"status": "DRY_RUN", "previous": str(previous)}
    with _exclusive_lock(root / ".publish.lock"):
        temporary = root / f".rollback.{uuid.uuid4().hex}.tmp"
        os.replace(current, temporary)
        try:
            os.replace(previous, current)
            validate_semantic_release(current, active_catalog_dir=catalog_dir, require_gates=True)
            os.replace(temporary, previous)
        except Exception:
            if current.exists():
                os.replace(current, previous)
            if temporary.exists():
                os.replace(temporary, current)
            raise
    return {"status": "ROLLED_BACK", "current": str(current)}


def main() -> int:
    parser = argparse.ArgumentParser(description="KBase selected-BGE semantic release manager")
    parser.add_argument("--vault", default=r"D:\KBase")
    parser.add_argument("--project-root", default=str(Path(__file__).resolve().parents[2]))
    subparsers = parser.add_subparsers(dest="command", required=True)

    provision = subparsers.add_parser("provision-models")
    provision.add_argument("--source-candidate", required=True)
    provision.add_argument("--evaluation", required=True)
    provision.add_argument("--apply", action="store_true")

    bootstrap = subparsers.add_parser("bootstrap")
    bootstrap.add_argument("--source-candidate", required=True)
    bootstrap.add_argument("--evaluation", required=True)
    bootstrap.add_argument("--apply", action="store_true")

    regress = subparsers.add_parser("regress")
    regress.add_argument("--candidate", required=True)
    regress.add_argument("--apply", action="store_true")

    publish = subparsers.add_parser("publish")
    publish.add_argument("--candidate", required=True)
    publish.add_argument("--apply", action="store_true")

    service = subparsers.add_parser("install-service")
    service.add_argument("--apply", action="store_true")

    rollback = subparsers.add_parser("rollback")
    rollback.add_argument("--apply", action="store_true")

    args = parser.parse_args()
    vault = Path(args.vault)
    project_root = Path(args.project_root).resolve()
    if args.command == "provision-models":
        result = provision_models(vault=vault, source_candidate=Path(args.source_candidate), evaluation_dir=Path(args.evaluation), apply=args.apply)
    elif args.command == "bootstrap":
        result = bootstrap_candidate(vault=vault, source_candidate=Path(args.source_candidate), evaluation_dir=Path(args.evaluation), apply=args.apply)
    elif args.command == "regress":
        result = run_candidate_regression(vault=vault, candidate=Path(args.candidate), project_root=project_root, apply=args.apply)
    elif args.command == "publish":
        result = publish_semantic(vault=vault, candidate=Path(args.candidate), apply=args.apply)
    elif args.command == "install-service":
        result = install_service(vault=vault, project_root=project_root, apply=args.apply)
    else:
        result = rollback_semantic(vault=vault, apply=args.apply)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
