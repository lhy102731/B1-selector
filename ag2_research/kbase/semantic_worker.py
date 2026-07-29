"""Offline CUDA worker for the production KBase semantic search layer.

The worker is deliberately independent from the frozen model-bakeoff runner.  It
loads only the two selected BGE models, never accesses the network, and writes
only to explicitly mounted generated-output directories.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import time
import uuid
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable


INDEX_SCHEMA = "kbase.semantic_index.v1"
SERVICE_SCHEMA = "kbase.semantic_service.v1"
EMBEDDING_MODEL_ID = "bge-m3"
RERANKER_MODEL_ID = "bge-reranker-v2-m3"
DIMENSION = 1024
MAX_LENGTH = 1024


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


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    payload = _canonical_bytes(value)
    with temporary.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


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


def _configure_cuda(torch: Any) -> Any:
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA_UNAVAILABLE")
    os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"
    torch.manual_seed(0)
    torch.cuda.manual_seed_all(0)
    torch.use_deterministic_algorithms(True)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    device = torch.device("cuda:0")
    torch.cuda.set_device(device)
    return device


def _is_cuda_oom(torch: Any, error: Exception) -> bool:
    oom_type = getattr(torch.cuda, "OutOfMemoryError", ())
    matched = (bool(oom_type) and isinstance(error, oom_type)) or "out of memory" in str(error).lower()
    if matched:
        torch.cuda.empty_cache()
    return bool(matched)


class SelectedModels:
    """The exact BGE embedding and reranker pair selected by the bakeoff."""

    def __init__(self, embedding_root: Path, reranker_root: Path, *, load_reranker: bool) -> None:
        import torch
        from transformers import AutoModel, AutoModelForSequenceClassification, AutoTokenizer

        self.torch = torch
        self.device = _configure_cuda(torch)
        torch.cuda.reset_peak_memory_stats(self.device)

        self.embedding_tokenizer = AutoTokenizer.from_pretrained(
            str(embedding_root), local_files_only=True, use_fast=True, trust_remote_code=False,
        )
        self.embedding_tokenizer.padding_side = "right"
        self.embedding_model = AutoModel.from_pretrained(
            str(embedding_root), local_files_only=True, trust_remote_code=False,
            torch_dtype=torch.float16, attn_implementation="eager", weights_only=True,
        ).to(self.device)
        self.embedding_model.eval()

        self.reranker_tokenizer = None
        self.reranker_model = None
        if load_reranker:
            self.reranker_tokenizer = AutoTokenizer.from_pretrained(
                str(reranker_root), local_files_only=True, use_fast=True, trust_remote_code=False,
            )
            self.reranker_tokenizer.padding_side = "right"
            self.reranker_model = AutoModelForSequenceClassification.from_pretrained(
                str(reranker_root), local_files_only=True, trust_remote_code=False,
                torch_dtype=torch.float16, attn_implementation="eager", weights_only=True,
            ).to(self.device)
            self.reranker_model.eval()

    def encode(self, texts: list[str], *, initial_batch_size: int = 16) -> tuple[Any, int]:
        import numpy as np

        if not texts:
            return np.empty((0, DIMENSION), dtype=np.float32), initial_batch_size
        output: list[Any] = []
        position = 0
        batch_size = max(1, int(initial_batch_size))
        while position < len(texts):
            batch = texts[position : position + batch_size]
            try:
                inputs = self.embedding_tokenizer(
                    batch, padding=True, truncation=True, max_length=MAX_LENGTH, return_tensors="pt",
                ).to(self.device)
                with self.torch.no_grad():
                    hidden = self.embedding_model(**inputs).last_hidden_state[:, 0]
                    pooled = self.torch.nn.functional.normalize(hidden.float(), p=2, dim=1)
                values = pooled.cpu().numpy().astype(np.float32, copy=False)
            except Exception as error:
                if not _is_cuda_oom(self.torch, error):
                    raise
                if batch_size == 1:
                    raise RuntimeError("CUDA_OOM_MIN_BATCH") from error
                batch_size = max(1, batch_size // 2)
                continue
            output.extend(values)
            position += len(batch)
        array = np.stack(output, axis=0).astype(np.float32, copy=False)
        if array.shape != (len(texts), DIMENSION) or not np.isfinite(array).all():
            raise RuntimeError("INVALID_EMBEDDING_OUTPUT")
        norms = np.linalg.norm(array.astype(np.float64), axis=1)
        if not np.all(np.abs(norms - 1.0) <= 1e-5):
            raise RuntimeError("NON_UNIT_EMBEDDING_OUTPUT")
        return array, batch_size

    def rerank(self, query: str, documents: list[str], *, initial_batch_size: int = 16) -> tuple[list[float], int]:
        if self.reranker_model is None or self.reranker_tokenizer is None:
            raise RuntimeError("RERANKER_NOT_LOADED")
        scores: list[float] = []
        position = 0
        batch_size = max(1, int(initial_batch_size))
        while position < len(documents):
            batch = documents[position : position + batch_size]
            try:
                inputs = self.reranker_tokenizer(
                    [query] * len(batch), batch, padding=True, truncation=True,
                    max_length=MAX_LENGTH, return_tensors="pt",
                ).to(self.device)
                with self.torch.no_grad():
                    logits = self.reranker_model(**inputs, return_dict=True).logits
                values = logits.detach().cpu().float().view(-1).tolist()
            except Exception as error:
                if not _is_cuda_oom(self.torch, error):
                    raise
                if batch_size == 1:
                    raise RuntimeError("CUDA_OOM_MIN_BATCH") from error
                batch_size = max(1, batch_size // 2)
                continue
            if len(values) != len(batch) or not all(math.isfinite(float(value)) for value in values):
                raise RuntimeError("INVALID_RERANKER_OUTPUT")
            scores.extend(float(value) for value in values)
            position += len(batch)
        return scores, batch_size


def _validate_documents(rows: list[dict[str, Any]]) -> None:
    expected = {"object_id", "source_id", "content_sha256", "document_type", "retrieval_text"}
    object_ids: set[str] = set()
    for row in rows:
        if set(row) != expected:
            raise ValueError("SEMANTIC_DOCUMENT_FIELDS_MISMATCH")
        if not all(isinstance(row.get(key), str) and row[key] for key in expected):
            raise ValueError("SEMANTIC_DOCUMENT_TEXT_REQUIRED")
        if row["object_id"] in object_ids:
            raise ValueError("DUPLICATE_SEMANTIC_OBJECT_ID")
        object_ids.add(row["object_id"])
        if sha256(row["retrieval_text"].encode("utf-8")).hexdigest() != row["content_sha256"]:
            raise ValueError("SEMANTIC_DOCUMENT_CONTENT_HASH_MISMATCH")


def build_index(
    *, documents_path: Path, request_path: Path, output_dir: Path,
    embedding_root: Path, reranker_root: Path, previous_dir: Path | None,
) -> dict[str, Any]:
    import numpy as np

    rows = _read_jsonl(documents_path)
    _validate_documents(rows)
    request = _read_json(request_path)
    if request.get("schema_version") != "kbase.semantic_build_request.v1":
        raise ValueError("SEMANTIC_BUILD_REQUEST_SCHEMA")
    if request.get("counts", {}).get("documents") != len(rows):
        raise ValueError("SEMANTIC_BUILD_DOCUMENT_COUNT_MISMATCH")

    previous_vectors = None
    previous_rows: list[dict[str, Any]] = []
    previous_manifest: dict[str, Any] = {}
    if previous_dir and (previous_dir / "manifest.json").is_file():
        previous_manifest = _read_json(previous_dir / "manifest.json")
        if (
            previous_manifest.get("schema_version") == INDEX_SCHEMA
            and previous_manifest.get("model_binding_sha256") == request.get("model_binding_sha256")
            and _sha256_file(previous_dir / "documents.jsonl") == previous_manifest.get("files", {}).get("documents_sha256")
            and _sha256_file(previous_dir / "vectors.npy") == previous_manifest.get("files", {}).get("vectors_sha256")
        ):
            previous_rows = _read_jsonl(previous_dir / "documents.jsonl")
            previous_vectors = np.load(previous_dir / "vectors.npy", mmap_mode="r", allow_pickle=False)
            if previous_vectors.shape != (len(previous_rows), DIMENSION):
                raise ValueError("PREVIOUS_SEMANTIC_INDEX_SHAPE_MISMATCH")

    previous_lookup = {
        row["object_id"]: (row["content_sha256"], index)
        for index, row in enumerate(previous_rows)
    }
    pending_rows = [
        row for row in rows
        if row["object_id"] not in previous_lookup
        or previous_lookup[row["object_id"]][0] != row["content_sha256"]
    ]

    models = None
    computed_vectors = np.empty((0, DIMENSION), dtype=np.float32)
    final_batch_size = 16
    if pending_rows:
        models = SelectedModels(embedding_root, reranker_root, load_reranker=False)
        computed_vectors, final_batch_size = models.encode([row["retrieval_text"] for row in pending_rows])
    computed_lookup = {row["object_id"]: computed_vectors[index] for index, row in enumerate(pending_rows)}

    vectors: list[Any] = []
    reused = 0
    for row in rows:
        old = previous_lookup.get(row["object_id"])
        if old and old[0] == row["content_sha256"] and previous_vectors is not None:
            vectors.append(np.asarray(previous_vectors[old[1]], dtype=np.float32))
            reused += 1
        else:
            vectors.append(computed_lookup[row["object_id"]])
    matrix = np.stack(vectors, axis=0).astype(np.float32, copy=False)
    if matrix.shape != (len(rows), DIMENSION) or not np.isfinite(matrix).all():
        raise RuntimeError("SEMANTIC_INDEX_MATRIX_INVALID")
    norms = np.linalg.norm(matrix.astype(np.float64), axis=1)
    if not np.all(np.abs(norms - 1.0) <= 1e-5):
        raise RuntimeError("SEMANTIC_INDEX_MATRIX_NOT_NORMALIZED")

    output_dir.mkdir(parents=True, exist_ok=True)
    vector_path = output_dir / "vectors.npy"
    with vector_path.open("xb") as handle:
        np.save(handle, matrix, allow_pickle=False)
        handle.flush()
        os.fsync(handle.fileno())
    documents_target = output_dir / "documents.jsonl"
    if documents_target.resolve() != documents_path.resolve():
        raise ValueError("DOCUMENTS_MUST_BE_PRESTAGED_IN_OUTPUT")

    manifest = {
        "schema_version": INDEX_SCHEMA,
        "generator": "ag2_research.kbase.semantic_worker",
        "generator_version": "1.0.0",
        "generated_at": _now(),
        "source_fingerprint": request["source_fingerprint"],
        "catalog_version": request["catalog_version"],
        "catalog_sha256": request["catalog_sha256"],
        "model_binding_sha256": request["model_binding_sha256"],
        "models": request["models"],
        "dimension": DIMENSION,
        "dtype": "float32",
        "counts": {
            "entries": request["counts"]["entries"],
            "source_packets": request["counts"]["source_packets"],
            "documents": len(rows),
            "computed": len(pending_rows),
            "reused": reused,
            "removed": max(0, len(previous_rows) - reused),
        },
        "files": {
            "documents": "documents.jsonl",
            "documents_sha256": _sha256_file(documents_target),
            "vectors": "vectors.npy",
            "vectors_sha256": _sha256_file(vector_path),
        },
        "runtime": {
            "backend": "torch_cuda" if models is not None else "reuse_only",
            "device": "cuda:0" if models is not None else None,
            "device_name": models.torch.cuda.get_device_name(0) if models is not None else None,
            "torch_version": models.torch.__version__ if models is not None else None,
            "cuda_version": models.torch.version.cuda if models is not None else None,
            "final_batch_size": final_batch_size,
            "peak_vram_bytes": int(models.torch.cuda.max_memory_allocated(models.device)) if models is not None else 0,
            "cpu_fallback": False,
        },
        "sanitized_config": {
            "embedding_model": EMBEDDING_MODEL_ID,
            "pooling": "cls",
            "normalize": True,
            "max_length": MAX_LENGTH,
            "network": "none",
        },
        "status": "COMPLETED",
    }
    _atomic_json(output_dir / "manifest.json", manifest)
    return manifest


class SemanticEngine:
    def __init__(self, semantic_root: Path, embedding_root: Path, reranker_root: Path) -> None:
        import numpy as np

        self.np = np
        self.semantic_root = semantic_root
        self.models = SelectedModels(embedding_root, reranker_root, load_reranker=True)
        self.loaded_identity = ""
        self.manifest: dict[str, Any] = {}
        self.documents: list[dict[str, Any]] = []
        self.vectors: Any = None
        self.source_to_indices: dict[str, list[int]] = {}
        self.reload_index(force=True)

    def reload_index(self, *, force: bool = False) -> None:
        release = self.semantic_root if (self.semantic_root / "manifest.json").is_file() else self.semantic_root / "current"
        manifest_path = release / "manifest.json"
        identity = _sha256_file(manifest_path)
        if not force and identity == self.loaded_identity:
            return
        manifest = _read_json(manifest_path)
        if manifest.get("schema_version") != INDEX_SCHEMA or manifest.get("status") != "COMPLETED":
            raise ValueError("SEMANTIC_INDEX_NOT_COMPLETED")
        documents_path = release / manifest["files"]["documents"]
        vectors_path = release / manifest["files"]["vectors"]
        if _sha256_file(documents_path) != manifest["files"]["documents_sha256"]:
            raise ValueError("SEMANTIC_DOCUMENTS_HASH_MISMATCH")
        if _sha256_file(vectors_path) != manifest["files"]["vectors_sha256"]:
            raise ValueError("SEMANTIC_VECTORS_HASH_MISMATCH")
        documents = _read_jsonl(documents_path)
        _validate_documents(documents)
        vectors = self.np.load(vectors_path, mmap_mode="r", allow_pickle=False)
        if vectors.shape != (len(documents), DIMENSION) or vectors.dtype != self.np.float32:
            raise ValueError("SEMANTIC_INDEX_SHAPE_OR_DTYPE_MISMATCH")
        source_to_indices: dict[str, list[int]] = {}
        for index, row in enumerate(documents):
            source_to_indices.setdefault(row["source_id"], []).append(index)
        self.manifest = manifest
        self.documents = documents
        self.vectors = vectors
        self.source_to_indices = source_to_indices
        self.loaded_identity = identity

    def search(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.reload_index()
        query = str(payload.get("query") or "").strip()
        lexical_ids = [str(value) for value in payload.get("lexical_ids") or []]
        allowed_ids = {str(value) for value in payload.get("allowed_ids") or []}
        candidate_limit = max(5, min(int(payload.get("candidate_limit") or 64), 128))
        result_limit = max(1, min(int(payload.get("result_limit") or 100), 100))
        if not query:
            raise ValueError("QUERY_REQUIRED")
        if not lexical_ids:
            return {
                "schema_version": SERVICE_SCHEMA,
                "catalog_version": self.manifest["catalog_version"],
                "index_source_fingerprint": self.manifest["source_fingerprint"],
                "results": [],
                "policy": "lexical_anchor_required",
            }

        query_vector, _ = self.models.encode([query], initial_batch_size=1)
        dense_scores = self.np.dot(self.vectors, query_vector[0])
        source_best: dict[str, tuple[float, int]] = {}
        for source_id, indices in self.source_to_indices.items():
            if allowed_ids and source_id not in allowed_ids:
                continue
            best_index = max(indices, key=lambda index: (float(dense_scores[index]), self.documents[index]["object_id"]))
            source_best[source_id] = (float(dense_scores[best_index]), best_index)
        dense_order = sorted(source_best, key=lambda source_id: (-source_best[source_id][0], source_id))
        dense_rank = {source_id: rank for rank, source_id in enumerate(dense_order, 1)}
        lexical_rank = {source_id: rank for rank, source_id in enumerate(lexical_ids, 1)}

        rrf: dict[str, float] = {}
        for source_id, rank in dense_rank.items():
            rrf[source_id] = rrf.get(source_id, 0.0) + 1.0 / (60.0 + rank)
        for source_id, rank in lexical_rank.items():
            if source_id in source_best:
                rrf[source_id] = rrf.get(source_id, 0.0) + 1.0 / (60.0 + rank)
        candidates = sorted(rrf, key=lambda source_id: (-rrf[source_id], source_id))[:candidate_limit]
        documents = [self.documents[source_best[source_id][1]]["retrieval_text"] for source_id in candidates]
        reranker_scores, _ = self.models.rerank(query, documents)
        rows = []
        for source_id, reranker_score in zip(candidates, reranker_scores):
            dense_score, document_index = source_best[source_id]
            rows.append({
                "source_id": source_id,
                "object_id": self.documents[document_index]["object_id"],
                "document_type": self.documents[document_index]["document_type"],
                "dense_score": dense_score,
                "dense_rank": dense_rank[source_id],
                "lexical_rank": lexical_rank.get(source_id),
                "rrf_score": rrf[source_id],
                "reranker_score": reranker_score,
            })
        rows.sort(key=lambda row: (-row["reranker_score"], -row["rrf_score"], row["source_id"]))
        return {
            "schema_version": SERVICE_SCHEMA,
            "catalog_version": self.manifest["catalog_version"],
            "index_source_fingerprint": self.manifest["source_fingerprint"],
            "models": {"embedding": EMBEDDING_MODEL_ID, "reranker": RERANKER_MODEL_ID},
            "results": rows[:result_limit],
            "policy": "lexical_dense_rrf_then_bge_reranker",
        }


def run_batch(*, engine: SemanticEngine, requests_path: Path, output_path: Path) -> dict[str, Any]:
    request_document = _read_json(requests_path)
    rows = request_document.get("requests")
    if not isinstance(rows, list):
        raise ValueError("BATCH_REQUESTS_REQUIRED")
    started = time.perf_counter()
    results = []
    for row in rows:
        request_id = str(row.get("request_id") or "")
        try:
            result = engine.search(row)
            results.append({"request_id": request_id, "status": "COMPLETED", "result": result})
        except Exception as error:
            results.append({"request_id": request_id, "status": "FAILED", "error": f"{type(error).__name__}: {error}"})
    document = {
        "schema_version": "kbase.semantic_batch_results.v1",
        "generator": "ag2_research.kbase.semantic_worker",
        "generator_version": "1.0.0",
        "generated_at": _now(),
        "source_fingerprint": engine.manifest["source_fingerprint"],
        "counts": {
            "requests": len(rows),
            "completed": sum(row["status"] == "COMPLETED" for row in results),
            "failed": sum(row["status"] == "FAILED" for row in results),
        },
        "timing_seconds": time.perf_counter() - started,
        "results": results,
        "status": "COMPLETED" if all(row["status"] == "COMPLETED" for row in results) else "FAILED",
    }
    _atomic_json(output_path, document)
    return document


def serve(*, engine: SemanticEngine, runtime_root: Path) -> None:
    requests_dir = runtime_root / "requests"
    processing_dir = runtime_root / "processing"
    responses_dir = runtime_root / "responses"
    for directory in (requests_dir, processing_dir, responses_dir):
        directory.mkdir(parents=True, exist_ok=True)
    instance_id = uuid.uuid4().hex
    last_heartbeat = 0.0

    def heartbeat() -> None:
        nonlocal last_heartbeat
        engine.reload_index()
        _atomic_json(runtime_root / "health.json", {
            "schema_version": SERVICE_SCHEMA,
            "status": "READY",
            "instance_id": instance_id,
            "heartbeat_at": _now(),
            "heartbeat_epoch": time.time(),
            "catalog_version": engine.manifest["catalog_version"],
            "index_source_fingerprint": engine.manifest["source_fingerprint"],
            "models": {"embedding": EMBEDDING_MODEL_ID, "reranker": RERANKER_MODEL_ID},
            "backend": "torch_cuda",
            "device": "cuda:0",
            "device_name": engine.models.torch.cuda.get_device_name(0),
            "torch_version": engine.models.torch.__version__,
            "cuda_version": engine.models.torch.version.cuda,
            "network": "none",
        })
        last_heartbeat = time.monotonic()

    heartbeat()
    while True:
        if time.monotonic() - last_heartbeat >= 5.0:
            heartbeat()
        pending = sorted(requests_dir.glob("*.json"), key=lambda path: path.name)
        if not pending:
            time.sleep(0.02)
            continue
        for request_path in pending:
            claimed = processing_dir / request_path.name
            try:
                os.replace(request_path, claimed)
            except FileNotFoundError:
                continue
            request_id = request_path.stem
            started = time.perf_counter()
            try:
                payload = _read_json(claimed)
                if payload.get("request_id") != request_id:
                    raise ValueError("REQUEST_ID_MISMATCH")
                result = engine.search(payload)
                response = {
                    "schema_version": SERVICE_SCHEMA,
                    "request_id": request_id,
                    "status": "COMPLETED",
                    "timing_seconds": time.perf_counter() - started,
                    "result": result,
                }
            except Exception as error:
                response = {
                    "schema_version": SERVICE_SCHEMA,
                    "request_id": request_id,
                    "status": "FAILED",
                    "timing_seconds": time.perf_counter() - started,
                    "error": f"{type(error).__name__}: {error}",
                }
            _atomic_json(responses_dir / request_path.name, response)
            claimed.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="KBase BGE semantic CUDA worker")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    build = subparsers.add_parser("build")
    build.add_argument("--documents", required=True)
    build.add_argument("--request", required=True)
    build.add_argument("--output", required=True)
    build.add_argument("--embedding-root", required=True)
    build.add_argument("--reranker-root", required=True)
    build.add_argument("--previous")

    batch = subparsers.add_parser("batch")
    batch.add_argument("--semantic-root", required=True)
    batch.add_argument("--embedding-root", required=True)
    batch.add_argument("--reranker-root", required=True)
    batch.add_argument("--requests", required=True)
    batch.add_argument("--output", required=True)

    service = subparsers.add_parser("serve")
    service.add_argument("--semantic-root", required=True)
    service.add_argument("--embedding-root", required=True)
    service.add_argument("--reranker-root", required=True)
    service.add_argument("--runtime-root", required=True)

    args = parser.parse_args()
    if args.mode == "build":
        manifest = build_index(
            documents_path=Path(args.documents), request_path=Path(args.request),
            output_dir=Path(args.output), embedding_root=Path(args.embedding_root),
            reranker_root=Path(args.reranker_root),
            previous_dir=Path(args.previous) if args.previous else None,
        )
        print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
        return 0
    engine = SemanticEngine(Path(args.semantic_root), Path(args.embedding_root), Path(args.reranker_root))
    if args.mode == "batch":
        document = run_batch(engine=engine, requests_path=Path(args.requests), output_path=Path(args.output))
        print(json.dumps(document["counts"], ensure_ascii=False, sort_keys=True))
        return 0 if document["status"] == "COMPLETED" else 1
    serve(engine=engine, runtime_root=Path(args.runtime_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
