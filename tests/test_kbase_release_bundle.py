from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest

from ag2_research.kbase.release_bundle import inspect_semantic_release_bundle


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


class SemanticReleaseBundleTests(unittest.TestCase):
    def _vault(self, root: Path) -> Path:
        catalog = root / "wiki/outputs/manifests/ag2-kbase/current"
        semantic = root / "wiki/outputs/manifests/ag2-kbase-semantic/current"
        runtime = root / "wiki/outputs/runtime/ag2-kbase-semantic"

        catalog.mkdir(parents=True)
        (catalog / "catalog.jsonl").write_text('{"source_id":"source-1"}\n', encoding="utf-8")
        (catalog / "facets.json").write_text("{}", encoding="utf-8")
        catalog_sha = hashlib.sha256((catalog / "catalog.jsonl").read_bytes()).hexdigest()
        _write_json(catalog / "manifest.json", {
            "catalog_schema_version": 1,
            "catalog_version": "catalog-v1",
            "source_fingerprint": "catalog-source-fingerprint",
        })
        _write_json(semantic / "manifest.json", {
            "schema_version": "kbase.semantic_index.v1",
            "status": "COMPLETED",
            "promotion_status": "current",
            "catalog_version": "catalog-v1",
            "catalog_sha256": catalog_sha,
            "source_fingerprint": "semantic-source-fingerprint",
            "model_binding_sha256": "model-binding",
            "models": {
                "embedding": {"model_id": "bge-m3", "revision": "embedding-revision"},
                "reranker": {"model_id": "bge-reranker-v2-m3", "revision": "reranker-revision"},
            },
            "files": {
                "documents": "documents.jsonl",
                "documents_sha256": "documents-sha",
                "vectors": "vectors.npy",
                "vectors_sha256": "vectors-sha",
            },
        })
        (semantic / "documents.jsonl").write_text("{}\n", encoding="utf-8")
        (semantic / "vectors.npy").write_bytes(b"fixture")
        _write_json(semantic / "gate-report.json", {
            "status": "APPROVED",
            "catalog_version": "catalog-v1",
            "source_fingerprint": "semantic-source-fingerprint",
            "model_binding_sha256": "model-binding",
        })
        _write_json(runtime / "health.json", {
            "status": "READY",
            "catalog_version": "catalog-v1",
            "index_source_fingerprint": "semantic-source-fingerprint",
            "models": {
                "embedding": "bge-m3",
                "reranker": "bge-reranker-v2-m3",
            },
            "heartbeat_epoch": time.time(),
        })
        return root

    def test_ready_bundle_binds_catalog_semantic_gate_and_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            bundle = inspect_semantic_release_bundle(self._vault(Path(directory)))

        self.assertEqual("READY", bundle["status"])
        self.assertEqual([], bundle["issues"])
        self.assertEqual("catalog-v1", bundle["catalog_version"])
        self.assertEqual("bge-m3", bundle["models"]["embedding"])
        self.assertEqual(64, len(bundle["bundle_fingerprint"]))

    def test_runtime_or_gate_version_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            vault = self._vault(Path(directory))
            health_path = vault / "wiki/outputs/runtime/ag2-kbase-semantic/health.json"
            health = json.loads(health_path.read_text(encoding="utf-8"))
            health["catalog_version"] = "stale-catalog"
            _write_json(health_path, health)
            bundle = inspect_semantic_release_bundle(vault)

        self.assertEqual("DEGRADED", bundle["status"])
        self.assertIn("runtime_catalog_version_mismatch", bundle["issues"])

    def test_stale_runtime_heartbeat_is_not_ready(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            vault = self._vault(Path(directory))
            health_path = vault / "wiki/outputs/runtime/ag2-kbase-semantic/health.json"
            health = json.loads(health_path.read_text(encoding="utf-8"))
            health["heartbeat_epoch"] = 100.0
            _write_json(health_path, health)
            bundle = inspect_semantic_release_bundle(vault, now_epoch=1000.0)

        self.assertEqual("DEGRADED", bundle["status"])
        self.assertIn("runtime_heartbeat_stale", bundle["issues"])

    def test_heartbeat_updates_do_not_change_release_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            vault = self._vault(Path(directory))
            first = inspect_semantic_release_bundle(vault)
            health_path = vault / "wiki/outputs/runtime/ag2-kbase-semantic/health.json"
            health = json.loads(health_path.read_text(encoding="utf-8"))
            health["heartbeat_epoch"] = time.time() + 1
            _write_json(health_path, health)
            second = inspect_semantic_release_bundle(vault)

        self.assertEqual(first["bundle_fingerprint"], second["bundle_fingerprint"])
        self.assertNotEqual(
            first["runtime_observation"]["health_sha256"],
            second["runtime_observation"]["health_sha256"],
        )

    def test_missing_release_is_reported_without_guessing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            bundle = inspect_semantic_release_bundle(Path(directory))

        self.assertEqual("UNAVAILABLE", bundle["status"])
        self.assertTrue(bundle["issues"])


if __name__ == "__main__":
    unittest.main()
