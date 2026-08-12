"""Tests for the handle-first final evaluation data boundary (P8R3 T3)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_automation.control_plane.final_eval_data import (
    FinalEvalHandleRejected,
    HandleFirstOpener,
    OpenedHoldoutArtifact,
    VerifiedRootHandle,
    verify_backend_rejects_raw_paths,
)


class VerifiedRootHandleTests(unittest.TestCase):
    def test_root_handle_seals_directory_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            handle = VerifiedRootHandle(root)
            self.assertEqual(handle.root, root.resolve())
            self.assertGreater(handle.volume_serial, 0)
            self.assertGreater(handle.file_id, 0)
            self.assertNotIn(str(root), repr(handle))

    def test_root_handle_rejects_missing_root(self) -> None:
        with self.assertRaises(FinalEvalHandleRejected):
            VerifiedRootHandle(Path("/definitely/not/exists"))


class HandleFirstOpenerTests(unittest.TestCase):
    def _fixture(self):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        holdout = root / "holdout.parquet"
        content = b"CANARY_7f3a9c2b|" + b"x" * 128
        holdout.write_bytes(content)
        return temporary, root, holdout, content

    def test_open_artifact_verifies_identity_and_content_from_same_handle(
        self,
    ) -> None:
        import hashlib

        temporary, root, holdout, content = self._fixture()
        self.addCleanup(temporary.cleanup)
        opener = HandleFirstOpener(VerifiedRootHandle(root))
        artifact = opener.open_artifact(
            ref="holdout.parquet",
            holdout_id="holdout-final-1",
            holdout_sha256=hashlib.sha256(content).hexdigest(),
        )
        self.assertIsInstance(artifact, OpenedHoldoutArtifact)
        self.assertEqual(artifact.read_bytes(), content)
        self.assertEqual(artifact.size, len(content))
        self.assertNotIn("holdout.parquet", repr(artifact))

    def test_open_artifact_rejects_traversal(self) -> None:
        temporary, root, holdout, content = self._fixture()
        self.addCleanup(temporary.cleanup)
        opener = HandleFirstOpener(VerifiedRootHandle(root))
        with self.assertRaises(FinalEvalHandleRejected):
            opener.open_artifact(
                ref="../escape.parquet",
                holdout_id="x",
                holdout_sha256="0" * 64,
            )

    def test_open_artifact_rejects_content_hash_mismatch(self) -> None:
        import hashlib

        temporary, root, holdout, content = self._fixture()
        self.addCleanup(temporary.cleanup)
        opener = HandleFirstOpener(VerifiedRootHandle(root))
        with self.assertRaises(FinalEvalHandleRejected):
            opener.open_artifact(
                ref="holdout.parquet",
                holdout_id="x",
                holdout_sha256=hashlib.sha256(b"different").hexdigest(),
            )

    def test_open_artifact_rejects_missing_child(self) -> None:
        temporary, root, holdout, content = self._fixture()
        self.addCleanup(temporary.cleanup)
        opener = HandleFirstOpener(VerifiedRootHandle(root))
        with self.assertRaises(FinalEvalHandleRejected):
            opener.open_artifact(
                ref="missing.parquet",
                holdout_id="x",
                holdout_sha256="0" * 64,
            )


class BackendProtocolTests(unittest.TestCase):
    def test_backend_rejects_raw_path(self) -> None:
        # verify_backend_rejects_raw_paths must not raise for a compliant
        # backend (i.e. the protocol rejects Path inputs fail-closed).
        verify_backend_rejects_raw_paths(object())  # type: ignore[arg-type]


class WorkerProtocolTests(unittest.TestCase):
    def test_worker_output_validates_bounded_contract(self) -> None:
        from research_automation.final_eval_worker import (
            FinalEvalWorkerOutputRejected,
            validate_worker_output,
        )

        payload = {
            "schema_version": "control_plane.final_eval_worker_result.v1",
            "metrics": {"sharpe": 0.5, "calibration_error": 0.01},
            "counts": {"rows": 10},
            "artifact_hashes": {"holdout": "a" * 64},
            "evidence_refs": [
                "research_state/control_plane/p8/attempts/p8-attempt-002/evidence/worker_result.json"
            ],
            "outcome": "SUCCEEDED",
        }
        result = validate_worker_output(payload)
        self.assertEqual(result["outcome"], "SUCCEEDED")

    def test_worker_output_rejects_unknown_fields(self) -> None:
        from research_automation.final_eval_worker import (
            FinalEvalWorkerOutputRejected,
            validate_worker_output,
        )

        payload = {
            "schema_version": "control_plane.final_eval_worker_result.v1",
            "metrics": {},
            "counts": {},
            "artifact_hashes": {},
            "evidence_refs": [],
            "outcome": "SUCCEEDED",
            "raw_labels": ["secret"],
        }
        with self.assertRaises(FinalEvalWorkerOutputRejected):
            validate_worker_output(payload)

    def test_worker_output_rejects_nan_metric(self) -> None:
        from research_automation.final_eval_worker import (
            FinalEvalWorkerOutputRejected,
            validate_worker_output,
        )

        payload = {
            "schema_version": "control_plane.final_eval_worker_result.v1",
            "metrics": {"sharpe": float("nan")},
            "counts": {},
            "artifact_hashes": {},
            "evidence_refs": [],
            "outcome": "SUCCEEDED",
        }
        with self.assertRaises(FinalEvalWorkerOutputRejected):
            validate_worker_output(payload)

    def test_worker_output_rejects_unbounded_metric(self) -> None:
        from research_automation.final_eval_worker import (
            FinalEvalWorkerOutputRejected,
            validate_worker_output,
        )

        payload = {
            "schema_version": "control_plane.final_eval_worker_result.v1",
            "metrics": {"totally_unbounded": 1e9},
            "counts": {},
            "artifact_hashes": {},
            "evidence_refs": [],
            "outcome": "SUCCEEDED",
        }
        with self.assertRaises(FinalEvalWorkerOutputRejected):
            validate_worker_output(payload)

    def test_worker_output_rejects_unsafe_evidence_ref(self) -> None:
        from research_automation.final_eval_worker import (
            FinalEvalWorkerOutputRejected,
            validate_worker_output,
        )

        payload = {
            "schema_version": "control_plane.final_eval_worker_result.v1",
            "metrics": {},
            "counts": {},
            "artifact_hashes": {},
            "evidence_refs": ["/etc/passwd"],
            "outcome": "SUCCEEDED",
        }
        with self.assertRaises(FinalEvalWorkerOutputRejected):
            validate_worker_output(payload)

    def test_worker_process_emits_bounded_result_from_stdin(self) -> None:
        import subprocess
        import sys
        from pathlib import Path

        content = b"CANARY_7f3a9c2b|" + b"x" * 128
        sha256 = __import__("hashlib").sha256(content).hexdigest()
        child = subprocess.run(
            [
                sys.executable,
                "-m",
                "research_automation.final_eval_worker",
                sha256,
                "holdout-final-1",
            ],
            input=content,
            capture_output=True,
            timeout=60,
            cwd=Path(__file__).resolve().parents[1],
        )
        self.assertEqual(child.returncode, 0, msg=child.stderr.decode())
        import json as _json

        payload = _json.loads(child.stdout.decode())
        self.assertEqual(payload["outcome"], "SUCCEEDED")
        self.assertEqual(payload["artifact_hashes"]["holdout"], sha256)

    def test_worker_process_rejects_hash_mismatch(self) -> None:
        import subprocess
        import sys
        from pathlib import Path

        content = b"CANARY_7f3a9c2b|" + b"x" * 128
        child = subprocess.run(
            [
                sys.executable,
                "-m",
                "research_automation.final_eval_worker",
                "f" * 64,
                "holdout-final-1",
            ],
            input=content,
            capture_output=True,
            timeout=60,
            cwd=Path(__file__).resolve().parents[1],
        )
        self.assertEqual(child.returncode, 3)
        self.assertIn(b"HASH_MISMATCH", child.stderr)


if __name__ == "__main__":
    unittest.main()
