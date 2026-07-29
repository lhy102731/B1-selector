from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ag2_research.kbase.reextraction_runner import run_queue


class ReextractionRunnerTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[Path, Path]:
        vault = root / "KBase"
        release = vault / "wiki/outputs/manifests/ag2-kbase/current"
        raw = vault / "raw"
        release.mkdir(parents=True)
        raw.mkdir()
        (release / "manifest.json").write_text(json.dumps({"catalog_schema_version": 1}), encoding="utf-8")
        (release / "facets.json").write_text("{}", encoding="utf-8")
        (release / "catalog.jsonl").write_text("", encoding="utf-8")
        (raw / "a.txt").write_text("first\nsecond", encoding="utf-8")
        queue = root / "queue.json"
        queue.write_text(json.dumps({"tasks": [
            {"queue_id": "q:a", "source_id": "a", "raw_path": "raw/a.txt", "route": "text"},
            {"queue_id": "q:b", "source_id": "b", "raw_path": "raw/missing.pdf", "route": "requires_ocr_or_visual_extraction"},
        ]}), encoding="utf-8")
        return vault, queue

    def test_dry_run_writes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, queue = self._fixture(Path(tmp))
            result = run_queue(queue_path=queue, vault_path=vault, dry_run=True)
            self.assertEqual(result["would_attempt"], 2)
            self.assertFalse((vault / "wiki/outputs/candidates/ag2-kbase/content-layer-repair/reextraction-runs").exists())

    def test_failure_isolated_and_rerun_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, queue = self._fixture(Path(tmp))
            raw_before = (vault / "raw/a.txt").read_bytes()
            first = run_queue(queue_path=queue, vault_path=vault)
            second = run_queue(queue_path=queue, vault_path=vault)
            state = json.loads(Path(first["state_path"]).read_text(encoding="utf-8"))
            artifact = vault / state["items"]["q:a"]["artifact_path"]
            document = json.loads(artifact.read_text(encoding="utf-8"))
            self.assertEqual(first["counts"], {"done": 1, "blocked": 1})
            self.assertEqual(second["attempted"], 0)
            self.assertFalse(document["policy"]["distilled_statements"])
            self.assertFalse(document["policy"]["accepted_evidence"])
            self.assertFalse(document["policy"]["publication_eligible"])
            self.assertEqual(document["quality_gate"]["stage"], "anchored_text_awaiting_distillation")
            self.assertEqual((vault / "raw/a.txt").read_bytes(), raw_before)

    def test_plugin_candidate_is_publishable_only_when_gate_accepts(self) -> None:
        cases = [(0.95, "accept", True), (0.50, "review", False)]
        for confidence, decision, eligible in cases:
            with self.subTest(decision=decision), tempfile.TemporaryDirectory() as tmp:
                vault, queue = self._fixture(Path(tmp))

                def plugin(path, task):
                    return ([{"anchor": "line:1", "text": "source text"}], "test_plugin", {
                        "record": {"claims": [{"text": "explicit claim",
                                                "evidence_anchor": "line:1",
                                                "confidence": confidence}]}})

                result = run_queue(queue_path=queue, vault_path=vault, limit=1, extractor=plugin)
                state = json.loads(Path(result["state_path"]).read_text(encoding="utf-8"))
                artifact = json.loads((vault / state["items"]["q:a"]["artifact_path"]).read_text(encoding="utf-8"))
                self.assertEqual(artifact["quality_gate"]["decision"], decision)
                self.assertEqual(artifact["quality_gate"]["publication_eligible"], eligible)
                self.assertEqual(state["items"]["q:a"]["publication_eligible"], eligible)

    def test_plugin_reject_cannot_be_published(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, queue = self._fixture(Path(tmp))
            plugin = lambda path, task: ([{"anchor": "line:1", "text": "text"}], "plugin", {
                "record": {"summary": "same", "claims": [{"text": "same", "confidence": 0.99}]}})
            result = run_queue(queue_path=queue, vault_path=vault, limit=1, extractor=plugin)
            state = json.loads(Path(result["state_path"]).read_text(encoding="utf-8"))
            artifact = json.loads((vault / state["items"]["q:a"]["artifact_path"]).read_text(encoding="utf-8"))
            self.assertEqual(artifact["quality_gate"]["decision"], "reject")
            self.assertFalse(artifact["quality_gate"]["publication_eligible"])

    def test_in_flight_checkpoint_resumes_after_crash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, queue = self._fixture(Path(tmp))
            with self.assertRaises(RuntimeError):
                run_queue(queue_path=queue, vault_path=vault, limit=1,
                          crash_hook=lambda task: (_ for _ in ()).throw(RuntimeError("process death")))
            result = run_queue(queue_path=queue, vault_path=vault, limit=1)
            state = json.loads(Path(result["state_path"]).read_text(encoding="utf-8"))
            self.assertEqual(state["items"]["q:a"]["status"], "done")
            self.assertEqual(state["items"]["q:a"]["attempts"], 2)


if __name__ == "__main__":
    unittest.main()
