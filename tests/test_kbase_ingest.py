from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ag2_research.kbase.catalog_builder import publish_catalog, rollback_catalog
from ag2_research.kbase.ingest import register_directory, register_resource
from ag2_research.kbase.repository import KBaseRepository
from ag2_research.kbase.telemetry import aggregate_usage, new_event, record_usage


class KBaseIngestTests(unittest.TestCase):
    def _vault(self, root: Path) -> Path:
        vault = root / "KBase"
        (vault / "wiki" / "maps").mkdir(parents=True)
        (vault / "wiki" / "sources").mkdir(parents=True)
        (vault / "wiki" / "maps" / "overview.md").write_text("# 总览\n", encoding="utf-8")
        return vault

    def test_text_pdf_video_intake_become_discoverable_without_external_upload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            vault = self._vault(root)
            text = root / "note.txt"
            text.write_text("第一行来源陈述。\n第二行上下文。", encoding="utf-8")
            pdf = root / "sample.pdf"
            pdf.write_bytes(b"%PDF-1.4\n% deterministic intake fixture\n%%EOF\n")
            video = root / "sample.mp4"
            video.write_bytes(b"not-a-real-video")

            text_result = register_resource(text, vault_path=vault)
            pdf_result = register_resource(pdf, vault_path=vault)
            video_result = register_resource(video, vault_path=vault)
            publication = publish_catalog(vault)

            self.assertTrue(publication["published"])
            repo = KBaseRepository(vault)
            for result in (text_result, pdf_result, video_result):
                source_id = result["state"]["source_id"]
                self.assertIsNotNone(repo.get(source_id))
                self.assertFalse(result["state"]["external_upload_used"])
            self.assertIn("timestamped_transcript_pending", video_result["state"]["pending"])

            raw_path = vault / text_result["state"]["raw_path"]
            before = hashlib.sha256(raw_path.read_bytes()).hexdigest()
            duplicate = register_resource(text, vault_path=vault)
            self.assertTrue(duplicate["state"]["duplicate_raw"])
            self.assertEqual(hashlib.sha256(raw_path.read_bytes()).hexdigest(), before)

    def test_incremental_reuse_failed_candidate_and_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            vault = self._vault(root)
            first_source = root / "one.txt"; first_source.write_text("来源一", encoding="utf-8")
            second_source = root / "two.txt"; second_source.write_text("来源二", encoding="utf-8")
            register_resource(first_source, vault_path=vault)
            first = publish_catalog(vault)
            first_version = first["manifest"]["catalog_version"]

            register_resource(second_source, vault_path=vault)
            second = publish_catalog(vault)
            second_version = second["manifest"]["catalog_version"]
            self.assertNotEqual(first_version, second_version)
            self.assertGreater(second["report"]["reused_entries"], 0)

            current_manifest = Path(second["current"]) / "manifest.json"
            before_failed = current_manifest.read_text(encoding="utf-8")
            with patch("ag2_research.kbase.catalog_builder.validate_release", return_value={"ok": False, "errors": ["forced"], "entries": 0}):
                failed = publish_catalog(vault)
            self.assertFalse(failed["published"])
            self.assertEqual(current_manifest.read_text(encoding="utf-8"), before_failed)

            rolled = rollback_catalog(vault)
            self.assertTrue(rolled["rolled_back"])
            self.assertEqual(rolled["catalog_version"], first_version)

    def test_usage_events_store_metadata_only_and_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            usage = Path(tmp) / "usage"
            event = new_event(
                event_type="search",
                tool="kbase_search",
                catalog_version="v1",
                latency_ms=12.5,
                query="敏感查询正文不应落盘",
                source_ids=["a" * 64],
                result_count=1,
            )
            path = record_usage(event, usage_root=usage)
            stored = path.read_text(encoding="utf-8")
            self.assertNotIn("敏感查询正文", stored)
            summary = aggregate_usage(usage_root=usage)
            self.assertEqual(summary["event_count"], 1)
            self.assertEqual(summary["top_source_ids"][0][0], "a" * 64)

    def test_directory_intake_filters_recurses_reuses_and_publishes_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            vault = self._vault(root)
            incoming = root / "incoming"
            nested = incoming / "nested"
            nested.mkdir(parents=True)
            (incoming / "one.txt").write_text("source one", encoding="utf-8")
            (nested / "two.md").write_text("source two", encoding="utf-8")
            (nested / "ignored.bin").write_bytes(b"ignored")

            with patch("ag2_research.kbase.ingest.publish_catalog", return_value={"published": True}) as publish:
                first = register_directory(
                    incoming, vault_path=vault, suffixes={"txt", ".md"}, publish=True,
                )
            self.assertEqual(first["counts"], {"registered": 2})
            self.assertEqual(first["skipped_by_suffix"], 1)
            self.assertTrue(Path(first["summary_path"]).is_file())
            publish.assert_called_once_with(vault.resolve())

            second = register_directory(incoming, vault_path=vault, suffixes={".txt", ".md"})
            self.assertEqual(second["counts"], {"reused": 2})

            shallow = register_directory(incoming, vault_path=vault, recursive=False, suffixes={".txt"})
            self.assertEqual(shallow["selected_files"], 1)
            self.assertEqual(shallow["counts"], {"reused": 1})

    def test_directory_dry_run_writes_nothing_and_failures_are_isolated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            vault = root / "not-created-vault"
            incoming = root / "incoming"
            incoming.mkdir()
            (incoming / "good.txt").write_text("good", encoding="utf-8")
            (incoming / "bad.txt").write_text("bad", encoding="utf-8")

            dry = register_directory(incoming, vault_path=vault, dry_run=True, suffixes={"txt"})
            self.assertEqual(dry["counts"], {"would_register": 2})
            self.assertFalse(vault.exists())

            original = register_resource
            def fail_one(path, **kwargs):
                if Path(path).name == "bad.txt":
                    raise RuntimeError("fixture failure")
                return original(path, **kwargs)

            with patch("ag2_research.kbase.ingest.register_resource", side_effect=fail_one):
                result = register_directory(incoming, vault_path=vault, suffixes={".txt"})
            self.assertEqual(result["counts"], {"failed": 1, "registered": 1})
            failed = next(item for item in result["files"] if item["status"] == "failed")
            self.assertIn("fixture failure", failed["error"])


if __name__ == "__main__":
    unittest.main()
