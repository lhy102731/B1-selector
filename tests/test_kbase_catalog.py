from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from ag2_research.kbase.adapters import adapt_source_packet, discover_source_packets
from ag2_research.kbase.catalog_builder import build_catalog, publish_catalog, validate_release


def packet(sha: str, version: int, title: str) -> dict:
    return {
        "schema_version": version,
        "pipeline_revision": 1 if version == 2 else None,
        "sha256": sha,
        "original_path": f"items/{title}.pdf",
        "kind": "pdf",
        "use_mode": "reference",
        "extraction_layers": ["direct_text"],
        "record": {
            "canonical_title": title,
            "aliases": [],
            "source_type": "pdf",
            "source_role": "primary_direct",
            "primary_people": ["作者甲"],
            "topics": ["市场情绪"],
            "summary": "来源摘要。",
            "methods": [{
                "text": "来源方法。",
                "evidence_anchor": "[L1]",
                "evidence_quote": "原文。",
                "certainty": "high",
                "source_voice": "author",
            }],
            "claims": [],
            "risks": [],
            "contradictions": [],
            "reliability": "medium",
            "review_flags": [],
        },
    }


class KBaseCatalogTests(unittest.TestCase):
    def _vault(self, root: Path) -> tuple[Path, Path]:
        vault = root / "KBase"
        packets = vault / "raw" / "imports" / "sample" / "distillation" / "source-packets"
        packets.mkdir(parents=True)
        (vault / "wiki" / "maps").mkdir(parents=True)
        (vault / "wiki" / "sources").mkdir(parents=True)
        family_root = vault / "wiki" / "outputs" / "source-families" / "manifest"
        family_root.mkdir(parents=True)
        (vault / "wiki" / "maps" / "overview.md").write_text("# 总览\n\n来源地图。\n", encoding="utf-8")
        return vault, packets

    def test_v1_v2_and_partial_packets_are_read_without_migration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, packets = self._vault(Path(tmp))
            sha1, sha2 = "1" * 64, "2" * 64
            p1 = packets / f"{sha1}.json"
            p2 = packets / f"{sha2}.json"
            p1.write_text(json.dumps(packet(sha1, 1, "V1来源"), ensure_ascii=False), encoding="utf-8")
            p2.write_text(json.dumps(packet(sha2, 2, "V2来源"), ensure_ascii=False), encoding="utf-8")
            before = p1.read_bytes()

            e1 = adapt_source_packet(p1, vault)
            e2 = adapt_source_packet(p2, vault)

            self.assertEqual(e1["source_schema_version"], 1)
            self.assertEqual(e2["source_schema_version"], 2)
            self.assertEqual(p1.read_bytes(), before)
            self.assertNotIn("hypothesis", e1)

    def test_catalog_covers_every_packet_and_incrementally_reuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, packets = self._vault(Path(tmp))
            sha1, sha2, sha3 = "1" * 64, "2" * 64, "3" * 64
            (packets / f"{sha1}.json").write_text(json.dumps(packet(sha1, 1, "一"), ensure_ascii=False), encoding="utf-8")
            (packets / f"{sha2}.json").write_text(json.dumps(packet(sha2, 2, "二"), ensure_ascii=False), encoding="utf-8")
            (packets / f"{sha3}.json").write_text("{broken", encoding="utf-8")
            entries, manifest, report = build_catalog(vault)

            self.assertEqual(len(discover_source_packets(vault)), 3)
            self.assertEqual(manifest["counts"]["source_packets"], 3)
            self.assertEqual(report["blocked_packet_entries"], 1)
            self.assertFalse(report["errors"])

            first = publish_catalog(vault)
            self.assertTrue(first["published"])
            self.assertTrue(validate_release(Path(first["current"]))["ok"])
            second = publish_catalog(vault)
            self.assertTrue(second["published"])
            self.assertGreater(second["report"]["reused_entries"], 0)
            self.assertTrue((vault / "wiki" / "outputs" / "manifests" / "ag2-kbase" / "previous").is_dir())

    def test_publish_does_not_change_raw_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, packets = self._vault(Path(tmp))
            sha = "a" * 64
            packet_path = packets / f"{sha}.json"
            packet_path.write_text(json.dumps(packet(sha, 2, "不可变"), ensure_ascii=False), encoding="utf-8")
            before = hashlib.sha256(packet_path.read_bytes()).hexdigest()

            result = publish_catalog(vault)

            self.assertTrue(result["published"])
            self.assertEqual(hashlib.sha256(packet_path.read_bytes()).hexdigest(), before)


if __name__ == "__main__":
    unittest.main()
