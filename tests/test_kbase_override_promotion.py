from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ag2_research.kbase.catalog_builder import publish_catalog
from ag2_research.kbase.coverage import build_navigation_coverage
from ag2_research.kbase.navigation_repair import generate_navigation_repair_candidate
from ag2_research.kbase.override_promotion import promote_candidate, promote_candidates
from ag2_research.kbase.repository import KBaseRepository


class OverridePromotionTests(unittest.TestCase):
    def test_promoted_override_survives_rebuild(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp) / "KBase"
            packets = vault / "raw/imports/test/distillation/source-packets"
            packets.mkdir(parents=True)
            sha = "d" * 64
            document = {"schema_version": 2, "sha256": sha, "original_path": "",
                "record": {"canonical_title": "lesson", "family_key": "Exact Course",
                    "source_type": "note", "source_role": "primary_direct", "primary_people": [],
                    "topics": ["test"], "summary": "summary", "methods": [
                        {"text": "statement", "evidence_anchor": "L1"}], "claims": [], "risks": [],
                    "contradictions": [], "reliability": "medium", "review_flags": []}}
            (packets / f"{sha}.json").write_text(json.dumps(document), encoding="utf-8")
            self.assertTrue(publish_catalog(vault)["published"])
            candidate = generate_navigation_repair_candidate(vault_path=vault)
            raw = vault / "raw/recovered.md"; raw.write_text("source", encoding="utf-8")
            trace = vault / "wiki/outputs/manifests/ag2-kbase/repair-candidates/trace-test"
            trace.mkdir(parents=True)
            base = KBaseRepository(vault).manifest["catalog_version"]
            report = {"schema_version": 1, "source_catalog_version": base, "gap_count": 1,
                "findings": [{"source_id": sha, "status": "recoverable", "reasons": ["missing_raw"],
                    "raw_resolution": {"path": "raw/recovered.md", "basis": "test"},
                    "candidate_patch": {"paths": {"raw": "raw/recovered.md"}}}]}
            (trace / "repair-report.json").write_text(json.dumps(report), encoding="utf-8")
            promoted = promote_candidates(vault_path=vault,
                candidate_dirs=[candidate["output_dir"], trace])
            self.assertTrue(promoted["published"])
            self.assertEqual(len(build_navigation_coverage(vault_path=vault)["orphans"]), 0)
            self.assertEqual(KBaseRepository(vault).get(sha)["paths"]["raw"], "raw/recovered.md")
            self.assertTrue(publish_catalog(vault)["published"])
            self.assertEqual(len(build_navigation_coverage(vault_path=vault)["orphans"]), 0)
            self.assertEqual(KBaseRepository(vault).get(sha)["paths"]["raw"], "raw/recovered.md")
            repeated = promote_candidates(vault_path=vault,
                candidate_dirs=[candidate["output_dir"], trace])
            self.assertTrue(repeated["idempotent"])
            manifest_path = vault / "wiki/outputs/manifests/ag2-kbase/current/manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["override_state"] = "mismatch"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            repaired_publication = promote_candidates(vault_path=vault,
                candidate_dirs=[candidate["output_dir"], trace])
            self.assertTrue(repaired_publication["published"])
            self.assertNotEqual(KBaseRepository(vault).manifest["override_state"], "mismatch")

    def test_rejects_stale_candidate_without_writing_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp) / "KBase"
            release = vault / "wiki/outputs/manifests/ag2-kbase/current"
            candidate = vault / "wiki/outputs/manifests/ag2-kbase/candidate-repairs/stale"
            release.mkdir(parents=True); candidate.mkdir(parents=True)
            (release / "manifest.json").write_text(json.dumps({"catalog_schema_version": 1,
                "catalog_version": "current"}), encoding="utf-8")
            (release / "facets.json").write_text("{}", encoding="utf-8")
            (release / "catalog.jsonl").write_text("", encoding="utf-8")
            (candidate / "plan.json").write_text(json.dumps({"candidate_id": "stale",
                "base_catalog_version": "old", "patches": []}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "source catalog version"):
                promote_candidate(vault_path=vault, candidate_dir=candidate)
            self.assertFalse((vault / "wiki/outputs/manifests/ag2-kbase/approved-overrides.json").exists())

    def test_path_only_override_changes_catalog_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp) / "KBase"
            packets = vault / "raw/imports/test/distillation/source-packets"; packets.mkdir(parents=True)
            sha = "e" * 64
            doc = {"schema_version": 2, "sha256": sha, "original_path": "", "record": {
                "canonical_title": "dated", "family_key": "", "source_type": "note",
                "source_role": "primary_direct", "primary_people": [], "topics": [], "summary": "s",
                "methods": [{"text": "x", "evidence_anchor": "L1"}], "claims": [], "risks": [],
                "contradictions": [], "reliability": "medium", "review_flags": []}}
            (packets / f"{sha}.json").write_text(json.dumps(doc), encoding="utf-8")
            first = publish_catalog(vault); old_version = first["manifest"]["catalog_version"]
            raw = vault / "raw/fixed.md"; raw.write_text("x", encoding="utf-8")
            trace = vault / "wiki/outputs/manifests/ag2-kbase/repair-candidates/path-only"; trace.mkdir(parents=True)
            report = {"schema_version": 1, "source_catalog_version": old_version, "gap_count": 1,
                "findings": [{"source_id": sha, "status": "recoverable", "reasons": ["missing_raw"],
                    "candidate_patch": {"paths": {"raw": "raw/fixed.md"}}}]}
            (trace / "repair-report.json").write_text(json.dumps(report), encoding="utf-8")
            result = promote_candidate(vault_path=vault, candidate_dir=trace)
            self.assertNotEqual(result["catalog_version"], old_version)


if __name__ == "__main__":
    unittest.main()
