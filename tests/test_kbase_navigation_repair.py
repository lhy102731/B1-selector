from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ag2_research.kbase.navigation_repair import generate_navigation_repair_candidate


class NavigationRepairTests(unittest.TestCase):
    def test_exact_family_key_creates_candidate_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp) / "KBase"
            release = vault / "wiki/outputs/manifests/ag2-kbase/current"
            packets = vault / "packets"
            release.mkdir(parents=True); packets.mkdir()
            sha = "a" * 64
            packet = {"record": {"family_key": "  Example_Key  ", "summary": "s",
                                  "methods": [{"text": "x", "evidence_anchor": "L1"}]}}
            (packets / "a.json").write_text(json.dumps(packet), encoding="utf-8")
            entry = {"source_id": sha, "object_type": "source_packet", "title": "x",
                     "family_id": None, "parent_ids": [], "date_start": None,
                     "available_layers": ["summary", "statements", "evidence"],
                     "paths": {"packet": "packets/a.json"}}
            (release / "manifest.json").write_text(json.dumps({"catalog_schema_version": 1,
                "catalog_version": "v1", "counts": {"source_packets": 1}}), encoding="utf-8")
            (release / "facets.json").write_text("{}", encoding="utf-8")
            (release / "catalog.jsonl").write_text(json.dumps(entry) + "\n", encoding="utf-8")
            out = vault / "candidate"
            first = generate_navigation_repair_candidate(vault_path=vault, output_dir=out)
            second = generate_navigation_repair_candidate(vault_path=vault, output_dir=out)
            plan = json.loads((out / "plan.json").read_text(encoding="utf-8"))
        self.assertEqual(first["candidate_id"], second["candidate_id"])
        self.assertEqual(first["coverage_comparison"]["orphans_after"], 0)
        self.assertEqual(plan["patches"][0]["rule"], "exact_normalized_family_key_create")
        self.assertEqual(plan["patches"][0]["confidence"], 1.0)


if __name__ == "__main__":
    unittest.main()
