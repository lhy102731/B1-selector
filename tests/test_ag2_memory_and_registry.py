from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ag2_research.orchestrator import MemoryRouter, RegistryGate
from research_automation.registry_updater import RegistryMergeError, RegistryUpdater


class AG2MemoryAndRegistryTests(unittest.TestCase):
    def test_chinese_near_duplicate_is_detected(self):
        gate = RegistryGate([{
            "id": "exp-1",
            "title": "缩量回踩黄线后的反弹概率",
            "short_result": "验证成交量收缩是否改善反弹胜率",
            "status": "VERIFIED",
        }])

        result = gate.classify("缩量回踩黄线能否提高反弹概率")

        self.assertNotEqual("none", result["registry_status"])

    def test_latest_memory_version_uses_numeric_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "registry_b1_v9.yaml").write_text("registry: {}", encoding="utf-8")
            expected = root / "registry_b1_v10.yaml"
            expected.write_text("registry: {}", encoding="utf-8")
            router = MemoryRouter("b1", root=root)

            self.assertEqual(expected, router._latest("registry_b1_v*.yaml"))

    def test_malformed_existing_memory_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot_b1.yaml"
            path.write_text("snapshot: [unterminated", encoding="utf-8")
            router = MemoryRouter("b1", root=directory)

            with self.assertRaisesRegex(RuntimeError, "cannot load required memory file"):
                router.build_packet("test")

    def test_registry_merge_absorbs_reviewed_result_and_blocks_duplicate_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry_path = root / "registry_brick_v2.yaml"
            registry_path.write_text("registry:\n  experiments: []\n", encoding="utf-8")
            router = MemoryRouter("brick", root=root)
            updater = RegistryUpdater("brick", router=router)
            entry = {
                "id": "brick-exp-041",
                "title": "KBase turnover overlay failed strict account validation",
                "status": "FAILED",
                "short_result": "no improvement under fair account metrics",
                "evidence_source": ["report.md"],
                "reopen_condition": "Reopen only with a new mechanism and Phase 6 validation.",
            }

            sink = MagicMock()
            sink.authorize.return_value = object()
            with patch(
                "research_automation.registry_updater.AuthorizedPathMutation",
                return_value=sink,
            ):
                updater.merge_entry(entry, registry_path=registry_path)

            self.assertEqual("brick-exp-041", router.registry_entries[0]["id"])
            self.assertEqual("failed", router.registry_gate.classify("KBase turnover overlay")["registry_status"])
            with self.assertRaises(RegistryMergeError):
                with patch(
                    "research_automation.registry_updater.AuthorizedPathMutation",
                    return_value=sink,
                ):
                    updater.merge_entry(dict(entry), registry_path=registry_path)


if __name__ == "__main__":
    unittest.main()
