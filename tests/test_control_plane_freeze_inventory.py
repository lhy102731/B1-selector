from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from research_automation.control_plane.artifact_semantics import (
    validate_code_freeze_manifest,
    validate_final_inventory,
)
from research_automation.control_plane.freeze_inventory import (
    UnstableInventoryError,
    build_stable_freeze_inventory,
)


PLAN_VERSION = "V3.4.2-P0R2"
ATTEMPT_ID = "p0r2-attempt-001"
IDENTITY = {
    "plan_hash": "a" * 64,
    "scope_hash": "b" * 64,
    "instruction_policy_hash": "c" * 64,
}


class StableFreezeInventoryBuilderTests(unittest.TestCase):
    def _repository(self, root: Path) -> None:
        sources = {
            "main.py": "print('main')\n",
            "research_automation/autonomous_runner.py": "class AutonomousRunnerV1:\n    pass\n",
            "research_automation/kbase_ag2_full_cycle.py": "def run_kbase_ag2_full_cycle():\n    pass\n",
            "research_automation/discovery_execution_bridge.py": "def execute_plan():\n    pass\n",
            "data/ignored.py": "raise RuntimeError('data must be excluded')\n",
            "research_state/ignored.py": "raise RuntimeError('state must be excluded')\n",
            "artifacts/ignored.py": "raise RuntimeError('outputs must be excluded')\n",
            "archive/ignored.py": "raise RuntimeError('archive must be excluded')\n",
            "tmp/ignored.py": "raise RuntimeError('tmp must be excluded')\n",
        }
        for relative, content in sources.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    @staticmethod
    def _scheduler_record() -> dict[str, str]:
        return {
            "task_path": r"\A股选股",
            "command": r"D:\workspace\a-share-quant-selector-main\run_select.bat",
            "task_xml_sha256": "d" * 64,
            "state": "Ready",
            "principal": "SYSTEM|ServiceAccount|HighestAvailable",
            "trigger": "Calendar|start=2026-07-27T20:00:00+08:00|days_interval=1|enabled=true",
            "acl_summary": "owner=BUILTIN\\Administrators;sddl=test-only",
        }

    def test_builds_semantically_valid_freeze_and_inventory_from_one_stable_scan(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)

            artifacts = build_stable_freeze_inventory(
                root,
                plan_version=PLAN_VERSION,
                phase="P0",
                attempt_id=ATTEMPT_ID,
                identity_binding=IDENTITY,
                scheduler_records=[self._scheduler_record()],
            )

            freeze = validate_code_freeze_manifest(
                json.dumps(artifacts.freeze_manifest).encode("utf-8"),
                expected_plan_version=PLAN_VERSION,
                expected_phase="P0",
                expected_attempt_id=ATTEMPT_ID,
                expected_identity=IDENTITY,
                repository_root=root,
            )
            inventory = validate_final_inventory(
                json.dumps(artifacts.final_inventory).encode("utf-8"),
                expected_plan_version=PLAN_VERSION,
                expected_phase="P0",
                expected_attempt_id=ATTEMPT_ID,
                expected_identity=IDENTITY,
                freeze_manifest=freeze,
            )

        frozen_paths = {item["path"] for item in freeze["files"]}
        self.assertEqual(
            frozen_paths,
            {
                "main.py",
                "research_automation/autonomous_runner.py",
                "research_automation/discovery_execution_bridge.py",
                "research_automation/kbase_ag2_full_cycle.py",
            },
        )
        self.assertEqual(inventory["entry_count"], 8)

    def test_rejects_same_bytes_file_replacement_between_scan_boundaries(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            target = root / "main.py"
            replacement = root / "main.py.replacement"
            replacement.write_bytes(target.read_bytes())
            original_read_bytes = Path.read_bytes
            target_reads = 0

            def replacing_read_bytes(path: Path) -> bytes:
                nonlocal target_reads
                raw = original_read_bytes(path)
                if path == target:
                    target_reads += 1
                    if target_reads == 3:
                        os.replace(replacement, target)
                return raw

            with patch.object(Path, "read_bytes", replacing_read_bytes):
                with self.assertRaisesRegex(
                    UnstableInventoryError,
                    "changed during scan",
                ):
                    build_stable_freeze_inventory(
                        root,
                        plan_version=PLAN_VERSION,
                        phase="P0",
                        attempt_id=ATTEMPT_ID,
                        identity_binding=IDENTITY,
                        scheduler_records=[self._scheduler_record()],
                    )

    def test_rejects_a_new_executable_file_appearing_mid_scan(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            target = root / "main.py"
            late_source = root / "research_automation" / "late_added.py"
            original_read_bytes = Path.read_bytes
            target_reads = 0

            def adding_read_bytes(path: Path) -> bytes:
                nonlocal target_reads
                raw = original_read_bytes(path)
                if path == target:
                    target_reads += 1
                    if target_reads == 2:
                        late_source.write_text("pass\n", encoding="utf-8")
                return raw

            with patch.object(Path, "read_bytes", adding_read_bytes):
                with self.assertRaisesRegex(
                    UnstableInventoryError,
                    "changed during scan",
                ):
                    build_stable_freeze_inventory(
                        root,
                        plan_version=PLAN_VERSION,
                        phase="P0",
                        attempt_id=ATTEMPT_ID,
                        identity_binding=IDENTITY,
                        scheduler_records=[self._scheduler_record()],
                    )


if __name__ == "__main__":
    unittest.main()
