from __future__ import annotations

import json
import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from research_automation.control_plane import inventory as inventory_module
from research_automation.control_plane.artifact_semantics import (
    ArtifactSemanticError,
    validate_final_inventory,
)
from research_automation.control_plane.contracts import (
    canonical_json,
    canonical_sha256,
)
from research_automation.control_plane.inventory import (
    UnstableInventoryError,
    build_code_freeze_manifest,
    build_final_entry_inventory,
    verify_current_git_inventory,
)


class GitSourceIdentityTests(unittest.TestCase):
    identity = {
        "plan_hash": "a" * 64,
        "scope_hash": "b" * 64,
        "instruction_policy_hash": "c" * 64,
    }

    @staticmethod
    def _git(root: Path, *arguments: str) -> str:
        completed = subprocess.run(
            ["git", *arguments],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        return completed.stdout.strip()

    @staticmethod
    def _git_runtime_closure_sha256(runtime_directory: Path) -> str:
        entries = []
        paths = [runtime_directory / "git.exe"]
        paths.extend(runtime_directory.glob("*.dll"))
        for path in sorted(paths, key=lambda item: item.name.casefold()):
            raw = path.read_bytes()
            entries.append(
                {
                    "name": path.name,
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "bytes": len(raw),
                }
            )
        return hashlib.sha256(
            canonical_json(
                {
                    "schema_version": "control_plane.git_runtime_closure.v1",
                    "entries": entries,
                }
            ).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _copy_git_runtime(runtime_directory: Path) -> None:
        source_directory = inventory_module._TRUSTED_GIT_EXECUTABLE.parent
        runtime_directory.mkdir(parents=True)
        shutil.copy2(source_directory / "git.exe", runtime_directory / "git.exe")
        for source in source_directory.glob("*.dll"):
            shutil.copy2(source, runtime_directory / source.name)

    def _repository(
        self,
        root: Path,
        *,
        legacy_sources: dict[str, bytes] | None = None,
        legacy_disposition: str = "LEGACY_UNAUDITED",
        legacy_trust_state: str = "legacy_unaudited",
        quarantine_eligible: bool = True,
    ) -> tuple[str, str]:
        legacy_sources = legacy_sources or {}
        policy_entries = [
            {
                "actor_type": "legacy_runner",
                "callable_name": "main",
                "content_sha256": hashlib.sha256(raw).hexdigest(),
                "declared_phase": None,
                "declared_side_effects": [],
                "disposition": legacy_disposition,
                "entry_id": f"file:{relative}",
                "external_metadata": {},
                "kind": "python_module",
                "path": relative,
                "resource_roots": [],
                "source": "filesystem_inventory",
                "trust_state": legacy_trust_state,
            }
            for relative, raw in sorted(legacy_sources.items())
        ]
        quarantine_paths = sorted(legacy_sources) if quarantine_eligible else []
        policy_path = (
            root
            / "research_automation"
            / "control_plane"
            / "entry_policy.json"
        )
        policy_path.parent.mkdir(parents=True)
        policy_path.write_text(
            json.dumps(
                {
                    "entries": policy_entries,
                    "plan_hash": "d" * 64,
                    "policy_hash": "e" * 64,
                    "review_state": "APPROVED",
                    "schema_version": "control_plane.entry_policy.v1",
                    "scope_hash": "f" * 64,
                    "quarantine_eligible_paths": quarantine_paths,
                    "quarantine_eligible_paths_sha256": canonical_sha256(
                        {
                            "schema_version": (
                                "control_plane.legacy_quarantine_paths.v1"
                            ),
                            "paths": quarantine_paths,
                        }
                    ),
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (root / ".gitattributes").write_text("*.py text eol=lf\n", encoding="utf-8")
        (root / "worker.py").write_text("print('tracked')\n", encoding="utf-8")
        (root / "CHANGELOG.md").write_text("baseline\n", encoding="utf-8")
        (root / "AGENTS.md").write_text("# instructions\n", encoding="utf-8")
        seam_sources = {
            "research_automation/autonomous_runner.py": (
                "class AutonomousRunnerV1:\n    def run(self):\n        pass\n"
            ),
            "research_automation/discovery_execution_bridge.py": (
                "def execute_plan():\n    pass\n"
            ),
            "research_automation/kbase_ag2_full_cycle.py": (
                "def run_kbase_ag2_full_cycle():\n    pass\n"
            ),
            "research_automation/control_plane/final_evaluator.py": (
                "class TrustedEvaluator:\n"
                "    def evaluate_v2(self):\n        pass\n"
            ),
            "research_automation/control_plane/final_eval_composition.py": (
                "def compose_final_eval_runtime(context):\n    pass\n"
            ),
        }
        for relative, content in seam_sources.items():
            target = root.joinpath(*relative.split("/"))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        self._git(root, "init", "--quiet")
        self._git(
            root,
            "add",
            ".gitattributes",
            "AGENTS.md",
            "CHANGELOG.md",
            "worker.py",
            policy_path.as_posix(),
            *seam_sources,
        )
        self._git(
            root,
            "-c",
            "user.name=Control Plane Tests",
            "-c",
            "user.email=control-plane@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        )
        for relative, raw in legacy_sources.items():
            target = root.joinpath(*relative.split("/"))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
        return (
            self._git(root, "rev-parse", "HEAD"),
            self._git(root, "rev-parse", "HEAD^{tree}"),
        )

    def test_clean_repository_freezes_git_commit_and_tree(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected_commit, expected_tree = self._repository(root)

            manifest = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )

        self.assertEqual(
            manifest["schema_version"],
            "control_plane.code_freeze_manifest.v2",
        )
        self.assertEqual(manifest["git_commit"], expected_commit)
        self.assertEqual(manifest["git_tree"], expected_tree)
        self.assertEqual(manifest["active_tracked_dirty_paths"], [])
        self.assertEqual(manifest["untracked_executables"], [])
        self.assertNotIn("files", manifest)

    def test_dirty_instruction_markdown_is_treated_as_source_policy(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            (root / "AGENTS.md").write_text("# changed policy\n", encoding="utf-8")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "tracked source is dirty",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_dirty_tracked_source_fails_closed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            (root / "worker.py").write_text("print('dirty')\n", encoding="utf-8")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "tracked source is dirty",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_dirty_tracked_report_is_recorded_without_changing_source_identity(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            clean = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            (root / "CHANGELOG.md").write_text("operator notes\n", encoding="utf-8")

            with_dirty_report = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )

        self.assertEqual(
            with_dirty_report["nonblocking_tracked_dirty_paths"],
            ["CHANGELOG.md"],
        )
        self.assertEqual(
            with_dirty_report["source_identity_sha256"],
            clean["source_identity_sha256"],
        )

    def test_policy_bound_untracked_legacy_source_is_quarantined_by_hash(self) -> None:
        legacy_path = "research/legacy_runner.py"
        legacy_raw = b"if __name__ == '__main__':\n    pass\n"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root, legacy_sources={legacy_path: legacy_raw})

            manifest = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )

        self.assertEqual(
            manifest["untracked_executables"],
            [
                {
                    "path": legacy_path,
                    "sha256": hashlib.sha256(legacy_raw).hexdigest(),
                    "disposition": "LEGACY_UNAUDITED",
                    "trust_state": "legacy_unaudited",
                }
            ],
        )

    def test_unknown_untracked_executable_fails_closed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            unknown = root / "tools" / "unknown_repair.py"
            unknown.parent.mkdir(parents=True)
            unknown.write_text("raise SystemExit(1)\n", encoding="utf-8")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "not in the legacy policy",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_unknown_top_level_executable_family_fails_closed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            unknown = root / "new_runner_family" / "run.py"
            unknown.parent.mkdir()
            unknown.write_text("raise SystemExit(1)\n", encoding="utf-8")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "not in the legacy policy",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_ignored_unknown_top_level_executable_family_fails_closed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            (root / ".gitignore").write_text(
                "/new_runner_family/\n",
                encoding="utf-8",
            )
            self._git(root, "add", ".gitignore")
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "ignore fixture",
            )
            unknown = root / "new_runner_family" / "run.py"
            unknown.parent.mkdir()
            unknown.write_text("raise SystemExit(1)\n", encoding="utf-8")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "not in the legacy policy",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_nested_data_named_source_directory_remains_in_scope(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            unknown = root / "research" / "experiment" / "data" / "run.py"
            unknown.parent.mkdir(parents=True)
            unknown.write_text("raise SystemExit(1)\n", encoding="utf-8")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "not in the legacy policy",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_tools_data_source_package_is_not_mistaken_for_generated_data(self) -> None:
        legacy_path = "tools/data/backfill.py"
        legacy_raw = b"raise SystemExit(0)\n"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root, legacy_sources={legacy_path: legacy_raw})

            manifest = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )

        self.assertEqual(
            [item["path"] for item in manifest["untracked_executables"]],
            [legacy_path],
        )

    def test_active_policy_classification_cannot_quarantine_untracked_source(self) -> None:
        legacy_path = "research/claimed_production.py"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(
                root,
                legacy_sources={legacy_path: b"pass\n"},
                legacy_disposition="PRODUCTION_DAILY",
                legacy_trust_state="production_daily",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "not quarantine-eligible",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_policy_record_without_explicit_quarantine_eligibility_is_rejected(self) -> None:
        legacy_path = "research/legacy_runner.py"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(
                root,
                legacy_sources={legacy_path: b"pass\n"},
                quarantine_eligible=False,
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "not quarantine-eligible",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_git_ignored_production_runtime_is_bound_separately(self) -> None:
        runtime_path = "tools/ths_yuanhang_bridge/YuanhangBridge.dll"
        runtime_raw = b"MZ-test-runtime"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            tracked_runtime_files = {
                ".gitignore": "/tools/ths_yuanhang_bridge/YuanhangBridge.dll\n",
                "utils/ths_yuanhang_bridge.py": "def fetch():\n    pass\n",
                "tools/ths_yuanhang_bridge/build.ps1": "Write-Output built\n",
                "tools/ths_yuanhang_bridge/YuanhangBridge.cs": "class Bridge {}\n",
                "tools/ths_yuanhang_bridge/YuanhangBridge.runtimeconfig.json": "{}\n",
                "tools/ths_yuanhang_bridge/workspace/datacenter.xml": "<root />\n",
                "tools/ths_yuanhang_bridge/workspace/DNSTest.xml": "<root />\n",
            }
            for relative, content in tracked_runtime_files.items():
                target = root.joinpath(*relative.split("/"))
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content, encoding="utf-8")
            runtime = root.joinpath(*runtime_path.split("/"))
            runtime.write_bytes(runtime_raw)
            self._git(root, "add", *tracked_runtime_files)
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "runtime fixture",
            )

            manifest = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=manifest,
                scheduler_records=[{"task_path": r"\A股选股"}],
            )

        self.assertEqual(
            manifest["runtime_dependencies"],
            [
                {
                    "path": runtime_path,
                    "sha256": hashlib.sha256(runtime_raw).hexdigest(),
                }
            ],
        )
        self.assertEqual(manifest["untracked_executables"], [])
        runtime_entries = [
            entry
            for entry in inventory["entries"]
            if entry["source"] == "runtime_dependency_inventory"
        ]
        self.assertEqual(len(runtime_entries), 5)
        self.assertEqual(
            [entry for entry in runtime_entries if entry["path"] == runtime_path][0][
                "content_sha256"
            ],
            hashlib.sha256(runtime_raw).hexdigest(),
        )

    def test_live_verifier_rejects_runtime_drift_after_evidence_commit(self) -> None:
        runtime_path = "tools/ths_yuanhang_bridge/YuanhangBridge.dll"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            tracked_runtime_files = {
                ".gitignore": f"/{runtime_path}\n",
                "utils/ths_yuanhang_bridge.py": "def fetch():\n    pass\n",
                "tools/ths_yuanhang_bridge/build.ps1": "Write-Output built\n",
                "tools/ths_yuanhang_bridge/YuanhangBridge.cs": "class Bridge {}\n",
                "tools/ths_yuanhang_bridge/YuanhangBridge.runtimeconfig.json": "{}\n",
                "tools/ths_yuanhang_bridge/workspace/datacenter.xml": "<root />\n",
                "tools/ths_yuanhang_bridge/workspace/DNSTest.xml": "<root />\n",
            }
            for relative, content in tracked_runtime_files.items():
                target = root.joinpath(*relative.split("/"))
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content, encoding="utf-8")
            runtime = root.joinpath(*runtime_path.split("/"))
            runtime.write_bytes(b"MZ-before-freeze")
            self._git(root, "add", *tracked_runtime_files)
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "runtime fixture",
            )
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence = root / "research_state" / "control_plane" / "gate.json"
            evidence.parent.mkdir(parents=True)
            evidence.write_text('{"verdict":"PASS"}', encoding="utf-8")
            self._git(root, "add", evidence.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "record evidence",
            )
            runtime.write_bytes(b"MZ-after-freeze")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "current executable surface",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_retains_worktree_guards_after_evidence_commit(
        self,
    ) -> None:
        cases = (
            ("dirty", "tracked source is dirty"),
            ("staged-only", "tracked source is dirty"),
            ("unsafe-index", "unsafe index flags"),
            ("untracked-executable", "not in the legacy policy"),
        )
        for mutation, expected_error in cases:
            with self.subTest(mutation=mutation), TemporaryDirectory() as tmp:
                root = Path(tmp)
                self._repository(root)
                freeze = build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )
                inventory = build_final_entry_inventory(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                    freeze_manifest=freeze,
                    scheduler_records=[],
                )
                evidence = (
                    root / "research_state" / "control_plane" / "gate.json"
                )
                evidence.parent.mkdir(parents=True)
                evidence.write_text('{"verdict":"PASS"}', encoding="utf-8")
                self._git(root, "add", evidence.relative_to(root).as_posix())
                self._git(
                    root,
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "record evidence",
                )
                worker = root / "worker.py"
                if mutation == "dirty":
                    worker.write_text("print('dirty')\n", encoding="utf-8")
                elif mutation == "staged-only":
                    baseline = worker.read_bytes()
                    worker.write_text("print('staged')\n", encoding="utf-8")
                    self._git(root, "add", "worker.py")
                    worker.write_bytes(baseline)
                elif mutation == "unsafe-index":
                    self._git(
                        root,
                        "update-index",
                        "--assume-unchanged",
                        "worker.py",
                    )
                    worker.write_text("print('hidden')\n", encoding="utf-8")
                else:
                    unknown = root / "tools" / "unknown_after_gate.py"
                    unknown.parent.mkdir(parents=True, exist_ok=True)
                    unknown.write_text("raise SystemExit(1)\n", encoding="utf-8")

                with self.assertRaisesRegex(
                    UnstableInventoryError,
                    expected_error,
                ):
                    verify_current_git_inventory(
                        root,
                        freeze_manifest=freeze,
                        final_inventory=inventory,
                    )

    def test_tracked_source_change_during_capture_fails_closed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            policy_path = (
                root
                / "research_automation"
                / "control_plane"
                / "entry_policy.json"
            )
            worker = root / "worker.py"
            original_read_bytes = Path.read_bytes
            changed = False

            def mutate_after_policy_read(path: Path) -> bytes:
                nonlocal changed
                raw = original_read_bytes(path)
                if path == policy_path and not changed:
                    changed = True
                    worker.write_text("print('changed mid-capture')\n", encoding="utf-8")
                return raw

            with patch.object(Path, "read_bytes", mutate_after_policy_read):
                with self.assertRaisesRegex(
                    UnstableInventoryError,
                    "changed during source identity capture",
                ):
                    build_code_freeze_manifest(
                        root,
                        plan_version="V3.4.2-P1",
                        phase="P1",
                        attempt_id="p1-attempt-001",
                        identity_binding=self.identity,
                    )

    def test_staged_only_source_change_fails_closed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            worker = root / "worker.py"
            baseline = worker.read_bytes()
            worker.write_text("print('staged')\n", encoding="utf-8")
            self._git(root, "add", "worker.py")
            worker.write_bytes(baseline)

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "tracked source is dirty",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_assume_unchanged_cannot_hide_dirty_source(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            self._git(root, "update-index", "--assume-unchanged", "worker.py")
            (root / "worker.py").write_text("print('hidden dirty')\n", encoding="utf-8")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "unsafe index flags",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_tracked_symlink_mode_fails_closed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            target = root / "link-target.txt"
            target.write_text("worker.py\n", encoding="utf-8")
            object_id = self._git(root, "hash-object", "-w", "link-target.txt")
            self._git(
                root,
                "update-index",
                "--add",
                "--cacheinfo",
                "120000",
                object_id,
                "linked_worker.py",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "unsafe Git mode",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_final_inventory_v3_binds_git_source_identity(self) -> None:
        scheduler = {
            "task_path": r"\A股选股",
            "command": r"D:\workspace\a-share-quant-selector-main\run_select.bat",
            "task_xml_sha256": "d" * 64,
            "state": "Ready",
            "principal": "SYSTEM|ServiceAccount|HighestAvailable",
            "trigger": "Calendar|days_interval=1|enabled=true",
            "acl_summary": "owner=BUILTIN\\Administrators;sddl=test-only",
        }
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )

            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[scheduler],
            )

        self.assertEqual(
            inventory["schema_version"],
            "control_plane.entry_inventory.v3",
        )
        self.assertEqual(
            inventory["source_identity_sha256"],
            freeze["source_identity_sha256"],
        )

    def test_live_verifier_accepts_one_immutable_evidence_only_commit(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence = (
                root
                / "research_state"
                / "control_plane"
                / "p1"
                / "attempts"
                / "p1-attempt-001"
                / "gates"
                / "official_gate.json"
            )
            evidence.parent.mkdir(parents=True)
            evidence.write_text('{"verdict":"PASS"}', encoding="utf-8")
            self._git(root, "add", evidence.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "record gate evidence",
            )

            verify_current_git_inventory(
                root,
                freeze_manifest=freeze,
                final_inventory=inventory,
            )

    def test_live_verifier_accepts_multiple_immutable_evidence_only_commits(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence_root = root / "research_state" / "control_plane" / "p1"
            for name in ("task_report.json", "official_gate.json"):
                evidence = evidence_root / name
                evidence.parent.mkdir(parents=True, exist_ok=True)
                evidence.write_text(
                    canonical_json({"name": name, "status": "PASS"}),
                    encoding="utf-8",
                )
                self._git(root, "add", evidence.relative_to(root).as_posix())
                self._git(
                    root,
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    f"record {name}",
                )

            verify_current_git_inventory(
                root,
                freeze_manifest=freeze,
                final_inventory=inventory,
            )

    def test_live_verifier_rejects_committed_source_after_freeze(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            (root / "worker.py").write_text(
                "print('changed after freeze')\n",
                encoding="utf-8",
            )
            self._git(root, "add", "worker.py")
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "change source",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "immutable add-only evidence",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_committed_non_evidence_categories(self) -> None:
        cases = (
            ("source-add", "new_worker.py", None),
            ("source-modify", "worker.py", "print('baseline')\n"),
            ("test-add", "tests/test_new.py", None),
            ("test-modify", "tests/test_existing.py", "def test_old(): pass\n"),
            ("config-add", "config/new.json", None),
            ("config-modify", "config/existing.json", "{}\n"),
            ("executable-add", "run_new.bat", None),
            ("executable-modify", "run_select.bat", "@echo off\n"),
        )
        for name, relative, baseline_content in cases:
            with self.subTest(name=name), TemporaryDirectory() as tmp:
                root = Path(tmp)
                self._repository(root)
                target = root.joinpath(*relative.split("/"))
                if baseline_content is not None:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(baseline_content, encoding="utf-8")
                    self._git(root, "add", relative)
                    self._git(
                        root,
                        "-c",
                        "user.name=Control Plane Tests",
                        "-c",
                        "user.email=control-plane@example.invalid",
                        "commit",
                        "--quiet",
                        "-m",
                        f"add baseline {name}",
                    )
                freeze = build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )
                inventory = build_final_entry_inventory(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                    freeze_manifest=freeze,
                    scheduler_records=[],
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(f"changed {name}\n", encoding="utf-8")
                self._git(root, "add", relative)
                self._git(
                    root,
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    f"change {name}",
                )

                with self.assertRaises(UnstableInventoryError):
                    verify_current_git_inventory(
                        root,
                        freeze_manifest=freeze,
                        final_inventory=inventory,
                    )

    def test_live_verifier_rejects_executable_mode_evidence_json(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence = root / "research_state" / "control_plane" / "gate.json"
            evidence.parent.mkdir(parents=True)
            evidence.write_text('{"verdict":"PASS"}', encoding="utf-8")
            relative = evidence.relative_to(root).as_posix()
            self._git(root, "add", relative)
            self._git(root, "update-index", "--chmod=+x", relative)
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add executable evidence",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "evidence mode",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_post_add_mode_or_type_change(self) -> None:
        for mutation in ("executable-mode", "symlink-type"):
            with self.subTest(mutation=mutation), TemporaryDirectory() as tmp:
                root = Path(tmp)
                self._repository(root)
                freeze = build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )
                inventory = build_final_entry_inventory(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                    freeze_manifest=freeze,
                    scheduler_records=[],
                )
                relative = "research_state/control_plane/gate.json"
                evidence = root.joinpath(*relative.split("/"))
                evidence.parent.mkdir(parents=True)
                evidence.write_text('{"verdict":"PASS"}', encoding="utf-8")
                self._git(root, "add", relative)
                self._git(
                    root,
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "add regular evidence",
                )
                if mutation == "executable-mode":
                    self._git(root, "update-index", "--chmod=+x", relative)
                else:
                    object_id = self._git(
                        root,
                        "hash-object",
                        "-w",
                        "--stdin",
                    )
                    self._git(
                        root,
                        "update-index",
                        "--cacheinfo",
                        "120000",
                        object_id,
                        relative,
                    )
                self._git(
                    root,
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    f"change evidence {mutation}",
                )

                with self.assertRaises(UnstableInventoryError):
                    verify_current_git_inventory(
                        root,
                        freeze_manifest=freeze,
                        final_inventory=inventory,
                    )

    def test_live_verifier_rejects_copied_post_freeze_evidence(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence_root = root / "research_state" / "control_plane" / "p1"
            first = evidence_root / "first.json"
            first.parent.mkdir(parents=True)
            first.write_text('{"receipt":"same"}', encoding="utf-8")
            self._git(root, "add", first.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add first evidence",
            )
            second = evidence_root / "second.json"
            second.write_bytes(first.read_bytes())
            self._git(root, "add", second.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "copy evidence",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "reuses an existing evidence blob",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_accepts_similar_but_distinct_refresh_evidence(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            evidence_root = root / "research_state" / "control_plane" / "p0"
            prior = evidence_root / "attempt-001.json"
            prior.parent.mkdir(parents=True)
            prior.write_bytes(
                canonical_json(
                    {
                        "attempt_id": "p0-attempt-001",
                        "entries": ["bounded-entry"] * 100,
                        "verdict": "PASS",
                    }
                ).encode("utf-8")
            )
            self._git(root, "add", prior.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add prior refresh evidence",
            )
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P0R2",
                phase="P0",
                attempt_id="p0-attempt-002",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P0R2",
                phase="P0",
                attempt_id="p0-attempt-002",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            refreshed = evidence_root / "attempt-002.json"
            refreshed.write_bytes(
                canonical_json(
                    {
                        "attempt_id": "p0-attempt-002",
                        "entries": ["bounded-entry"] * 100,
                        "verdict": "PASS",
                    }
                ).encode("utf-8")
            )
            self.assertNotEqual(
                self._git(root, "hash-object", prior.relative_to(root).as_posix()),
                self._git(
                    root,
                    "hash-object",
                    refreshed.relative_to(root).as_posix(),
                ),
            )
            self._git(root, "add", refreshed.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add distinct refresh evidence",
            )

            verify_current_git_inventory(
                root,
                freeze_manifest=freeze,
                final_inventory=inventory,
            )

    def test_live_verifier_rejects_exact_blob_reuse_without_copy_heuristics(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            frozen = root / "research_state" / "control_plane" / "frozen.json"
            frozen.parent.mkdir(parents=True)
            frozen.write_text('{"receipt":"same"}', encoding="utf-8")
            self._git(root, "add", frozen.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add frozen evidence",
            )
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            copied = frozen.with_name("copied.json")
            copied.write_bytes(frozen.read_bytes())
            self._git(root, "add", copied.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "reuse frozen blob",
            )
            original_run_git = inventory_module._run_git

            def disable_similarity(repository_root, *arguments):
                if arguments[0] == "diff-tree" and "-C" in arguments:
                    arguments = tuple(
                        argument
                        for argument in arguments
                        if argument not in {"-C", "--find-copies-harder"}
                    )
                    arguments = (*arguments[:4], "--no-renames", *arguments[4:])
                return original_run_git(repository_root, *arguments)

            with patch.object(
                inventory_module,
                "_run_git",
                disable_similarity,
            ), self.assertRaisesRegex(
                UnstableInventoryError,
                "reuses an existing evidence blob",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_allows_frozen_non_json_control_plane_history(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            historical = (
                root
                / "research_state"
                / "control_plane"
                / "p1"
                / "evidence"
                / "historical_check.ps1"
            )
            historical.parent.mkdir(parents=True)
            historical.write_text("exit 0\n", encoding="utf-8")
            self._git(root, "add", historical.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add historical control-plane script",
            )
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence = historical.with_name("new_gate.json")
            evidence.write_text('{"verdict":"PASS"}', encoding="utf-8")
            self._git(root, "add", evidence.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add new immutable evidence",
            )

            verify_current_git_inventory(
                root,
                freeze_manifest=freeze,
                final_inventory=inventory,
            )

    def test_live_verifier_rejects_add_then_modify_evidence(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence = root / "research_state" / "control_plane" / "gate.json"
            evidence.parent.mkdir(parents=True)
            relative = evidence.relative_to(root).as_posix()
            for content, message in (
                ('{"verdict":"PASS"}', "add evidence"),
                ('{"verdict":"FAIL"}', "modify evidence"),
            ):
                evidence.write_text(content, encoding="utf-8")
                self._git(root, "add", relative)
                self._git(
                    root,
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    message,
                )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "immutable add-only evidence",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_post_freeze_merge_commit(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            branch = self._git(root, "branch", "--show-current")
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence_root = root / "research_state" / "control_plane"
            self._git(root, "checkout", "--quiet", "-b", "evidence-side")
            side = evidence_root / "side.json"
            side.parent.mkdir(parents=True)
            side.write_text('{"side":true}', encoding="utf-8")
            self._git(root, "add", side.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add side evidence",
            )
            self._git(root, "checkout", "--quiet", branch)
            main = evidence_root / "main.json"
            main.parent.mkdir(parents=True)
            main.write_text('{"main":true}', encoding="utf-8")
            self._git(root, "add", main.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add main evidence",
            )
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "merge",
                "--quiet",
                "--no-ff",
                "evidence-side",
                "-m",
                "merge evidence",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "linear single-parent descendant",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_non_descendant_commit(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            replacement = subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit-tree",
                    str(freeze["git_tree"]),
                ],
                cwd=root,
                input="replacement history\n",
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
            ).stdout.strip()
            self._git(root, "checkout", "--quiet", "--detach", replacement)

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "Git identity",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_deleted_or_renamed_evidence(self) -> None:
        for operation in ("delete", "rename"):
            with self.subTest(operation=operation), TemporaryDirectory() as tmp:
                root = Path(tmp)
                self._repository(root)
                freeze = build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )
                inventory = build_final_entry_inventory(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                    freeze_manifest=freeze,
                    scheduler_records=[],
                )
                evidence = (
                    root / "research_state" / "control_plane" / "gate.json"
                )
                evidence.parent.mkdir(parents=True)
                evidence.write_text('{"verdict":"PASS"}', encoding="utf-8")
                relative = evidence.relative_to(root).as_posix()
                self._git(root, "add", relative)
                self._git(
                    root,
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "add evidence",
                )
                if operation == "delete":
                    evidence.unlink()
                    self._git(root, "add", "--update", relative)
                else:
                    renamed = evidence.with_name("renamed.json")
                    self._git(
                        root,
                        "mv",
                        relative,
                        renamed.relative_to(root).as_posix(),
                    )
                self._git(
                    root,
                    "-c",
                    "user.name=Control Plane Tests",
                    "-c",
                    "user.email=control-plane@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    f"{operation} evidence",
                )

                expected_error = (
                    "immutable add-only evidence"
                    if operation == "delete"
                    else "copied or renamed"
                )
                with self.assertRaisesRegex(
                    UnstableInventoryError,
                    expected_error,
                ):
                    verify_current_git_inventory(
                        root,
                        freeze_manifest=freeze,
                        final_inventory=inventory,
                    )

    def test_live_verifier_rejects_symlink_type_evidence(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            object_id = self._git(
                root,
                "hash-object",
                "-w",
                "--stdin",
            )
            self._git(
                root,
                "update-index",
                "--add",
                "--cacheinfo",
                "120000",
                object_id,
                "research_state/control_plane/gate.json",
            )
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add symlink evidence",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "unsafe Git mode",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_noncanonical_json_suffix(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence = root / "research_state" / "control_plane" / "gate.JSON"
            evidence.parent.mkdir(parents=True)
            evidence.write_text('{"verdict":"PASS"}', encoding="utf-8")
            self._git(root, "add", evidence.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add noncanonical evidence",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "outside immutable Gate evidence",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_non_json_evidence_bytes(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence = root / "research_state" / "control_plane" / "gate.json"
            evidence.parent.mkdir(parents=True)
            evidence.write_bytes(b"not-json")
            self._git(root, "add", evidence.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add invalid evidence bytes",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "not canonical JSON",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_noncanonical_json_evidence_bytes(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence = root / "research_state" / "control_plane" / "gate.json"
            evidence.parent.mkdir(parents=True)
            evidence.write_bytes(b'{"b":2, "a":1}')
            self._git(root, "add", evidence.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add noncanonical JSON evidence",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "not canonical JSON",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_evidence_over_the_byte_limit(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence = root / "research_state" / "control_plane" / "gate.json"
            evidence.parent.mkdir(parents=True)
            evidence.write_bytes(b'{"value":"bounded"}')
            self._git(root, "add", evidence.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add oversized evidence",
            )

            with patch.object(
                inventory_module,
                "_MAX_IMMUTABLE_GATE_EVIDENCE_BYTES",
                8,
            ), self.assertRaisesRegex(
                UnstableInventoryError,
                "exceeds its byte limit",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_noncanonical_evidence_path_case(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            evidence = root / "research_state" / "control_plane" / "Gate.json"
            evidence.parent.mkdir(parents=True)
            evidence.write_text('{"verdict":"PASS"}', encoding="utf-8")
            self._git(root, "add", evidence.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add noncanonical evidence path",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "Windows path",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_rejects_case_alias_of_frozen_evidence(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            frozen_evidence = (
                root / "research_state" / "control_plane" / "p1" / "gate.json"
            )
            frozen_evidence.parent.mkdir(parents=True)
            frozen_evidence.write_text('{"version":1}', encoding="utf-8")
            frozen_relative = frozen_evidence.relative_to(root).as_posix()
            self._git(root, "add", frozen_relative)
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add frozen evidence",
            )
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            frozen_evidence.write_text('{"version":2}', encoding="utf-8")
            object_id = self._git(root, "hash-object", "-w", frozen_relative)
            frozen_evidence.write_text('{"version":1}', encoding="utf-8")
            self._git(
                root,
                "update-index",
                "--add",
                "--cacheinfo",
                "100644",
                object_id,
                "research_state/control_plane/p1/Gate.json",
            )
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add case alias evidence",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "tracked source is dirty",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=inventory,
                )

    def test_live_verifier_binds_frozen_tree_to_frozen_commit(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[],
            )
            forged_freeze = {**freeze, "git_tree": "0" * 40}
            forged_source_identity = {
                "schema_version": "control_plane.git_source_identity.v1",
                "git_commit": forged_freeze["git_commit"],
                "git_tree": forged_freeze["git_tree"],
                "active_tracked_dirty_paths": [],
                "untracked_executables": forged_freeze[
                    "untracked_executables"
                ],
                "runtime_dependencies": forged_freeze[
                    "runtime_dependencies"
                ],
                "legacy_policy_path": forged_freeze["legacy_policy_path"],
                "legacy_policy_sha256": forged_freeze[
                    "legacy_policy_sha256"
                ],
                "legacy_quarantine_sha256": forged_freeze[
                    "legacy_quarantine_sha256"
                ],
            }
            forged_freeze["source_identity_sha256"] = canonical_sha256(
                forged_source_identity
            )
            forged_freeze["freeze_payload_sha256"] = canonical_sha256(
                {
                    key: value
                    for key, value in forged_freeze.items()
                    if key != "freeze_payload_sha256"
                }
            )
            forged_inventory = {
                **inventory,
                "source_identity_sha256": forged_freeze[
                    "source_identity_sha256"
                ],
            }
            forged_inventory["inventory_payload_sha256"] = canonical_sha256(
                {
                    key: value
                    for key, value in forged_inventory.items()
                    if key != "inventory_payload_sha256"
                }
            )
            evidence = root / "research_state" / "control_plane" / "gate.json"
            evidence.parent.mkdir(parents=True)
            evidence.write_text('{"verdict":"PASS"}', encoding="utf-8")
            self._git(root, "add", evidence.relative_to(root).as_posix())
            self._git(
                root,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "add evidence after forged freeze",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "frozen Git tree",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=forged_freeze,
                    final_inventory=forged_inventory,
                )

    def test_repository_selection_environment_cannot_redirect_git_identity(self) -> None:
        with TemporaryDirectory() as first_tmp, TemporaryDirectory() as second_tmp:
            root = Path(first_tmp)
            other = Path(second_tmp)
            expected_commit, _ = self._repository(root)
            self._repository(other)
            (other / "other.py").write_text("print('other')\n", encoding="utf-8")
            self._git(other, "add", "other.py")
            self._git(
                other,
                "-c",
                "user.name=Control Plane Tests",
                "-c",
                "user.email=control-plane@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "other fixture",
            )

            with patch.dict(
                os.environ,
                {
                    "GIT_DIR": str(other / ".git"),
                    "GIT_WORK_TREE": str(other),
                    "GIT_INDEX_FILE": str(other / ".git" / "index"),
                },
            ):
                manifest = build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

        self.assertEqual(manifest["git_commit"], expected_commit)

    @unittest.skipUnless(os.name == "nt", "Windows executable search regression")
    def test_path_cannot_redirect_the_git_executable(self) -> None:
        with TemporaryDirectory() as tmp, TemporaryDirectory() as attacker_tmp:
            root = Path(tmp)
            expected_commit, _ = self._repository(root)
            attacker = Path(attacker_tmp)
            fake_git = attacker / "git.exe"
            command_interpreter = os.environ.get("COMSPEC")
            self.assertIsNotNone(command_interpreter)
            shutil.copy2(str(command_interpreter), fake_git)
            hostile_path = str(attacker) + os.pathsep + os.environ.get("PATH", "")

            with patch.dict(os.environ, {"PATH": hostile_path}):
                manifest = build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

        self.assertEqual(manifest["git_commit"], expected_commit)

    @unittest.skipUnless(os.name == "nt", "Windows executable search regression")
    def test_repository_local_executable_cannot_impersonate_git(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            command_interpreter = os.environ.get("COMSPEC")
            self.assertIsNotNone(command_interpreter)
            shutil.copy2(str(command_interpreter), root / "git.exe")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "not in the legacy policy",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    @unittest.skipUnless(os.name == "nt", "Windows executable search regression")
    def test_pre_import_path_poisoning_cannot_select_git(self) -> None:
        with TemporaryDirectory() as tmp, TemporaryDirectory() as attacker_tmp:
            root = Path(tmp)
            expected_commit, _ = self._repository(root)
            attacker = Path(attacker_tmp)
            command_interpreter = os.environ.get("COMSPEC")
            self.assertIsNotNone(command_interpreter)
            shutil.copy2(str(command_interpreter), attacker / "git.exe")
            environment = os.environ.copy()
            environment["PATH"] = (
                str(attacker) + os.pathsep + environment.get("PATH", "")
            )
            script = (
                "import sys\n"
                "from research_automation.control_plane.inventory import "
                "build_code_freeze_manifest\n"
                "identity={'plan_hash':'a'*64,'scope_hash':'b'*64,"
                "'instruction_policy_hash':'c'*64}\n"
                "manifest=build_code_freeze_manifest(sys.argv[1],"
                "plan_version='V3.4.2-P1',phase='P1',"
                "attempt_id='p1-attempt-001',identity_binding=identity)\n"
                "print(manifest['git_commit'])\n"
            )
            completed = subprocess.run(
                [sys.executable, "-c", script, str(root)],
                cwd=Path(__file__).resolve().parents[1],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=30,
            )

        self.assertEqual(completed.stdout.strip(), expected_commit)

    def test_git_process_never_uses_the_repository_as_working_directory(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            original_run = subprocess.run
            observed: list[tuple[list[str], Path]] = []

            def capture_run(*args, **kwargs):
                command = list(args[0])
                if Path(command[0]).name.casefold() == "git.exe":
                    observed.append((command, Path(kwargs["cwd"]).resolve()))
                return original_run(*args, **kwargs)

            with patch.object(inventory_module.subprocess, "run", capture_run):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

        self.assertTrue(observed)
        for command, process_cwd in observed:
            self.assertNotEqual(process_cwd, root.resolve())
            self.assertIn("-C", command)
            self.assertEqual(command[command.index("-C") + 1], str(root.resolve()))

    def test_git_runtime_closure_must_match_its_deployment_lock(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)

            with patch.object(
                inventory_module,
                "_TRUSTED_GIT_RUNTIME_CLOSURE_SHA256",
                "0" * 64,
            ), patch.object(
                inventory_module,
                "_TRUSTED_GIT_RUNTIME_IDENTITIES",
                None,
            ), self.assertRaisesRegex(
                UnstableInventoryError,
                "runtime closure differs from its lock",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_git_runtime_closure_rejects_a_mutated_dll(self) -> None:
        with TemporaryDirectory() as tmp, TemporaryDirectory() as runtime_tmp:
            root = Path(tmp)
            self._repository(root)
            runtime_directory = Path(runtime_tmp) / "bin"
            self._copy_git_runtime(runtime_directory)
            expected_closure = self._git_runtime_closure_sha256(
                runtime_directory
            )
            dll = next(runtime_directory.glob("*.dll"))
            dll.write_bytes(dll.read_bytes() + b"mutated")

            with patch.multiple(
                inventory_module,
                _TRUSTED_GIT_EXECUTABLE=runtime_directory / "git.exe",
                _TRUSTED_GIT_CANONICAL_EXECUTABLE=runtime_directory / "git.exe",
                _TRUSTED_GIT_RUNTIME_CLOSURE_SHA256=expected_closure,
                _TRUSTED_GIT_RUNTIME_IDENTITIES=None,
            ), self.assertRaisesRegex(
                UnstableInventoryError,
                "runtime closure differs from its lock",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_git_runtime_closure_rejects_an_added_dll(self) -> None:
        with TemporaryDirectory() as tmp, TemporaryDirectory() as runtime_tmp:
            root = Path(tmp)
            self._repository(root)
            runtime_directory = Path(runtime_tmp) / "bin"
            self._copy_git_runtime(runtime_directory)
            expected_closure = self._git_runtime_closure_sha256(
                runtime_directory
            )
            (runtime_directory / "injected.dll").write_bytes(b"injected")

            with patch.multiple(
                inventory_module,
                _TRUSTED_GIT_EXECUTABLE=runtime_directory / "git.exe",
                _TRUSTED_GIT_CANONICAL_EXECUTABLE=runtime_directory / "git.exe",
                _TRUSTED_GIT_RUNTIME_CLOSURE_SHA256=expected_closure,
                _TRUSTED_GIT_RUNTIME_IDENTITIES=None,
            ), self.assertRaisesRegex(
                UnstableInventoryError,
                "runtime closure differs from its lock",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    @unittest.skipUnless(os.name == "nt", "Windows reparse regression")
    def test_git_runtime_rejects_a_reparse_alias_before_resolution(self) -> None:
        with TemporaryDirectory() as tmp, TemporaryDirectory() as alias_tmp:
            root = Path(tmp)
            self._repository(root)
            alias = Path(alias_tmp) / "trusted-bin"
            command_interpreter = os.environ.get("COMSPEC")
            self.assertIsNotNone(command_interpreter)
            subprocess.run(
                [
                    str(command_interpreter),
                    "/d",
                    "/c",
                    "mklink",
                    "/J",
                    str(alias),
                    str(inventory_module._TRUSTED_GIT_EXECUTABLE.parent),
                ],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
            )

            with patch.object(
                inventory_module,
                "_TRUSTED_GIT_EXECUTABLE",
                alias / "git.exe",
            ), patch.object(
                inventory_module,
                "_TRUSTED_GIT_RUNTIME_IDENTITIES",
                None,
            ), self.assertRaisesRegex(
                UnstableInventoryError,
                "path contains a reparse point",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    @unittest.skipUnless(os.name == "nt", "Windows reparse regression")
    def test_git_runtime_rejects_a_reparse_dll_candidate(self) -> None:
        with TemporaryDirectory() as tmp, TemporaryDirectory() as runtime_tmp:
            root = Path(tmp)
            self._repository(root)
            runtime_directory = Path(runtime_tmp) / "bin"
            runtime_directory.mkdir()
            (runtime_directory / "git.exe").write_bytes(b"not executed")
            outside = Path(runtime_tmp) / "outside"
            outside.mkdir()
            injected = runtime_directory / "injected.dll"
            command_interpreter = os.environ.get("COMSPEC")
            self.assertIsNotNone(command_interpreter)
            subprocess.run(
                [
                    str(command_interpreter),
                    "/d",
                    "/c",
                    "mklink",
                    "/J",
                    str(injected),
                    str(outside),
                ],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
            )

            with patch.multiple(
                inventory_module,
                _TRUSTED_GIT_EXECUTABLE=runtime_directory / "git.exe",
                _TRUSTED_GIT_CANONICAL_EXECUTABLE=runtime_directory / "git.exe",
                _TRUSTED_GIT_RUNTIME_IDENTITIES=None,
            ), self.assertRaisesRegex(
                UnstableInventoryError,
                "runtime closure contains an unsafe file",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_git_runtime_rejects_dll_drift_during_invocation(self) -> None:
        with TemporaryDirectory() as tmp, TemporaryDirectory() as runtime_tmp:
            root = Path(tmp)
            self._repository(root)
            runtime_directory = Path(runtime_tmp) / "bin"
            self._copy_git_runtime(runtime_directory)
            expected_closure = self._git_runtime_closure_sha256(
                runtime_directory
            )
            dll = next(runtime_directory.glob("*.dll"))
            original_run = subprocess.run
            changed = False

            def mutate_after_git(*args, **kwargs):
                nonlocal changed
                completed = original_run(*args, **kwargs)
                if not changed and Path(args[0][0]).name.casefold() == "git.exe":
                    dll.write_bytes(dll.read_bytes() + b"drift")
                    changed = True
                return completed

            with patch.multiple(
                inventory_module,
                _TRUSTED_GIT_EXECUTABLE=runtime_directory / "git.exe",
                _TRUSTED_GIT_CANONICAL_EXECUTABLE=runtime_directory / "git.exe",
                _TRUSTED_GIT_RUNTIME_CLOSURE_SHA256=expected_closure,
                _TRUSTED_GIT_RUNTIME_IDENTITIES=None,
            ), patch.object(
                inventory_module.subprocess,
                "run",
                mutate_after_git,
            ), self.assertRaisesRegex(
                UnstableInventoryError,
                "runtime closure changed during use",
            ):
                build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

        self.assertTrue(changed)

    def test_repository_root_must_be_the_git_toplevel(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "Git top-level",
            ):
                build_code_freeze_manifest(
                    root / "research_automation",
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

    def test_data_tree_executable_bytes_are_outside_source_identity(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            hidden = root / "data" / "hidden.py"
            hidden.parent.mkdir()
            hidden.write_text("raise RuntimeError('data')\n", encoding="utf-8")
            original_read_bytes = Path.read_bytes

            def reject_data_reads(path: Path) -> bytes:
                if path == hidden:
                    raise AssertionError("source identity read data bytes")
                return original_read_bytes(path)

            with patch.object(Path, "read_bytes", reject_data_reads):
                manifest = build_code_freeze_manifest(
                    root,
                    plan_version="V3.4.2-P1",
                    phase="P1",
                    attempt_id="p1-attempt-001",
                    identity_binding=self.identity,
                )

        self.assertEqual(manifest["untracked_executables"], [])

    def test_detached_head_is_allowed_when_commit_and_tree_are_stable(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected_commit, expected_tree = self._repository(root)
            self._git(root, "checkout", "--quiet", "--detach", expected_commit)

            manifest = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )

        self.assertEqual(manifest["git_commit"], expected_commit)
        self.assertEqual(manifest["git_tree"], expected_tree)

    def test_non_git_and_unborn_repositories_fail_closed(self) -> None:
        for initialize_git in (False, True):
            with self.subTest(initialize_git=initialize_git):
                with TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    if initialize_git:
                        self._git(root, "init", "--quiet")

                    with self.assertRaisesRegex(
                        UnstableInventoryError,
                        "Git identity",
                    ):
                        build_code_freeze_manifest(
                            root,
                            plan_version="V3.4.2-P1",
                            phase="P1",
                            attempt_id="p1-attempt-001",
                            identity_binding=self.identity,
                        )

    def test_inventory_v3_cannot_omit_a_quarantined_executable(self) -> None:
        legacy_path = "research/legacy_runner.py"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root, legacy_sources={legacy_path: b"pass\n"})
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[{"task_path": r"\A股选股"}],
            )
            entries = [
                entry
                for entry in inventory["entries"]
                if entry["entry_id"] != f"file:{legacy_path}"
            ]
            tampered = {
                **inventory,
                "entries": entries,
                "entry_count": len(entries),
            }
            tampered["inventory_payload_sha256"] = canonical_sha256(
                {
                    key: value
                    for key, value in tampered.items()
                    if key != "inventory_payload_sha256"
                }
            )

            with self.assertRaisesRegex(
                ArtifactSemanticError,
                "quarantined executable",
            ):
                validate_final_inventory(
                    canonical_json(tampered).encode("utf-8"),
                    expected_plan_version="V3.4.2-P1",
                    expected_phase="P1",
                    expected_attempt_id="p1-attempt-001",
                    expected_identity=self.identity,
                    freeze_manifest=freeze,
                )

    def test_live_verifier_rejects_omitted_tracked_inventory_entry(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = build_code_freeze_manifest(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
            )
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P1",
                phase="P1",
                attempt_id="p1-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[{"task_path": r"\A股选股"}],
            )
            entries = [
                entry
                for entry in inventory["entries"]
                if entry["entry_id"] != "file:worker.py"
            ]
            tampered = {
                **inventory,
                "entries": entries,
                "entry_count": len(entries),
            }
            tampered["inventory_payload_sha256"] = canonical_sha256(
                {
                    key: value
                    for key, value in tampered.items()
                    if key != "inventory_payload_sha256"
                }
            )
            validate_final_inventory(
                canonical_json(tampered).encode("utf-8"),
                expected_plan_version="V3.4.2-P1",
                expected_phase="P1",
                expected_attempt_id="p1-attempt-001",
                expected_identity=self.identity,
                freeze_manifest=freeze,
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "current bounded entry surface",
            ):
                verify_current_git_inventory(
                    root,
                    freeze_manifest=freeze,
                    final_inventory=tampered,
                )


if __name__ == "__main__":
    unittest.main()
