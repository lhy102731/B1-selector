from __future__ import annotations

import json
import hashlib
import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

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
