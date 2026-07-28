from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier
import unittest
from unittest.mock import patch

import research_automation.control_plane as control_plane
from research_automation.control_plane import entry_guard as entry_guard_module
from research_automation.control_plane.contracts import (
    ACTOR_TYPES,
    Actor,
    IdentityBinding,
    Phase,
    SideEffect,
    SideEffectLease,
)
from research_automation.control_plane.entry_guard import (
    AuthorizationError,
    EntryInventory,
    EntryNotDeclaredError,
    EntryRecord,
    ReviewedEntryPolicy,
    EntryGuard,
    PhaseAuthorizer,
)
from research_automation.control_plane.inventory import (
    UnstableInventoryError,
    build_code_freeze_manifest,
    build_final_entry_inventory,
    unavailable_scheduler_sha256,
)


class ControlPlanePublicApiTests(unittest.TestCase):
    def test_package_exports_capability_chain_without_legacy_authority(self) -> None:
        expected = {
            "Actor",
            "IdentityBinding",
            "Phase",
            "PhaseGrant",
            "SideEffect",
            "SideEffectLease",
            "TaskTicket",
            "TicketSnapshot",
            "begin_side_effect",
            "claim_phase",
            "finish_side_effect",
            "issue_task_ticket",
            "mark_side_effect_in_doubt",
        }
        retired = {"AuthorizationGrant", "EntryGuard", "PhaseAuthorizer"}

        self.assertTrue(expected.issubset(set(control_plane.__all__)))
        self.assertTrue(all(hasattr(control_plane, name) for name in expected))
        self.assertTrue(retired.isdisjoint(set(control_plane.__all__)))
        self.assertTrue(all(not hasattr(control_plane, name) for name in retired))


class EntryInventoryTests(unittest.TestCase):
    @staticmethod
    def _identity_binding() -> IdentityBinding:
        return IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )

    def _assert_against_reviewed_policy(
        self,
        actual_records: tuple[EntryRecord, ...],
        declared_records: tuple[EntryRecord, ...],
    ) -> None:
        payload = {
            "schema_version": "control_plane.entry_policy.v1",
            "review_state": "APPROVED",
            "plan_hash": "a" * 64,
            "scope_hash": "b" * 64,
            "policy_hash": "c" * 64,
            "entries": EntryInventory._manifest_payload(
                declared_records
            )["entries"],
        }
        with TemporaryDirectory() as tmp:
            policy_path = Path(tmp) / "entry_policy.json"
            policy_path.write_text(
                json.dumps(payload, sort_keys=True),
                encoding="utf-8",
            )
            EntryInventory.assert_declared(
                actual_records,
                policy_path,
                identity_binding=self._identity_binding(),
            )

    def test_scan_covers_bounded_executable_surface_without_root_data(self) -> None:
        root = Path(__file__).resolve().parents[1]
        records = EntryInventory.scan(root)
        paths = {record.path for record in records}

        self.assertIn("run_research.py", paths)
        self.assertIn("tools/data/backfill_fund_flow.py", paths)
        self.assertIn("apps/web_server.py", paths)
        self.assertIn("research_automation/control_plane/entry_guard.py", paths)
        self.assertIn("ag2_research/orchestrator.py", paths)
        self.assertIn("research/brick_ag2_kbase_sqnav_autorun.py", paths)
        self.assertIn("l2/__main__.py", paths)
        self.assertIn("strategy/unified_b1_strategy.py", paths)
        self.assertIn("utils/s1_filter.py", paths)
        self.assertIn("tests/test_control_plane_entry_guard.py", paths)
        self.assertNotIn("data", paths)
        self.assertTrue(all(not path.startswith("data/") for path in paths))
        self.assertTrue(
            all(
                record.content_sha256 is not None
                and len(record.content_sha256) == 64
                for record in records
                if record.source == "filesystem_inventory"
            )
        )
        entry_ids = {record.entry_id for record in records}
        self.assertIn(
            "callable:research_automation.autonomous_runner:AutonomousRunnerV1.run",
            entry_ids,
        )
        self.assertIn(
            "callable:research_automation.kbase_ag2_full_cycle:run_kbase_ag2_full_cycle",
            entry_ids,
        )
        self.assertIn(
            "callable:research_automation.discovery_execution_bridge:execute_plan",
            entry_ids,
        )

    def test_scan_includes_the_exact_ths_bridge_runtime_surface(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            bridge_module = root / "utils" / "ths_yuanhang_bridge.py"
            bridge_module.parent.mkdir(parents=True)
            bridge_module.write_text("pass\n", encoding="utf-8")
            runtime_files = {
                "tools/ths_yuanhang_bridge/build.ps1": "Write-Output built\n",
                "tools/ths_yuanhang_bridge/YuanhangBridge.cs": "class Bridge {}\n",
                "tools/ths_yuanhang_bridge/YuanhangBridge.dll": b"MZ-test-runtime",
                "tools/ths_yuanhang_bridge/YuanhangBridge.runtimeconfig.json": "{}\n",
                "tools/ths_yuanhang_bridge/workspace/datacenter.xml": "<root />\n",
                "tools/ths_yuanhang_bridge/workspace/DNSTest.xml": "<root />\n",
            }
            for relative, content in runtime_files.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                if isinstance(content, bytes):
                    path.write_bytes(content)
                else:
                    path.write_text(content, encoding="utf-8")

            records = EntryInventory.scan(
                root,
                include_required_import_seams=False,
            )

        by_path = {record.path: record for record in records}
        self.assertEqual(set(runtime_files), set(runtime_files) & set(by_path))
        self.assertTrue(
            all(by_path[path].disposition == "PRODUCTION_DAILY" for path in runtime_files)
        )

    def test_scan_classifies_the_observed_ths_daily_chain_without_promoting_run_select1(self) -> None:
        production_daily_paths = {
            "tools/update_today_ths.py",
            "tools/update_ths_market_assets.py",
            "tools/backfill_daily_pcf_baostock.py",
            "build_indicators_cache.py",
            "tools/select_etf_candidates.py",
            "backtest_brick_v2.py",
            "filter_exec_reduce.py",
            "tools/ths_yuanhang_bridge/build.ps1",
        }
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for relative in production_daily_paths | {"run_select1.bat"}:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("pass\n", encoding="utf-8")

            records = EntryInventory.scan(
                root,
                include_required_import_seams=False,
            )

        disposition_by_path = {record.path: record.disposition for record in records}
        self.assertEqual(
            {path: "PRODUCTION_DAILY" for path in production_daily_paths},
            {path: disposition_by_path[path] for path in production_daily_paths},
        )
        self.assertEqual(disposition_by_path["run_select1.bat"], "ADMIN_ONLY")

    def test_scan_rejects_a_missing_inventory_root(self) -> None:
        with TemporaryDirectory() as tmp:
            missing_root = Path(tmp) / "missing"

            with self.assertRaisesRegex(
                EntryNotDeclaredError,
                "inventory root",
            ):
                EntryInventory.scan(missing_root)

    def test_scan_fails_closed_when_a_source_directory_is_unreadable(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "research_automation").mkdir()

            def unreadable_walk(*args, onerror=None, **kwargs):
                if onerror is not None:
                    onerror(PermissionError("source directory denied"))
                return ()

            with patch.object(
                entry_guard_module.os,
                "walk",
                side_effect=unreadable_walk,
            ):
                with self.assertRaisesRegex(
                    EntryNotDeclaredError,
                    "inventory scan failed",
                ):
                    EntryInventory.scan(root)

    def test_scan_fails_closed_when_source_hashing_is_denied(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "main.py").write_text("pass\n", encoding="utf-8")

            with patch.object(
                Path,
                "read_bytes",
                side_effect=PermissionError("source hashing denied"),
            ):
                with self.assertRaisesRegex(
                    EntryNotDeclaredError,
                    "hash executable source",
                ):
                    EntryInventory.scan(
                        root,
                        include_required_import_seams=False,
                    )

    def test_complete_scan_rejects_a_missing_required_import_seam(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "main.py").write_text("pass\n", encoding="utf-8")

            with self.assertRaisesRegex(
                EntryNotDeclaredError,
                "required import seam",
            ):
                EntryInventory.scan(root)

    def test_scan_excludes_generated_directories_but_keeps_tools_data(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "tools" / "data").mkdir(parents=True)
            (root / "tools" / "_output").mkdir(parents=True)
            (root / "research_automation" / "__pycache__").mkdir(parents=True)
            (root / "research_automation" / "artifacts").mkdir(parents=True)
            (root / "research_automation" / "_output").mkdir(parents=True)
            (root / "data").mkdir()
            (root / "main.py").write_text("pass\n", encoding="utf-8")
            (root / "tools" / "data" / "included.py").write_text(
                "pass\n",
                encoding="utf-8",
            )
            (root / "tools" / "_output" / "included.py").write_text(
                "pass\n",
                encoding="utf-8",
            )
            (root / "research_automation" / "__pycache__" / "ignored.py").write_text(
                "pass\n",
                encoding="utf-8",
            )
            (root / "research_automation" / "artifacts" / "ignored.py").write_text(
                "pass\n",
                encoding="utf-8",
            )
            (root / "research_automation" / "_output" / "ignored.py").write_text(
                "pass\n",
                encoding="utf-8",
            )
            (root / "data" / "ignored.py").write_text("pass\n", encoding="utf-8")

            paths = {
                record.path
                for record in EntryInventory.scan(
                    root,
                    include_required_import_seams=False,
                )
            }

        self.assertIn("main.py", paths)
        self.assertIn("tools/data/included.py", paths)
        self.assertNotIn("tools/_output/included.py", paths)
        self.assertNotIn("data/ignored.py", paths)
        self.assertNotIn("research_automation/__pycache__/ignored.py", paths)
        self.assertNotIn("research_automation/artifacts/ignored.py", paths)
        self.assertNotIn("research_automation/_output/ignored.py", paths)

    def test_scan_rejects_symlinked_files_or_directories(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            bounded = root / "research_automation"
            outside = root / "outside"
            bounded.mkdir()
            outside.mkdir()
            (outside / "escaped.py").write_text("pass\n", encoding="utf-8")
            (outside / "standalone.py").write_text("pass\n", encoding="utf-8")
            directory_link = bounded / "linked-directory"
            file_link = bounded / "linked-file.py"
            try:
                directory_link.symlink_to(outside, target_is_directory=True)
                file_link.symlink_to(outside / "standalone.py")
            except OSError as error:
                self.skipTest(f"symlink creation unavailable: {error}")

            with self.assertRaisesRegex(
                EntryNotDeclaredError,
                "reparse point",
            ):
                EntryInventory.scan(
                    root,
                    include_required_import_seams=False,
                )

    def test_scan_rejects_a_generic_windows_reparse_root(self) -> None:
        fake_stat = type(
            "FakeStat",
            (),
            {"st_file_attributes": 0x00000400},
        )()
        with TemporaryDirectory() as tmp:
            with patch.object(Path, "is_symlink", return_value=False):
                with patch.object(Path, "is_junction", return_value=False):
                    with patch.object(Path, "lstat", return_value=fake_stat):
                        with self.assertRaisesRegex(
                            EntryNotDeclaredError,
                            "non-reparse",
                        ):
                            EntryInventory.scan(Path(tmp))

    def test_scan_assigns_conservative_closed_dispositions(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for directory in ("apps", "tools", "tests", "research"):
                (root / directory).mkdir()
            for relative in (
                "apps/web.py",
                "tools/admin.py",
                "tests/test_entry.py",
                "research/legacy.py",
                "daily_run.py",
            ):
                (root / relative).write_text("pass\n", encoding="utf-8")

            records = {
                record.path: record
                for record in EntryInventory.scan(
                    root,
                    include_required_import_seams=False,
                )
                if record.source == "filesystem_inventory"
            }

        self.assertEqual(records["apps/web.py"].disposition, "DENIED_WEB")
        self.assertEqual(records["tools/admin.py"].disposition, "ADMIN_ONLY")
        self.assertEqual(records["tests/test_entry.py"].disposition, "TEST_ONLY")
        self.assertEqual(
            records["research/legacy.py"].disposition,
            "LEGACY_UNAUDITED",
        )
        self.assertEqual(records["daily_run.py"].disposition, "PRODUCTION_DAILY")
        self.assertTrue(
            all(record.actor_type in ACTOR_TYPES for record in records.values())
        )

    def test_entry_record_rejects_actor_type_outside_the_contract(self) -> None:
        with self.assertRaisesRegex(ValueError, "actor_type"):
            EntryRecord(
                entry_id="file:web.py",
                path="web.py",
                kind="python_module",
                callable_name="main",
                actor_type="web",
            )

    def test_assert_declared_rejects_an_unlisted_entry(self) -> None:
        record = EntryRecord(
            entry_id="file:new_entry.py",
            path="new_entry.py",
            kind="python_module",
            callable_name="main",
            actor_type="automation",
        )

        with self.assertRaises(EntryNotDeclaredError):
            self._assert_against_reviewed_policy((record,), ())

        spoofed = EntryRecord(
            entry_id=record.entry_id,
            path="different.py",
            kind=record.kind,
            callable_name=record.callable_name,
            actor_type=record.actor_type,
        )
        with self.assertRaises(EntryNotDeclaredError):
            self._assert_against_reviewed_policy((record,), (spoofed,))

        stale = replace(
            record,
            entry_id="file:removed_entry.py",
            path="removed_entry.py",
        )
        with self.assertRaisesRegex(
            EntryNotDeclaredError,
            "absent from inventory",
        ):
            self._assert_against_reviewed_policy(
                (record,),
                (record, stale),
            )

    def test_assert_declared_rejects_duplicate_actual_entry_ids(self) -> None:
        declared = EntryRecord(
            entry_id="callable:test:run",
            path="test_entry.py",
            kind="python_callable",
            callable_name="run",
            actor_type="automation",
            declared_side_effects=(SideEffect.READ,),
            declared_phase=Phase.P0,
        )
        substituted = replace(
            declared,
            path="substituted_entry.py",
            declared_side_effects=(SideEffect.RUN_RESEARCH,),
        )

        with self.assertRaisesRegex(
            EntryNotDeclaredError,
            "duplicate actual entry id",
        ):
            self._assert_against_reviewed_policy(
                (substituted, declared),
                (declared,),
            )

    def test_assert_declared_rejects_content_hash_substitution(self) -> None:
        actual = EntryRecord(
            entry_id="file:test_entry.py",
            path="test_entry.py",
            kind="python_module",
            callable_name="main",
            actor_type="automation",
        )
        substituted = replace(actual, content_sha256="b" * 64)

        with self.assertRaisesRegex(
            EntryNotDeclaredError,
            "metadata differs",
        ):
            self._assert_against_reviewed_policy(
                (actual,),
                (substituted,),
            )

    def test_assert_declared_rejects_disposition_substitution(self) -> None:
        actual = EntryRecord(
            entry_id="file:test_entry.py",
            path="test_entry.py",
            kind="python_module",
            callable_name="main",
            actor_type="automation",
        )
        substituted = replace(actual, disposition="CONTROLLED_RESEARCH")

        with self.assertRaisesRegex(
            EntryNotDeclaredError,
            "metadata differs",
        ):
            self._assert_against_reviewed_policy(
                (actual,),
                (substituted,),
            )

    def test_assert_declared_rejects_trust_state_substitution(self) -> None:
        actual = EntryRecord(
            entry_id="file:test_entry.py",
            path="test_entry.py",
            kind="python_module",
            callable_name="main",
            actor_type="automation",
        )
        substituted = replace(actual, trust_state="controlled_research")

        with self.assertRaisesRegex(EntryNotDeclaredError, "metadata differs"):
            self._assert_against_reviewed_policy(
                (actual,),
                (substituted,),
            )

    def test_assert_declared_rejects_external_metadata_substitution(self) -> None:
        actual = EntryRecord(
            entry_id="external:scheduler:/A",
            path="/A",
            kind="external_scheduler",
            callable_name="run.bat",
            actor_type="scheduler",
            disposition="PRODUCTION_DAILY",
            trust_state="production_daily",
            external_metadata=(("state", "Ready"),),
            source="external_scheduler_inventory",
        )
        substituted = replace(
            actual,
            external_metadata=(("state", "Disabled"),),
        )

        with self.assertRaisesRegex(EntryNotDeclaredError, "metadata differs"):
            self._assert_against_reviewed_policy(
                (actual,),
                (substituted,),
            )

    def test_assert_declared_rejects_source_substitution(self) -> None:
        actual = EntryRecord(
            entry_id="file:test_entry.py",
            path="test_entry.py",
            kind="python_module",
            callable_name="main",
            actor_type="automation",
        )
        substituted = replace(actual, source="required_import_seam")

        with self.assertRaisesRegex(EntryNotDeclaredError, "metadata differs"):
            self._assert_against_reviewed_policy(
                (actual,),
                (substituted,),
            )

    def test_assert_declared_rejects_resource_root_substitution(self) -> None:
        actual = EntryRecord(
            entry_id="file:test_entry.py",
            path="test_entry.py",
            kind="python_module",
            callable_name="main",
            actor_type="automation",
        )
        substituted = replace(
            actual,
            resource_roots=("D:/unapproved",),
        )

        with self.assertRaisesRegex(
            EntryNotDeclaredError,
            "metadata differs",
        ):
            self._assert_against_reviewed_policy(
                (actual,),
                (substituted,),
            )

    def test_assert_declared_rejects_side_effect_metadata_substitution(self) -> None:
        actual = EntryRecord(
            entry_id="callable:test:run",
            path="test_entry.py",
            kind="python_callable",
            callable_name="run",
            actor_type="automation",
            declared_side_effects=(SideEffect.READ,),
            declared_phase=Phase.P0,
        )
        expanded = replace(
            actual,
            declared_side_effects=(SideEffect.READ, SideEffect.RUN_RESEARCH),
        )

        with self.assertRaisesRegex(
            EntryNotDeclaredError,
            "metadata differs",
        ):
            self._assert_against_reviewed_policy(
                (actual,),
                (expanded,),
            )

    def test_assert_declared_rejects_phase_metadata_substitution(self) -> None:
        actual = EntryRecord(
            entry_id="callable:test:run",
            path="test_entry.py",
            kind="python_callable",
            callable_name="run",
            actor_type="automation",
            declared_side_effects=(SideEffect.READ,),
            declared_phase=Phase.P0,
        )
        wrong_phase = replace(actual, declared_phase=Phase.P1)

        with self.assertRaisesRegex(
            EntryNotDeclaredError,
            "metadata differs",
        ):
            self._assert_against_reviewed_policy(
                (actual,),
                (wrong_phase,),
            )

    def test_scheduler_unknown_is_explicit_not_silent(self) -> None:
        root = Path(__file__).resolve().parents[1]
        records = EntryInventory.scan(root, scheduler_records=[{}])
        scheduler = [record for record in records if record.kind == "external_scheduler"]

        self.assertEqual(len(scheduler), 1)
        self.assertEqual(scheduler[0].path, "/A\u80a1\u9009\u80a1")
        self.assertEqual(scheduler[0].source, "external_scheduler_inventory")
        self.assertEqual(
            dict(scheduler[0].external_metadata)["state"],
            "UNKNOWN",
        )
        self.assertEqual(scheduler[0].disposition, "PRODUCTION_DAILY")

    def test_scheduler_omission_becomes_an_explicit_unknown_record(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "main.py").write_text("pass\n", encoding="utf-8")

            records = EntryInventory.scan(
                root,
                scheduler_records=[],
                include_required_import_seams=False,
            )

        scheduler = [
            record for record in records if record.kind == "external_scheduler"
        ]
        self.assertEqual(len(scheduler), 1)
        self.assertEqual(scheduler[0].path, "/A\u80a1\u9009\u80a1")
        self.assertEqual(
            dict(scheduler[0].external_metadata)["state"],
            "UNKNOWN",
        )

    def test_scheduler_inventory_preserves_the_declared_action(self) -> None:
        action = r"D:\workspace\project\run_select.bat"
        with TemporaryDirectory() as tmp:
            records = EntryInventory.scan(
                Path(tmp),
                scheduler_records=[
                    {
                        "task_path": r"\A\u80a1\u9009\u80a1",
                        "action": action,
                    }
                ],
                include_required_import_seams=False,
            )

        scheduler = [
            record for record in records if record.kind == "external_scheduler"
        ][0]
        self.assertEqual(scheduler.callable_name, action)
        self.assertEqual(
            dict(scheduler.external_metadata)["action"],
            action,
        )

    def test_manifest_round_trip_is_deterministic(self) -> None:
        root = Path(__file__).resolve().parents[1]
        records = EntryInventory.scan(root, scheduler_records=[{"task_path": r"\A股选股"}])

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "entry_inventory.json"
            first_text, first_hash = EntryInventory.render_manifest(records)
            path.write_text(first_text, encoding="utf-8")
            loaded = EntryInventory.load_manifest(path)
            second_text, second_hash = EntryInventory.render_manifest(loaded)

        self.assertEqual(records, loaded)
        self.assertEqual(first_text, second_text)
        self.assertEqual(first_hash, second_hash)

    def test_manifest_writer_fails_before_an_unticketed_file_effect(self) -> None:
        record = EntryRecord(
            entry_id="file:main.py",
            path="main.py",
            kind="python_module",
            callable_name="main",
            actor_type="automation",
        )
        with TemporaryDirectory() as tmp:
            destination = Path(tmp) / "unapproved" / "entry_inventory.json"

            with self.assertRaisesRegex(
                AuthorizationError,
                "unticketed manifest writes are disabled",
            ):
                EntryInventory.write_manifest(destination, (record,))

            self.assertFalse(destination.parent.exists())

    def test_scanner_manifest_cannot_approve_itself_as_entry_policy(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "main.py").write_text("pass\n", encoding="utf-8")
            records = EntryInventory.scan(
                root,
                include_required_import_seams=False,
            )
            manifest = root / "entry_inventory.json"
            text, _ = EntryInventory.render_manifest(records)
            manifest.write_text(text, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "reviewed entry policy"):
                EntryInventory.load_policy(
                    manifest,
                    identity_binding=IdentityBinding(
                        plan_hash="a" * 64,
                        scope_hash="b" * 64,
                        policy_hash="c" * 64,
                    ),
                )

    def test_scanner_records_cannot_approve_themselves_as_entry_policy(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "main.py").write_text("pass\n", encoding="utf-8")
            records = EntryInventory.scan(
                root,
                include_required_import_seams=False,
            )

            with self.assertRaisesRegex(
                EntryNotDeclaredError,
                "reviewed entry policy",
            ):
                EntryInventory.assert_declared(records, records)

    def test_hand_wrapped_scanner_records_cannot_approve_themselves(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "main.py").write_text("pass\n", encoding="utf-8")
            records = EntryInventory.scan(
                root,
                include_required_import_seams=False,
            )
            hand_wrapped = ReviewedEntryPolicy(
                plan_hash="a" * 64,
                scope_hash="b" * 64,
                policy_hash="c" * 64,
                records=records,
            )

            with self.assertRaisesRegex(
                EntryNotDeclaredError,
                "reviewed entry policy file",
            ):
                EntryInventory.assert_declared(records, hand_wrapped)

    def test_reviewed_policy_loads_and_matches_the_complete_inventory(self) -> None:
        record = EntryRecord(
            entry_id="callable:test:run",
            path="test_entry.py",
            kind="python_callable",
            callable_name="run",
            actor_type="automation",
            content_sha256="d" * 64,
            disposition="CONTROLLED_RESEARCH",
            trust_state="controlled_research",
            declared_side_effects=(SideEffect.READ,),
            declared_phase=Phase.P0,
            resource_roots=("research_state/control_plane/p0",),
            source="reviewed_entry_policy",
        )
        payload = {
            "schema_version": "control_plane.entry_policy.v1",
            "review_state": "APPROVED",
            "plan_hash": "a" * 64,
            "scope_hash": "b" * 64,
            "policy_hash": "c" * 64,
            "entries": [
                {
                    "entry_id": record.entry_id,
                    "path": record.path,
                    "kind": record.kind,
                    "callable_name": record.callable_name,
                    "actor_type": record.actor_type,
                    "content_sha256": record.content_sha256,
                    "disposition": record.disposition,
                    "trust_state": record.trust_state,
                    "declared_side_effects": [SideEffect.READ.value],
                    "declared_phase": Phase.P0.value,
                    "resource_roots": list(record.resource_roots),
                    "external_metadata": {},
                    "source": record.source,
                }
            ],
        }

        with TemporaryDirectory() as tmp:
            policy_path = Path(tmp) / "entry_policy.json"
            policy_path.write_text(
                json.dumps(payload, sort_keys=True),
                encoding="utf-8",
            )
            loaded = EntryInventory.load_policy(
                policy_path,
                identity_binding=IdentityBinding(
                    plan_hash="a" * 64,
                    scope_hash="b" * 64,
                    policy_hash="c" * 64,
                ),
            )

            EntryInventory.assert_declared(
                (record,),
                policy_path,
                identity_binding=self._identity_binding(),
            )
        self.assertEqual(loaded.records, (record,))

    def test_reviewed_policy_rejects_a_different_identity_binding(self) -> None:
        payload = {
            "schema_version": "control_plane.entry_policy.v1",
            "review_state": "APPROVED",
            "plan_hash": "a" * 64,
            "scope_hash": "b" * 64,
            "policy_hash": "c" * 64,
            "entries": [],
        }
        wrong_binding = IdentityBinding(
            plan_hash="d" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )

        with TemporaryDirectory() as tmp:
            policy_path = Path(tmp) / "entry_policy.json"
            policy_path.write_text(
                json.dumps(payload, sort_keys=True),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "identity binding mismatch"):
                EntryInventory.load_policy(
                    policy_path,
                    identity_binding=wrong_binding,
                )

    def test_reviewed_policy_requires_every_record_field(self) -> None:
        complete_entry = {
            "entry_id": "file:main.py",
            "path": "main.py",
            "kind": "python_module",
            "callable_name": "main",
            "actor_type": "automation",
            "content_sha256": "d" * 64,
            "disposition": "LEGACY_UNAUDITED",
            "trust_state": "legacy_unaudited",
            "declared_side_effects": [],
            "declared_phase": None,
            "resource_roots": [],
            "external_metadata": {},
            "source": "filesystem_inventory",
        }
        payload = {
            "schema_version": "control_plane.entry_policy.v1",
            "review_state": "APPROVED",
            "plan_hash": "a" * 64,
            "scope_hash": "b" * 64,
            "policy_hash": "c" * 64,
            "entries": [],
        }

        with TemporaryDirectory() as tmp:
            policy_path = Path(tmp) / "entry_policy.json"
            for missing_field in complete_entry:
                incomplete_entry = dict(complete_entry)
                del incomplete_entry[missing_field]
                payload["entries"] = [incomplete_entry]
                policy_path.write_text(
                    json.dumps(payload, sort_keys=True),
                    encoding="utf-8",
                )
                with self.subTest(missing_field=missing_field):
                    with self.assertRaisesRegex(ValueError, "required fields"):
                        EntryInventory.load_policy(
                            policy_path,
                            identity_binding=self._identity_binding(),
                        )


class StableFreezeInventoryBuilderTests(unittest.TestCase):
    identity = {
        "plan_hash": "a" * 64,
        "scope_hash": "b" * 64,
        "instruction_policy_hash": "c" * 64,
    }

    def _repository(self, root: Path) -> None:
        (root / ".gitattributes").write_text(
            "*.py text eol=lf\n",
            encoding="utf-8",
        )
        sources = {
            "main.py": "print('main')\n",
            "research_automation/autonomous_runner.py": (
                "class AutonomousRunnerV1:\n    pass\n"
            ),
            "research_automation/kbase_ag2_full_cycle.py": (
                "def run_kbase_ag2_full_cycle():\n    pass\n"
            ),
            "research_automation/discovery_execution_bridge.py": (
                "def execute_plan():\n    pass\n"
            ),
            "data/ignored.py": "raise RuntimeError('data must be excluded')\n",
            "research_state/ignored.py": (
                "raise RuntimeError('state must be excluded')\n"
            ),
            "artifacts/ignored.py": (
                "raise RuntimeError('outputs must be excluded')\n"
            ),
            "archive/ignored.py": (
                "raise RuntimeError('archive must be excluded')\n"
            ),
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
            "command": (
                r"D:\workspace\a-share-quant-selector-main\run_select.bat"
            ),
            "task_xml_sha256": "d" * 64,
            "state": "Ready",
            "principal": "SYSTEM|ServiceAccount|HighestAvailable",
            "trigger": (
                "Calendar|start=2026-07-27T20:00:00+08:00|"
                "days_interval=1|enabled=true"
            ),
            "acl_summary": (
                "owner=BUILTIN\\Administrators;sddl=test-only"
            ),
        }

    def _freeze(self, root: Path) -> dict[str, object]:
        return build_code_freeze_manifest(
            root,
            plan_version="V3.4.2-P0R2",
            phase="P0",
            attempt_id="p0r2-attempt-001",
            identity_binding=self.identity,
        )

    def test_builds_t7_freeze_then_t8_inventory_from_an_independent_scan(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = self._freeze(root)
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P0R2",
                phase="P0",
                attempt_id="p0r2-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=freeze,
                scheduler_records=[self._scheduler_record()],
            )

        frozen_paths = {item["path"] for item in freeze["files"]}
        self.assertEqual(
            frozen_paths,
            {
                ".gitattributes",
                "main.py",
                "research_automation/autonomous_runner.py",
                "research_automation/discovery_execution_bridge.py",
                "research_automation/kbase_ag2_full_cycle.py",
            },
        )
        self.assertEqual(inventory["entry_count"], 9)

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
                    self._freeze(root)

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
                    self._freeze(root)

    def test_t8_rejects_byte_identity_policy_drift_after_t7(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            freeze = self._freeze(root)
            (root / ".gitattributes").write_text(
                "*.py text eol=crlf\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "code freeze",
            ):
                build_final_entry_inventory(
                    root,
                    plan_version="V3.4.2-P0R2",
                    phase="P0",
                    attempt_id="p0r2-attempt-001",
                    identity_binding=self.identity,
                    freeze_manifest=freeze,
                    scheduler_records=[self._scheduler_record()],
                )

    def test_t7_rejects_an_unknown_top_level_source_root(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            unknown = root / "new_runner_family"
            unknown.mkdir()
            (unknown / "run.py").write_text("pass\n", encoding="utf-8")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "scope decision",
            ):
                self._freeze(root)

    def test_t7_accepts_only_the_approved_ths_data_and_output_roots(self) -> None:
        approved_roots = {
            "data_ths",
            "data_pre_ths_backup_20260727_110350",
            "outputs",
            "ths-rebuild-1s3f37j2",
            "ths-rebuild-2f8lznck",
            "ths-rebuild-gzsqa360",
            "ths-rebuild-rn72aj5e",
        }
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            for directory in approved_roots:
                path = root / directory
                path.mkdir()
                (path / "evidence.csv").write_text(
                    "date,close\n2026-07-27,10\n",
                    encoding="utf-8",
                )

            freeze = self._freeze(root)

        frozen_paths = {item["path"] for item in freeze["files"]}
        self.assertFalse(
            any(
                path == directory or path.startswith(directory + "/")
                for path in frozen_paths
                for directory in approved_roots
            )
        )

    def test_t7_rejects_executable_content_hidden_in_an_approved_data_root(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            hidden = root / "data_ths" / "nested" / "run.py"
            hidden.parent.mkdir(parents=True)
            hidden.write_text("raise RuntimeError('hidden')\n", encoding="utf-8")

            with self.assertRaisesRegex(
                UnstableInventoryError,
                "executable file",
            ):
                self._freeze(root)

    def test_t8_preserves_explicit_unknown_scheduler_evidence(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._repository(root)
            inventory = build_final_entry_inventory(
                root,
                plan_version="V3.4.2-P0R2",
                phase="P0",
                attempt_id="p0r2-attempt-001",
                identity_binding=self.identity,
                freeze_manifest=self._freeze(root),
                scheduler_records=[{}],
            )

        scheduler = [
            entry
            for entry in inventory["entries"]
            if entry["kind"] == "external_scheduler"
        ][0]
        self.assertEqual(scheduler["external_metadata"]["state"], "UNKNOWN")
        self.assertEqual(
            scheduler["content_sha256"],
            unavailable_scheduler_sha256("/A股选股"),
        )


class PhaseAuthorizerTests(unittest.TestCase):
    @staticmethod
    def _provision_v2_authorization(
        path: Path,
        *,
        authorization_ref: str,
        bearer_secret: str,
        actor: Actor,
        binding: IdentityBinding,
    ) -> None:
        connection = sqlite3.connect(path)
        try:
            connection.executescript(
                """
                CREATE TABLE control_plane_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE authorizations (
                    authorization_ref TEXT PRIMARY KEY,
                    phase TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_type TEXT NOT NULL,
                    invocation_id TEXT NOT NULL,
                    plan_hash TEXT NOT NULL,
                    scope_hash TEXT NOT NULL,
                    policy_hash TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    state TEXT NOT NULL
                );
                CREATE TABLE phase_grants (
                    grant_id TEXT PRIMARY KEY,
                    authorization_ref TEXT NOT NULL UNIQUE,
                    phase TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_type TEXT NOT NULL,
                    invocation_id TEXT NOT NULL,
                    plan_hash TEXT NOT NULL,
                    scope_hash TEXT NOT NULL,
                    policy_hash TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    allowed_effects TEXT NOT NULL,
                    state TEXT NOT NULL
                );
                CREATE TABLE entry_permissions (
                    entry_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    effect TEXT NOT NULL,
                    resource_root TEXT NOT NULL,
                    PRIMARY KEY(entry_id, phase, effect, resource_root)
                );
                CREATE TABLE task_tickets (
                    ticket_id TEXT PRIMARY KEY,
                    grant_id TEXT NOT NULL,
                    entry_id TEXT NOT NULL,
                    effect TEXT NOT NULL,
                    resource_scope TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    lease_id TEXT,
                    lease_secret_hash TEXT,
                    state TEXT NOT NULL,
                    UNIQUE(grant_id, idempotency_key)
                );
                CREATE TABLE side_effect_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    evidence_ref TEXT
                );
                """
            )
            connection.executemany(
                "INSERT INTO control_plane_meta(key, value) VALUES (?, ?)",
                (
                    ("schema_version", "2"),
                    ("plan_hash", binding.plan_hash),
                    ("scope_hash", binding.scope_hash),
                    ("policy_hash", binding.policy_hash),
                ),
            )
            connection.execute(
                """
                INSERT INTO authorizations
                (authorization_ref, phase, actor_id, actor_type, invocation_id,
                 plan_hash, scope_hash, policy_hash, secret_hash, state)
                VALUES (?, 'P0', ?, ?, ?, ?, ?, ?, ?, 'PENDING')
                """,
                (
                    authorization_ref,
                    actor.actor_id,
                    actor.actor_type,
                    actor.invocation_id,
                    binding.plan_hash,
                    binding.scope_hash,
                    binding.policy_hash,
                    hashlib.sha256(bearer_secret.encode("utf-8")).hexdigest(),
                ),
            )
            connection.commit()
        finally:
            connection.close()

    @staticmethod
    def _allow_v2_entry(
        path: Path,
        *,
        entry_id: str,
        effect: SideEffect,
        resource_root: Path,
    ) -> None:
        connection = sqlite3.connect(path)
        try:
            connection.execute(
                """
                INSERT INTO entry_permissions(entry_id, phase, effect, resource_root)
                VALUES (?, 'P0', ?, ?)
                """,
                (entry_id, effect.value, str(resource_root.resolve())),
            )
            connection.commit()
        finally:
            connection.close()

    def test_legacy_phase_authorizer_is_fail_closed_and_creates_no_store(self) -> None:
        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "legacy" / "control.sqlite3"
            authorizer = PhaseAuthorizer(
                store,
                approved_plan_hash="a" * 64,
                approved_scope_hash="b" * 64,
                approved_policy_hash="c" * 64,
            )
            calls = (
                lambda: authorizer.issue_phase_token(
                    Phase.P0,
                    Actor("tester", "automation", "inv-legacy"),
                    plan_hash="a" * 64,
                    scope_hash="b" * 64,
                    policy_hash="c" * 64,
                ),
                lambda: authorizer.consume_phase_token(
                    "legacy-token",
                    plan_hash="a" * 64,
                    scope_hash="b" * 64,
                    policy_hash="c" * 64,
                ),
                lambda: authorizer.record_gate(
                    Phase.P0,
                    "PASS",
                    report_hash="d" * 64,
                    token_id="legacy-token",
                ),
                lambda: authorizer.assert_side_effect(SideEffect.READ, None),
                lambda: EntryGuard(authorizer).assert_side_effect(SideEffect.READ),
            )

            self.assertFalse(store.exists())
            for call in calls:
                with self.subTest(call=call):
                    with self.assertRaisesRegex(
                        AuthorizationError,
                        "legacy phase-token authorization is disabled",
                    ):
                        call()
            self.assertFalse(store.exists())

    def test_legacy_authorization_grant_never_authorizes_an_effect(self) -> None:
        grant = entry_guard_module.AuthorizationGrant(
            token_id="legacy-token",
            phase=Phase.P0,
            actor_type="legacy_runner",
            allowed_side_effects=(SideEffect.READ,),
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )

        self.assertFalse(grant.allows(SideEffect.READ))

    def test_legacy_entry_guard_rejects_a_permissive_authorizer(self) -> None:
        class PermissiveLegacyAuthorizer:
            def assert_side_effect(self, *args: object, **kwargs: object) -> None:
                return None

        guard = EntryGuard(PermissiveLegacyAuthorizer())  # type: ignore[arg-type]

        with self.assertRaisesRegex(
            AuthorizationError,
            "legacy phase-token authorization is disabled",
        ):
            guard.assert_side_effect(SideEffect.READ)

    def test_public_hashes_cannot_bootstrap_phase_grant(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-public-hashes")

        with TemporaryDirectory() as tmp:
            attacker_store = Path(tmp) / "attacker-created.sqlite3"
            with patch.object(
                entry_guard_module,
                "_CONTROL_PLANE_DB_PATH",
                attacker_store,
                create=True,
            ):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "pre-provisioned control-plane store",
                ):
                    entry_guard_module.claim_phase(
                        "public-authorization-ref",
                        "public-bearer-secret",
                        actor,
                        binding,
                    )

            self.assertFalse(attacker_store.exists())

    def test_deleted_control_plane_store_is_not_recreated_during_claim(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-deleted-store")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-deleted-store",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            original_connect = sqlite3.connect

            def delete_then_connect(
                database: object,
                *args: object,
                **kwargs: object,
            ) -> sqlite3.Connection:
                store.unlink(missing_ok=True)
                return original_connect(database, *args, **kwargs)

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with patch.object(
                    entry_guard_module.sqlite3,
                    "connect",
                    side_effect=delete_then_connect,
                ):
                    with self.assertRaises(AuthorizationError):
                        entry_guard_module.claim_phase(
                            "auth-deleted-store",
                            "phase-secret",
                            actor,
                            binding,
                        )

            self.assertFalse(store.exists())

    def test_locked_control_plane_store_fails_as_authorization_error(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-locked-store")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-locked-store",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-locked-store",
                    "phase-secret",
                    actor,
                    binding,
                )
                lock_connection = sqlite3.connect(store, isolation_level=None)
                try:
                    lock_connection.execute("BEGIN EXCLUSIVE")
                    original_connect = sqlite3.connect

                    def connect_without_waiting(
                        database: object,
                        *args: object,
                        **kwargs: object,
                    ) -> sqlite3.Connection:
                        kwargs["timeout"] = 0
                        return original_connect(database, *args, **kwargs)

                    with patch.object(
                        entry_guard_module.sqlite3,
                        "connect",
                        side_effect=connect_without_waiting,
                    ):
                        with self.assertRaisesRegex(
                            AuthorizationError,
                            "control-plane store is unavailable",
                        ):
                            entry_guard_module.issue_task_ticket(
                                grant,
                                entry_id="callable:p0:write-gate",
                                effect=SideEffect.WRITE_CONTROL_PLANE,
                                resource_scope=control_root / "gate.json",
                                idempotency_key="locked-store-1",
                            )
                finally:
                    lock_connection.rollback()
                    lock_connection.close()

    def test_preprovisioned_authorization_is_claimed_once(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-claim")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-once",
                bearer_secret="one-time-secret",
                actor=actor,
                binding=binding,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-once",
                    "one-time-secret",
                    actor,
                    binding,
                )
                with self.assertRaisesRegex(AuthorizationError, "already claimed"):
                    entry_guard_module.claim_phase(
                        "auth-once",
                        "one-time-secret",
                        actor,
                        binding,
                    )

        self.assertEqual(grant.phase, Phase.P0)
        self.assertEqual(grant.authorization_ref, "auth-once")
        self.assertEqual(
            grant.allowed_side_effects,
            (SideEffect.READ, SideEffect.WRITE_CONTROL_PLANE),
        )

    def test_p1_authorization_is_rejected_without_being_consumed_by_p0(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-p1-envelope")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-p1-envelope",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            connection = sqlite3.connect(store)
            try:
                connection.execute(
                    """
                    UPDATE authorizations SET phase = 'P1'
                    WHERE authorization_ref = 'auth-p1-envelope'
                    """
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "authorization is not for P0",
                ):
                    entry_guard_module.claim_phase(
                        "auth-p1-envelope",
                        "phase-secret",
                        actor,
                        binding,
                    )

            connection = sqlite3.connect(store)
            try:
                state = connection.execute(
                    """
                    SELECT state FROM authorizations
                    WHERE authorization_ref = 'auth-p1-envelope'
                    """
                ).fetchone()[0]
            finally:
                connection.close()

        self.assertEqual(state, "PENDING")

    def test_wrong_bearer_secret_does_not_consume_authorization(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-wrong-secret")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-wrong-secret",
                bearer_secret="correct-secret",
                actor=actor,
                binding=binding,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "bearer secret mismatch",
                ):
                    entry_guard_module.claim_phase(
                        "auth-wrong-secret",
                        "wrong-secret",
                        actor,
                        binding,
                    )
                grant = entry_guard_module.claim_phase(
                    "auth-wrong-secret",
                    "correct-secret",
                    actor,
                    binding,
                )

        self.assertEqual(grant.authorization_ref, "auth-wrong-secret")

    def test_stolen_authorization_secret_is_bound_to_actor_and_invocation(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        approved_actor = Actor("tester", "human", "inv-approved")
        impostor = Actor("tester", "human", "inv-impostor")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-actor-bound",
                bearer_secret="stolen-secret",
                actor=approved_actor,
                binding=binding,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "identity or bearer secret mismatch",
                ):
                    entry_guard_module.claim_phase(
                        "auth-actor-bound",
                        "stolen-secret",
                        impostor,
                        binding,
                    )
                grant = entry_guard_module.claim_phase(
                    "auth-actor-bound",
                    "stolen-secret",
                    approved_actor,
                    binding,
                )

        self.assertEqual(grant.actor, approved_actor)

    def test_authorization_envelope_cannot_override_approved_meta_identity(self) -> None:
        approved_binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        forged_binding = IdentityBinding(
            plan_hash="d" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-meta-binding")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-meta-binding",
                bearer_secret="phase-secret",
                actor=actor,
                binding=approved_binding,
            )
            connection = sqlite3.connect(store)
            try:
                connection.execute(
                    "UPDATE authorizations SET plan_hash = ? WHERE authorization_ref = ?",
                    (forged_binding.plan_hash, "auth-meta-binding"),
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "approved control-plane identity binding mismatch",
                ):
                    entry_guard_module.claim_phase(
                        "auth-meta-binding",
                        "phase-secret",
                        actor,
                        forged_binding,
                    )

    def test_claim_phase_has_exactly_one_winner_under_concurrency(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-concurrent-claim")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-concurrent-claim",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            barrier = Barrier(3)

            def contender():
                barrier.wait()
                try:
                    return (
                        "winner",
                        entry_guard_module.claim_phase(
                            "auth-concurrent-claim",
                            "phase-secret",
                            actor,
                            binding,
                        ),
                    )
                except AuthorizationError as error:
                    return ("loser", error)

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(contender) for _ in range(2)]
                    barrier.wait()
                    results = [future.result(timeout=10) for future in futures]

        winners = [value for state, value in results if state == "winner"]
        losers = [value for state, value in results if state == "loser"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        self.assertEqual(winners[0].authorization_ref, "auth-concurrent-claim")

    def test_unknown_future_schema_fails_closed_without_claiming_authorization(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-future-schema")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-future-schema",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            connection = sqlite3.connect(store)
            try:
                connection.execute(
                    "UPDATE control_plane_meta SET value = '999' WHERE key = 'schema_version'"
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "unsupported control-plane schema version",
                ):
                    entry_guard_module.claim_phase(
                        "auth-future-schema",
                        "phase-secret",
                        actor,
                        binding,
                    )

            connection = sqlite3.connect(store)
            try:
                state = connection.execute(
                    "SELECT state FROM authorizations WHERE authorization_ref = ?",
                    ("auth-future-schema",),
                ).fetchone()[0]
            finally:
                connection.close()

        self.assertEqual(state, "PENDING")

    def test_unknown_legacy_schema_is_preserved_without_authority_upgrade(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-legacy-schema")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            connection = sqlite3.connect(store)
            try:
                connection.execute(
                    "CREATE TABLE phase_tokens(token_id TEXT PRIMARY KEY, evidence TEXT)"
                )
                connection.execute(
                    "INSERT INTO phase_tokens(token_id, evidence) VALUES ('old-token', 'keep')"
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                for attempt in range(2):
                    with self.subTest(attempt=attempt):
                        with self.assertRaisesRegex(
                            AuthorizationError,
                            "unsupported control-plane schema version",
                        ):
                            entry_guard_module.claim_phase(
                                "legacy-auth-ref",
                                "legacy-secret",
                                actor,
                                binding,
                            )

            connection = sqlite3.connect(store)
            try:
                evidence = connection.execute(
                    "SELECT evidence FROM phase_tokens WHERE token_id = 'old-token'"
                ).fetchone()[0]
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
            finally:
                connection.close()

        self.assertEqual(evidence, "keep")
        self.assertEqual(tables, {"phase_tokens"})

    def test_legacy_token_evidence_is_preserved_without_authority_upgrade(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-legacy-schema")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            connection = sqlite3.connect(store)
            try:
                connection.executescript(
                    """
                    CREATE TABLE phase_tokens (
                        token_id TEXT PRIMARY KEY,
                        phase TEXT NOT NULL,
                        actor_json TEXT NOT NULL,
                        plan_hash TEXT NOT NULL,
                        scope_hash TEXT NOT NULL,
                        policy_hash TEXT NOT NULL,
                        allowed_effects TEXT NOT NULL,
                        issued_at TEXT NOT NULL,
                        consumed_at TEXT
                    );
                    CREATE TABLE phase_gates (
                        phase TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        report_hash TEXT NOT NULL,
                        token_id TEXT NOT NULL
                    );
                    CREATE TABLE authorizer_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    """
                )
                connection.execute(
                    """
                    INSERT INTO phase_tokens
                    (token_id, phase, actor_json, plan_hash, scope_hash, policy_hash,
                     allowed_effects, issued_at, consumed_at)
                    VALUES ('old-token', 'P0', '{}', ?, ?, ?, '["READ"]', 'old-time', 'old-time')
                    """,
                    (binding.plan_hash, binding.scope_hash, binding.policy_hash),
                )
                connection.execute(
                    """
                    INSERT INTO phase_gates(phase, status, report_hash, token_id)
                    VALUES ('P0', 'PASS', ?, 'old-token')
                    """,
                    ("d" * 64,),
                )
                connection.executemany(
                    "INSERT INTO authorizer_meta(key, value) VALUES (?, ?)",
                    (
                        ("plan_hash", binding.plan_hash),
                        ("scope_hash", binding.scope_hash),
                        ("policy_hash", binding.policy_hash),
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                for attempt in range(2):
                    with self.subTest(attempt=attempt):
                        with self.assertRaisesRegex(
                            AuthorizationError,
                            "approved control-plane identity binding mismatch",
                        ):
                            entry_guard_module.claim_phase(
                                "legacy-auth-ref",
                                "legacy-secret",
                                actor,
                                binding,
                            )

            connection = sqlite3.connect(store)
            try:
                token = connection.execute(
                    """
                    SELECT token_id, phase, trust_state
                    FROM legacy_phase_tokens WHERE token_id = 'old-token'
                    """
                ).fetchone()
                gate = connection.execute(
                    """
                    SELECT phase, status, trust_state
                    FROM legacy_phase_gates WHERE token_id = 'old-token'
                    """
                ).fetchone()
                legacy_meta = connection.execute(
                    """
                    SELECT value, trust_state FROM legacy_authorizer_meta
                    WHERE key = 'plan_hash'
                    """
                ).fetchone()
                meta = dict(connection.execute("SELECT key, value FROM control_plane_meta"))
                authorization_count = connection.execute(
                    "SELECT COUNT(*) FROM authorizations"
                ).fetchone()[0]
                grant_count = connection.execute(
                    "SELECT COUNT(*) FROM phase_grants"
                ).fetchone()[0]
            finally:
                connection.close()

        self.assertEqual(token, ("old-token", "P0", "LEGACY_UNTRUSTED"))
        self.assertEqual(gate, ("P0", "PASS", "LEGACY_UNTRUSTED"))
        self.assertEqual(
            legacy_meta,
            (binding.plan_hash, "LEGACY_UNTRUSTED"),
        )
        self.assertEqual(meta["schema_version"], "2")
        self.assertEqual(meta["migration_state"], "LEGACY_QUARANTINED")
        self.assertNotIn("plan_hash", meta)
        self.assertEqual(authorization_count, 0)
        self.assertEqual(grant_count, 0)

    def test_legacy_schema_migration_rolls_back_atomically_on_failure(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-legacy-rollback")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            connection = sqlite3.connect(store)
            try:
                connection.executescript(
                    """
                    CREATE TABLE phase_tokens (
                        token_id TEXT PRIMARY KEY,
                        trust_state TEXT NOT NULL
                    );
                    CREATE TABLE phase_gates (
                        phase TEXT PRIMARY KEY,
                        status TEXT NOT NULL
                    );
                    CREATE TABLE authorizer_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    INSERT INTO phase_tokens(token_id, trust_state)
                    VALUES ('old-token', 'PREEXISTING');
                    """
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "legacy schema migration failed",
                ):
                    entry_guard_module.claim_phase(
                        "legacy-auth-ref",
                        "legacy-secret",
                        actor,
                        binding,
                    )

            connection = sqlite3.connect(store)
            try:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                token = connection.execute(
                    "SELECT token_id, trust_state FROM phase_tokens"
                ).fetchone()
            finally:
                connection.close()

        self.assertEqual(
            tables,
            {"phase_tokens", "phase_gates", "authorizer_meta"},
        )
        self.assertEqual(token, ("old-token", "PREEXISTING"))

    def test_incomplete_v2_schema_fails_closed_as_authorization_error(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-incomplete-schema")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            connection = sqlite3.connect(store)
            try:
                connection.execute(
                    "CREATE TABLE control_plane_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO control_plane_meta(key, value) VALUES ('schema_version', '2')"
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "incomplete control-plane schema",
                ):
                    entry_guard_module.claim_phase(
                        "missing-auth",
                        "missing-secret",
                        actor,
                        binding,
                    )

    def test_v2_schema_with_drifted_columns_fails_closed(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-drifted-schema")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-drifted-schema",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            connection = sqlite3.connect(store)
            try:
                connection.execute("DROP TABLE task_tickets")
                connection.execute(
                    "CREATE TABLE task_tickets(ticket_id TEXT PRIMARY KEY)"
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "incomplete control-plane schema",
                ):
                    entry_guard_module.claim_phase(
                        "auth-drifted-schema",
                        "phase-secret",
                        actor,
                        binding,
                    )

    def test_v2_schema_without_required_uniqueness_fails_closed(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-drifted-unique-index")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-drifted-unique-index",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            connection = sqlite3.connect(store)
            try:
                connection.executescript(
                    """
                    DROP TABLE task_tickets;
                    CREATE TABLE task_tickets (
                        ticket_id TEXT PRIMARY KEY,
                        grant_id TEXT NOT NULL,
                        entry_id TEXT NOT NULL,
                        effect TEXT NOT NULL,
                        resource_scope TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL,
                        request_hash TEXT NOT NULL,
                        secret_hash TEXT NOT NULL,
                        lease_id TEXT,
                        lease_secret_hash TEXT,
                        state TEXT NOT NULL
                    );
                    """
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "incomplete control-plane schema",
                ):
                    entry_guard_module.claim_phase(
                        "auth-drifted-unique-index",
                        "phase-secret",
                        actor,
                        binding,
                    )

    def test_v2_schema_without_append_event_primary_key_fails_closed(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-drifted-event-key")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-drifted-event-key",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            connection = sqlite3.connect(store)
            try:
                connection.executescript(
                    """
                    DROP TABLE side_effect_events;
                    CREATE TABLE side_effect_events (
                        event_id INTEGER,
                        ticket_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        evidence_ref TEXT
                    );
                    """
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "incomplete control-plane schema",
                ):
                    entry_guard_module.claim_phase(
                        "auth-drifted-event-key",
                        "phase-secret",
                        actor,
                        binding,
                    )

    def test_schema_change_invalidates_an_existing_phase_grant(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-schema-change")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-schema-change",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-schema-change",
                    "phase-secret",
                    actor,
                    binding,
                )
                connection = sqlite3.connect(store)
                try:
                    connection.execute(
                        "UPDATE control_plane_meta SET value = '999' WHERE key = 'schema_version'"
                    )
                    connection.commit()
                finally:
                    connection.close()

                with self.assertRaisesRegex(
                    AuthorizationError,
                    "unsupported control-plane schema version",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=control_root,
                        idempotency_key="schema-change-1",
                    )

    def test_p0_cannot_issue_forbidden_task_tickets(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-ticket-deny")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-ticket-deny",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-ticket-deny",
                    "phase-secret",
                    actor,
                    binding,
                )
                forbidden = set(SideEffect) - {
                    SideEffect.READ,
                    SideEffect.WRITE_CONTROL_PLANE,
                }
                for effect in forbidden:
                    with self.subTest(effect=effect.value):
                        with self.assertRaisesRegex(
                            AuthorizationError,
                            "not allowed by P0",
                        ):
                            entry_guard_module.issue_task_ticket(
                                grant,
                                entry_id="callable:test:forbidden",
                                effect=effect,
                                resource_scope=Path(tmp) / "forbidden-output",
                                idempotency_key=f"ticket-deny-{effect.value}",
                            )

    def test_task_ticket_binds_entry_effect_scope_and_grant(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-ticket")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-ticket",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-ticket",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="gate-write-1",
                )

        self.assertEqual(ticket.grant_id, grant.grant_id)
        self.assertEqual(ticket.entry_id, "callable:p0:write-gate")
        self.assertEqual(ticket.effect, SideEffect.WRITE_CONTROL_PLANE)
        expected_scope = str(control_root.resolve())
        if os.name == "nt":
            expected_scope = os.path.normcase(expected_scope)
        self.assertEqual(ticket.resource_scope, expected_scope)
        self.assertEqual(ticket.idempotency_key, "gate-write-1")

    def test_task_ticket_idempotency_requires_identical_semantics(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-ticket-idempotency")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            different_scope = control_root / "different"
            different_scope.mkdir(parents=True)
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-ticket-idempotency",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-ticket-idempotency",
                    "phase-secret",
                    actor,
                    binding,
                )
                first = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="same-key",
                )
                repeated = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="same-key",
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "different semantics",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=different_scope,
                        idempotency_key="same-key",
                    )

        self.assertEqual(repeated.ticket_id, first.ticket_id)
        self.assertEqual(repeated.bearer_secret, first.bearer_secret)

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_windows_case_alias_has_one_ticket_identity(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-case-alias")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-case-alias",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-case-alias",
                    "phase-secret",
                    actor,
                    binding,
                )
                first = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="case-alias-1",
                )
                repeated = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=str(control_root).upper(),
                    idempotency_key="case-alias-1",
                )

        self.assertEqual(repeated.ticket_id, first.ticket_id)
        self.assertEqual(repeated.resource_scope, first.resource_scope)

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_windows_missing_leaf_case_alias_has_one_ticket_identity(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-missing-case-alias")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-missing-case-alias",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-missing-case-alias",
                    "phase-secret",
                    actor,
                    binding,
                )
                first = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root / "Gate.JSON",
                    idempotency_key="missing-case-alias-1",
                )
                repeated = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root / "gate.json",
                    idempotency_key="missing-case-alias-1",
                )

        self.assertEqual(repeated.ticket_id, first.ticket_id)
        self.assertEqual(repeated.resource_scope, first.resource_scope)

    def test_forged_phase_grant_secret_cannot_issue_ticket(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-forged-grant")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-forged-grant",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-forged-grant",
                    "phase-secret",
                    actor,
                    binding,
                )
                forged = replace(grant, bearer_secret="forged-secret")
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "phase grant capability mismatch",
                ):
                    entry_guard_module.issue_task_ticket(
                        forged,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=control_root,
                        idempotency_key="forged-grant-1",
                    )

    def test_forged_phase_grant_cannot_expand_allowed_effects(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-forged-effects")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-forged-effects",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:research",
                effect=SideEffect.RUN_RESEARCH,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-forged-effects",
                    "phase-secret",
                    actor,
                    binding,
                )
                forged = replace(
                    grant,
                    allowed_side_effects=(
                        *grant.allowed_side_effects,
                        SideEffect.RUN_RESEARCH,
                    ),
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "phase grant capability mismatch",
                ):
                    entry_guard_module.issue_task_ticket(
                        forged,
                        entry_id="callable:p0:research",
                        effect=SideEffect.RUN_RESEARCH,
                        resource_scope=control_root,
                        idempotency_key="forged-effects-1",
                    )

    def test_begin_side_effect_atomically_claims_ticket(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-lease")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-lease",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-lease",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="lease-1",
                )
                lease = entry_guard_module.begin_side_effect(
                    ticket,
                    expected_entry_id="callable:p0:write-gate",
                    expected_effect=SideEffect.WRITE_CONTROL_PLANE,
                    expected_resource=control_root,
                )
                with self.assertRaisesRegex(AuthorizationError, "not ISSUED"):
                    entry_guard_module.begin_side_effect(
                        ticket,
                        expected_entry_id="callable:p0:write-gate",
                        expected_effect=SideEffect.WRITE_CONTROL_PLANE,
                        expected_resource=control_root,
                    )

            connection = sqlite3.connect(store)
            try:
                state = connection.execute(
                    "SELECT state FROM task_tickets WHERE ticket_id = ?",
                    (ticket.ticket_id,),
                ).fetchone()[0]
            finally:
                connection.close()

        self.assertEqual(lease.ticket_id, ticket.ticket_id)
        self.assertEqual(lease.entry_id, ticket.entry_id)
        self.assertEqual(state, "IN_PROGRESS")

    def test_revoked_phase_grant_blocks_unstarted_ticket(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-revoked-grant")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-revoked-grant",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-revoked-grant",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="revoked-grant-1",
                )
                connection = sqlite3.connect(store)
                try:
                    connection.execute(
                        "UPDATE phase_grants SET state = 'REVOKED' WHERE grant_id = ?",
                        (grant.grant_id,),
                    )
                    connection.commit()
                finally:
                    connection.close()

                with self.assertRaisesRegex(
                    AuthorizationError,
                    "phase grant is not active",
                ):
                    entry_guard_module.begin_side_effect(
                        ticket,
                        expected_entry_id=ticket.entry_id,
                        expected_effect=ticket.effect,
                        expected_resource=control_root,
                    )

    def test_begin_side_effect_rechecks_revoked_entry_permission(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-revoked-permission")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-revoked-permission",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-revoked-permission",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root / "gate.json",
                    idempotency_key="revoked-permission-1",
                )
                connection = sqlite3.connect(store)
                try:
                    connection.execute(
                        """
                        DELETE FROM entry_permissions
                        WHERE entry_id = ? AND phase = 'P0' AND effect = ?
                        """,
                        (ticket.entry_id, ticket.effect.value),
                    )
                    connection.commit()
                finally:
                    connection.close()

                with self.assertRaisesRegex(
                    AuthorizationError,
                    "resource scope is no longer approved",
                ):
                    entry_guard_module.begin_side_effect(
                        ticket,
                        expected_entry_id=ticket.entry_id,
                        expected_effect=ticket.effect,
                        expected_resource=control_root / "gate.json",
                    )

    def test_begin_side_effect_rechecks_missing_approved_root(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-missing-approved-root")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            moved_root = Path(tmp) / "moved-control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-missing-approved-root",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-missing-approved-root",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root / "gate.json",
                    idempotency_key="missing-approved-root-1",
                )
                control_root.rename(moved_root)

                with self.assertRaisesRegex(
                    AuthorizationError,
                    "approved resource root is not stable",
                ):
                    entry_guard_module.begin_side_effect(
                        ticket,
                        expected_entry_id=ticket.entry_id,
                        expected_effect=ticket.effect,
                        expected_resource=control_root / "gate.json",
                    )

    def test_forged_task_ticket_secret_cannot_begin_effect(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-forged-ticket")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-forged-ticket",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-forged-ticket",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="forged-ticket-1",
                )
                forged = replace(ticket, bearer_secret="forged-secret")
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "task ticket capability mismatch",
                ):
                    entry_guard_module.begin_side_effect(
                        forged,
                        expected_entry_id=ticket.entry_id,
                        expected_effect=ticket.effect,
                        expected_resource=control_root,
                    )

    def test_ticket_holder_cannot_derive_winning_lease_secret(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-derived-lease")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-derived-lease",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-derived-lease",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="derived-lease-1",
                )
                lease = entry_guard_module.begin_side_effect(
                    ticket,
                    expected_entry_id=ticket.entry_id,
                    expected_effect=ticket.effect,
                    expected_resource=control_root,
                )
                derived_secret = hmac.new(
                    ticket.bearer_secret.encode("utf-8"),
                    lease.lease_id.encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()
                forged = SideEffectLease(
                    lease_id=lease.lease_id,
                    bearer_secret=derived_secret,
                    ticket_id=ticket.ticket_id,
                    grant_id=ticket.grant_id,
                    authorization_ref=ticket.authorization_ref,
                    entry_id=ticket.entry_id,
                    effect=ticket.effect,
                    resource_scope=ticket.resource_scope,
                    actor=ticket.actor,
                    identity_binding=ticket.identity_binding,
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "side effect lease capability mismatch",
                ):
                    entry_guard_module.finish_side_effect(
                        forged,
                        outcome="SUCCEEDED",
                        evidence_ref="evidence:derived-lease",
                    )
                snapshot = entry_guard_module.finish_side_effect(
                    lease,
                    outcome="IN_DOUBT",
                    evidence_ref="evidence:derived-lease-cleanup",
                )

        self.assertEqual(snapshot.state, "IN_DOUBT")

    def test_forged_task_ticket_authorization_ref_cannot_begin_effect(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-forged-ticket-authref")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-ticket-authref",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-ticket-authref",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="ticket-authref-1",
                )
                forged = replace(ticket, authorization_ref="forged-authref")
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "task ticket capability mismatch",
                ):
                    entry_guard_module.begin_side_effect(
                        forged,
                        expected_entry_id=ticket.entry_id,
                        expected_effect=ticket.effect,
                        expected_resource=control_root,
                    )

    def test_forged_task_ticket_actor_cannot_begin_effect(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-ticket-actor")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-ticket-actor",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-ticket-actor",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="ticket-actor-1",
                )
                forged = replace(
                    ticket,
                    actor=Actor("impostor", "automation", "inv-impostor"),
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "task ticket capability mismatch",
                ):
                    entry_guard_module.begin_side_effect(
                        forged,
                        expected_entry_id=ticket.entry_id,
                        expected_effect=ticket.effect,
                        expected_resource=control_root,
                    )

    def test_forged_task_ticket_identity_binding_cannot_begin_effect(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-ticket-binding")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-ticket-binding",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-ticket-binding",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="ticket-binding-1",
                )
                forged = replace(
                    ticket,
                    identity_binding=IdentityBinding(
                        plan_hash="a" * 64,
                        scope_hash="b" * 64,
                        policy_hash="d" * 64,
                    ),
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "task ticket capability mismatch",
                ):
                    entry_guard_module.begin_side_effect(
                        forged,
                        expected_entry_id=ticket.entry_id,
                        expected_effect=ticket.effect,
                        expected_resource=control_root,
                    )

    def test_forged_task_ticket_idempotency_key_cannot_begin_effect(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-ticket-key")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-ticket-key",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-ticket-key",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="ticket-key-1",
                )
                forged = replace(ticket, idempotency_key="forged-key")
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "task ticket capability mismatch",
                ):
                    entry_guard_module.begin_side_effect(
                        forged,
                        expected_entry_id=ticket.entry_id,
                        expected_effect=ticket.effect,
                        expected_resource=control_root,
                    )

    def test_begin_side_effect_has_exactly_one_winner_under_concurrency(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-concurrent-ticket")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-concurrent-ticket",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-concurrent-ticket",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="concurrent-ticket-1",
                )
                barrier = Barrier(3)

                def contender():
                    barrier.wait()
                    try:
                        return (
                            "winner",
                            entry_guard_module.begin_side_effect(
                                ticket,
                                expected_entry_id=ticket.entry_id,
                                expected_effect=ticket.effect,
                                expected_resource=control_root,
                            ),
                        )
                    except AuthorizationError as error:
                        return ("loser", error)

                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(contender) for _ in range(2)]
                    barrier.wait()
                    results = [future.result(timeout=10) for future in futures]

                winners = [value for state, value in results if state == "winner"]
                losers = [value for state, value in results if state == "loser"]
                self.assertEqual(len(winners), 1)
                self.assertEqual(len(losers), 1)
                snapshot = entry_guard_module.finish_side_effect(
                    winners[0],
                    outcome="IN_DOUBT",
                    evidence_ref="test:concurrent-claim-cleanup",
                )

        self.assertEqual(snapshot.state, "IN_DOUBT")

    def test_control_plane_store_contains_only_bearer_secret_hashes(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-secret-storage")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            phase_secret = "phase-secret-not-for-storage"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-secret-storage",
                bearer_secret=phase_secret,
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-secret-storage",
                    phase_secret,
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="secret-storage-1",
                )
                lease = entry_guard_module.begin_side_effect(
                    ticket,
                    expected_entry_id=ticket.entry_id,
                    expected_effect=ticket.effect,
                    expected_resource=control_root,
                )

            connection = sqlite3.connect(store)
            try:
                authorization_hash = connection.execute(
                    "SELECT secret_hash FROM authorizations WHERE authorization_ref = ?",
                    ("auth-secret-storage",),
                ).fetchone()[0]
                grant_hash = connection.execute(
                    "SELECT secret_hash FROM phase_grants WHERE grant_id = ?",
                    (grant.grant_id,),
                ).fetchone()[0]
                ticket_hash, lease_hash = connection.execute(
                    "SELECT secret_hash, lease_secret_hash FROM task_tickets WHERE ticket_id = ?",
                    (ticket.ticket_id,),
                ).fetchone()
            finally:
                connection.close()

        self.assertEqual(
            authorization_hash,
            hashlib.sha256(phase_secret.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(
            grant_hash,
            hashlib.sha256(grant.bearer_secret.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(
            ticket_hash,
            hashlib.sha256(ticket.bearer_secret.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(
            lease_hash,
            hashlib.sha256(lease.bearer_secret.encode("utf-8")).hexdigest(),
        )
        self.assertNotIn(
            phase_secret,
            {authorization_hash, grant_hash, ticket_hash, lease_hash},
        )

    def test_finish_side_effect_records_terminal_state_once(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-finish")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-finish",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-finish",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="finish-1",
                )
                lease = entry_guard_module.begin_side_effect(
                    ticket,
                    expected_entry_id=ticket.entry_id,
                    expected_effect=ticket.effect,
                    expected_resource=control_root,
                )
                snapshot = entry_guard_module.finish_side_effect(
                    lease,
                    outcome="SUCCEEDED",
                    evidence_ref="evidence:test-finish",
                )
                with self.assertRaisesRegex(AuthorizationError, "not IN_PROGRESS"):
                    entry_guard_module.finish_side_effect(
                        lease,
                        outcome="SUCCEEDED",
                        evidence_ref="evidence:duplicate",
                    )

        self.assertEqual(snapshot.ticket_id, ticket.ticket_id)
        self.assertEqual(snapshot.state, "SUCCEEDED")
        self.assertEqual(snapshot.evidence_ref, "evidence:test-finish")

    def test_forged_side_effect_lease_metadata_cannot_finish_effect(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-forged-lease")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-forged-lease",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-forged-lease",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="forged-lease-1",
                )
                lease = entry_guard_module.begin_side_effect(
                    ticket,
                    expected_entry_id=ticket.entry_id,
                    expected_effect=ticket.effect,
                    expected_resource=control_root,
                )
                forged = replace(lease, authorization_ref="forged-authref")
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "side effect lease capability mismatch",
                ):
                    entry_guard_module.finish_side_effect(
                        forged,
                        outcome="SUCCEEDED",
                        evidence_ref="evidence:forged-lease",
                    )
                forged_actor = replace(
                    lease,
                    actor=Actor("impostor", "automation", "inv-impostor"),
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "side effect lease capability mismatch",
                ):
                    entry_guard_module.finish_side_effect(
                        forged_actor,
                        outcome="SUCCEEDED",
                        evidence_ref="evidence:forged-lease-actor",
                    )
                forged_binding = replace(
                    lease,
                    identity_binding=IdentityBinding(
                        plan_hash="a" * 64,
                        scope_hash="b" * 64,
                        policy_hash="d" * 64,
                    ),
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "side effect lease capability mismatch",
                ):
                    entry_guard_module.finish_side_effect(
                        forged_binding,
                        outcome="SUCCEEDED",
                        evidence_ref="evidence:forged-lease-binding",
                    )
                snapshot = entry_guard_module.finish_side_effect(
                    lease,
                    outcome="IN_DOUBT",
                    evidence_ref="evidence:forged-lease-cleanup",
                )

        self.assertEqual(snapshot.state, "IN_DOUBT")

    def test_crashed_side_effect_is_marked_in_doubt_without_replay(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-crash-recovery")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-crash-recovery",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-crash-recovery",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root,
                    idempotency_key="crash-recovery-1",
                )
                lease = entry_guard_module.begin_side_effect(
                    ticket,
                    expected_entry_id=ticket.entry_id,
                    expected_effect=ticket.effect,
                    expected_resource=control_root,
                )
                del lease

                snapshot = entry_guard_module.mark_side_effect_in_doubt(
                    grant,
                    ticket_id=ticket.ticket_id,
                    evidence_ref="process_exit:137",
                )

                with self.assertRaisesRegex(AuthorizationError, "not ISSUED"):
                    entry_guard_module.begin_side_effect(
                        ticket,
                        expected_entry_id=ticket.entry_id,
                        expected_effect=ticket.effect,
                        expected_resource=control_root,
                    )

            connection = sqlite3.connect(store)
            try:
                event = connection.execute(
                    """
                    SELECT event_type, evidence_ref FROM side_effect_events
                    WHERE ticket_id = ? ORDER BY event_id DESC LIMIT 1
                    """,
                    (ticket.ticket_id,),
                ).fetchone()
            finally:
                connection.close()

        self.assertEqual(snapshot.ticket_id, ticket.ticket_id)
        self.assertEqual(snapshot.state, "IN_DOUBT")
        self.assertEqual(snapshot.evidence_ref, "process_exit:137")
        self.assertEqual(event, ("IN_DOUBT", "process_exit:137"))

    def test_in_doubt_effect_cannot_be_reissued_with_a_new_idempotency_key(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-in-doubt-reissue")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-in-doubt-reissue",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-in-doubt-reissue",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root / "gate.json",
                    idempotency_key="in-doubt-reissue-1",
                )
                entry_guard_module.begin_side_effect(
                    ticket,
                    expected_entry_id=ticket.entry_id,
                    expected_effect=ticket.effect,
                    expected_resource=control_root / "gate.json",
                )
                entry_guard_module.mark_side_effect_in_doubt(
                    grant,
                    ticket_id=ticket.ticket_id,
                    evidence_ref="process_exit:137",
                )

                with self.assertRaisesRegex(
                    AuthorizationError,
                    "unresolved IN_DOUBT side effect",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id=ticket.entry_id,
                        effect=ticket.effect,
                        resource_scope=control_root / "gate.json",
                        idempotency_key="in-doubt-reissue-2",
                    )

    def test_fresh_phase_grant_cannot_replay_an_in_doubt_effect(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-fresh-grant-in-doubt")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-in-doubt-first",
                bearer_secret="first-phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            connection = sqlite3.connect(store)
            try:
                connection.execute(
                    """
                    INSERT INTO authorizations
                    (authorization_ref, phase, actor_id, actor_type, invocation_id,
                     plan_hash, scope_hash, policy_hash, secret_hash, state)
                    VALUES (?, 'P0', ?, ?, ?, ?, ?, ?, ?, 'PENDING')
                    """,
                    (
                        "auth-in-doubt-second",
                        actor.actor_id,
                        actor.actor_type,
                        actor.invocation_id,
                        binding.plan_hash,
                        binding.scope_hash,
                        binding.policy_hash,
                        hashlib.sha256(b"second-phase-secret").hexdigest(),
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                first_grant = entry_guard_module.claim_phase(
                    "auth-in-doubt-first",
                    "first-phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    first_grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root / "gate.json",
                    idempotency_key="first-grant-effect-1",
                )
                entry_guard_module.begin_side_effect(
                    ticket,
                    expected_entry_id=ticket.entry_id,
                    expected_effect=ticket.effect,
                    expected_resource=control_root / "gate.json",
                )
                entry_guard_module.mark_side_effect_in_doubt(
                    first_grant,
                    ticket_id=ticket.ticket_id,
                    evidence_ref="process_exit:137",
                )
                second_grant = entry_guard_module.claim_phase(
                    "auth-in-doubt-second",
                    "second-phase-secret",
                    actor,
                    binding,
                )

                with self.assertRaisesRegex(
                    AuthorizationError,
                    "unresolved IN_DOUBT side effect",
                ):
                    entry_guard_module.issue_task_ticket(
                        second_grant,
                        entry_id=ticket.entry_id,
                        effect=ticket.effect,
                        resource_scope=control_root / "gate.json",
                        idempotency_key="second-grant-effect-1",
                    )

    def test_preissued_ticket_cannot_overlap_or_bypass_in_doubt_effect(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-preissued-in-doubt")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-preissued-in-doubt",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            for entry_id in (
                "callable:p0:write-gate-primary",
                "callable:p0:write-gate-alternate",
            ):
                self._allow_v2_entry(
                    store,
                    entry_id=entry_id,
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_root=control_root,
                )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-preissued-in-doubt",
                    "phase-secret",
                    actor,
                    binding,
                )
                first = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate-primary",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root / "gate.json",
                    idempotency_key="preissued-in-doubt-1",
                )
                second = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate-alternate",
                    effect=first.effect,
                    resource_scope=control_root / "gate.json",
                    idempotency_key="preissued-in-doubt-2",
                )
                entry_guard_module.begin_side_effect(
                    first,
                    expected_entry_id=first.entry_id,
                    expected_effect=first.effect,
                    expected_resource=control_root / "gate.json",
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "side effect is already IN_PROGRESS",
                ):
                    entry_guard_module.begin_side_effect(
                        second,
                        expected_entry_id=second.entry_id,
                        expected_effect=second.effect,
                        expected_resource=control_root / "gate.json",
                    )
                entry_guard_module.mark_side_effect_in_doubt(
                    grant,
                    ticket_id=first.ticket_id,
                    evidence_ref="process_exit:137",
                )

                with self.assertRaisesRegex(
                    AuthorizationError,
                    "unresolved IN_DOUBT side effect",
                ):
                    entry_guard_module.begin_side_effect(
                        second,
                        expected_entry_id=second.entry_id,
                        expected_effect=second.effect,
                        expected_resource=control_root / "gate.json",
                    )

    def test_begin_cannot_race_past_an_in_doubt_transition(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-begin-mark-race")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-begin-mark-race",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-begin-mark-race",
                    "phase-secret",
                    actor,
                    binding,
                )
                first = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root / "gate.json",
                    idempotency_key="begin-mark-race-1",
                )
                second = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id=first.entry_id,
                    effect=first.effect,
                    resource_scope=control_root / "gate.json",
                    idempotency_key="begin-mark-race-2",
                )
                entry_guard_module.begin_side_effect(
                    first,
                    expected_entry_id=first.entry_id,
                    expected_effect=first.effect,
                    expected_resource=control_root / "gate.json",
                )
                barrier = Barrier(3)

                def mark_first_in_doubt() -> tuple[str, object]:
                    barrier.wait()
                    return (
                        "marked",
                        entry_guard_module.mark_side_effect_in_doubt(
                            grant,
                            ticket_id=first.ticket_id,
                            evidence_ref="process_exit:137",
                        ),
                    )

                def try_begin_second() -> tuple[str, object]:
                    barrier.wait()
                    try:
                        lease = entry_guard_module.begin_side_effect(
                            second,
                            expected_entry_id=second.entry_id,
                            expected_effect=second.effect,
                            expected_resource=control_root / "gate.json",
                        )
                    except AuthorizationError as error:
                        return ("denied", error)
                    return ("began", lease)

                with ThreadPoolExecutor(max_workers=2) as pool:
                    mark_future = pool.submit(mark_first_in_doubt)
                    begin_future = pool.submit(try_begin_second)
                    barrier.wait()
                    mark_result = mark_future.result(timeout=10)
                    begin_result = begin_future.result(timeout=10)

        self.assertEqual(mark_result[0], "marked")
        self.assertEqual(mark_result[1].state, "IN_DOUBT")
        self.assertEqual(begin_result[0], "denied")
        self.assertRegex(str(begin_result[1]), "IN_PROGRESS|IN_DOUBT")

    def test_alternate_entry_cannot_replay_an_in_doubt_resource_effect(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-alternate-entry-in-doubt")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-alternate-entry-in-doubt",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            for entry_id in (
                "callable:p0:write-gate-primary",
                "callable:p0:write-gate-alternate",
            ):
                self._allow_v2_entry(
                    store,
                    entry_id=entry_id,
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_root=control_root,
                )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-alternate-entry-in-doubt",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate-primary",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=control_root / "gate.json",
                    idempotency_key="alternate-entry-in-doubt-1",
                )
                entry_guard_module.begin_side_effect(
                    ticket,
                    expected_entry_id=ticket.entry_id,
                    expected_effect=ticket.effect,
                    expected_resource=control_root / "gate.json",
                )
                entry_guard_module.mark_side_effect_in_doubt(
                    grant,
                    ticket_id=ticket.ticket_id,
                    evidence_ref="process_exit:137",
                )

                with self.assertRaisesRegex(
                    AuthorizationError,
                    "unresolved IN_DOUBT side effect",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate-alternate",
                        effect=ticket.effect,
                        resource_scope=control_root / "gate.json",
                        idempotency_key="alternate-entry-in-doubt-2",
                    )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_windows_alternate_data_stream_scope(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-ads")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-ads",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-ads",
                    "phase-secret",
                    actor,
                    binding,
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "unsafe Windows resource path",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=control_root / "gate.json:secret-stream",
                        idempotency_key="ads-1",
                    )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_windows_device_namespace_alias(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-device-alias")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-device-alias",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-device-alias",
                    "phase-secret",
                    actor,
                    binding,
                )
                aliases = (rf"\\.\{control_root}", rf"\\?\{control_root}")
                for index, device_alias in enumerate(aliases, start=1):
                    with self.subTest(device_alias=device_alias):
                        with self.assertRaisesRegex(
                            AuthorizationError,
                            "unsafe Windows resource path",
                        ):
                            entry_guard_module.issue_task_ticket(
                                grant,
                                entry_id="callable:p0:write-gate",
                                effect=SideEffect.WRITE_CONTROL_PLANE,
                                resource_scope=device_alias,
                                idempotency_key=f"device-alias-{index}",
                            )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_dos_device_name_before_resolution(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-dos-device")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-dos-device",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-dos-device",
                    "phase-secret",
                    actor,
                    binding,
                )
                with patch.object(
                    Path,
                    "resolve",
                    side_effect=AssertionError(
                        "filesystem resolution must not run for a DOS device"
                    ),
                ):
                    with self.assertRaisesRegex(
                        AuthorizationError,
                        "reserved Windows path component",
                    ):
                        entry_guard_module.issue_task_ticket(
                            grant,
                            entry_id="callable:p0:write-gate",
                            effect=SideEffect.WRITE_CONTROL_PLANE,
                            resource_scope=control_root / "NUL",
                            idempotency_key="dos-device-1",
                        )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_trailing_dot_before_resolution(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-trailing-dot")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-trailing-dot",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-trailing-dot",
                    "phase-secret",
                    actor,
                    binding,
                )
                with patch.object(
                    Path,
                    "resolve",
                    side_effect=AssertionError(
                        "filesystem resolution must not run for a trailing-dot alias"
                    ),
                ):
                    with self.assertRaisesRegex(
                        AuthorizationError,
                        "reserved Windows path component",
                    ):
                        entry_guard_module.issue_task_ticket(
                            grant,
                            entry_id="callable:p0:write-gate",
                            effect=SideEffect.WRITE_CONTROL_PLANE,
                            resource_scope=control_root / "gate.json.",
                            idempotency_key="trailing-dot-1",
                        )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_trailing_space_before_resolution(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-trailing-space")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-trailing-space",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-trailing-space",
                    "phase-secret",
                    actor,
                    binding,
                )
                with patch.object(
                    Path,
                    "resolve",
                    side_effect=AssertionError(
                        "filesystem resolution must not run for a trailing-space alias"
                    ),
                ):
                    with self.assertRaisesRegex(
                        AuthorizationError,
                        "reserved Windows path component",
                    ):
                        entry_guard_module.issue_task_ticket(
                            grant,
                            entry_id="callable:p0:write-gate",
                            effect=SideEffect.WRITE_CONTROL_PLANE,
                            resource_scope=control_root / "gate.json ",
                            idempotency_key="trailing-space-1",
                        )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_parent_trailing_dot_before_resolution(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-parent-trailing-dot")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-parent-trailing-dot",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-parent-trailing-dot",
                    "phase-secret",
                    actor,
                    binding,
                )
                with patch.object(
                    Path,
                    "resolve",
                    side_effect=AssertionError(
                        "filesystem resolution must not run for a trailing-dot parent"
                    ),
                ):
                    with self.assertRaisesRegex(
                        AuthorizationError,
                        "reserved Windows path component",
                    ):
                        entry_guard_module.issue_task_ticket(
                            grant,
                            entry_id="callable:p0:write-gate",
                            effect=SideEffect.WRITE_CONTROL_PLANE,
                            resource_scope=control_root / "nested." / "gate.json",
                            idempotency_key="parent-trailing-dot-1",
                        )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_windows_drive_relative_scope(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-drive-relative")
        workspace_root = Path.cwd().resolve()
        drive_relative = (
            f"{workspace_root.drive}"
            r"research_state\control_plane\p0"
        )

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-drive-relative",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=workspace_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-drive-relative",
                    "phase-secret",
                    actor,
                    binding,
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "drive-relative",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=drive_relative,
                        idempotency_key="drive-relative-1",
                    )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_windows_root_relative_scope(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-root-relative")
        workspace_root = Path.cwd().resolve()
        root_relative = str(workspace_root)[len(workspace_root.drive):]

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-root-relative",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=workspace_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-root-relative",
                    "phase-secret",
                    actor,
                    binding,
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "root-relative",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=root_relative,
                        idempotency_key="root-relative-1",
                    )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_unapproved_unc_scope(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-unc")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-unc",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-unc",
                    "phase-secret",
                    actor,
                    binding,
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "unsafe Windows resource path",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=r"\\example.invalid\share\gate.json",
                        idempotency_key="unc-1",
                    )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_mixed_separator_unc_before_resolution(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-mixed-unc")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-mixed-unc",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-mixed-unc",
                    "phase-secret",
                    actor,
                    binding,
                )
                with patch.object(
                    Path,
                    "resolve",
                    side_effect=AssertionError(
                        "filesystem resolution must not run for a UNC namespace"
                    ),
                ):
                    with self.assertRaisesRegex(
                        AuthorizationError,
                        "UNC paths are not approved",
                    ):
                        entry_guard_module.issue_task_ticket(
                            grant,
                            entry_id="callable:p0:write-gate",
                            effect=SideEffect.WRITE_CONTROL_PLANE,
                            resource_scope=r"\/example.invalid\share\gate.json",
                            idempotency_key="mixed-unc-1",
                        )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_task_ticket_rejects_inverse_mixed_unc_before_resolution(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-inverse-mixed-unc")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-inverse-mixed-unc",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-inverse-mixed-unc",
                    "phase-secret",
                    actor,
                    binding,
                )
                with patch.object(
                    Path,
                    "resolve",
                    side_effect=AssertionError(
                        "filesystem resolution must not run for a UNC namespace"
                    ),
                ):
                    with self.assertRaisesRegex(
                        AuthorizationError,
                        "UNC paths are not approved",
                    ):
                        entry_guard_module.issue_task_ticket(
                            grant,
                            entry_id="callable:p0:write-gate",
                            effect=SideEffect.WRITE_CONTROL_PLANE,
                            resource_scope=r"/\example.invalid\share\gate.json",
                            idempotency_key="inverse-mixed-unc-1",
                        )

    def test_task_ticket_rejects_parent_traversal_even_within_scope(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-dotdot")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-dotdot",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-dotdot",
                    "phase-secret",
                    actor,
                    binding,
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "parent traversal",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=control_root / "nested" / ".." / "gate.json",
                        idempotency_key="dotdot-1",
                    )

    def test_task_ticket_rejects_symlink_escape_at_issue(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-symlink-issue")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            outside = Path(tmp) / "outside"
            control_root.mkdir()
            outside.mkdir()
            link = control_root / "jump"
            try:
                link.symlink_to(outside, target_is_directory=True)
            except OSError as error:
                self.skipTest(f"symlink creation unavailable: {error}")
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-symlink-issue",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-symlink-issue",
                    "phase-secret",
                    actor,
                    binding,
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "resource scope is not approved",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=link / "gate.json",
                        idempotency_key="symlink-issue-1",
                    )

    @unittest.skipUnless(os.name == "nt", "Windows path semantics")
    def test_unsafe_approved_root_is_rejected_before_filesystem_access(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-unsafe-approved-root")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            control_root.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-unsafe-approved-root",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            connection = sqlite3.connect(store)
            try:
                connection.execute(
                    """
                    INSERT INTO entry_permissions(entry_id, phase, effect, resource_root)
                    VALUES (?, 'P0', ?, ?)
                    """,
                    (
                        "callable:p0:write-gate",
                        SideEffect.WRITE_CONTROL_PLANE.value,
                        r"\/example.invalid\share\control-state",
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-unsafe-approved-root",
                    "phase-secret",
                    actor,
                    binding,
                )
                with patch.object(
                    Path,
                    "is_symlink",
                    side_effect=AssertionError(
                        "unsafe approved roots must be rejected before filesystem access"
                    ),
                ):
                    with self.assertRaisesRegex(
                        AuthorizationError,
                        "UNC paths are not approved",
                    ):
                        entry_guard_module.issue_task_ticket(
                            grant,
                            entry_id="callable:p0:write-gate",
                            effect=SideEffect.WRITE_CONTROL_PLANE,
                            resource_scope=control_root / "gate.json",
                            idempotency_key="unsafe-approved-root-1",
                        )

    @unittest.skipUnless(os.name == "nt", "Windows reparse semantics")
    def test_task_ticket_rejects_reparse_replacement_of_approved_root(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-root-reparse")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            original_root = Path(tmp) / "original-control-state"
            outside = Path(tmp) / "outside"
            control_root.mkdir()
            outside.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-root-reparse",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            control_root.rename(original_root)
            try:
                control_root.symlink_to(outside, target_is_directory=True)
            except OSError as error:
                original_root.rename(control_root)
                self.skipTest(f"symlink creation unavailable: {error}")

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-root-reparse",
                    "phase-secret",
                    actor,
                    binding,
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "approved resource root is not stable",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=control_root / "gate.json",
                        idempotency_key="root-reparse-1",
                    )

    def test_task_ticket_rejects_reparse_replacement_of_root_parent(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-root-parent-reparse")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            container = Path(tmp) / "container"
            original_container = Path(tmp) / "original-container"
            control_root = container / "control-state"
            outside = Path(tmp) / "outside"
            control_root.mkdir(parents=True)
            (outside / "control-state").mkdir(parents=True)
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-root-parent-reparse",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            container.rename(original_container)
            try:
                container.symlink_to(outside, target_is_directory=True)
            except OSError as error:
                original_container.rename(container)
                self.skipTest(f"symlink creation unavailable: {error}")

            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-root-parent-reparse",
                    "phase-secret",
                    actor,
                    binding,
                )
                with self.assertRaisesRegex(
                    AuthorizationError,
                    "approved resource root is not stable",
                ):
                    entry_guard_module.issue_task_ticket(
                        grant,
                        entry_id="callable:p0:write-gate",
                        effect=SideEffect.WRITE_CONTROL_PLANE,
                        resource_scope=control_root / "gate.json",
                        idempotency_key="root-parent-reparse-1",
                    )

    def test_begin_side_effect_rejects_symlink_swap_after_issue(self) -> None:
        binding = IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        actor = Actor("tester", "human", "inv-symlink-begin")

        with TemporaryDirectory() as tmp:
            store = Path(tmp) / "control-plane.sqlite3"
            control_root = Path(tmp) / "control-state"
            parent = control_root / "parent"
            outside = Path(tmp) / "outside"
            parent.mkdir(parents=True)
            outside.mkdir()
            self._provision_v2_authorization(
                store,
                authorization_ref="auth-symlink-begin",
                bearer_secret="phase-secret",
                actor=actor,
                binding=binding,
            )
            self._allow_v2_entry(
                store,
                entry_id="callable:p0:write-gate",
                effect=SideEffect.WRITE_CONTROL_PLANE,
                resource_root=control_root,
            )
            resource = parent / "gate.json"
            with patch.object(entry_guard_module, "_CONTROL_PLANE_DB_PATH", store):
                grant = entry_guard_module.claim_phase(
                    "auth-symlink-begin",
                    "phase-secret",
                    actor,
                    binding,
                )
                ticket = entry_guard_module.issue_task_ticket(
                    grant,
                    entry_id="callable:p0:write-gate",
                    effect=SideEffect.WRITE_CONTROL_PLANE,
                    resource_scope=resource,
                    idempotency_key="symlink-begin-1",
                )
                original_parent = control_root / "original-parent"
                parent.rename(original_parent)
                try:
                    parent.symlink_to(outside, target_is_directory=True)
                except OSError as error:
                    original_parent.rename(parent)
                    self.skipTest(f"symlink creation unavailable: {error}")

                with patch.object(
                    Path,
                    "resolve",
                    side_effect=AssertionError(
                        "reparse parents must be rejected before path resolution"
                    ),
                ):
                    with self.assertRaisesRegex(
                        AuthorizationError,
                        "resource path contains a reparse point",
                    ):
                        entry_guard_module.begin_side_effect(
                            ticket,
                            expected_entry_id=ticket.entry_id,
                            expected_effect=ticket.effect,
                            expected_resource=resource,
                        )

if __name__ == "__main__":
    unittest.main()
