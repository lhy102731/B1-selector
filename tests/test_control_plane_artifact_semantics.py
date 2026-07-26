from __future__ import annotations

import json
import hashlib
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from research_automation.control_plane.artifact_semantics import (
    ArtifactSemanticError,
    parse_strict_json,
    validate_code_freeze_manifest,
    validate_final_inventory,
    validate_implementation_baseline,
    validate_reviewed_entry_policy,
    validate_scheduler_inventory,
)
from research_automation.control_plane.contracts import (
    canonical_json,
    canonical_sha256,
)


ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = (
    ROOT
    / "research_state"
    / "control_plane"
    / "p0r2"
    / "implementation_baseline_v342_p0r2.json"
)


class StrictArtifactJsonTests(unittest.TestCase):
    def test_duplicate_keys_are_rejected(self) -> None:
        with self.assertRaises(ArtifactSemanticError):
            parse_strict_json(
                b'{"schema_version":"x","schema_version":"y"}',
                artifact_name="fixture",
            )

    def test_nonfinite_constants_are_rejected(self) -> None:
        with self.assertRaises(ArtifactSemanticError):
            parse_strict_json(b'{"value":NaN}', artifact_name="fixture")

    def test_nested_json_depth_is_bounded(self) -> None:
        raw = ("{" * 70) + ("}" * 70)
        with self.assertRaises(ArtifactSemanticError):
            parse_strict_json(raw.encode("ascii"), artifact_name="fixture")


class ImplementationBaselineTests(unittest.TestCase):
    def _payload(self) -> dict[str, object]:
        return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))

    def _validate(self, payload: dict[str, object]) -> None:
        validate_implementation_baseline(
            canonical_json(payload).encode("utf-8"),
            expected_plan_version="V3.4.2-P0R2",
            expected_phase="P0",
            expected_attempt_id="p0r2-attempt-001",
            repository_root=ROOT,
        )

    def test_existing_p0r2_baseline_is_semantically_valid(self) -> None:
        self._validate(self._payload())

    def test_nested_file_state_tamper_is_rejected_even_after_outer_rehash(self) -> None:
        payload = self._payload()
        baseline = payload["baseline"]
        self.assertIsInstance(baseline, dict)
        states = baseline["file_states"]
        self.assertIsInstance(states, dict)
        first_path = ".gitattributes"
        first_state = states[first_path]
        self.assertIsInstance(first_state, dict)
        first_state["bytes"] += 1

        payload["baseline_payload_sha256"] = canonical_sha256(baseline)
        with self.assertRaises(ArtifactSemanticError):
            self._validate(payload)

    def test_file_state_count_must_match_mapping(self) -> None:
        payload = self._payload()
        baseline = payload["baseline"]
        self.assertIsInstance(baseline, dict)
        baseline["file_state_count"] += 1
        with self.assertRaises(ArtifactSemanticError):
            self._validate(payload)

    def test_started_or_large_data_flags_are_rejected(self) -> None:
        for field_name in (
            "large_data_scanned",
            "production_or_research_task_started",
        ):
            with self.subTest(field_name=field_name):
                payload = self._payload()
                baseline = payload["baseline"]
                self.assertIsInstance(baseline, dict)
                baseline[field_name] = True
                with self.assertRaises(ArtifactSemanticError):
                    self._validate(payload)


class CodeFreezeManifestTests(unittest.TestCase):
    identity = {
        "plan_hash": "a" * 64,
        "scope_hash": "b" * 64,
        "instruction_policy_hash": "c" * 64,
    }

    def _payload(self, root: Path) -> dict[str, object]:
        source = root / "research_automation" / "worker.py"
        source.parent.mkdir(parents=True)
        source.write_bytes(b"print('frozen')\n")
        files = [
            {
                "path": "research_automation/worker.py",
                "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                "bytes": source.stat().st_size,
            }
        ]
        payload: dict[str, object] = {
            "schema_version": "control_plane.code_freeze_manifest.v1",
            "plan_version": "V3.4.2-P0R2",
            "phase": "P0",
            "attempt_id": "p0r2-attempt-001",
            "identity_binding": dict(self.identity),
            "files": files,
            "file_count": len(files),
        }
        payload["freeze_payload_sha256"] = canonical_sha256(payload)
        return payload

    def _validate(self, payload: dict[str, object], root: Path) -> None:
        validate_code_freeze_manifest(
            canonical_json(payload).encode("utf-8"),
            expected_plan_version="V3.4.2-P0R2",
            expected_phase="P0",
            expected_attempt_id="p0r2-attempt-001",
            expected_identity=self.identity,
            repository_root=root,
        )

    def test_valid_freeze_manifest_binds_current_source_bytes(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._validate(self._payload(root), root)

    def test_source_drift_after_freeze_is_rejected(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self._payload(root)
            (root / "research_automation" / "worker.py").write_bytes(b"changed")

            with self.assertRaises(ArtifactSemanticError):
                self._validate(payload, root)

    def test_validation_rejects_a_reparse_parent_in_the_frozen_path(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self._payload(root)
            reparse_parent = root / "research_automation"

            with patch.object(
                Path,
                "is_junction",
                autospec=True,
                side_effect=lambda path: path == reparse_parent,
            ):
                with self.assertRaisesRegex(
                    ArtifactSemanticError,
                    "reparse",
                ):
                    self._validate(payload, root)

    def test_freeze_rejects_data_paths_and_count_drift(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self._payload(root)
            files = payload["files"]
            self.assertIsInstance(files, list)
            files[0]["path"] = "data/worker.py"
            payload["file_count"] = 2
            payload["freeze_payload_sha256"] = canonical_sha256(
                {
                    key: value
                    for key, value in payload.items()
                    if key != "freeze_payload_sha256"
                }
            )

            with self.assertRaises(ArtifactSemanticError):
                self._validate(payload, root)


class FinalInventoryTests(unittest.TestCase):
    identity = CodeFreezeManifestTests.identity
    seam_specs = (
        (
            "research_automation/autonomous_runner.py",
            "callable:research_automation.autonomous_runner:AutonomousRunnerV1.run",
            "AutonomousRunnerV1.run",
        ),
        (
            "research_automation/discovery_execution_bridge.py",
            "callable:research_automation.discovery_execution_bridge:execute_plan",
            "execute_plan",
        ),
        (
            "research_automation/kbase_ag2_full_cycle.py",
            "callable:research_automation.kbase_ag2_full_cycle:run_kbase_ag2_full_cycle",
            "run_kbase_ag2_full_cycle",
        ),
    )

    def _freeze(self, root: Path) -> dict[str, object]:
        files: list[dict[str, object]] = []
        for path, _, _ in self.seam_specs:
            target = root.joinpath(*path.split("/"))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(f"# {path}\n".encode("utf-8"))
            raw = target.read_bytes()
            files.append(
                {
                    "path": path,
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "bytes": len(raw),
                }
            )
        files.sort(key=lambda item: str(item["path"]))
        payload: dict[str, object] = {
            "schema_version": "control_plane.code_freeze_manifest.v1",
            "plan_version": "V3.4.2-P0R2",
            "phase": "P0",
            "attempt_id": "p0r2-attempt-001",
            "identity_binding": dict(self.identity),
            "files": files,
            "file_count": len(files),
        }
        payload["freeze_payload_sha256"] = canonical_sha256(payload)
        return validate_code_freeze_manifest(
            canonical_json(payload).encode("utf-8"),
            expected_plan_version="V3.4.2-P0R2",
            expected_phase="P0",
            expected_attempt_id="p0r2-attempt-001",
            expected_identity=self.identity,
            repository_root=root,
        )

    def _inventory(
        self,
        freeze: dict[str, object],
    ) -> dict[str, object]:
        freeze_files = freeze["files"]
        self.assertIsInstance(freeze_files, list)
        digests = {
            item["path"]: item["sha256"]
            for item in freeze_files
        }
        entries: list[dict[str, object]] = [
            {
                "entry_id": "external:scheduler:/A\u80a1\u9009\u80a1",
                "path": "/A\u80a1\u9009\u80a1",
                "kind": "external_scheduler",
                "callable_name": "D:/workspace/run_select.bat",
                "actor_type": "scheduler",
                "content_sha256": "d" * 64,
                "disposition": "PRODUCTION_DAILY",
                "trust_state": "production_daily",
                "declared_side_effects": [],
                "declared_phase": None,
                "resource_roots": [],
                "external_metadata": {
                    "acl_summary": "owner=BUILTIN\\Administrators;sddl=O:BA",
                    "action": "D:/workspace/run_select.bat",
                    "principal": "Administrator|Interactive|Limited",
                    "state": "Ready",
                    "trigger": (
                        "MSFT_TaskDailyTrigger|start=2026-03-16T20:00:00|"
                        "days_interval=1|enabled=true"
                    ),
                },
                "source": "external_scheduler_inventory",
            }
        ]
        for path, entry_id, callable_name in self.seam_specs:
            entries.append(
                {
                    "entry_id": entry_id,
                    "path": path,
                    "kind": "python_callable",
                    "callable_name": callable_name,
                    "actor_type": "legacy_runner",
                    "content_sha256": digests[path],
                    "disposition": "LEGACY_UNAUDITED",
                    "trust_state": "legacy_unaudited",
                    "declared_side_effects": ["RUN_RESEARCH"],
                    "declared_phase": None,
                    "resource_roots": [],
                    "external_metadata": {},
                    "source": "required_import_seam",
                }
            )
        entries.sort(key=lambda item: (item["kind"], item["path"], item["entry_id"]))
        payload: dict[str, object] = {
            "schema_version": "control_plane.entry_inventory.v2",
            "plan_version": "V3.4.2-P0R2",
            "phase": "P0",
            "attempt_id": "p0r2-attempt-001",
            "identity_binding": dict(self.identity),
            "freeze_payload_sha256": freeze["freeze_payload_sha256"],
            "entries": entries,
            "entry_count": len(entries),
        }
        payload["inventory_payload_sha256"] = canonical_sha256(payload)
        return payload

    def _validate(
        self,
        inventory: dict[str, object],
        freeze: dict[str, object],
    ) -> None:
        validate_final_inventory(
            canonical_json(inventory).encode("utf-8"),
            expected_plan_version="V3.4.2-P0R2",
            expected_phase="P0",
            expected_attempt_id="p0r2-attempt-001",
            expected_identity=self.identity,
            freeze_manifest=freeze,
        )

    def test_valid_inventory_matches_freeze_and_required_seams(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            freeze = self._freeze(root)
            self._validate(self._inventory(freeze), freeze)

    def test_inventory_rejects_missing_seam_or_freeze_path(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            freeze = self._freeze(root)
            inventory = self._inventory(freeze)
            entries = inventory["entries"]
            self.assertIsInstance(entries, list)
            entries.pop()
            inventory["entry_count"] = len(entries)
            inventory["inventory_payload_sha256"] = canonical_sha256(
                {
                    key: value
                    for key, value in inventory.items()
                    if key != "inventory_payload_sha256"
                }
            )

            with self.assertRaises(ArtifactSemanticError):
                self._validate(inventory, freeze)

    def test_inventory_rejects_duplicate_entry_ids(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            freeze = self._freeze(root)
            inventory = self._inventory(freeze)
            entries = inventory["entries"]
            self.assertIsInstance(entries, list)
            entries[1]["entry_id"] = entries[0]["entry_id"]

            with self.assertRaises(ArtifactSemanticError):
                self._validate(inventory, freeze)


class ReviewedEntryPolicyTests(unittest.TestCase):
    identity = FinalInventoryTests.identity

    def _artifacts(
        self,
        root: Path,
    ) -> tuple[dict[str, object], dict[str, object]]:
        helper = FinalInventoryTests()
        freeze = helper._freeze(root)
        inventory = helper._inventory(freeze)
        validate_final_inventory(
            canonical_json(inventory).encode("utf-8"),
            expected_plan_version="V3.4.2-P0R2",
            expected_phase="P0",
            expected_attempt_id="p0r2-attempt-001",
            expected_identity=self.identity,
            freeze_manifest=freeze,
        )
        return freeze, inventory

    def _policy(self, inventory: dict[str, object]) -> dict[str, object]:
        entries = json.loads(canonical_json(inventory["entries"]))
        payload: dict[str, object] = {
            "schema_version": "control_plane.entry_policy.v1",
            "plan_version": "V3.4.2-P0R2",
            "phase": "P0",
            "attempt_id": "p0r2-attempt-001",
            "identity_binding": dict(self.identity),
            "review_state": "APPROVED",
            "reviewer_id": "independent-reviewer",
            "review_receipt_sha256": "e" * 64,
            "inventory_payload_sha256": inventory[
                "inventory_payload_sha256"
            ],
            "entries": entries,
            "entry_count": len(entries),
        }
        payload["policy_payload_sha256"] = canonical_sha256(payload)
        return payload

    def _validate(
        self,
        policy: dict[str, object],
        inventory: dict[str, object],
    ) -> None:
        validate_reviewed_entry_policy(
            canonical_json(policy).encode("utf-8"),
            expected_plan_version="V3.4.2-P0R2",
            expected_phase="P0",
            expected_attempt_id="p0r2-attempt-001",
            expected_identity=self.identity,
            final_inventory=inventory,
        )

    def test_valid_reviewed_policy_matches_inventory_exactly(self) -> None:
        with TemporaryDirectory() as tmp:
            _, inventory = self._artifacts(Path(tmp))
            self._validate(self._policy(inventory), inventory)

    def test_scanner_cannot_self_approve_policy(self) -> None:
        with TemporaryDirectory() as tmp:
            _, inventory = self._artifacts(Path(tmp))
            policy = self._policy(inventory)
            policy["reviewer_id"] = "scanner"
            policy["policy_payload_sha256"] = canonical_sha256(
                {
                    key: value
                    for key, value in policy.items()
                    if key != "policy_payload_sha256"
                }
            )

            with self.assertRaises(ArtifactSemanticError):
                self._validate(policy, inventory)

    def test_reviewed_policy_entry_drift_is_rejected(self) -> None:
        with TemporaryDirectory() as tmp:
            _, inventory = self._artifacts(Path(tmp))
            policy = self._policy(inventory)
            entries = policy["entries"]
            self.assertIsInstance(entries, list)
            entries[0]["trust_state"] = "forged"
            policy["policy_payload_sha256"] = canonical_sha256(
                {
                    key: value
                    for key, value in policy.items()
                    if key != "policy_payload_sha256"
                }
            )

            with self.assertRaises(ArtifactSemanticError):
                self._validate(policy, inventory)

    def test_old_source_tree_policy_contract_is_rejected(self) -> None:
        old_policy = (
            ROOT
            / "research_automation"
            / "control_plane"
            / "entry_policy.json"
        ).read_bytes()
        with self.assertRaises(ArtifactSemanticError):
            validate_reviewed_entry_policy(
                old_policy,
                expected_plan_version="V3.4.2-P0R2",
                expected_phase="P0",
                expected_attempt_id="p0r2-attempt-001",
                expected_identity=self.identity,
                final_inventory={"entries": [], "inventory_payload_sha256": "f" * 64},
            )


class SchedulerInventoryTests(unittest.TestCase):
    def _inventory(self, root: Path) -> dict[str, object]:
        helper = FinalInventoryTests()
        freeze = helper._freeze(root)
        inventory = helper._inventory(freeze)
        validate_final_inventory(
            canonical_json(inventory).encode("utf-8"),
            expected_plan_version="V3.4.2-P0R2",
            expected_phase="P0",
            expected_attempt_id="p0r2-attempt-001",
            expected_identity=FinalInventoryTests.identity,
            freeze_manifest=freeze,
        )
        return inventory

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": "control_plane.external_scheduler_inventory.v1",
            "phase": "P0",
            "observed_at": "2026-07-26T01:18:14+08:00",
            "collection_mode": "READ_ONLY",
            "task_path": "\\A\u80a1\u9009\u80a1",
            "task_state": "Ready",
            "operational_classification": "PRODUCTION_DAILY",
            "task_xml": {
                "path": "C:/Windows/System32/Tasks/A\u80a1\u9009\u80a1",
                "sha256": "d" * 64,
            },
            "action": {
                "execute": "D:/workspace/run_select.bat",
                "arguments": None,
                "working_directory": None,
                "content_sha256": "f" * 64,
            },
            "principal": {
                "user_id": "Administrator",
                "logon_type": "Interactive",
                "run_level": "Limited",
            },
            "trigger": {
                "type": "MSFT_TaskDailyTrigger",
                "start_boundary": "2026-03-16T20:00:00",
                "enabled": True,
                "days_interval": 1,
            },
            "acl": {
                "owner": "BUILTIN\\Administrators",
                "sddl": "O:BA",
            },
            "altered_by_p0": False,
            "unresolved_risk": "Writable production chain remains explicit.",
        }

    def test_scheduler_status_is_derived_from_bound_evidence(self) -> None:
        with TemporaryDirectory() as tmp:
            inventory = self._inventory(Path(tmp))
            _, status = validate_scheduler_inventory(
                canonical_json(self._payload()).encode("utf-8"),
                expected_phase="P0",
                final_inventory=inventory,
            )
            self.assertEqual(status, "VERIFIED")

    def test_unavailable_scheduler_evidence_derives_unknown(self) -> None:
        with TemporaryDirectory() as tmp:
            inventory = self._inventory(Path(tmp))
            payload = self._payload()
            payload["collection_mode"] = "UNAVAILABLE"

            _, status = validate_scheduler_inventory(
                canonical_json(payload).encode("utf-8"),
                expected_phase="P0",
                final_inventory=inventory,
            )
            self.assertEqual(status, "UNKNOWN")

    def test_scheduler_inventory_must_match_final_inventory(self) -> None:
        with TemporaryDirectory() as tmp:
            inventory = self._inventory(Path(tmp))
            payload = self._payload()
            action = payload["action"]
            self.assertIsInstance(action, dict)
            action["execute"] = "D:/workspace/forged.bat"

            with self.assertRaises(ArtifactSemanticError):
                validate_scheduler_inventory(
                    canonical_json(payload).encode("utf-8"),
                    expected_phase="P0",
                    final_inventory=inventory,
                )

if __name__ == "__main__":
    unittest.main()
