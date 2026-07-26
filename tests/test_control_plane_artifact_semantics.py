from __future__ import annotations

import json
import hashlib
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from research_automation.control_plane.artifact_semantics import (
    ArtifactSemanticError,
    parse_strict_json,
    validate_code_freeze_manifest,
    validate_final_inventory,
    validate_implementation_baseline,
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
                "external_metadata": {"state": "Ready"},
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

if __name__ == "__main__":
    unittest.main()
