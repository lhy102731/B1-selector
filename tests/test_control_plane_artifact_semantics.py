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
                {key: value for key, value in payload.items() if key != "freeze_payload_sha256"}
            )

            with self.assertRaises(ArtifactSemanticError):
                self._validate(payload, root)


if __name__ == "__main__":
    unittest.main()
