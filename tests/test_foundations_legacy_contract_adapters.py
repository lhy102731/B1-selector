from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from research_automation.control_plane.contracts import canonical_json
from research_automation.control_plane.task_reports import build_task_report_v2
from research_automation.foundations.legacy_contract_adapters import (
    LegacyContractAdapterError,
    LegacyKBaseCatalogEntryView,
    LegacyKBaseArtifactReference,
    LegacyP0TaskReportV2View,
    preserve_legacy_kbase_artifact_reference,
    read_legacy_kbase_catalog_entry,
    read_legacy_p0_task_report_v2,
)


class LegacyContractAdapterTests(unittest.TestCase):
    @staticmethod
    def _catalog_entry(*, catalog_schema_version: int = 1) -> dict[str, object]:
        return {
            "catalog_schema_version": catalog_schema_version,
            "source_id": "source-1",
            "object_type": "source_packet",
            "title": "Source One",
            "aliases": ["S1"],
            "people": [],
            "family_id": None,
            "voice_role": "primary",
            "source_type": "book",
            "date_start": None,
            "date_end": None,
            "topics": ["volume"],
            "summary": "A source-only summary.",
            "reliability": "high",
            "review_status": "source_only",
            "available_layers": ["summary", "evidence"],
            "warnings": [],
            "parent_ids": [],
            "paths": {"packet": "wiki/source-1.json"},
            "content_fingerprint": "b" * 64,
            "source_schema_version": 1,
        }

    @staticmethod
    def _task_report_bytes() -> bytes:
        report = build_task_report_v2(
            {
                "plan_version": "V3.4.2-P0R2",
                "phase": "P0",
                "task_id": "P0R2-TYPED-READER",
                "attempt_id": "p0r2-attempt-test",
                "authorization_ref": "authorization-test",
                "ticket_id": "ticket-test",
                "identity_binding": {
                    "plan_hash": "1" * 64,
                    "scope_hash": "2" * 64,
                    "instruction_policy_hash": "3" * 64,
                },
                "objective": "Exercise the read-only typed adapter.",
                "dependencies": [],
                "idempotency_key": "typed-reader-test",
                "task_spec_ref": "research_state/control_plane/p0r2/task_specs/test.json",
                "task_spec_sha256": "4" * 64,
                "requirements": {
                    "required_test_receipt_ids": [],
                    "required_review_receipt_ids": [],
                    "required_evidence_ids": [],
                },
                "allowed_files": ["research_automation/foundations/"],
                "forbidden_files": ["data/"],
                "baseline_ref": "research_state/control_plane/p0r2/baseline.json",
                "baseline_sha256": "5" * 64,
                "input_evidence_refs": [],
                "test_receipts": [],
                "review_receipts": [],
                "review_findings": [],
                "changed_files": [],
                "external_invocations": [],
                "side_effect_summary": {"observed": [], "unauthorized": []},
                "ticket_state": "SUCCEEDED",
                "started_at": "2026-07-28T10:00:00+08:00",
                "completed_at": "2026-07-28T10:01:00+08:00",
            }
        )
        return canonical_json(report).encode("utf-8")

    def test_catalog_adapter_does_not_load_the_legacy_ag2_runtime(self) -> None:
        raw = json.dumps(
            self._catalog_entry(),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        real_import = __import__

        def reject_legacy_runtime(name: str, *args: object, **kwargs: object) -> object:
            if name == "ag2_research" or name.startswith("ag2_research."):
                raise AssertionError("legacy AG2 runtime import is forbidden")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=reject_legacy_runtime):
            read_legacy_kbase_catalog_entry(raw)

    def test_kbase_hash_is_preserved_without_promotion_to_artifact_identity(self) -> None:
        historical_hash = "a" * 64

        reference = preserve_legacy_kbase_artifact_reference(historical_hash)

        self.assertIsInstance(reference, LegacyKBaseArtifactReference)
        self.assertEqual(reference.legacy_artifact_id, historical_hash)
        self.assertFalse(reference.authorization_eligible)
        self.assertFalse(hasattr(reference, "artifact_id"))

    def test_kbase_catalog_v1_is_validated_then_exposed_as_a_typed_view(self) -> None:
        raw = json.dumps(
            self._catalog_entry(),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        view = read_legacy_kbase_catalog_entry(raw)

        self.assertIsInstance(view, LegacyKBaseCatalogEntryView)
        self.assertEqual(view.catalog_schema_version, 1)
        self.assertEqual(view.content_fingerprint, "b" * 64)
        self.assertEqual(view.paths[0].role, "packet")

    def test_unknown_legacy_kbase_catalog_version_fails_closed(self) -> None:
        raw = json.dumps(
            self._catalog_entry(catalog_schema_version=2),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        with self.assertRaisesRegex(LegacyContractAdapterError, "unsupported"):
            read_legacy_kbase_catalog_entry(raw)

    def test_kbase_source_contract_keeps_the_legacy_inference_boundary(self) -> None:
        payload = self._catalog_entry()
        payload["paths"] = {"factor": "wiki/forbidden.json"}
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")

        with self.assertRaisesRegex(LegacyContractAdapterError, "derivation fields"):
            read_legacy_kbase_catalog_entry(raw)

    def test_p0_task_report_is_sealed_validated_before_typed_read(self) -> None:
        view = read_legacy_p0_task_report_v2(self._task_report_bytes())

        self.assertIsInstance(view, LegacyP0TaskReportV2View)
        self.assertEqual(view.schema_version, "control_plane.task_report.v2")
        self.assertEqual(view.outcome, "PASS")
        self.assertIsInstance(view.allowed_files, tuple)

    def test_p0_task_report_tampering_is_rejected_by_the_sealed_validator(self) -> None:
        payload = json.loads(self._task_report_bytes())
        payload["objective"] = "Tampered after the sealed hash was computed."
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")

        with self.assertRaisesRegex(LegacyContractAdapterError, "sha256 mismatch"):
            read_legacy_p0_task_report_v2(raw)


if __name__ == "__main__":
    unittest.main()
