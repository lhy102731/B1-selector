from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.audit_addendum import (
    AuditAddendumConflict,
    build_historical_audit_addendum,
)
from research_automation.control_plane.contracts import Actor, Phase, SideEffect


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"


class HistoricalAuditAddendumTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.authority_path = self.root / "authority.sqlite3"
        self.operational_path = self.root / "operational.sqlite3"
        self.paths = patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=self.authority_path,
            _OPERATIONAL_STORE_PATH=self.operational_path,
        )
        self.paths.start()
        stores_module._expected_schema_sha256.cache_clear()
        stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
        self.actor = Actor("audit-operator", "human", "audit-invocation")
        authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
        identity = stores_module.AuthorityIdentity("a" * 64, "b" * 64, "c" * 64)
        p0_envelope = authority._provision_authorization(
            phase=Phase.P0,
            attempt_id="p0-policy-test",
            actor=self.actor,
            identity=identity,
            expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
            allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
        )
        p0_grant = authority.claim_authorization(
            p0_envelope,
            expected_phase=Phase.P0,
            expected_attempt_id="p0-policy-test",
            actor=self.actor,
            identity=identity,
        )
        policy_ticket = authority._issue_task_ticket(
            p0_grant,
            {
                "task_id": "P0-POLICY-SEED",
                "objective": "Seed a reviewed policy for the test fixture.",
                "dependencies": [],
                "idempotency_key": "p0-policy-seed",
                "task_spec_ref": "research_state/control_plane/p0/policy-seed.json",
                "task_spec_sha256": "1" * 64,
                "requirements": {"required_test_receipt_ids": [], "required_review_receipt_ids": [], "required_evidence_ids": []},
                "allowed_files": ["research_state/control_plane/policies/"],
                "forbidden_files": ["data/"],
                "baseline_ref": "research_state/control_plane/p0/baseline.json",
                "baseline_sha256": "2" * 64,
                "input_evidence_refs": [],
            },
            allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
        )
        policy_sha = "3" * 64
        connection = stores_module.sqlite3.connect(self.authority_path)
        try:
            connection.execute(
                "INSERT INTO reviewed_entry_policies_v1 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (policy_sha, "4" * 64, "5" * 64, "6" * 64,
                 self.actor.actor_id, self.actor.actor_type, self.actor.invocation_id,
                 policy_ticket.ticket_id, "P0", "p0-policy-test", identity.plan_hash,
                 identity.scope_hash, identity.instruction_policy_hash,
                 "2026-07-30T00:00:00+00:00"),
            )
            connection.execute(
                "INSERT INTO active_entry_policy_v1 VALUES (1, ?, ?)",
                (policy_sha, "2026-07-30T00:00:00+00:00"),
            )
            connection.commit()
        finally:
            connection.close()
        envelope = authority._provision_authorization(
            phase=Phase.P3,
            attempt_id="p3-audit-test",
            actor=self.actor,
            identity=identity,
            expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
            allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
        )
        grant = authority.claim_authorization(
            envelope,
            expected_phase=Phase.P3,
            expected_attempt_id="p3-audit-test",
            actor=self.actor,
            identity=identity,
        )
        task_spec = {
            "task_id": "P3R1-T3-HISTORICAL-AUDIT-ADDENDUM",
            "objective": "Publish the bounded historical correction.",
            "dependencies": [],
            "idempotency_key": "p3-audit-test",
            "task_spec_ref": "research_state/control_plane/p3/task_specs/t3.json",
            "task_spec_sha256": "d" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_state/control_plane/audit_addenda/"],
            "forbidden_files": ["data/"],
            "baseline_ref": "research_state/control_plane/p3/baseline.json",
            "baseline_sha256": "e" * 64,
            "input_evidence_refs": [],
        }
        ticket = authority._issue_task_ticket(
            grant,
            task_spec,
            allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
        )
        self.lease = authority._begin_task(ticket)
        self.source = self.root / "historical" / "results.json"
        self.source.parent.mkdir()
        self.source.write_text(
            json.dumps({"test_outcomes_opened": False, "comparisons": 9}),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.paths.stop()
        stores_module._expected_schema_sha256.cache_clear()
        self.temporary.cleanup()

    def build(self, **changes: object) -> Path:
        values: dict[str, object] = {
            "repository_root": self.root,
            "source_refs": ("historical/results.json",),
            "output_ref": "research_state/control_plane/audit_addenda/p0b.json",
            "supersedes": ("historical/results.json#test_outcomes_opened",),
            "downstream_parent_refs": ("legacy:child-001",),
            "recorded_at": "2026-07-30T00:00:00Z",
            "authority_lease": self.lease,
        }
        values.update(changes)
        return build_historical_audit_addendum(**values)

    def test_addendum_is_authorized_create_only_and_does_not_modify_sources(self) -> None:
        before = hashlib.sha256(self.source.read_bytes()).hexdigest()
        result = self.build()
        after = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.assertEqual(before, after)
        self.assertEqual(result, self.build())

    def test_addendum_records_the_normative_correction_and_quarantine(self) -> None:
        payload = json.loads(self.build().read_bytes())
        self.assertEqual(payload["comparison_pass_count"], 0)
        self.assertEqual(payload["comparison_total"], 9)
        self.assertEqual(payload["data_cutoff"], "2026-07-08")
        self.assertEqual(
            payload["corrected_access_state"],
            "TEST_LABELS_AND_TEST_DERIVED_RANKIC_MATERIALIZED_NOT_USED_FOR_PREFLIGHT_GATE",
        )
        self.assertEqual(payload["protocol_reconstruction"], "PARTIAL")
        self.assertIn("test_outcomes_opened", payload["invalidated_fields"])
        self.assertEqual(payload["downstream_parent_quarantine"], ["legacy:child-001"])

    def test_conflicting_existing_addendum_is_rejected(self) -> None:
        destination = self.root / "research_state/control_plane/audit_addenda/p0b.json"
        destination.parent.mkdir(parents=True)
        destination.write_text("{}", encoding="utf-8")
        with self.assertRaises(AuditAddendumConflict):
            self.build()

    def test_inactive_or_wrong_task_lease_is_rejected(self) -> None:
        forged = object()
        with self.assertRaises(PermissionError):
            self.build(authority_lease=forged)
        connection = stores_module.sqlite3.connect(self.authority_path)
        try:
            connection.execute(
                "UPDATE task_tickets_v2 SET state = 'FAILED' WHERE ticket_id = ?",
                (self.lease.ticket_id,),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(stores_module.TaskTicketStateError):
            self.build()

    def test_unsafe_path_and_naive_timestamp_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.build(output_ref="research_state/control_plane/audit_addenda/../escape.json")
        with self.assertRaises(ValueError):
            self.build(recorded_at="2026-07-30T00:00:00")

    def test_concurrent_identical_publication_is_idempotent(self) -> None:
        (self.root / "research_state/control_plane/audit_addenda").mkdir(parents=True)
        with ThreadPoolExecutor(max_workers=8) as executor:
            paths = tuple(executor.map(lambda _: self.build(), range(16)))
        self.assertEqual(len(set(paths)), 1)
        self.assertTrue(paths[0].is_file())


if __name__ == "__main__":
    unittest.main()
