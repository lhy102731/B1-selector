from __future__ import annotations

import json
import sqlite3
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import Actor, Phase, SideEffect
from research_automation.control_plane.stores import (
    AuthorityIdentity,
    AuthorityReader,
    TaskTicketError,
)


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"


class ExecutionLeaseBindingTests(unittest.TestCase):
    @contextmanager
    def _live_lease(
        self,
    ) -> Iterator[tuple[Path, dict[str, object], object, object]]:
        now = datetime(2026, 7, 26, 10, 0, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-sink-guard")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )
        task_spec = {
            "task_id": "P0R2-T4-SUBPROCESS",
            "objective": "Authorize one bounded subprocess tracer.",
            "dependencies": [],
            "idempotency_key": "p0r2-t4-subprocess-001",
            "task_spec_ref": "research_state/control_plane/p0r2/tasks/subprocess.json",
            "task_spec_sha256": "d" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": ["execution-intent"],
            },
            "allowed_files": ["research_automation/control_plane/sink_guard.py"],
            "forbidden_files": ["data/"],
            "baseline_ref": "research_state/control_plane/p0r2/baseline.json",
            "baseline_sha256": "e" * 64,
            "input_evidence_refs": [
                {
                    "evidence_id": "execution-intent",
                    "evidence_ref": "research_state/control_plane/p0r2/intents/intent.json",
                    "evidence_sha256": "f" * 64,
                    "status": "VERIFIED",
                }
            ],
        }

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
                _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=datetime(
                        2026,
                        7,
                        26,
                        11,
                        0,
                        tzinfo=timezone.utc,
                    ),
                    allowed_side_effects=(SideEffect.START_SUBPROCESS,),
                )
                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                ticket = authority._issue_task_ticket(
                    grant,
                    task_spec,
                    allowed_side_effects=(SideEffect.START_SUBPROCESS,),
                )
                lease = authority._begin_task(ticket)
                yield root, task_spec, ticket, lease

    def test_reader_verifies_live_lease_and_returns_frozen_task_spec(self) -> None:
        with self._live_lease() as (_, task_spec, ticket, lease):
            binding = AuthorityReader().execution_lease_binding(lease)

            self.assertEqual(binding.ticket_id, ticket.ticket_id)
            self.assertEqual(binding.lease_id, lease.lease_id)
            self.assertEqual(
                binding.allowed_side_effects,
                (SideEffect.START_SUBPROCESS,),
            )
            self.assertEqual(
                json.loads(binding.task_spec_payload_json),
                task_spec,
            )

    def test_reader_rejects_a_corrupted_frozen_task_spec(self) -> None:
        with self._live_lease() as (root, _, ticket, lease):
            connection = sqlite3.connect(root / "authority.sqlite3")
            try:
                connection.execute(
                    """
                    UPDATE task_tickets_v2
                    SET task_spec_payload_json = ?
                    WHERE ticket_id = ?
                    """,
                    ("{}", ticket.ticket_id),
                )
                connection.commit()
            finally:
                connection.close()

            with self.assertRaises(TaskTicketError):
                AuthorityReader().execution_lease_binding(lease)


if __name__ == "__main__":
    unittest.main()
