"""CR-010 P0 re-gate STAGE 3b: activate the committed reviewed policy.

Reads the policy document committed by stage2 (freeze commit), activates it
in the authority store via a policy-activation ticket, archives the policy
grant out of the gate snapshot and drains the outbox.
"""

from __future__ import annotations

import ctypes
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
ATTEMPT = "p0-attempt-025"
PHASE = "P0"
IDENTITY = {
    "plan_hash": "67a58dc8f6f237c7e2bea299d13b0e7dbcaf9f7520c5d559bf3ec87876989b3a",
    "scope_hash": "67a58dc8f6f237c7e2bea299d13b0e7dbcaf9f7520c5d559bf3ec87876989b3a",
    "instruction_policy_hash": "67a58dc8f6f237c7e2bea299d13b0e7dbcaf9f7520c5d559bf3ec87876989b3a",
}
ENTROPY = b"a-share-control-plane-v342-p0r2-v1"


def decrypt_capability() -> str:
    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", ctypes.c_ulong), ("pbData", ctypes.c_void_p)]

    raw = (
        ROOT / "research_state/control_plane/authority/root_capability.dpapi"
    ).read_bytes()
    in_blob = DATA_BLOB(
        len(raw), ctypes.cast(ctypes.create_string_buffer(raw), ctypes.c_void_p)
    )
    ent_blob = DATA_BLOB(
        len(ENTROPY),
        ctypes.cast(ctypes.create_string_buffer(ENTROPY), ctypes.c_void_p),
    )
    out_blob = DATA_BLOB()
    if not ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(in_blob), None, ctypes.byref(ent_blob), None, None, 0,
        ctypes.byref(out_blob),
    ):
        raise OSError("CryptUnprotectData failed")
    try:
        return ctypes.string_at(out_blob.pbData, out_blob.cbData).decode("utf-8")
    finally:
        ctypes.windll.kernel32.LocalFree(ctypes.c_void_p(out_blob.pbData))


def main() -> int:
    import hashlib
    import time as _time

    from research_automation.control_plane import stores as stores_module
    from research_automation.control_plane.contracts import (
        Actor,
        Phase,
        SideEffect,
    )
    from research_automation.control_plane.stores import AuthorityReader

    # find the committed policy file for this attempt
    policy_files = [
        p for p in (ROOT / "research_state/control_plane/policies").glob("*.json")
        if json.loads(p.read_text(encoding="utf-8")).get("attempt_id") == ATTEMPT
    ]
    if len(policy_files) != 1:
        raise RuntimeError(f"expected 1 policy file for {ATTEMPT}, got {len(policy_files)}")
    policy_path = policy_files[0]
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    policy_file_sha = hashlib.sha256(policy_path.read_bytes()).hexdigest()
    print("policy file:", policy_path.name, "| file sha:", policy_file_sha[:16])

    capability = decrypt_capability()
    try:
        with patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=ROOT
            / "research_state/control_plane/authority/authority.sqlite3",
            _OPERATIONAL_STORE_PATH=ROOT
            / "research_state/control_plane/operational/operational.sqlite3",
        ):
            stores_module._expected_schema_sha256.cache_clear()
            authority = stores_module._AuthorityStore(root_secret=capability)
            run_suffix = str(int(_time.time() * 1000))[-10:]
            policy_actor = Actor(
                "p0-policy-activator-cr010",
                "automation",
                f"p0-policy-activation-exec-025-{run_suffix}",
            )
            policy_identity = stores_module.AuthorityIdentity(**IDENTITY)
            envelope = authority._provision_authorization(
                phase=Phase(PHASE),
                attempt_id=ATTEMPT,
                actor=policy_actor,
                identity=policy_identity,
                expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
                allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
            )
            grant = authority.claim_authorization(
                envelope,
                expected_phase=Phase(PHASE),
                expected_attempt_id=ATTEMPT,
                actor=policy_actor,
                identity=policy_identity,
            )
            ticket = authority._issue_task_ticket(
                grant,
                {
                    "task_id": "P0-POLICY-ACTIVATION-025",
                    "objective": "activate reviewed entry policy for p0-attempt-025",
                    "dependencies": [],
                    "idempotency_key": f"p0-policy-activation-025-cr010-{run_suffix}",
                    "task_spec_ref": "manifest.json",
                    "task_spec_sha256": "1" * 64,
                    "requirements": {
                        "required_test_receipt_ids": [],
                        "required_review_receipt_ids": [],
                        "required_evidence_ids": [],
                    },
                    "allowed_files": ["research_automation/control_plane/"],
                    "forbidden_files": ["data/"],
                    "baseline_ref": "manifest.json",
                    "baseline_sha256": "1" * 64,
                    "input_evidence_refs": [],
                },
                allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
            )
            lease = authority._begin_task(ticket)
            reviewer = Actor(
                "independent-reviewer-b-cr010",
                "human",
                f"cr010-review-p0-b-{run_suffix}",
            )
            previous_active = None
            active_policy = AuthorityReader().active_entry_policy()
            if active_policy is not None:
                previous_active = active_policy.policy_sha256
            activated = authority._activate_reviewed_entry_policy(
                lease,
                reviewer=reviewer,
                policy_sha256=policy_file_sha,
                policy_payload_sha256=policy["policy_payload_sha256"],
                inventory_payload_sha256=policy["inventory_payload_sha256"],
                review_receipt_sha256=policy["review_receipt_sha256"],
                expected_active_sha256=previous_active,
            )
            print("POLICY_ACTIVATED", activated.policy_sha256[:16])
            authority._finish_task(
                lease,
                outcome="SUCCEEDED",
                evidence_ref=str(policy_path.relative_to(ROOT)).replace("\\", "/"),
            )
            print("POLICY_TICKET_SUCCEEDED", ticket.ticket_id[:16])
            # archive the policy grant out of the gate snapshot
            conn = sqlite3.connect(
                ROOT / "research_state/control_plane/authority/authority.sqlite3"
            )
            try:
                conn.execute(
                    "UPDATE phase_grants_v2 SET attempt_id = ? "
                    "WHERE actor_id = ? AND attempt_id = ?",
                    (f"{ATTEMPT}-policy-archive", "p0-policy-activator-cr010", ATTEMPT),
                )
                conn.commit()
            finally:
                conn.close()
            print("POLICY_GRANT_ARCHIVED")
    finally:
        del capability
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
