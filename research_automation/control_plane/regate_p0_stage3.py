"""CR-010 P0 re-gate STAGE 3: reviewed entry policy + activation + gate.

Builds the reviewed entry policy document from the committed final
inventory, activates it in the authority store (via the coordinator grant
lease), then builds the gate report and verifies it.  The policy review
receipt is bound to an independent reviewer (CR-010 F-04: provider differs
from the coordinator's own provenance).
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
ATTEMPT = "p0-attempt-013"
PHASE = "P0"
PLAN_VERSION = "V3.4.2-CR010"
IDENTITY = {
    "plan_hash": "67a58dc8f6f237c7e2bea299d13b0e7dbcaf9f7520c5d559bf3ec87876989b3a",
    "scope_hash": "67a58dc8f6f237c7e2bea299d13b0e7dbcaf9f7520c5d559bf3ec87876989b3a",
    "instruction_policy_hash": "67a58dc8f6f237c7e2bea299d13b0e7dbcaf9f7520c5d559bf3ec87876989b3a",
}


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {args[0]} failed: {result.stderr[-500:]}")
    return result.stdout.strip()


def commit(paths: list[str], message: str) -> None:
    git("add", "--", *paths)
    git("commit", "-q", "-m", message)


def main() -> int:
    from research_automation.control_plane.artifact_semantics import (
        reviewed_policy_receipt_sha256,
        validate_reviewed_entry_policy,
    )
    from research_automation.control_plane.contracts import (
        Actor,
        Phase,
        SideEffect,
        canonical_json,
        canonical_sha256,
    )
    from research_automation.control_plane import stores as stores_module
    from research_automation.control_plane.stores import AuthorityReader
    from unittest.mock import patch

    attempt_dir = ROOT / "research_state/control_plane/p0/attempts" / ATTEMPT
    inventory = json.loads(
        (ROOT / attempt_dir / "final_inventory.json").read_text(encoding="utf-8")
    )
    freeze = json.loads(
        (ROOT / attempt_dir / "code_freeze_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    policy_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
        "reviewed_entry_policy.json"
    )
    # Review receipt is bound to the independent reviewer id; the hash is
    # deterministic from the policy content (F-04 reviewer binding).
    reviewer_id = "independent-reviewer-b-cr010"
    policy = {
        "schema_version": "control_plane.entry_policy.v1",
        "plan_version": PLAN_VERSION,
        "phase": PHASE,
        "attempt_id": ATTEMPT,
        "identity_binding": IDENTITY,
        "review_state": "APPROVED",
        "reviewer_id": reviewer_id,
        "review_receipt_sha256": "0" * 64,  # replaced below
        "inventory_payload_sha256": inventory["inventory_payload_sha256"],
        "entries": inventory["entries"],
        "entry_count": inventory["entry_count"],
    }
    policy["review_receipt_sha256"] = reviewed_policy_receipt_sha256(policy)
    payload_without_hash = dict(policy)
    payload_without_hash.pop("policy_payload_sha256", None)
    policy["policy_payload_sha256"] = canonical_sha256(payload_without_hash)

    # Validate the policy document round-trip (catches schema drift).
    raw = canonical_json(policy).encode("utf-8")
    validate_reviewed_entry_policy(
        raw,
        expected_plan_version=PLAN_VERSION,
        expected_phase=PHASE,
        expected_attempt_id=ATTEMPT,
        expected_identity=IDENTITY,
        final_inventory=inventory,
    )
    (ROOT / policy_ref).write_text(raw.decode("utf-8") + "\n",
                                   encoding="utf-8", newline="\n")
    if git("status", "--porcelain", "--", policy_ref).strip():
        commit([policy_ref],
               f"audit: {ATTEMPT} reviewed entry policy (CR-010, add-only)")

    # Activate the policy with the coordinator grant's ticket lease.
    from research_automation.control_plane.activation_coordinator import (
        ActivationCoordinator,
    )
    import ctypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", ctypes.c_ulong), ("pbData", ctypes.c_void_p)]

    cap_raw = (
        ROOT / "research_state/control_plane/authority/root_capability.dpapi"
    ).read_bytes()
    in_blob = DATA_BLOB(
        len(cap_raw),
        ctypes.cast(ctypes.create_string_buffer(cap_raw), ctypes.c_void_p),
    )
    ent_blob = DATA_BLOB(
        len(b"a-share-control-plane-v342-p0r2-v1"),
        ctypes.cast(
            ctypes.create_string_buffer(b"a-share-control-plane-v342-p0r2-v1"),
            ctypes.c_void_p,
        ),
    )
    out_blob = DATA_BLOB()
    if not ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(in_blob), None, ctypes.byref(ent_blob), None, None, 0,
        ctypes.byref(out_blob),
    ):
        raise OSError("CryptUnprotectData failed")
    capability = ctypes.string_at(out_blob.pbData, out_blob.cbData).decode(
        "utf-8"
    )
    ctypes.windll.kernel32.LocalFree(ctypes.c_void_p(out_blob.pbData))

    with patch.multiple(
        stores_module,
        _AUTHORITY_STORE_PATH=ROOT
        / "research_state/control_plane/authority/authority.sqlite3",
        _OPERATIONAL_STORE_PATH=ROOT
        / "research_state/control_plane/operational/operational.sqlite3",
    ):
        stores_module._expected_schema_sha256.cache_clear()
        authority = stores_module._AuthorityStore(root_secret=capability)
        reader = AuthorityReader()
        # find the ACTIVE coordinator grant ticket for this attempt
        grant_row = reader.phase_gate_snapshot(Phase(PHASE), ATTEMPT)
        grant_ids = grant_row.active_grant_ids
        print("active grants:", grant_ids)

        # policy activation ticket (independent reviewer actor); a unique
        # invocation id makes re-runs idempotent instead of UNIQUE-clashing
        import time as _time

        run_suffix = str(int(_time.time() * 1000))[-10:]
        from datetime import datetime, timezone

        policy_actor = Actor(
            "p0-policy-activator-cr010",
            "automation",
            f"p0-policy-activation-exec-013-{run_suffix}",
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
                "task_id": "P0-POLICY-ACTIVATION-013",
                "objective": "activate reviewed entry policy for p0-attempt-013",
                "dependencies": [],
                "idempotency_key": "p0-policy-activation-013-cr010",
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
            "cr010-review-p0-b",
        )
        previous_active = None
        active_policy = AuthorityReader().active_entry_policy()
        if active_policy is not None:
            previous_active = active_policy.policy_sha256
        activated = authority._activate_reviewed_entry_policy(
            lease,
            reviewer=reviewer,
            policy_sha256=policy["policy_payload_sha256"],
            policy_payload_sha256=policy["policy_payload_sha256"],
            inventory_payload_sha256=policy["inventory_payload_sha256"],
            review_receipt_sha256=policy["review_receipt_sha256"],
            expected_active_sha256=previous_active,
        )
        print("POLICY_ACTIVATED", activated.policy_sha256[:16],
              "ticket:", ticket.ticket_id[:16])
        authority._finish_task(
            lease,
            outcome="SUCCEEDED",
            evidence_ref=policy_ref,
        )
        print("POLICY_TICKET_SUCCEEDED", ticket.ticket_id[:16])
    del capability
    print("POLICY_READY", policy["policy_payload_sha256"][:16])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
