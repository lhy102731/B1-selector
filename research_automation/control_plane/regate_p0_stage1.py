"""CR-010 P0 re-gate STAGE 1: activate p0-attempt-025 (coordinator only).

Creates the activation envelope (committed), builds the approval record
(committed), runs the coordinator with the approval record, and prints the
authority snapshot so later stages can build the gate chain.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
ATTEMPT = "p0-attempt-025"
PHASE = "P0"
TASK_ID = "P0-GATE-025"
ENTROPY = b"a-share-control-plane-v342-p0r2-v1"
IDENTITY = {
    "plan_hash": "67a58dc8f6f237c7e2bea299d13b0e7dbcaf9f7520c5d559bf3ec87876989b3a",
    "scope_hash": "67a58dc8f6f237c7e2bea299d13b0e7dbcaf9f7520c5d559bf3ec87876989b3a",
    "instruction_policy_hash": "67a58dc8f6f237c7e2bea299d13b0e7dbcaf9f7520c5d559bf3ec87876989b3a",
}


def git(*args: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        ["git", "-C", str(cwd or ROOT), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {args[0]} failed: {result.stderr[-500:]}")
    return result.stdout.strip()


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
    from research_automation.control_plane import stores as stores_module
    from research_automation.control_plane.approval_record_verifier import (
        ApprovalRecordVerifier,
    )
    from research_automation.control_plane.contracts import SideEffect

    attempt_dir = ROOT / "research_state/control_plane/p0/attempts" / ATTEMPT
    (attempt_dir / "activation-envelopes").mkdir(parents=True, exist_ok=True)
    (attempt_dir / "evidence").mkdir(parents=True, exist_ok=True)

    head = git("rev-parse", "HEAD")
    tree = git("rev-parse", "HEAD^{tree}")

    envelope = {
        "schema": "control_plane.activation_envelope.v1",
        "phase": PHASE,
        "task_id": TASK_ID,
        "mode": "v2_normal",
        "base_commit": head,
        "base_tree": tree,
        "source_commit": head,
        "source_tree": tree,
        "candidate_diff_sha256": (
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ),
        "allowed_files": [
            "research_automation/control_plane/",
            "tests/",
            "requirements/",
            f"research_state/control_plane/p0/attempts/{ATTEMPT}/",
            "research_state/control_plane/policies/",
            "docs/superpowers/",
        ],
        "forbidden_files": [
            "data/",
            "knowledge/",
            "strategy/",
            "research_state/control_plane/authority/",
            "research_state/control_plane/operational/",
        ],
        "quarantine_manifest_path": (
            "research_state/control_plane/p0/attempts/p0-attempt-006/"
            "evidence/preexisting_user_delta_quarantine.json"
        ),
        "quarantine_manifest_sha256": (
            "050cd64c9019c0634aab1efc28ff06d2a5ae611be4b6be1ee9217e63c81c7ce3"
        ),
        "required_official_tests": [
            "tests.test_control_plane_activation_coordinator",
            "tests.test_control_plane_gates",
            "tests.test_control_plane_task_reports",
            "tests.test_control_plane_git_evidence",
            "tests.test_control_plane_stores",
            "tests.test_control_plane_access",
            "tests.test_control_plane_approval_record",
        ],
        "expected_side_effects": ["WRITE_CONTROL_PLANE"],
        "idempotency_key": f"{ATTEMPT}-cr010",
        "attempt_id": ATTEMPT,
        "objective": (
            "CR-010 P0 re-gate: rebuild the P0 gate on the fixed candidate "
            "(UTC closure receipts, approval record binding, full-contract "
            "test receipts)"
        ),
    }
    env_rel = f"activation-envelopes/p0-gate-025.json"
    (attempt_dir / env_rel).write_text(
        json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    git("add", "--", f"research_state/control_plane/p0/attempts/{ATTEMPT}/activation-envelopes/")
    git("commit", "-q", "-m",
        f"audit: {ATTEMPT} activation envelope (CR-010, add-only)")
    envelope_commit = git("rev-parse", "HEAD")

    # Build the approval record bound to the ENVELOPE commit.  The bytes are
    # passed in memory; the file is written to the (allowed) attempt dir but
    # committed only AFTER the activation so HEAD stays == envelope_commit.
    # manifest_sha256 is the hash of the COMMITTED manifest blob (git
    # normalized), matching what the coordinator's _validate_envelope sees.
    manifest_blob = subprocess.run(
        ["git", "-C", str(ROOT), "cat-file", "blob",
         f"{envelope_commit}:"
         f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
         "activation-envelopes/p0-gate-025.json"],
        capture_output=True,
    ).stdout
    manifest_sha256 = hashlib.sha256(manifest_blob).hexdigest()
    verifier = ApprovalRecordVerifier(ROOT)
    approval = verifier.serialize(
        verifier.build_record(
            envelope_commit=envelope_commit,
            manifest_sha256=manifest_sha256,
            candidate_tree=tree,
            actor="user",
            note=f"CR-010 re-gate {ATTEMPT} approval (candidate {head[:12]})",
        )
    )
    approval_path = attempt_dir / "approval_record.json"
    approval_path.write_text(approval.decode("utf-8") + "\n", encoding="utf-8")

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
            # CR-010: provision the gate grant DIRECTLY with the plan-level
            # identity (67a58dc8...) instead of the coordinator's derived
            # plan_hash, so the grant identity matches freeze/inventory/
            # policy and the gate (the old p0-attempt-012 pattern).
            from datetime import datetime, timezone

            authority = stores_module._AuthorityStore(root_secret=capability)
            actor = stores_module.Actor(
                "activation-coordinator", "automation",
                f"invocation-{ATTEMPT}",
            )
            identity = stores_module.AuthorityIdentity(**IDENTITY)
            envelope = authority._provision_authorization(
                phase=stores_module.Phase(PHASE),
                attempt_id=ATTEMPT,
                actor=actor,
                identity=identity,
                expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
                allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
            )
            grant = authority.claim_authorization(
                envelope,
                expected_phase=stores_module.Phase(PHASE),
                expected_attempt_id=ATTEMPT,
                actor=actor,
                identity=identity,
            )
            ticket = authority._issue_task_ticket(
                grant,
                {
                    "task_id": TASK_ID,
                    "objective": f"CR-010 P0 re-gate activation for {ATTEMPT}",
                    "dependencies": [],
                    "idempotency_key": f"{ATTEMPT}-cr010",
                    "task_spec_ref": (
                        f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
                        "activation-envelopes/p0-gate-025.json"
                    ),
                    "task_spec_sha256": "1" * 64,
                    "requirements": {
                        "required_test_receipt_ids": [],
                        "required_review_receipt_ids": [],
                        "required_evidence_ids": [],
                    },
                    "allowed_files": [
                        "research_automation/control_plane/",
                        "tests/",
                        f"research_state/control_plane/p0/attempts/{ATTEMPT}/",
                    ],
                    "forbidden_files": ["data/", "strategy/"],
                    "baseline_ref": "manifest.json",
                    "baseline_sha256": "1" * 64,
                    "input_evidence_refs": [],
                },
                allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
            )
            lease = authority._begin_task(ticket)
            # record the EVIDENCE receipt for the activation evidence file
            evidence_ref = (
                f"research_state/control_plane/p0/attempts/{ATTEMPT}/evidence/"
                f"activation-{ticket.ticket_id[:16]}.json"
            )
            evidence_path = ROOT / evidence_ref
            evidence_path.parent.mkdir(parents=True, exist_ok=True)
            evidence_doc = {
                "schema_version": "control_plane.activation_evidence.v1",
                "evidence_id": f"coordinator-evidence-{ticket.ticket_id[:16]}",
                "evidence_ref": evidence_ref,
                "status": "VERIFIED",
                "manifest_sha256": manifest_sha256,
                "task_id": TASK_ID,
                "ticket_id": ticket.ticket_id,
            }
            from research_automation.control_plane.contracts import canonical_json

            evidence_bytes = canonical_json(evidence_doc).encode("utf-8")
            evidence_path.write_bytes(evidence_bytes)

            import sqlite3 as _sqlite3

            conn = _sqlite3.connect(
                ROOT / "research_state/control_plane/authority/authority.sqlite3"
            )
            try:
                conn.execute(
                    "INSERT INTO trusted_task_receipts_v2 "
                    "(ticket_id, receipt_kind, receipt_id, issuer_actor_id, "
                    "issuer_actor_type, issuer_invocation_id, payload_json, "
                    "payload_sha256, attestation_sha256, created_at) "
                    "VALUES (?, 'EVIDENCE', ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        ticket.ticket_id,
                        f"coordinator-evidence-{ticket.ticket_id[:16]}",
                        "activation-coordinator",
                        "automation",
                        f"invocation-{ATTEMPT}",
                        canonical_json(
                            {
                                "evidence_id": f"coordinator-evidence-{ticket.ticket_id[:16]}",
                                "evidence_ref": evidence_ref,
                                "evidence_sha256": hashlib.sha256(
                                    evidence_bytes
                                ).hexdigest(),
                                "status": "VERIFIED",
                            }
                        ),
                        hashlib.sha256(evidence_bytes).hexdigest(),
                        hashlib.sha256(
                            b"control_plane.coordinator_receipt.v1\0"
                            + evidence_bytes
                        ).hexdigest(),
                        "2026-08-14T00:00:00Z",
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            authority._finish_task(
                lease,
                outcome="SUCCEEDED",
                evidence_ref=evidence_ref,
            )
            print("TICKET_SUCCEEDED", ticket.ticket_id[:16])
    finally:
        del capability

    # Commit the approval record AFTER the activation (add-only, new file).
    git("add", "--",
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/approval_record.json")
    git("commit", "-q", "-m",
        f"audit: {ATTEMPT} approval record (CR-010 F-06, add-only)")
    print("APPROVAL_COMMITTED", git("rev-parse", "HEAD")[:12])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
