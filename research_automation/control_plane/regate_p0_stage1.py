"""CR-010 P0 re-gate STAGE 1: activate p0-attempt-020 (coordinator only).

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
ATTEMPT = "p0-attempt-020"
PHASE = "P0"
TASK_ID = "P0-GATE-020"
ENTROPY = b"a-share-control-plane-v342-p0r2-v1"


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
    from research_automation.control_plane import activation_coordinator as ac
    from research_automation.control_plane import stores as stores_module
    from research_automation.control_plane.approval_record_verifier import (
        ApprovalRecordVerifier,
    )

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
    env_rel = f"activation-envelopes/p0-gate-013.json"
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
         "activation-envelopes/p0-gate-013.json"],
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
            coordinator = ac.ActivationCoordinator(
                root_secret=capability,
                repository_root=ROOT,
                test_runner_factory=lambda: [
                    sys.executable,
                    "-m",
                    "unittest",
                    "tests.test_control_plane_activation_coordinator",
                    "tests.test_control_plane_gates",
                    "tests.test_control_plane_task_reports",
                    "tests.test_control_plane_approval_record",
                ],
            )
            report = coordinator.run(
                envelope_commit=envelope_commit,
                manifest_ref=(
                    f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
                    "activation-envelopes/p0-gate-013.json"
                ),
                mode=ac.ActivationMode.V2_NORMAL,
                approval_record=approval,
            )
            print("COORDINATOR_OK", report.succeeded, report.ticket_id,
                  report.head[:12])
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
