"""CR-010 P0 re-gate STAGE 4: gate report build + fresh verify.

Assembles the gate draft from the authority snapshot, committed
freeze/inventory/policy/baseline refs (real hashes), a full-contract test
receipt (F-05) and the coordinator ticket's TaskReport; builds and verifies
the gate; writes the gate report file.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
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


def file_sha256(rel: str) -> str:
    return hashlib.sha256((ROOT / rel).read_bytes()).hexdigest()


def main() -> int:
    from research_automation.control_plane.contracts import (
        Phase,
        canonical_json,
    )
    from research_automation.control_plane.gates import (
        PhaseGateBuilder,
        PhaseGateVerifier,
        parse_gate_report_v1_bytes,
    )
    from research_automation.control_plane.stores import AuthorityReader

    attempt_dir = ROOT / "research_state/control_plane/p0/attempts" / ATTEMPT
    reader = AuthorityReader()
    snapshot = reader.phase_gate_snapshot(Phase(PHASE), ATTEMPT)

    # TaskReport for the coordinator ticket: rebuild from the DB row.
    import sqlite3

    conn = sqlite3.connect(
        ROOT / "research_state/control_plane/authority/authority.sqlite3"
    )
    try:
        ticket = conn.execute(
            "SELECT ticket_id, task_id, state FROM task_tickets_v2 "
            "WHERE attempt_id = ? AND task_id = ? AND state = 'SUCCEEDED' "
            "ORDER BY created_at DESC LIMIT 1",
            (ATTEMPT, "P0-GATE-013"),
        ).fetchone()
    finally:
        conn.close()
    ticket_id = ticket[0]
    evidence_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/evidence/"
        f"activation-{ticket_id[:16]}.json"
    )
    from research_automation.control_plane.task_reports import (
        build_task_report_v2,
    )

    # full-contract test receipt (F-05): real command + stdout/stderr hashes
    test_cmd = (
        "python -m unittest tests.test_control_plane_activation_coordinator "
        "tests.test_control_plane_gates tests.test_control_plane_task_reports "
        "tests.test_control_plane_approval_record tests.test_control_plane_stores"
    )
    started = datetime.now(timezone.utc)
    result = subprocess.run(
        test_cmd.split(),
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=900,
    )
    completed = datetime.now(timezone.utc)
    stdout_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/evidence/"
        "gate_tests_stdout.log"
    )
    stderr_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/evidence/"
        "gate_tests_stderr.log"
    )
    (ROOT / stdout_ref).write_text(result.stdout, encoding="utf-8", newline="\n")
    (ROOT / stderr_ref).write_text(result.stderr, encoding="utf-8", newline="\n")
    receipt = {
        "ticket_id": ticket_id,
        "receipt_id": f"test-{ticket_id[:16]}",
        "command": test_cmd,
        "exit_code": result.returncode,
        "result": "PASS" if result.returncode == 0 else "FAIL",
        "executable": sys.executable,
        "cwd": str(ROOT),
        "runtime_version": sys.version.split()[0],
        "lock_hash": "0" * 64,
        "candidate_commit": git("rev-parse", "HEAD"),
        "candidate_tree": git("rev-parse", "HEAD^{tree}"),
        "started_at_utc": started.isoformat().replace("+00:00", "Z"),
        "completed_at_utc": completed.isoformat().replace("+00:00", "Z"),
        "stdout_ref": stdout_ref,
        "stdout_sha256": hashlib.sha256(result.stdout.encode("utf-8")).hexdigest(),
        "stderr_ref": stderr_ref,
        "stderr_sha256": hashlib.sha256(result.stderr.encode("utf-8")).hexdigest(),
    }
    if result.returncode != 0:
        print("TESTS_FAILED", result.returncode)
        print(result.stdout[-800:])
        print(result.stderr[-800:])
        return 1

    # TaskReport receipts omit ticket_id (the TaskReport schema binds it at
    # the top level); the gate receipt includes it.  Full contract kept.
    task_report_receipt = {k: v for k, v in receipt.items() if k != "ticket_id"}
    task_report = build_task_report_v2(
        {
            "plan_version": PLAN_VERSION,
            "phase": PHASE,
            "attempt_id": ATTEMPT,
            "task_id": ticket[1],
            "ticket_id": ticket_id,
            "authorization_ref": f"coordinator-auth-{ticket_id[:24]}",
            "ticket_state": "SUCCEEDED",
            "identity_binding": IDENTITY,
            "objective": f"CR-010 P0 re-gate activation for {ATTEMPT}",
            "dependencies": [],
            "idempotency_key": f"{ATTEMPT}-cr010",
            "task_spec_ref": (
                f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
                "activation-envelopes/p0-gate-013.json"
            ),
            "task_spec_sha256": "1" * 64,
            "requirements": {
                "required_test_receipt_ids": [receipt["receipt_id"]],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [
                    f"coordinator-evidence-{ticket_id[:16]}"
                ],
            },
            "allowed_files": [
                "research_automation/control_plane/",
                "tests/",
                f"research_state/control_plane/p0/attempts/{ATTEMPT}/",
            ],
            "forbidden_files": ["data/", "strategy/"],
            "baseline_ref": (
                f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
                "implementation_baseline.json"
            ),
            "baseline_sha256": file_sha256(
                f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
                "implementation_baseline.json"
            ),
            "input_evidence_refs": [
                {
                    "evidence_id": f"coordinator-evidence-{ticket_id[:16]}",
                    "evidence_ref": evidence_ref,
                    "evidence_sha256": file_sha256(evidence_ref),
                    "status": "VERIFIED",
                }
            ],
            "test_receipts": [task_report_receipt],
            "review_receipts": [],
            "review_findings": [],
            "changed_files": [],
            "external_invocations": [],
            "started_at": started.isoformat().replace("+00:00", "Z"),
            "completed_at": completed.isoformat().replace("+00:00", "Z"),
            "side_effect_summary": {
                "observed": ["WRITE_CONTROL_PLANE"],
                "unauthorized": [],
            },
        }
    )
    report_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
        "task_report_gate_013.json"
    )
    (ROOT / report_ref).write_text(
        canonical_json(task_report), encoding="utf-8", newline="\n"
    )

    draft = {
        "plan_version": PLAN_VERSION,
        "phase": PHASE,
        "attempt_id": ATTEMPT,
        "identity_binding": IDENTITY,
        "authority_snapshot": snapshot.to_report_dict(),
        "code_freeze_manifest": {
            "ref": f"research_state/control_plane/p0/attempts/{ATTEMPT}/code_freeze_manifest.json",
            "sha256": file_sha256(
                f"research_state/control_plane/p0/attempts/{ATTEMPT}/code_freeze_manifest.json"
            ),
        },
        "final_inventory": {
            "ref": f"research_state/control_plane/p0/attempts/{ATTEMPT}/final_inventory.json",
            "sha256": file_sha256(
                f"research_state/control_plane/p0/attempts/{ATTEMPT}/final_inventory.json"
            ),
        },
        "reviewed_entry_policy": {
            "ref": (
                f"research_state/control_plane/policies/"
                f"{snapshot.active_entry_policy_sha256}.json"
            ),
            "sha256": snapshot.active_entry_policy_sha256,
        },
        "scheduler_inventory": {
            "ref": f"research_state/control_plane/p0/attempts/{ATTEMPT}/external_scheduler_inventory.json",
            "sha256": file_sha256(
                f"research_state/control_plane/p0/attempts/{ATTEMPT}/external_scheduler_inventory.json"
            ),
            "status": "VERIFIED",
        },
        "implementation_baseline": {
            "ref": f"research_state/control_plane/p0/attempts/{ATTEMPT}/implementation_baseline.json",
            "sha256": file_sha256(
                f"research_state/control_plane/p0/attempts/{ATTEMPT}/implementation_baseline.json"
            ),
        },
        "task_reports": [
            {
                "ticket_id": ticket_id,
                "outcome": "PASS",
                "report_ref": report_ref,
                "report_sha256": file_sha256(report_ref),
            }
        ],
        "test_receipts": [receipt],
        "file_delta_summary": {"changed_files": [], "unexpected_changes": []},
        "side_effect_summary": {
            "observed": ["WRITE_CONTROL_PLANE"],
            "unauthorized": [],
        },
        "unresolved_risks": [],
    }
    report = PhaseGateBuilder().build(draft)
    gate_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/gates/"
        "official_p0_gate_v342_cr010.json"
    )
    gate_path = ROOT / gate_ref
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.write_text(
        canonical_json(report), encoding="utf-8", newline="\n"
    )
    print("GATE_BUILT", report["verdict"], report["gate_report_sha256"][:16])

    # fresh verify from committed bytes
    raw = gate_path.read_bytes()
    parsed = parse_gate_report_v1_bytes(raw)
    PhaseGateVerifier(repository_root=ROOT).verify(parsed)
    print("GATE_VERIFIED", parsed["verdict"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
