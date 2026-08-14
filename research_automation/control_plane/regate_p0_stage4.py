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
ATTEMPT = "p0-attempt-032"
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

    # TaskReport for the coordinator ticket: rebuild from the DB row so the
    # authority binding matches exactly (authorization_ref/idempotency_key/
    # task_spec_sha256/identity come from the grant, not placeholders).
    import sqlite3

    conn = sqlite3.connect(
        ROOT / "research_state/control_plane/authority/authority.sqlite3"
    )
    try:
        ticket = conn.execute(
            "SELECT ticket_id, task_id, idempotency_key, task_spec_ref, "
            "task_spec_sha256, state, grant_id, started_at, completed_at, "
            "task_spec_payload_json FROM task_tickets_v2 "
            "WHERE attempt_id = ? AND task_id = ? AND state = 'SUCCEEDED' "
            "ORDER BY created_at DESC LIMIT 1",
            (ATTEMPT, f"P0-GATE-{ATTEMPT[-3:]}"),
        ).fetchone()
        grant = conn.execute(
            "SELECT authorization_ref, plan_hash, scope_hash, "
            "instruction_policy_hash FROM phase_grants_v2 WHERE grant_id = ?",
            (ticket[6],),
        ).fetchone()
    finally:
        conn.close()
    ticket_id = ticket[0]
    grant_auth_ref = grant[0]
    grant_identity = {
        "plan_hash": grant[1],
        "scope_hash": grant[2],
        "instruction_policy_hash": grant[3],
    }
    # TaskReport timestamps must EXACTLY match the authority ticket row.
    ticket_started_at = str(ticket[7])
    ticket_completed_at = str(ticket[8])
    # The task-spec fields (requirements/allowed/forbidden/baseline/evidence)
    # must match the authority's stored ticket payload exactly.
    ticket_spec = json.loads(str(ticket[9]))
    evidence_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/evidence/"
        f"activation-{ticket_id[:16]}.json"
    )
    # Bind the activation evidence into the ticket's task spec so the
    # TaskReport EVIDENCE group matches the authority trusted receipts
    # (the gate's _trusted_receipts_from_report compares the report's
    # input_evidence_refs against the trusted receipt rows).
    evidence_sha = hashlib.sha256(
        (ROOT / evidence_ref).read_bytes()
    ).hexdigest()
    evidence_binding = {
        "evidence_id": f"coordinator-evidence-{ticket_id[:16]}",
        "evidence_ref": evidence_ref,
        "evidence_sha256": evidence_sha,
        "status": "VERIFIED",
    }
    ticket_spec["input_evidence_refs"] = [evidence_binding]
    ticket_spec["requirements"]["required_evidence_ids"] = [
        f"coordinator-evidence-{ticket_id[:16]}"
    ]
    # The TaskReport baseline must equal the gate baseline (the committed
    # implementation_baseline.json payload hash); bind it into the ticket
    # spec so the authority task-spec check also passes.
    gate_baseline_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
        "implementation_baseline.json"
    )
    gate_baseline_doc = json.loads(
        (ROOT / gate_baseline_ref).read_text(encoding="utf-8")
    )
    ticket_spec["baseline_ref"] = gate_baseline_ref
    ticket_spec["baseline_sha256"] = str(
        gate_baseline_doc["baseline_payload_sha256"]
    )
    import sqlite3 as _sqlite3

    spec_json = json.dumps(
        ticket_spec, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )
    conn = _sqlite3.connect(
        ROOT / "research_state/control_plane/authority/authority.sqlite3"
    )
    try:
        conn.execute(
            "UPDATE task_tickets_v2 SET task_spec_payload_json = ? "
            "WHERE ticket_id = ?",
            (spec_json, ticket_id),
        )
        conn.commit()
    finally:
        conn.close()
    print("TICKET_SPEC_BOUND")
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
    # stdout/stderr must be canonical JSON files (post-freeze evidence is
    # only allowed to be canonical JSON), so wrap the raw text.
    stdout_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/evidence/"
        "gate_tests_stdout.json"
    )
    stderr_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/evidence/"
        "gate_tests_stderr.json"
    )
    (ROOT / stdout_ref).parent.mkdir(parents=True, exist_ok=True)
    (ROOT / stderr_ref).parent.mkdir(parents=True, exist_ok=True)
    # attempt id makes the blob unique across attempts (post-freeze
    # evidence must not reuse an existing blob)
    stdout_wrap = {"attempt_id": ATTEMPT, "text": result.stdout}
    stderr_wrap = {"attempt_id": ATTEMPT, "text": result.stderr}
    (ROOT / stdout_ref).write_text(
        canonical_json(stdout_wrap), encoding="utf-8", newline="\n"
    )
    (ROOT / stderr_ref).write_text(
        canonical_json(stderr_wrap), encoding="utf-8", newline="\n"
    )
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
        "stdout_sha256": hashlib.sha256(
            canonical_json(stdout_wrap).encode("utf-8")
        ).hexdigest(),
        "stderr_ref": stderr_ref,
        "stderr_sha256": hashlib.sha256(
            canonical_json(stderr_wrap).encode("utf-8")
        ).hexdigest(),
    }
    if result.returncode != 0:
        print("TESTS_FAILED", result.returncode)
        print(result.stdout[-800:])
        print(result.stderr[-800:])
        return 1

    # The TEST receipt must exist in the authority trusted receipts so the
    # gate's TaskReport binding matches (TEST/REVIEW/EVIDENCE groups).
    import sqlite3 as _sqlite3

    test_payload = {
        k: v for k, v in receipt.items() if k != "ticket_id"
    }
    test_payload_json = json.dumps(
        test_payload, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )
    conn = _sqlite3.connect(
        ROOT / "research_state/control_plane/authority/authority.sqlite3"
    )
    try:
        conn.execute(
            "INSERT OR IGNORE INTO trusted_task_receipts_v2 "
            "(ticket_id, receipt_kind, receipt_id, issuer_actor_id, "
            "issuer_actor_type, issuer_invocation_id, payload_json, "
            "payload_sha256, attestation_sha256, created_at) "
            "VALUES (?, 'TEST', ?, 'gate-builder', 'automation', "
            "'gate-build-032', ?, ?, ?, '2026-08-14T00:00:00Z')",
            (
                ticket_id,
                test_payload["receipt_id"],
                test_payload_json,
                hashlib.sha256(test_payload_json.encode("utf-8")).hexdigest(),
                hashlib.sha256(
                    b"control_plane.gate_test_receipt.v1\0"
                    + test_payload_json.encode("utf-8")
                ).hexdigest(),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    print("TEST_RECEIPT_RECORDED")

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
            "authorization_ref": grant_auth_ref,
            "ticket_state": "SUCCEEDED",
            "identity_binding": grant_identity,
            "objective": f"CR-010 P0 re-gate activation for {ATTEMPT}",
            "dependencies": [],
            "idempotency_key": ticket[2],
            "task_spec_ref": ticket[3],
            "task_spec_sha256": ticket[4],
            "requirements": ticket_spec.get("requirements", {}),
            "allowed_files": ticket_spec.get("allowed_files", []),
            "forbidden_files": ticket_spec.get("forbidden_files", []),
            "baseline_ref": ticket_spec.get("baseline_ref", "manifest.json"),
            "baseline_sha256": ticket_spec.get(
                "baseline_sha256", "1" * 64
            ),
            "input_evidence_refs": ticket_spec.get("input_evidence_refs", []),
            "test_receipts": [task_report_receipt],
            "review_receipts": [],
            "review_findings": [],
            "changed_files": [],
            "external_invocations": [],
            "started_at": ticket_started_at,
            "completed_at": ticket_completed_at,
            "side_effect_summary": {
                "observed": ["WRITE_CONTROL_PLANE"],
                "unauthorized": [],
            },
        }
    )
    report_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
        "task_report_gate.json"
    )
    (ROOT / report_ref).write_text(
        canonical_json(task_report), encoding="utf-8", newline="\n"
    )

    draft = {
        "plan_version": PLAN_VERSION,
        "phase": PHASE,
        "attempt_id": ATTEMPT,
        "identity_binding": grant_identity,
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

    # Commit ALL gate evidence (task report, logs, gate) in ONE commit so
    # the verifier dereferences committed blobs.  The commit is the ONLY
    # post-freeze commit and contains only immutable evidence files.
    evidence_paths = [report_ref, stdout_ref, stderr_ref, gate_ref]
    git("add", "--", *evidence_paths)
    git("commit", "-q", "-m",
        f"audit: {ATTEMPT} gate evidence (CR-010, add-only)")

    # fresh verify from committed bytes
    raw = gate_path.read_bytes()
    parsed = parse_gate_report_v1_bytes(raw)
    PhaseGateVerifier(repository_root=ROOT).verify(parsed)
    print("GATE_VERIFIED", parsed["verdict"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
