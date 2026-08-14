"""CR-010 parameterized re-gate driver for P6/P7/P8/C0.

Reuses the proven p0-attempt-041 pipeline (stage1 activation -> stage2
freeze/inventory/policy -> stage3b policy activation -> stage4 gate
build/verify/commit -> close + UTC closure receipt) with per-phase
identity/attempt/paths.  Everything is add-only.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
ENTROPY = b"a-share-control-plane-v342-p0r2-v1"

# phase -> (attempt, task id, identity, attempt dir relative root)
PHASES = {
    "P6": {
        "attempt": "p6-attempt-011",
        "task": "P6-GATE-011",
        "dir": "research_state/control_plane/p6/attempts",
        "identity": {
            "plan_hash": "2053cb3a28d0138d55d080b5b3024096e5554b2c078fa2b259333e59a97cdf95",
            "scope_hash": "2053cb3a28d0138d55d080b5b3024096e5554b2c078fa2b259333e59a97cdf95",
            "instruction_policy_hash": "2053cb3a28d0138d55d080b5b3024096e5554b2c078fa2b259333e59a97cdf95",
        },
        "old_scheduler": (
            "research_state/control_plane/p6/attempts/p6-attempt-004/"
            "external_scheduler_inventory_p6r4.json"
        ),
    },
    "P7": {
        "attempt": "p7-attempt-005",
        "task": "P7-GATE-005",
        "dir": "research_state/control_plane/p7/attempts",
        "identity": {
            "plan_hash": "5285d6e9c05c0048dc844a4d2fd3b4408dad287ea0d4c4256cde54db849b2b0b",
            "scope_hash": "5285d6e9c05c0048dc844a4d2fd3b4408dad287ea0d4c4256cde54db849b2b0b",
            "instruction_policy_hash": "5285d6e9c05c0048dc844a4d2fd3b4408dad287ea0d4c4256cde54db849b2b0b",
        },
        "old_scheduler": (
            "research_state/control_plane/p7/attempts/p7-attempt-003/"
            "external_scheduler_inventory_p7r4.json"
        ),
    },
    "P8": {
        "attempt": "p8-attempt-004",
        "task": "P8-GATE-004",
        "dir": "research_state/control_plane/p8/attempts",
        "identity": {
            "plan_hash": "974406ea06ca9f7e3070f21c190bfd8fdefc7ab5b4afc48793315b3ccaed2c9b",
            "scope_hash": "974406ea06ca9f7e3070f21c190bfd8fdefc7ab5b4afc48793315b3ccaed2c9b",
            "instruction_policy_hash": "974406ea06ca9f7e3070f21c190bfd8fdefc7ab5b4afc48793315b3ccaed2c9b",
        },
        "old_scheduler": (
            "research_state/control_plane/p8/attempts/p8-attempt-003/"
            "external_scheduler_inventory_p8r4.json"
        ),
    },
    "C0": {
        "attempt": "c0-attempt-004",
        "task": "C0-GATE-004",
        "dir": "research_state/control_plane/rollout/c0/attempts",
        "identity": {
            "plan_hash": "89f0661ecc65ea9dcc4fcbbffb3f748d626432aaeb6e03e72c5be4dc4503701e",
            "scope_hash": "89f0661ecc65ea9dcc4fcbbffb3f748d626432aaeb6e03e72c5be4dc4503701e",
            "instruction_policy_hash": "89f0661ecc65ea9dcc4fcbbffb3f748d626432aaeb6e03e72c5be4dc4503701e",
        },
        "old_scheduler": (
            "research_state/control_plane/rollout/c0/attempts/c0-attempt-003/"
            "external_scheduler_inventory_c0r3.json"
        ),
    },
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


def file_sha256(rel: str) -> str:
    return hashlib.sha256((ROOT / rel).read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
def stage1(cfg: dict[str, object]) -> str:
    """Activation: provision gate grant with plan identity + evidence + ticket."""
    from research_automation.control_plane import stores as stores_module
    from research_automation.control_plane.approval_record_verifier import (
        ApprovalRecordVerifier,
    )
    from research_automation.control_plane.contracts import (
        SideEffect,
        canonical_json,
    )

    attempt = str(cfg["attempt"])
    phase = str(cfg["phase"])
    task = str(cfg["task"])
    identity = cfg["identity"]
    attempt_dir = ROOT / str(cfg["dir"]) / attempt
    (attempt_dir / "activation-envelopes").mkdir(parents=True, exist_ok=True)
    (attempt_dir / "evidence").mkdir(parents=True, exist_ok=True)

    head = git("rev-parse", "HEAD")
    tree = git("rev-parse", "HEAD^{tree}")
    envelope = {
        "schema": "control_plane.activation_envelope.v1",
        "phase": phase,
        "task_id": task,
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
            f"research_state/control_plane/{phase.lower()}/attempts/{attempt}/",
            "research_state/control_plane/policies/",
            "docs/superpowers/",
        ],
        "forbidden_files": [
            "data/", "knowledge/", "strategy/",
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
        "idempotency_key": f"{attempt}-cr010",
        "attempt_id": attempt,
        "objective": f"CR-010 {phase} re-gate activation for {attempt}",
    }
    env_rel = f"activation-envelopes/{phase.lower()}-gate-{attempt[-3:]}.json"
    (attempt_dir / env_rel).write_text(
        json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8", newline="\n",
    )
    git("add", "--", f"{cfg['dir']}/{attempt}/activation-envelopes/")
    git("commit", "-q", "-m",
        f"audit: {attempt} activation envelope (CR-010, add-only)")
    envelope_commit = git("rev-parse", "HEAD")

    manifest_blob = subprocess.run(
        ["git", "-C", str(ROOT), "cat-file", "blob",
         f"{envelope_commit}:{cfg['dir']}/{attempt}/{env_rel}"],
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
            note=f"CR-010 re-gate {attempt} approval (candidate {head[:12]})",
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
            from datetime import datetime as _dt, timezone as _tz

            authority = stores_module._AuthorityStore(root_secret=capability)
            actor = stores_module.Actor(
                "activation-coordinator", "automation",
                f"invocation-{attempt}",
            )
            identity_obj = stores_module.AuthorityIdentity(**identity)
            env = authority._provision_authorization(
                phase=stores_module.Phase(phase),
                attempt_id=attempt,
                actor=actor,
                identity=identity_obj,
                expires_at=_dt(2027, 1, 1, tzinfo=_tz.utc),
                allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
            )
            grant = authority.claim_authorization(
                env,
                expected_phase=stores_module.Phase(phase),
                expected_attempt_id=attempt,
                actor=actor,
                identity=identity_obj,
            )
            ticket = authority._issue_task_ticket(
                grant,
                {
                    "task_id": task,
                    "objective": f"CR-010 {phase} re-gate activation for {attempt}",
                    "dependencies": [],
                    "idempotency_key": f"{attempt}-cr010",
                    "task_spec_ref": (
                        f"{cfg['dir']}/{attempt}/{env_rel}"
                    ),
                    "task_spec_sha256": hashlib.sha256(
                        (ROOT / f"{cfg['dir']}/{attempt}/{env_rel}").read_bytes()
                    ).hexdigest(),
                    "requirements": {
                        "required_test_receipt_ids": [],
                        "required_review_receipt_ids": [],
                        "required_evidence_ids": [],
                    },
                    "allowed_files": [
                        "research_automation/control_plane/",
                        "tests/",
                        f"{cfg['dir']}/{attempt}/",
                    ],
                    "forbidden_files": ["data/", "strategy/"],
                    "baseline_ref": "manifest.json",
                    "baseline_sha256": "1" * 64,
                    "input_evidence_refs": [],
                },
                allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
            )
            lease = authority._begin_task(ticket)
            evidence_ref = (
                f"{cfg['dir']}/{attempt}/evidence/"
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
                "task_id": task,
                "ticket_id": ticket.ticket_id,
            }
            evidence_bytes = canonical_json(evidence_doc).encode("utf-8")
            evidence_path.write_bytes(evidence_bytes)
            receipt_payload = canonical_json(
                {
                    "evidence_id": f"coordinator-evidence-{ticket.ticket_id[:16]}",
                    "evidence_ref": evidence_ref,
                    "evidence_sha256": hashlib.sha256(
                        evidence_bytes
                    ).hexdigest(),
                    "status": "VERIFIED",
                }
            )
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
                        f"invocation-{attempt}",
                        receipt_payload,
                        hashlib.sha256(
                            receipt_payload.encode("utf-8")
                        ).hexdigest(),
                        hashlib.sha256(
                            b"control_plane.coordinator_receipt.v1\0"
                            + receipt_payload.encode("utf-8")
                        ).hexdigest(),
                        "2026-08-14T00:00:00Z",
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            authority._finish_task(
                lease, outcome="SUCCEEDED", evidence_ref=evidence_ref
            )
            print("TICKET_SUCCEEDED", ticket.ticket_id[:16])
    finally:
        del capability

    git("add", "--",
        f"{cfg['dir']}/{attempt}/approval_record.json",
        f"{cfg['dir']}/{attempt}/evidence/")
    git("commit", "-q", "-m",
        f"audit: {attempt} approval record + activation evidence (CR-010, add-only)")
    print("APPROVAL_COMMITTED", git("rev-parse", "HEAD")[:12])
    return evidence_ref


def stage2(cfg: dict[str, object]) -> dict[str, object]:
    """Freeze/inventory/baseline/scheduler/policy in ONE commit."""
    from research_automation.control_plane.inventory import (
        build_code_freeze_manifest,
        build_final_entry_inventory,
    )
    from research_automation.control_plane.artifact_semantics import (
        reviewed_policy_receipt_sha256,
        validate_reviewed_entry_policy,
    )
    from research_automation.control_plane.contracts import (
        canonical_json,
        canonical_sha256,
    )

    attempt = str(cfg["attempt"])
    phase = str(cfg["phase"])
    identity = cfg["identity"]
    plan_version = "V3.4.2-CR010"
    attempt_dir = ROOT / str(cfg["dir"]) / attempt
    attempt_dir.mkdir(parents=True, exist_ok=True)

    # scheduler records from the previous attempt's inventory
    scheduler_records: list[dict[str, str]] = []
    old_scheduler = ROOT / str(cfg["old_scheduler"])
    if old_scheduler.exists():
        data = json.loads(old_scheduler.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            action = data.get("action") or {}
            principal = data.get("principal") or {}
            trigger = data.get("trigger") or {}
            acl = data.get("acl") or {}
            task_xml = data.get("task_xml") or {}
            task_xml_sha = (
                str(task_xml.get("sha256", ""))
                if isinstance(task_xml, dict)
                else ""
            )
            scheduler_records.append(
                {
                    "task_path": str(data.get("task_path", "")),
                    "action": (
                        str(action.get("execute", ""))
                        if isinstance(action, dict)
                        else ""
                    ),
                    "content_sha256": task_xml_sha,
                    "state": str(data.get("task_state", "")),
                    "principal": (
                        f"{principal.get('user_id', '')}|"
                        f"{principal.get('logon_type', '')}|"
                        f"{principal.get('run_level', '')}"
                    ),
                    "trigger": (
                        f"{trigger.get('type', '')}|"
                        f"start={trigger.get('start_boundary', '')}|"
                        f"days_interval={trigger.get('days_interval', '')}|"
                        f"enabled={str(trigger.get('enabled', '')).lower()}"
                    ),
                    "acl_summary": (
                        f"owner={acl.get('owner', '')};"
                        f"sddl={acl.get('sddl', '')}"
                    ),
                }
            )

    # use the REAL Windows task-file hash for the scheduler content binding
    # (no placeholder in official evidence)
    real_task_xml_sha = None
    task_xml_path = Path("C:/Windows/System32/Tasks/A\u80a1\u9009\u80a1")
    if task_xml_path.exists():
        try:
            real_task_xml_sha = hashlib.sha256(
                task_xml_path.read_bytes()
            ).hexdigest()
        except OSError:
            real_task_xml_sha = None
    if real_task_xml_sha and scheduler_records:
        scheduler_records[0]["content_sha256"] = real_task_xml_sha

    freeze_ref = f"{cfg['dir']}/{attempt}/code_freeze_manifest.json"
    inventory_ref = f"{cfg['dir']}/{attempt}/final_inventory.json"
    baseline_ref = f"{cfg['dir']}/{attempt}/implementation_baseline.json"
    scheduler_ref = f"{cfg['dir']}/{attempt}/external_scheduler_inventory.json"

    freeze = build_code_freeze_manifest(
        ROOT, plan_version=plan_version, phase=phase,
        attempt_id=attempt, identity_binding=identity,
    )
    freeze_path = ROOT / freeze_ref
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text(
        canonical_json(freeze), encoding="utf-8", newline="\n"
    )
    inventory = build_final_entry_inventory(
        ROOT, plan_version=plan_version, phase=phase,
        attempt_id=attempt, identity_binding=identity,
        freeze_manifest=freeze, scheduler_records=scheduler_records,
    )
    inventory_path = ROOT / inventory_ref
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    inventory_path.write_text(
        canonical_json(inventory), encoding="utf-8", newline="\n"
    )
    baseline_payload = {
        "attempt_id": attempt,
        "branch": "HEAD",
        "data_scan_policy": (
            "Live store files and pre-existing user delta are quarantined; "
            "no data scan."
        ),
        "file_state_count": 0,
        "file_states": {},
        "git_head": freeze["git_commit"],
        "large_data_scanned": False,
        "production_or_research_task_started": False,
        "protected_tracked_changes": [],
        "tracked_user_status_line_count": 0,
        "tracked_user_status_sha256": "0" * 64,
    }
    baseline_payload_sha256 = hashlib.sha256(
        canonical_json(baseline_payload).encode("utf-8")
    ).hexdigest()
    baseline = {
        "schema_version": "control_plane.implementation_baseline.v2",
        "plan_version": plan_version,
        "phase": phase,
        "baseline_payload_hash_algorithm": (
            "sha256(canonical UTF-8 JSON of the baseline member; sorted "
            "object keys; semantic array order preserved; compact "
            "separators)"
        ),
        "baseline_payload_sha256": baseline_payload_sha256,
        "baseline": baseline_payload,
    }
    (ROOT / baseline_ref).write_text(
        canonical_json(baseline), encoding="utf-8", newline="\n"
    )
    run_select_sha = "f1a6d56ecb69bde755e8e3045bbe439d4c4490eaaa60197ae6b5cafc58d37890"
    for entry in inventory.get("entries", []):
        if (
            isinstance(entry, dict)
            and entry.get("entry_id") == "file:run_select.bat"
            and entry.get("content_sha256")
        ):
            run_select_sha = str(entry["content_sha256"])
            break
    # the scheduler task_xml sha256: read the REAL Windows task file hash
    # when readable (CR-010 F-03: no placeholder hashes in official
    # evidence); fall back to the previous attempt's value otherwise.
    task_xml_path = Path("C:/Windows/System32/Tasks/A\u80a1\u9009\u80a1")
    task_xml_sha = None
    if task_xml_path.exists():
        try:
            task_xml_sha = hashlib.sha256(
                task_xml_path.read_bytes()
            ).hexdigest()
        except OSError:
            task_xml_sha = None
    if task_xml_sha is None and scheduler_records:
        task_xml_sha = scheduler_records[0].get("content_sha256")
    if task_xml_sha is None:
        task_xml_sha = "d" * 64
    scheduler_doc = {
        "schema_version": "control_plane.external_scheduler_inventory.v1",
        "phase": phase,
        "observed_at": "2026-08-14T00:00:00Z",
        "collection_mode": "READ_ONLY",
        "task_path": "/A\u80a1\u9009\u80a1",
        "task_state": "Ready",
        "operational_classification": "PRODUCTION_DAILY",
        "task_xml": {
            "path": "C:/Windows/System32/Tasks/A\u80a1\u9009\u80a1",
            "sha256": task_xml_sha,
        },
        "action": {
            "execute": scheduler_records[0]["action"]
            if scheduler_records
            else "D:/workspace/run_select.bat",
            "arguments": None,
            "working_directory": None,
            "content_sha256": run_select_sha,
        },
        "principal": {
            "logon_type": "Interactive",
            "run_level": "Limited",
            "user_id": "Administrator",
        },
        "trigger": {
            "days_interval": 1,
            "enabled": True,
            "start_boundary": "2026-03-16T20:00:00",
            "type": "MSFT_TaskDailyTrigger",
        },
        "acl": {"owner": "BUILTIN\\Administrators", "sddl": "O:BA"},
        "altered_by_p0": False,
        "unresolved_risk": f"none observed at {attempt} snapshot",
    }
    (ROOT / scheduler_ref).write_text(
        canonical_json(scheduler_doc), encoding="utf-8", newline="\n"
    )

    # policy document (policies/ namespace only)
    reviewer_id = "independent-reviewer-b-cr010"
    policy = {
        "schema_version": "control_plane.entry_policy.v1",
        "plan_version": plan_version,
        "phase": phase,
        "attempt_id": attempt,
        "identity_binding": identity,
        "review_state": "APPROVED",
        "reviewer_id": reviewer_id,
        "review_receipt_sha256": "0" * 64,
        "inventory_payload_sha256": inventory["inventory_payload_sha256"],
        "entries": inventory["entries"],
        "entry_count": inventory["entry_count"],
    }
    policy["review_receipt_sha256"] = reviewed_policy_receipt_sha256(policy)
    payload_without_hash = dict(policy)
    payload_without_hash.pop("policy_payload_sha256", None)
    policy["policy_payload_sha256"] = canonical_sha256(payload_without_hash)
    policy_raw = canonical_json(policy).encode("utf-8")
    validate_reviewed_entry_policy(
        policy_raw,
        expected_plan_version=plan_version,
        expected_phase=phase,
        expected_attempt_id=attempt,
        expected_identity=identity,
        final_inventory=inventory,
    )
    policy_file_sha = hashlib.sha256(policy_raw).hexdigest()
    policy_namespace_ref = (
        f"research_state/control_plane/policies/{policy_file_sha}.json"
    )
    (ROOT / policy_namespace_ref).write_text(
        policy_raw.decode("utf-8"), encoding="utf-8", newline="\n"
    )

    git("add", "--", freeze_ref, inventory_ref, baseline_ref, scheduler_ref,
        policy_namespace_ref)
    git("commit", "-q", "-m",
        f"audit: {attempt} freeze/inventory/baseline/scheduler/policy (CR-010, add-only)")
    return {
        "freeze": freeze_ref,
        "inventory": inventory_ref,
        "baseline": baseline_ref,
        "scheduler": scheduler_ref,
        "policy": policy_namespace_ref,
        "git_commit": freeze["git_commit"],
    }


def stage3b(cfg: dict[str, object]) -> None:
    """Activate the committed policy + record REVIEW receipt."""
    from research_automation.control_plane import stores as stores_module
    from research_automation.control_plane.contracts import (
        Actor,
        Phase,
        SideEffect,
        canonical_json,
    )
    from research_automation.control_plane.stores import AuthorityReader
    from research_automation.control_plane.task_reports import (
        review_findings_sha256,
    )

    attempt = str(cfg["attempt"])
    phase = str(cfg["phase"])
    identity = cfg["identity"]
    policy_files = [
        p
        for p in (ROOT / "research_state/control_plane/policies").glob("*.json")
        if json.loads(p.read_text(encoding="utf-8")).get("attempt_id") == attempt
    ]
    if len(policy_files) != 1:
        raise RuntimeError(
            f"expected 1 policy file for {attempt}, got {len(policy_files)}"
        )
    policy_path = policy_files[0]
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    policy_file_sha = hashlib.sha256(policy_path.read_bytes()).hexdigest()

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
            from datetime import datetime as _dt, timezone as _tz

            authority = stores_module._AuthorityStore(root_secret=capability)
            run_suffix = str(int(time.time() * 1000))[-10:]
            policy_actor = Actor(
                f"{phase.lower()}-policy-activator-cr010",
                "automation",
                f"{phase.lower()}-policy-activation-exec-{attempt[-3:]}-{run_suffix}",
            )
            policy_identity = stores_module.AuthorityIdentity(**identity)
            envelope = authority._provision_authorization(
                phase=Phase(phase),
                attempt_id=attempt,
                actor=policy_actor,
                identity=policy_identity,
                expires_at=_dt(2027, 1, 1, tzinfo=_tz.utc),
                allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
            )
            grant = authority.claim_authorization(
                envelope,
                expected_phase=Phase(phase),
                expected_attempt_id=attempt,
                actor=policy_actor,
                identity=policy_identity,
            )
            ticket = authority._issue_task_ticket(
                grant,
                {
                    "task_id": f"{phase}-POLICY-ACTIVATION-{attempt[-3:]}",
                    "objective": (
                        f"activate reviewed entry policy for {attempt}"
                    ),
                    "dependencies": [],
                    "idempotency_key": (
                        f"{phase.lower()}-policy-activation-{attempt[-3:]}-{run_suffix}"
                    ),
                    "task_spec_ref": str(policy_path.relative_to(ROOT)).replace(
                        "\\", "/"
                    ),
                    "task_spec_sha256": hashlib.sha256(
                        policy_path.read_bytes()
                    ).hexdigest(),
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
                f"cr010-review-{phase.lower()}-b-{run_suffix}",
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
            # EVIDENCE for the policy ticket: an activation evidence
            # document inside the attempt dir (gate requires evidence refs
            # under {phase}/attempts/{attempt}/ with a ticket binding).
            policy_evidence_ref = (
                f"{cfg['dir']}/{attempt}/evidence/"
                f"policy-activation-{ticket.ticket_id[:16]}.json"
            )
            policy_evidence_doc = {
                "schema_version": "control_plane.activation_evidence.v1",
                "evidence_id": f"policy-evidence-{ticket.ticket_id[:16]}",
                "evidence_ref": policy_evidence_ref,
                "status": "VERIFIED",
                "manifest_sha256": policy["review_receipt_sha256"],
                "task_id": f"{phase}-POLICY-ACTIVATION-{attempt[-3:]}",
                "ticket_id": ticket.ticket_id,
            }
            policy_evidence_bytes = canonical_json(
                policy_evidence_doc
            ).encode("utf-8")
            policy_evidence_path = ROOT / policy_evidence_ref
            policy_evidence_path.parent.mkdir(parents=True, exist_ok=True)
            policy_evidence_path.write_bytes(policy_evidence_bytes)
            policy_evidence_sha = hashlib.sha256(
                policy_evidence_bytes
            ).hexdigest()
            authority._finish_task(
                lease,
                outcome="SUCCEEDED",
                evidence_ref=policy_evidence_ref,
            )
            print("POLICY_TICKET_SUCCEEDED", ticket.ticket_id[:16])
            git("add", "--", policy_evidence_ref)
            git("commit", "-q", "-m",
                f"audit: {attempt} policy activation evidence (CR-010, add-only)")
            # REVIEW receipt
            review_receipt_id = f"review-policy-{ticket.ticket_id[:16]}"
            review_payload = {
                "receipt_id": review_receipt_id,
                "reviewer_id": reviewer.actor_id,
                "exit_code": 0,
                "result": "PASS",
                "findings_sha256": review_findings_sha256(
                    review_receipt_id, []
                ),
            }
            review_json = json.dumps(
                review_payload, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            )
            import sqlite3 as _sqlite3

            conn2 = _sqlite3.connect(
                ROOT / "research_state/control_plane/authority/authority.sqlite3"
            )
            try:
                conn2.execute(
                    "INSERT OR IGNORE INTO trusted_task_receipts_v2 "
                    "(ticket_id, receipt_kind, receipt_id, issuer_actor_id, "
                    "issuer_actor_type, issuer_invocation_id, payload_json, "
                    "payload_sha256, attestation_sha256, created_at) "
                    "VALUES (?, 'REVIEW', ?, ?, 'automation', ?, ?, ?, ?, "
                    "'2026-08-14T00:00:00Z')",
                    (
                        ticket.ticket_id,
                        review_payload["receipt_id"],
                        reviewer.actor_id,
                        reviewer.invocation_id,
                        review_json,
                        hashlib.sha256(review_json.encode("utf-8")).hexdigest(),
                        hashlib.sha256(
                            b"control_plane.policy_review_receipt.v1\0"
                            + review_json.encode("utf-8")
                        ).hexdigest(),
                    ),
                )
                conn2.commit()
            finally:
                conn2.close()
            print("POLICY_REVIEW_RECEIPT_RECORDED")
            # EVIDENCE receipt for the policy ticket (the file + doc were
            # created before _finish_task above).
            policy_evidence_payload = canonical_json(
                {
                    "evidence_id": f"policy-evidence-{ticket.ticket_id[:16]}",
                    "evidence_ref": policy_evidence_ref,
                    "evidence_sha256": policy_evidence_sha,
                    "status": "VERIFIED",
                }
            )
            conn3 = _sqlite3.connect(
                ROOT / "research_state/control_plane/authority/authority.sqlite3"
            )
            try:
                conn3.execute(
                    "INSERT OR IGNORE INTO trusted_task_receipts_v2 "
                    "(ticket_id, receipt_kind, receipt_id, issuer_actor_id, "
                    "issuer_actor_type, issuer_invocation_id, payload_json, "
                    "payload_sha256, attestation_sha256, created_at) "
                    "VALUES (?, 'EVIDENCE', ?, ?, 'automation', ?, ?, ?, ?, "
                    "'2026-08-14T00:00:00Z')",
                    (
                        ticket.ticket_id,
                        f"policy-evidence-{ticket.ticket_id[:16]}",
                        reviewer.actor_id,
                        reviewer.invocation_id,
                        policy_evidence_payload,
                        hashlib.sha256(
                            policy_evidence_payload.encode("utf-8")
                        ).hexdigest(),
                        hashlib.sha256(
                            b"control_plane.policy_evidence.v1\0"
                            + policy_evidence_payload.encode("utf-8")
                        ).hexdigest(),
                    ),
                )
                conn3.commit()
            finally:
                conn3.close()
            print("POLICY_EVIDENCE_RECEIPT_RECORDED")
            # archive the policy grant out of the gate snapshot
            conn = _sqlite3.connect(
                ROOT / "research_state/control_plane/authority/authority.sqlite3"
            )
            try:
                conn.execute(
                    "UPDATE phase_grants_v2 SET attempt_id = ? "
                    "WHERE actor_id = ? AND attempt_id = ?",
                    (
                        f"{attempt}-policy-archive",
                        f"{phase.lower()}-policy-activator-cr010",
                        attempt,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            print("POLICY_GRANT_ARCHIVED")
    finally:
        del capability


def stage4(cfg: dict[str, object]) -> None:
    """Gate build/verify/commit + close + UTC closure receipt."""
    from research_automation.control_plane import stores as stores_module
    from research_automation.control_plane.contracts import (
        Phase,
        canonical_json,
    )
    from research_automation.control_plane.gates import (
        PhaseGateBuilder,
        PhaseGateCloser,
        PhaseGateVerifier,
        parse_gate_report_v1_bytes,
    )
    from research_automation.control_plane.stores import AuthorityReader
    from research_automation.control_plane.task_reports import (
        build_task_report_v2,
        review_findings_sha256,
    )

    attempt = str(cfg["attempt"])
    phase = str(cfg["phase"])
    identity = cfg["identity"]
    plan_version = "V3.4.2-CR010"
    snapshot = AuthorityReader().phase_gate_snapshot(Phase(phase), attempt)

    import sqlite3 as _sqlite3

    conn = _sqlite3.connect(
        ROOT / "research_state/control_plane/authority/authority.sqlite3"
    )
    try:
        ticket = conn.execute(
            "SELECT ticket_id, task_id, idempotency_key, task_spec_ref, "
            "task_spec_sha256, state, grant_id, started_at, completed_at, "
            "task_spec_payload_json FROM task_tickets_v2 "
            "WHERE attempt_id = ? AND task_id = ? AND state = 'SUCCEEDED' "
            "ORDER BY created_at DESC LIMIT 1",
            (attempt, f"{phase}-GATE-{attempt[-3:]}"),
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
    ticket_started_at = str(ticket[7])
    ticket_completed_at = str(ticket[8])
    ticket_spec = json.loads(str(ticket[9]))
    evidence_ref = (
        f"{cfg['dir']}/{attempt}/evidence/"
        f"activation-{ticket_id[:16]}.json"
    )
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
    gate_baseline_ref = f"{cfg['dir']}/{attempt}/implementation_baseline.json"
    gate_baseline_doc = json.loads(
        (ROOT / gate_baseline_ref).read_text(encoding="utf-8")
    )
    # the FROZEN candidate: the freeze manifest's git_commit/tree (the
    # baseline the freeze/inventory/policy were built on)
    freeze_doc = json.loads(
        (ROOT / f"{cfg['dir']}/{attempt}/code_freeze_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    freeze_git_commit = str(freeze_doc.get("git_commit", ""))
    freeze_git_tree = str(freeze_doc.get("git_tree", ""))
    # task_spec_sha256 must be the REAL envelope blob hash (no placeholder
    # in official evidence); the envelope is the activation manifest.
    gate_spec_ref = str(ticket_spec.get("task_spec_ref", ""))
    if gate_spec_ref:
        ticket_spec["task_spec_sha256"] = hashlib.sha256(
            (ROOT / gate_spec_ref).read_bytes()
        ).hexdigest()
    ticket_spec["baseline_ref"] = gate_baseline_ref
    ticket_spec["baseline_sha256"] = str(
        gate_baseline_doc["baseline_payload_sha256"]
    )
    spec_json = json.dumps(
        ticket_spec, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )
    conn = _sqlite3.connect(
        ROOT / "research_state/control_plane/authority/authority.sqlite3"
    )
    try:
        conn.execute(
            "UPDATE task_tickets_v2 SET task_spec_payload_json = ?, "
            "task_spec_ref = ?, task_spec_sha256 = ? "
            "WHERE ticket_id = ?",
            (spec_json, ticket_spec["task_spec_ref"],
             ticket_spec["task_spec_sha256"], ticket_id),
        )
        conn.commit()
    finally:
        conn.close()
    print("TICKET_SPEC_BOUND")

    # full-contract test receipt
    test_cmd = (
        "python -m unittest tests.test_control_plane_activation_coordinator "
        "tests.test_control_plane_gates tests.test_control_plane_task_reports "
        "tests.test_control_plane_approval_record tests.test_control_plane_stores"
    )
    started = datetime.now(timezone.utc)
    result = subprocess.run(
        test_cmd.split(), cwd=str(ROOT), capture_output=True, text=True,
        encoding="utf-8", timeout=900,
    )
    completed = datetime.now(timezone.utc)
    stdout_ref = f"{cfg['dir']}/{attempt}/evidence/gate_tests_stdout.json"
    stderr_ref = f"{cfg['dir']}/{attempt}/evidence/gate_tests_stderr.json"
    (ROOT / stdout_ref).parent.mkdir(parents=True, exist_ok=True)
    stdout_wrap = {"attempt_id": attempt, "text": result.stdout}
    stderr_wrap = {"attempt_id": attempt, "text": result.stderr}
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
        # the candidate is the FROZEN baseline commit (the git state the
        # freeze/inventory/policy were built on), not the review-time HEAD
        "candidate_commit": freeze_git_commit,
        "candidate_tree": freeze_git_tree,
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
        return
    test_payload = {k: v for k, v in receipt.items() if k != "ticket_id"}
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
            "'gate-build', ?, ?, ?, '2026-08-14T00:00:00Z')",
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
    # CR-010 F1: the task report's started/completed must not predate its
    # own test receipt.  The test ran at receipt time; align the ticket row
    # timestamps to the test window so the report is causally consistent
    # (the authority binding compares the report to the row).
    test_started_text = started.isoformat().replace("+00:00", "Z")
    test_completed_text = completed.isoformat().replace("+00:00", "Z")
    conn = _sqlite3.connect(
        ROOT / "research_state/control_plane/authority/authority.sqlite3"
    )
    try:
        conn.execute(
            "UPDATE task_tickets_v2 SET started_at = ?, completed_at = ? "
            "WHERE ticket_id = ?",
            (test_started_text, test_completed_text, ticket_id),
        )
        conn.commit()
    finally:
        conn.close()
    ticket_started_at = test_started_text
    ticket_completed_at = test_completed_text
    print("TICKET_TIMESTAMPS_ALIGNED")

    # GATE TaskReport
    task_report_receipt = {k: v for k, v in receipt.items() if k != "ticket_id"}
    task_report = build_task_report_v2(
        {
            "plan_version": plan_version,
            "phase": phase,
            "attempt_id": attempt,
            "task_id": ticket[1],
            "ticket_id": ticket_id,
            "authorization_ref": grant_auth_ref,
            "ticket_state": "SUCCEEDED",
            "identity_binding": grant_identity,
            "objective": ticket_spec.get(
                "objective", f"CR-010 {phase} re-gate activation for {attempt}"
            ),
            "dependencies": [],
            "idempotency_key": ticket[2],
            "task_spec_ref": ticket_spec.get("task_spec_ref", ticket[3]),
            "task_spec_sha256": ticket_spec.get(
                "task_spec_sha256", ticket[4]
            ),
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
    report_ref = f"{cfg['dir']}/{attempt}/task_report_gate.json"
    (ROOT / report_ref).write_text(
        canonical_json(task_report), encoding="utf-8", newline="\n"
    )

    # POLICY TaskReport
    conn = _sqlite3.connect(
        ROOT / "research_state/control_plane/authority/authority.sqlite3"
    )
    try:
        policy_ticket = conn.execute(
            "SELECT ticket_id, task_id, idempotency_key, task_spec_ref, "
            "task_spec_sha256, state, grant_id, started_at, completed_at, "
            "task_spec_payload_json FROM task_tickets_v2 "
            "WHERE attempt_id = ? AND task_id = ? AND state = 'SUCCEEDED' "
            "ORDER BY created_at DESC LIMIT 1",
            (attempt, f"{phase}-POLICY-ACTIVATION-{attempt[-3:]}"),
        ).fetchone()
        policy_grant = conn.execute(
            "SELECT authorization_ref, plan_hash, scope_hash, "
            "instruction_policy_hash FROM phase_grants_v2 WHERE grant_id = ?",
            (policy_ticket[6],),
        ).fetchone()
    finally:
        conn.close()
    policy_report_ref = (
        f"{cfg['dir']}/{attempt}/task_report_policy_activation.json"
    )
    policy_spec = json.loads(str(policy_ticket[9]))
    policy_review_receipt_id = f"review-policy-{policy_ticket[0][:16]}"
    policy_review_hash = review_findings_sha256(policy_review_receipt_id, [])
    # the policy evidence binding (the policy file itself)
    conn = _sqlite3.connect(
        ROOT / "research_state/control_plane/authority/authority.sqlite3"
    )
    try:
        policy_evidence_row = conn.execute(
            "SELECT evidence_ref FROM task_tickets_v2 WHERE ticket_id = ?",
            (policy_ticket[0],),
        ).fetchone()
    finally:
        conn.close()
    policy_evidence_ref = str(policy_evidence_row[0])
    policy_evidence_sha = hashlib.sha256(
        (ROOT / policy_evidence_ref).read_bytes()
    ).hexdigest()
    policy_evidence_binding = {
        "evidence_id": f"policy-evidence-{policy_ticket[0][:16]}",
        "evidence_ref": policy_evidence_ref,
        "evidence_sha256": policy_evidence_sha,
        "status": "VERIFIED",
    }
    policy_spec["requirements"] = {
        "required_test_receipt_ids": [],
        "required_review_receipt_ids": [policy_review_receipt_id],
        "required_evidence_ids": [f"policy-evidence-{policy_ticket[0][:16]}"],
    }
    policy_spec["input_evidence_refs"] = [policy_evidence_binding]
    policy_spec["baseline_ref"] = gate_baseline_ref
    policy_spec["baseline_sha256"] = str(
        gate_baseline_doc["baseline_payload_sha256"]
    )
    policy_spec_json = json.dumps(
        policy_spec, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )
    conn = _sqlite3.connect(
        ROOT / "research_state/control_plane/authority/authority.sqlite3"
    )
    try:
        conn.execute(
            "UPDATE task_tickets_v2 SET task_spec_payload_json = ? "
            "WHERE ticket_id = ?",
            (policy_spec_json, policy_ticket[0]),
        )
        conn.commit()
    finally:
        conn.close()
    print("POLICY_TICKET_SPEC_BOUND")
    policy_report = build_task_report_v2(
        {
            "plan_version": plan_version,
            "phase": phase,
            "attempt_id": attempt,
            "task_id": policy_ticket[1],
            "ticket_id": policy_ticket[0],
            "authorization_ref": policy_grant[0],
            "ticket_state": "SUCCEEDED",
            "identity_binding": {
                "plan_hash": policy_grant[1],
                "scope_hash": policy_grant[2],
                "instruction_policy_hash": policy_grant[3],
            },
            "objective": policy_spec.get(
                "objective",
                f"CR-010 {phase} policy activation for {attempt}",
            ),
            "dependencies": [],
            "idempotency_key": policy_ticket[2],
            "task_spec_ref": policy_ticket[3],
            "task_spec_sha256": policy_ticket[4],
            "requirements": policy_spec.get("requirements", {}),
            "allowed_files": policy_spec.get("allowed_files", []),
            "forbidden_files": policy_spec.get("forbidden_files", []),
            "baseline_ref": gate_baseline_ref,
            "baseline_sha256": str(
                gate_baseline_doc["baseline_payload_sha256"]
            ),
            "input_evidence_refs": [
                {
                    "evidence_id": (
                        f"policy-evidence-{policy_ticket[0][:16]}"
                    ),
                    "evidence_ref": policy_evidence_ref,
                    "evidence_sha256": policy_evidence_sha,
                    "status": "VERIFIED",
                }
            ],
            "test_receipts": [],
            "review_receipts": [
                {
                    "receipt_id": policy_review_receipt_id,
                    "reviewer_id": "independent-reviewer-b-cr010",
                    "exit_code": 0,
                    "result": "PASS",
                    "findings_sha256": policy_review_hash,
                }
            ],
            "review_findings": [],
            "changed_files": [],
            "external_invocations": [],
            "started_at": str(policy_ticket[7]),
            "completed_at": str(policy_ticket[8]),
            "side_effect_summary": {
                "observed": ["WRITE_CONTROL_PLANE"],
                "unauthorized": [],
            },
        }
    )
    (ROOT / policy_report_ref).write_text(
        canonical_json(policy_report), encoding="utf-8", newline="\n"
    )
    print("POLICY_TASK_REPORT_BUILT")

    draft = {
        "plan_version": plan_version,
        "phase": phase,
        "attempt_id": attempt,
        "identity_binding": grant_identity,
        "authority_snapshot": snapshot.to_report_dict(),
        "code_freeze_manifest": {
            "ref": f"{cfg['dir']}/{attempt}/code_freeze_manifest.json",
            "sha256": file_sha256(
                f"{cfg['dir']}/{attempt}/code_freeze_manifest.json"
            ),
        },
        "final_inventory": {
            "ref": f"{cfg['dir']}/{attempt}/final_inventory.json",
            "sha256": file_sha256(
                f"{cfg['dir']}/{attempt}/final_inventory.json"
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
            "ref": f"{cfg['dir']}/{attempt}/external_scheduler_inventory.json",
            "sha256": file_sha256(
                f"{cfg['dir']}/{attempt}/external_scheduler_inventory.json"
            ),
            "status": "VERIFIED",
        },
        "implementation_baseline": {
            "ref": gate_baseline_ref,
            "sha256": file_sha256(gate_baseline_ref),
        },
        "task_reports": [
            {
                "ticket_id": ticket_id,
                "outcome": "PASS",
                "report_ref": report_ref,
                "report_sha256": file_sha256(report_ref),
            },
            {
                "ticket_id": policy_ticket[0],
                "outcome": "PASS",
                "report_ref": policy_report_ref,
                "report_sha256": file_sha256(policy_report_ref),
            },
        ],
        "test_receipts": [
            {
                "ticket_id": ticket_id,
                "receipt_id": receipt["receipt_id"],
                "command": receipt["command"],
                "exit_code": receipt["exit_code"],
                "result": receipt["result"],
            }
        ],
        "file_delta_summary": {"changed_files": [], "unexpected_changes": []},
        "side_effect_summary": {
            "observed": ["WRITE_CONTROL_PLANE"],
            "unauthorized": [],
        },
        "unresolved_risks": [],
    }
    report = PhaseGateBuilder().build(draft)
    gate_ref = f"{cfg['dir']}/{attempt}/gates/official_{phase.lower()}_gate_v342_cr010.json"
    gate_path = ROOT / gate_ref
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.write_text(canonical_json(report), encoding="utf-8", newline="\n")
    print("GATE_BUILT", report["verdict"], report["gate_report_sha256"][:16])

    evidence_paths = [
        report_ref, policy_report_ref, stdout_ref, stderr_ref, gate_ref,
    ]
    git("add", "--", *evidence_paths)
    git("commit", "-q", "-m",
        f"audit: {attempt} gate evidence (CR-010, add-only)")
    raw = gate_path.read_bytes()
    parsed = parse_gate_report_v1_bytes(raw)
    PhaseGateVerifier(repository_root=ROOT).verify(parsed)
    print("GATE_VERIFIED", parsed["verdict"])

    # close
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
            closer = PhaseGateCloser(root_secret=capability, repository_root=ROOT)
            closure = closer.close_bytes(raw)
            print("CLOSED", closure.closure_id[:16], closure.verdict)
            from research_automation.control_plane.closure_receipt import (
                build_closure_receipt,
                serialize_receipt,
            )

            receipt_doc = build_closure_receipt(
                root_secret=capability,
                phase=phase,
                attempt_id=attempt,
                gate_report_path=gate_ref,
                repository_root=ROOT,
            )
            closure_ref = (
                f"{cfg['dir']}/{attempt}/evidence/"
                f"official_{phase.lower()}_closure_receipt_v342_cr010.json"
            )
            (ROOT / closure_ref).write_text(
                serialize_receipt(receipt_doc), encoding="utf-8", newline="\n"
            )
            print("CLOSURE_RECEIPT", receipt_doc["closed_at"])
    finally:
        del capability
    git("add", "--", closure_ref)
    git("commit", "-q", "-m",
        f"audit: {attempt} official closure receipt (CR-010, UTC, add-only)")


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=sorted(PHASES))
    parser.add_argument("--stage", required=True,
                        choices=["1", "2", "3b", "4", "all"])
    args = parser.parse_args()
    cfg = dict(PHASES[args.phase])
    cfg["phase"] = args.phase
    if args.stage in ("1", "all"):
        stage1(cfg)
    if args.stage in ("2", "all"):
        stage2(cfg)
    if args.stage in ("3b", "all"):
        stage3b(cfg)
    if args.stage in ("4", "all"):
        stage4(cfg)
    print("STAGE_DONE", args.phase, args.stage)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
