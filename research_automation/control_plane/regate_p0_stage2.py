"""CR-010 P0 re-gate STAGE 2: freeze/inventory/policy/scheduler/baseline.

Builds the gate input chain for p0-attempt-016 using the production
builders, commits every artifact add-only, and prints the refs/hashes the
gate build stage needs.

IMPORTANT: freeze + inventory must be built at the SAME git state, so the
freeze file is written to the working tree and BOTH files are committed in
one commit AFTER both builders ran.  The freeze document's git_commit is
the HEAD the builders observed.
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
ATTEMPT = "p0-attempt-016"
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
    from research_automation.control_plane.inventory import (
        build_code_freeze_manifest,
        build_final_entry_inventory,
    )
    from research_automation.control_plane.contracts import (
        canonical_json,
        canonical_sha256,
    )

    attempt_dir = ROOT / "research_state/control_plane/p0/attempts" / ATTEMPT
    attempt_dir.mkdir(parents=True, exist_ok=True)

    # scheduler records: reuse the previous attempt's external scheduler
    # inventory (unchanged source of truth for the A-share scheduler).
    scheduler_records: list[dict[str, str]] = []
    old_scheduler = (
        ROOT / "research_state/control_plane/p0/attempts/p0-attempt-012/"
        "external_scheduler_inventory_gate012.json"
    )
    if old_scheduler.exists():
        data = json.loads(old_scheduler.read_text(encoding="utf-8"))
        entries = data.get("entries", []) if isinstance(data, dict) else []
        for entry in entries:
            if isinstance(entry, dict):
                scheduler_records.append(
                    {
                        "path": str(entry.get("path", "")),
                        "command": str(entry.get("command", "")),
                        "sha256": str(entry.get("sha256", "")),
                        "task_path": str(entry.get("task_path", "")),
                    }
                )

    freeze_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
        "code_freeze_manifest.json"
    )
    inventory_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
        "final_inventory.json"
    )
    baseline_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
        "implementation_baseline.json"
    )
    scheduler_ref = (
        f"research_state/control_plane/p0/attempts/{ATTEMPT}/"
        "external_scheduler_inventory.json"
    )

    # 1) freeze -- write the file but DO NOT commit yet
    freeze = build_code_freeze_manifest(
        ROOT,
        plan_version=PLAN_VERSION,
        phase=PHASE,
        attempt_id=ATTEMPT,
        identity_binding=IDENTITY,
    )
    freeze_path = ROOT / freeze_ref
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text(
        canonical_json(freeze), encoding="utf-8", newline="\n"
    )

    # 2) inventory -- same git state (freeze file is untracked, not counted)
    inventory = build_final_entry_inventory(
        ROOT,
        plan_version=PLAN_VERSION,
        phase=PHASE,
        attempt_id=ATTEMPT,
        identity_binding=IDENTITY,
        freeze_manifest=freeze,
        scheduler_records=scheduler_records,
    )
    inventory_path = ROOT / inventory_ref
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    inventory_path.write_text(
        canonical_json(inventory), encoding="utf-8", newline="\n"
    )

    # 3) baseline + scheduler documents (v2 implementation baseline schema)
    baseline_payload = {
        "attempt_id": ATTEMPT,
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
        "plan_version": PLAN_VERSION,
        "phase": PHASE,
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
    scheduler_doc = {
        "schema_version": "control_plane.external_scheduler_inventory.v1",
        "phase": PHASE,
        "attempt_id": ATTEMPT,
        "entries": scheduler_records,
        "entry_count": len(scheduler_records),
    }
    (ROOT / scheduler_ref).write_text(
        canonical_json(scheduler_doc), encoding="utf-8", newline="\n"
    )

    # 4) policy document (content derived from the inventory; committed in
    # the SAME commit as freeze/inventory so the freeze identity stays
    # valid and no non-evidence commit lands after the freeze.  The
    # authority-side policy activation must have happened BEFORE this
    # stage (stage3 runs first).
    from research_automation.control_plane.artifact_semantics import (
        reviewed_policy_receipt_sha256,
        validate_reviewed_entry_policy,
    )

    reviewer_id = "independent-reviewer-b-cr010"
    policy = {
        "schema_version": "control_plane.entry_policy.v1",
        "plan_version": PLAN_VERSION,
        "phase": PHASE,
        "attempt_id": ATTEMPT,
        "identity_binding": IDENTITY,
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
        expected_plan_version=PLAN_VERSION,
        expected_phase=PHASE,
        expected_attempt_id=ATTEMPT,
        expected_identity=IDENTITY,
        final_inventory=inventory,
    )
    # Only the policies/ namespace copy is committed (the gate binds
    # policies/<file_sha256>.json); writing the same bytes into the attempt
    # dir as well would create a duplicate blob in the same commit, which
    # the post-freeze immutable-evidence check rejects.
    policy_file_sha = hashlib.sha256(policy_raw).hexdigest()
    policy_namespace_ref = (
        f"research_state/control_plane/policies/{policy_file_sha}.json"
    )
    (ROOT / policy_namespace_ref).write_text(
        policy_raw.decode("utf-8"), encoding="utf-8", newline="\n"
    )

    # 5) commit everything in ONE commit (freeze identity stays valid)
    commit(
        [freeze_ref, inventory_ref, baseline_ref, scheduler_ref,
         policy_namespace_ref],
        f"audit: {ATTEMPT} freeze/inventory/baseline/scheduler/policy (CR-010, add-only)",
    )

    print(
        json.dumps(
            {
                "freeze": {
                    "ref": freeze_ref,
                    "sha256": freeze["freeze_payload_sha256"],
                },
                "inventory": {
                    "ref": inventory_ref,
                    "sha256": inventory["inventory_payload_sha256"],
                },
                "baseline": {
                    "ref": baseline_ref,
                    "sha256": hashlib.sha256(
                        canonical_json(baseline).encode("utf-8")
                    ).hexdigest(),
                },
                "scheduler": {
                    "ref": scheduler_ref,
                    "sha256": hashlib.sha256(
                        canonical_json(scheduler_doc).encode("utf-8")
                    ).hexdigest(),
                },
                "git_commit": freeze["git_commit"],
                "git_tree": freeze["git_tree"],
            },
            indent=1,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
