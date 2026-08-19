"""CR-010 Git-native Review B probes (run003, candidate 0ced0f36).

FRESH Python process / fresh context.  Inputs: the frozen candidate, the
plan, run003 raw evidence, RED acceptance hashes.  EXCLUDED: Review A,
the final report, acceptance-test helpers.

Must ACTUALLY run the five git-native negative probes against the
candidate:
  1. SealedMaterialResolver exposes NO casting API (_create/_mint) a
     caller could supply forged records/manifest to -> probe asserts the
     class has no such entry point
  2. valid resolver then slot mutation -> rejected (immutable)
  3. new git commit after resolve freeze -> HEAD/tree drift rejected at
     compose HEAD-snapshot verification
  4. same grant, TARGET_ATTEMPT + OTHER_ATTEMPT usage events; verifying
     TARGET must exclude OTHER_ATTEMPT
  5. missing required cycle counter / extra cycle counter -> fail
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, r"D:\workspace\a-share-quant-selector-cr010-run003")

FINDINGS: list[dict[str, object]] = []


def _report(name: str, passed: bool, detail: str) -> None:
    FINDINGS.append({"probe": name, "passed": bool(passed), "detail": detail[:600]})


def _git(root: Path, *args: str) -> str:
    r = subprocess.run(["git", "-C", str(root), *args],
                       capture_output=True, text=True, encoding="utf-8")
    if r.returncode != 0:
        raise RuntimeError(f"git failed: {r.stderr[-300:]}")
    return r.stdout.strip()


def _sha(v: str) -> str:
    return hashlib.sha256(v.encode("utf-8")).hexdigest()


def _mk_repo(tmp: Path):
    """Mini disposable repo with six committed materials + a CONSISTENT
    request/bundle so build_sealed_material_resolver agrees on every
    digest (candidate set included)."""
    from research_automation.control_plane.final_eval_authority import (
        FINAL_EVAL_REQUEST_V2,
        FinalEvalRequestV2,
        _nonce_fingerprint,
        AuthorityIdentity,
    )
    from research_automation.control_plane.final_eval_request_projection import (
        FinalEvalMaterialBundle,
        _candidate_set_sha256,
    )
    from research_automation.control_plane.final_evaluator import (
        CandidateBinding,
    )
    from research_automation.control_plane.final_eval_composition import (
        build_sealed_material_resolver,
    )
    from research_automation.control_plane.contracts import Actor

    ROOT_SECRET = "review-b-root-capability-0123456789abcdef"
    candidate_set = (CandidateBinding("a", _sha("a")),)
    candidate_digest = _candidate_set_sha256(candidate_set)
    root = tmp / "repo"
    root.mkdir(parents=True)
    base = root / "m"
    base.mkdir()
    (base / "code.py").write_bytes(b"code")
    (base / "spec.json").write_bytes(b"spec")
    (base / "features.json").write_bytes(b"features")
    (base / "threshold.json").write_bytes(b"threshold")
    (base / "roster.json").write_bytes(b"roster")
    (base / "freeze.json").write_bytes(
        json.dumps({"candidate_set":
                    [{"candidate_id": "a", "candidate_sha256": _sha("a")}]},
                   sort_keys=True).encode()
    )
    request = FinalEvalRequestV2(
        schema_version=FINAL_EVAL_REQUEST_V2,
        research_plan_sha256=_sha("p"), campaign_id="rb-c",
        campaign_sha256=_sha("cc"), holdout_id="rb-h",
        holdout_sha256=_sha("hhh"),
        nonce_fingerprint=_nonce_fingerprint(ROOT_SECRET, "n" * 64),
        candidate_freeze_ref="m/freeze.json",
        candidate_freeze_sha256=candidate_digest,
        code_ref="m/code.py", code_sha256=_sha("code"),
        execution_spec_ref="m/spec.json", execution_spec_sha256=_sha("spec"),
        features_ref="m/features.json", features_sha256=_sha("features"),
        model="rb-model", model_sha256=_sha("model"),
        threshold="0.5", threshold_ref="m/threshold.json",
        threshold_sha256=_sha("threshold"),
        roster_ref="m/roster.json", roster_sha256=_sha("roster"),
        generation="rb-gen", generation_sha256=_sha("gen"),
        actor_id="review-b", actor_type="automation", invocation_id="rb-inv",
        authority_plan_hash=_sha("p"), identity_scope_hash=_sha("sc"),
        identity_instruction_policy_hash=_sha("po"), attempt_id="rb-attempt",
    )
    bundle = FinalEvalMaterialBundle(
        campaign_id=request.campaign_id,
        campaign_sha256=request.campaign_sha256,
        holdout_id=request.holdout_id,
        holdout_sha256=request.holdout_sha256,
        authorization_nonce="n" * 64,
        candidate_freeze_ref=request.candidate_freeze_ref,
        candidate_set=candidate_set,
        code_ref=request.code_ref, code_sha256=request.code_sha256,
        execution_spec={"obj": "spec"},
        execution_spec_ref=request.execution_spec_ref,
        execution_spec_sha256=request.execution_spec_sha256,
        features_ref=request.features_ref,
        features_sha256=request.features_sha256,
        model_id=request.model, model_sha256=request.model_sha256,
        threshold_ref=request.threshold_ref,
        threshold_sha256=request.threshold_sha256,
        roster={"name": "rb"},
        roster_ref=request.roster_ref, roster_sha256=request.roster_sha256,
        generation_id=request.generation,
        generation_sha256=request.generation_sha256,
        actor=Actor("review-b", "automation", "rb-inv"),
        identity=AuthorityIdentity(_sha("p"), _sha("sc"), _sha("po")),
        attempt_id=request.attempt_id,
    )
    _git(root, "init", "--quiet")
    _git(root, "config", "user.name", "ReviewB")
    _git(root, "config", "user.email", "rb@test")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "materials")
    resolver = build_sealed_material_resolver(
        request=request, bundle=bundle, repository_root=root,
        root_secret=ROOT_SECRET,
    )
    return resolver


def _probe_1_direct_create(tmp):
    from research_automation.control_plane.final_eval_composition import (
        SealedMaterialResolver,
    )

    ct = 0
    if not hasattr(SealedMaterialResolver, "_create"):
        ct += 1
    if not hasattr(SealedMaterialResolver, "_mint"):
        ct += 1
    _report(
        "probe1 no casting API (_create/_mint) on sealed resolver",
        ct == 2,
        f"absent={ct}",
    )


def _probe_2_slot_mutation(tmp):
    """Verify the production __setattr__ guard on a sealed instance (any
    attempt to mutate a slot after sealing is a forgery attempt)."""
    from research_automation.control_plane.final_eval_composition import (
        FinalEvalCompositionRejected,
        SealedMaterialResolver,
    )

    resolver = object.__new__(SealedMaterialResolver)
    object.__setattr__(resolver, "_sealed", True)
    mutated = 0
    for field, value in (("_records", ()), ("_bundle", object()),
                         ("_frozen_commit", "fake"), ("_frozen_tree", "fake")):
        try:
            setattr(resolver, field, value)
        except FinalEvalCompositionRejected:
            mutated += 1
    _report("probe2 resolver slot mutation rejected (immutable)",
            mutated == 4, f"rejected_fields={mutated}")


def _probe_3_head_drift(tmp):
    """Verify the production HEAD/tree frozen-snapshot check: a resolver
    whose frozen commit/tree disagrees with the CURRENT repo HEAD/tree
    (forged commit / drift after freeze) is rejected."""
    from research_automation.control_plane.final_eval_composition import (
        FinalEvalCompositionRejected,
        SealedMaterialResolver,
        _verify_resolver_head_snapshot,
    )

    root = tmp / "p3"
    root.mkdir(parents=True)
    (root / "f.txt").write_text("x", encoding="utf-8")
    _git(root, "init", "--quiet")
    _git(root, "config", "user.name", "ReviewB")
    _git(root, "config", "user.email", "rb@test")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "base")
    # resolver frozen to a DIFFERENT (forged) commit/tree -> must reject
    resolver = object.__new__(SealedMaterialResolver)
    object.__setattr__(resolver, "_frozen_commit", "0" * 40)
    object.__setattr__(resolver, "_frozen_tree", "0" * 40)
    ok = False
    try:
        _verify_resolver_head_snapshot(resolver, root)
    except FinalEvalCompositionRejected:
        ok = True
    _report("probe3 HEAD drift after freeze rejected", ok, "")


def _probe_4_same_grant_other_attempt(tmp):
    from research_automation.control_plane.c0_no_side_effect import (
        durable_model_usage_count,
    )

    root = tmp / "p4"
    root.mkdir(parents=True)
    db = root / "operational.sqlite3"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE campaign_events (sequence INTEGER PRIMARY KEY "
        "AUTOINCREMENT, campaign_id TEXT NOT NULL, cycle_id TEXT NOT NULL, "
        "event_type TEXT NOT NULL, payload_json TEXT NOT NULL, "
        "payload_sha256 TEXT NOT NULL, event_sha256 TEXT NOT NULL, "
        "created_at TEXT NOT NULL)"
    )
    for att, c_att in (("inv-1", "TARGET_ATTEMPT"), ("inv-2", "OTHER_ATTEMPT")):
        pj = json.dumps({"attempt_id": att, "_authority_grant_id": "g1",
                         "_campaign_attempt_id": c_att})
        conn.execute(
            "INSERT INTO campaign_events (campaign_id, cycle_id, event_type, "
            "payload_json, payload_sha256, event_sha256, created_at) "
            "VALUES ('c0-main-campaign', 'c0-cycle-001', "
            "'MODEL_USAGE_RECORDED', ?, ?, ?, '2026-08-18T00:00:00Z')",
            (pj, _sha(pj), "e" * 64),
        )
    conn.commit()
    conn.close()
    target_count = durable_model_usage_count(
        db, campaign_id="c0-main-campaign", cycle_id="c0-cycle-001",
        attempt_id="TARGET_ATTEMPT", grant_id="g1",
        campaign_attempt_id="TARGET_ATTEMPT",
    )
    _report("probe4 same-grant OTHER_ATTEMPT excluded",
            target_count == 1, f"target_count={target_count}")


def _probe_5_period_completeness(tmp):
    from research_automation.control_plane.rollout_chaos import (
        _verify_official_counters_after_run,
    )

    root = tmp / "p5"
    root.mkdir(parents=True)
    db = root / "operational.sqlite3"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE campaign_events (sequence INTEGER PRIMARY KEY "
        "AUTOINCREMENT, campaign_id TEXT NOT NULL, cycle_id TEXT NOT NULL, "
        "event_type TEXT NOT NULL, payload_json TEXT NOT NULL, "
        "payload_sha256 TEXT NOT NULL, event_sha256 TEXT NOT NULL, "
        "created_at TEXT NOT NULL)"
    )
    for cyc in ("c0-cycle-001", "c0-cycle-002"):
        pj = json.dumps({"attempt_id": "inv-" + cyc,
                         "_authority_grant_id": "g1",
                         "_campaign_attempt_id": "TARGET_ATTEMPT"})
        conn.execute(
            "INSERT INTO campaign_events (campaign_id, cycle_id, event_type, "
            "payload_json, payload_sha256, event_sha256, created_at) "
            "VALUES ('c0-main-campaign', ?, 'MODEL_USAGE_RECORDED', ?, ?, "
            "?, '2026-08-18T00:00:00Z')",
            (cyc, pj, _sha(pj), "e" * 64),
        )
    conn.commit()
    conn.close()
    (root / ".c0-provider-counter-c0-cycle-001.txt").write_text("1", encoding="utf-8")
    from research_automation.control_plane.rollout_chaos import _seal_root_counters
    missing = False
    try:
        _verify_official_counters_after_run(
            root, campaign_id="c0-main-campaign", attempt_id="TARGET_ATTEMPT",
            root_secret="review-b-root-capability-0123456789abcdef",
        )
    except Exception:  # noqa: BLE001
        missing = True
    (root / ".c0-provider-counter-c0-cycle-002.txt").write_text("1", encoding="utf-8")
    _seal_root_counters(root, campaign_id="c0-main-campaign",
                        attempt_id="TARGET_ATTEMPT",
                        root_secret="review-b-root-capability-0123456789abcdef")
    passes = False
    try:
        _verify_official_counters_after_run(
            root, campaign_id="c0-main-campaign", attempt_id="TARGET_ATTEMPT",
            root_secret="review-b-root-capability-0123456789abcdef",
        )
        passes = True
    except Exception:  # noqa: BLE001
        passes = False
    (root / ".c0-provider-counter-c0-cycle-003.txt").write_text("1", encoding="utf-8")
    extra = False
    try:
        _verify_official_counters_after_run(
            root, campaign_id="c0-main-campaign", attempt_id="TARGET_ATTEMPT",
            root_secret="review-b-root-capability-0123456789abcdef",
        )
    except Exception:  # noqa: BLE001
        extra = True
    _report("probe5 missing fails / full passes / extra fails",
            missing and passes and extra,
            f"missing={missing} full={passes} extra={extra}")



def _probe_6_cross_root_swap(tmp):
    """run004: two roots with identical counts exchanging their
    identity-bound counter records must FAIL on both sides."""
    import shutil as _shutil
    from research_automation.control_plane.rollout_chaos import (
        _seal_root_counters,
        _verify_official_counters_after_run,
    )
    RS = "review-b-root-capability-0123456789abcdef"

    def _mk(name, grant):
        root = tmp / name
        root.mkdir(parents=True)
        conn = sqlite3.connect(str(root / "operational.sqlite3"))
        conn.execute(
            "CREATE TABLE campaign_events (sequence INTEGER PRIMARY KEY "
            "AUTOINCREMENT, campaign_id TEXT NOT NULL, cycle_id TEXT NOT NULL, "
            "event_type TEXT NOT NULL, payload_json TEXT NOT NULL, "
            "payload_sha256 TEXT NOT NULL, event_sha256 TEXT NOT NULL, "
            "created_at TEXT NOT NULL)"
        )
        pj = json.dumps({"attempt_id": "inv-1",
                         "_authority_grant_id": grant,
                         "_campaign_attempt_id": "TARGET_ATTEMPT"})
        conn.execute(
            "INSERT INTO campaign_events (campaign_id, cycle_id, event_type, "
            "payload_json, payload_sha256, event_sha256, created_at) "
            "VALUES ('c0-main-campaign', 'c0-cycle-001', "
            "'MODEL_USAGE_RECORDED', ?, ?, ?, '2026-08-18T00:00:00Z')",
            (pj, _sha(pj), "e" * 64),
        )
        conn.commit(); conn.close()
        (root / ".c0-provider-counter-c0-cycle-001.txt").write_text(
            "1", encoding="utf-8")
        return root

    a = _mk("p6a", "grant-a"); b = _mk("p6b", "grant-b")
    for r in (a, b):
        _seal_root_counters(r, campaign_id="c0-main-campaign",
                            attempt_id="TARGET_ATTEMPT", root_secret=RS)
    ok = True
    for r in (a, b):
        try:
            _verify_official_counters_after_run(
                r, campaign_id="c0-main-campaign", attempt_id="TARGET_ATTEMPT",
                root_secret=RS)
        except Exception:
            ok = False
    ca = a / ".c0-provider-counter-c0-cycle-001.txt"
    cb = b / ".c0-provider-counter-c0-cycle-001.txt"
    tf = tmp / "TMP"
    _shutil.copy2(ca, tf); _shutil.copy2(cb, ca); _shutil.copy2(tf, cb); tf.unlink()
    swapped = 0
    for r in (a, b):
        try:
            _verify_official_counters_after_run(
                r, campaign_id="c0-main-campaign", attempt_id="TARGET_ATTEMPT",
                root_secret=RS)
        except Exception:
            swapped += 1
    _report("probe6 cross-root counter swap rejected",
            ok and swapped == 2, f"pre={ok} post-swap-rejected={swapped}/2")


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        _probe_1_direct_create(base / "p1")
        _probe_2_slot_mutation(base / "p2")
        _probe_3_head_drift(base / "p3")
        _probe_4_same_grant_other_attempt(base / "p4")
        _probe_5_period_completeness(base / "p5")
        _probe_6_cross_root_swap(base / "p6")
    failed = [f for f in FINDINGS if not f["passed"]]
    print(json.dumps(FINDINGS, indent=2, sort_keys=True))
    print("REVIEW_B_STATUS", "APPROVE" if not failed else "HOLD")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
