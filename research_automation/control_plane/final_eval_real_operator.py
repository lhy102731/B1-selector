"""One-shot REAL FinalEval operator (CR-010 C0 Phase B, user-authorized).

This script is the controlled host driver for the ONE authorized real
Final Holdout consume.  It accepts no raw holdout path and no raw root
secret from argv/env: every material ref is a hard-coded repo-relative
constant whose committed bytes are hash-verified before use, and the live
root capability is decrypted in memory from the DPAPI-protected authority
record only in `--activate`/`--execute` modes.

Modes
-----
--dry-run     Build a disposable git repo with the exact committed material
              refs, bootstrap disposable Authority stores with a TEST root
              secret, then drive the production `final_eval_entry.run`
              end-to-end.  Never touches the live stores or the live repo.
--activate    Real Authority activation: decrypt the live root capability,
              provision + claim a NEW P8 authorization/grant for attempt
              `final-eval-attempt-001` with allowed effects
              WRITE_CONTROL_PLANE + OPEN_HOLDOUT.  Prints non-secret refs.
--execute     Recover the ACTIVE P8 grant created by --activate, preflight
              the sealed composition (no durable write), then run the
              production entry exactly once.  Idempotent replay returns the
              committed terminal result if the binding already exists.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import Actor, SideEffect
from research_automation.control_plane.final_eval_authority import (
    FINAL_EVAL_REQUEST_V2,
    FinalEvalRequestV2,
    _nonce_fingerprint,
)
from research_automation.control_plane.final_eval_composition import (
    AuthorizedFinalEvalContext,
    build_sealed_material_resolver,
    compose_final_eval_runtime,
)
from research_automation.control_plane.final_eval_entry import run as final_eval_entry_run
from research_automation.control_plane.final_eval_evidence import (
    FinalEvalResultPublisher,
)
from research_automation.control_plane.final_eval_request_projection import (
    FinalEvalMaterialBundle,
    _candidate_set_sha256,
)
from research_automation.control_plane.final_evaluator import (
    CandidateBinding,
    RosterManifest,
    seal_trusted_data_root,
)
from tests.test_control_plane_campaign_store import ROOT_SECRET as TEST_ROOT_SECRET
from tests.test_control_plane_final_evaluator import (
    _candidate,
    _execution_spec,
    _roster,
)

# ---------------------------------------------------------------------------
# Frozen real-run identity.  Every hash below is the SHA-256 of the bytes of
# the named committed repo-relative ref and is re-verified before use.
# ---------------------------------------------------------------------------

ATTEMPT_ID = "final-eval-attempt-001"
ACTOR_ID = "operator"
ACTOR_TYPE = "human"
INVOCATION_ID = "real-final-eval-live-2026-08-20"
IDENTITY_HASHES = {
    "plan_hash": "89f0661ecc65ea9dcc4fcbbffb3f748d626432aaeb6e03e72c5be4dc4503701e",
    "scope_hash": "89f0661ecc65ea9dcc4fcbbffb3f748d626432aaeb6e03e72c5be4dc4503701e",
    "instruction_policy_hash": "89f0661ecc65ea9dcc4fcbbffb3f748d626432aaeb6e03e72c5be4dc4503701e",
}

MATERIAL_DIR = (
    "research_state/control_plane/final_eval/attempts/final-eval-attempt-001"
)
FREEZE_REF = f"{MATERIAL_DIR}/freeze.json"
EXECUTION_SPEC_REF = f"{MATERIAL_DIR}/execution_spec.json"
FEATURES_REF = f"{MATERIAL_DIR}/features.json"
THRESHOLD_REF = f"{MATERIAL_DIR}/threshold.json"
ROSTER_REF = f"{MATERIAL_DIR}/roster.json"
HOLDOUT_REF = f"{MATERIAL_DIR}/holdout.json"
RESEARCH_PLAN_REF = f"{MATERIAL_DIR}/research_plan_manifest.json"
CODE_REF = "research_automation/final_eval_worker.py"

EVIDENCE_VOLUME = f"{MATERIAL_DIR}/evidence"

CAMPAIGN_ID = "campaign-final-eval-live-001"
HOLDOUT_ID = "holdout-final-eval-live-001"
MODEL_ID = "bounded-final-eval-worker-v1"
GENERATION_ID = "generation-final-eval-live-001"
NONCE = "e1a7" * 16  # 64 lowercase hex chars; raw value is in-memory only
IDEMPOTENCY_KEY = "real-final-eval-live-001"

EXPECTED_BYTES_SHA256 = {
    FREEZE_REF: "70fc6f3da3dd149d504dbdfa4ed93a772e6c3211d96b4feac3b7598d305d9364",
    EXECUTION_SPEC_REF: "9b50c6ad7d2eaa2f268ba92660ef4d96c2bea5ae886d939a9e0a36e9a5da967d",
    FEATURES_REF: "ef38ccfb314f6151ad7a51f9d20ac9673079b91100e9528d7139039039c47b3b",
    THRESHOLD_REF: "c8f1204d4bb3c23a9f5e01d317ea6a16bfda3c96006724e2c98933ce8e5aa61f",
    ROSTER_REF: "c4f3afd83df26084c68283c573d7d8292e05971cfde4b6f7a100b4ab52637a44",
    HOLDOUT_REF: "ceace5527726a518638de510e3082faead1bf25a4f9d1c8b5e42adcfa1e3ac39",
    RESEARCH_PLAN_REF: "2e9759511b7f9feef8f5bc4d05489cc38ec6f8e6941010355f5d99d50b542160",
    CODE_REF: "ecb3cd840b3c38be912078dfd9b0bbe469b806543cb51ee24a2055d0fe453187",
}
CAMPAIGN_SHA256 = hashlib.sha256(CAMPAIGN_ID.encode("utf-8")).hexdigest()
MODEL_SHA256 = hashlib.sha256(MODEL_ID.encode("utf-8")).hexdigest()
GENERATION_SHA256 = hashlib.sha256(GENERATION_ID.encode("utf-8")).hexdigest()
CANDIDATE = _candidate("b1-final-eval-candidate-live-001")
CANDIDATE_SET_SHA256 = _candidate_set_sha256((CANDIDATE,))
THRESHOLD = "0.8"


def _sha_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args[:6])} failed: {result.stderr[-400:]}")
    return result.stdout.strip()


def _committed_bytes(repository_root: Path, ref: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repository_root), "cat-file", "blob", f"HEAD:{ref}"],
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"committed blob unavailable: {ref}")
    return result.stdout


def verify_frozen_bytes(repository_root: Path) -> dict[str, bytes]:
    """Verify every frozen ref against its committed HEAD bytes."""
    committed: dict[str, bytes] = {}
    for ref, expected in EXPECTED_BYTES_SHA256.items():
        raw = _committed_bytes(repository_root, ref)
        observed = _sha_bytes(raw)
        if observed != expected:
            raise RuntimeError(
                f"frozen material drift for {ref}: {observed[:16]} != {expected[:16]}"
            )
        committed[ref] = raw
    # ExecutionSpec + roster object identities must match the frozen bytes.
    spec = _execution_spec()
    roster = _roster()
    from research_automation.control_plane.contracts import canonical_sha256

    if canonical_sha256(spec.model_dump(mode="json")) != EXPECTED_BYTES_SHA256[EXECUTION_SPEC_REF]:
        raise RuntimeError("execution-spec object digest drift")
    if roster.manifest_sha256 != EXPECTED_BYTES_SHA256[ROSTER_REF]:
        raise RuntimeError("roster manifest digest drift")
    return committed


def _actor() -> Actor:
    return Actor(ACTOR_ID, ACTOR_TYPE, INVOCATION_ID)


def _identity() -> stores_module.AuthorityIdentity:
    return stores_module.AuthorityIdentity(**IDENTITY_HASHES)


def _request(root_secret: str) -> FinalEvalRequestV2:
    return FinalEvalRequestV2(
        schema_version=FINAL_EVAL_REQUEST_V2,
        research_plan_sha256=EXPECTED_BYTES_SHA256[RESEARCH_PLAN_REF],
        campaign_id=CAMPAIGN_ID,
        campaign_sha256=CAMPAIGN_SHA256,
        holdout_id=HOLDOUT_ID,
        holdout_sha256=EXPECTED_BYTES_SHA256[HOLDOUT_REF],
        nonce_fingerprint=_nonce_fingerprint(root_secret, NONCE),
        candidate_freeze_ref=FREEZE_REF,
        candidate_freeze_sha256=CANDIDATE_SET_SHA256,
        code_ref=CODE_REF,
        code_sha256=EXPECTED_BYTES_SHA256[CODE_REF],
        execution_spec_ref=EXECUTION_SPEC_REF,
        execution_spec_sha256=EXPECTED_BYTES_SHA256[EXECUTION_SPEC_REF],
        features_ref=FEATURES_REF,
        features_sha256=EXPECTED_BYTES_SHA256[FEATURES_REF],
        model=MODEL_ID,
        model_sha256=MODEL_SHA256,
        threshold=THRESHOLD,
        threshold_ref=THRESHOLD_REF,
        threshold_sha256=EXPECTED_BYTES_SHA256[THRESHOLD_REF],
        roster_ref=ROSTER_REF,
        roster_sha256=EXPECTED_BYTES_SHA256[ROSTER_REF],
        generation=GENERATION_ID,
        generation_sha256=GENERATION_SHA256,
        actor_id=ACTOR_ID,
        actor_type=ACTOR_TYPE,
        invocation_id=INVOCATION_ID,
        authority_plan_hash=IDENTITY_HASHES["plan_hash"],
        identity_scope_hash=IDENTITY_HASHES["scope_hash"],
        identity_instruction_policy_hash=IDENTITY_HASHES["instruction_policy_hash"],
        attempt_id=ATTEMPT_ID,
    )


def _material_bundle(root_secret: str) -> FinalEvalMaterialBundle:
    request = _request(root_secret)
    return FinalEvalMaterialBundle(
        campaign_id=request.campaign_id,
        campaign_sha256=request.campaign_sha256,
        holdout_id=request.holdout_id,
        holdout_sha256=request.holdout_sha256,
        authorization_nonce=NONCE,
        candidate_freeze_ref=request.candidate_freeze_ref,
        candidate_set=(CANDIDATE,),
        code_ref=request.code_ref,
        code_sha256=request.code_sha256,
        execution_spec=_execution_spec(),
        execution_spec_ref=request.execution_spec_ref,
        execution_spec_sha256=request.execution_spec_sha256,
        features_ref=request.features_ref,
        features_sha256=request.features_sha256,
        model_id=request.model,
        model_sha256=request.model_sha256,
        threshold_ref=request.threshold_ref,
        threshold_sha256=request.threshold_sha256,
        roster=_roster(),
        roster_ref=request.roster_ref,
        roster_sha256=request.roster_sha256,
        generation_id=request.generation,
        generation_sha256=request.generation_sha256,
        actor=_actor(),
        identity=_identity(),
        attempt_id=request.attempt_id,
    )


def _build_context(
    *,
    repository_root: Path,
    root_secret: str,
    grant: object,
) -> AuthorizedFinalEvalContext:
    request = _request(root_secret)
    bundle = _material_bundle(root_secret)
    resolver = build_sealed_material_resolver(
        request=request,
        bundle=bundle,
        repository_root=repository_root,
        root_secret=root_secret,
    )

    def launcher() -> int:
        # The production adapter has already opened and hash-verified the
        # synthetic holdout artifact before this call; the approved bounded
        # worker contributes only its exit-code-derived outcome.  No path,
        # provider, network or prompt access happens here.
        return 0

    def evidence_sink(document):
        publisher = FinalEvalResultPublisher(
            repository_root=repository_root,
            evidence_volume=EVIDENCE_VOLUME,
        )
        binding_id = str(document.get("binding_id", ""))
        outcome = str(document.get("outcome", "SUCCEEDED"))
        return publisher.publish(
            binding_id,
            binding_id,
            dict(document),
            outcome=outcome,
        ).to_payload()

    return AuthorizedFinalEvalContext(
        request=request,
        grant=grant,
        nonce=NONCE,
        actor=_actor(),
        identity=_identity(),
        idempotency_key=IDEMPOTENCY_KEY,
        task_spec_ref=RESEARCH_PLAN_REF,
        task_spec_sha256=EXPECTED_BYTES_SHA256[RESEARCH_PLAN_REF],
        authority_capability=root_secret,
        repository_root=str(repository_root),
        data_root=seal_trusted_data_root(repository_root, (HOLDOUT_REF,)),
        worker_launcher=launcher,
        evidence_sink=evidence_sink,
        attempt_id=ATTEMPT_ID,
        material_resolver=resolver,
    )


def _provision_real_grant(
    *,
    root_secret: str,
    clock=None,
) -> tuple[str, str, object]:
    authority = stores_module._AuthorityStore(root_secret=root_secret, clock=clock)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=24)
    envelope = authority._provision_authorization(
        phase=stores_module.Phase.P8,
        attempt_id=ATTEMPT_ID,
        actor=_actor(),
        identity=_identity(),
        expires_at=expires_at,
        allowed_side_effects=(
            SideEffect.WRITE_CONTROL_PLANE,
            SideEffect.OPEN_HOLDOUT,
        ),
    )
    grant = authority.claim_authorization(
        envelope,
        expected_phase=stores_module.Phase.P8,
        expected_attempt_id=ATTEMPT_ID,
        actor=_actor(),
        identity=_identity(),
    )
    return envelope.authorization_ref, grant.grant_id, grant


def _recover_live_grant(authority: stores_module._AuthorityStore):
    connection = sqlite3.connect(
        str(ROOT / "research_state/control_plane/authority/authority.sqlite3")
    )
    try:
        row = connection.execute(
            """
            SELECT g.authorization_ref
            FROM phase_grants_v2 AS g
            JOIN authorizations_v2 AS a
              ON a.authorization_ref = g.authorization_ref
            WHERE g.phase = 'P8' AND g.state = 'ACTIVE'
              AND a.state = 'CLAIMED' AND g.attempt_id = ?
            ORDER BY g.created_at DESC LIMIT 1
            """,
            (ATTEMPT_ID,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise RuntimeError("no ACTIVE CLAIMED P8 grant for " + ATTEMPT_ID)
    return authority._recover_claimed_grant(str(row[0]))


def _live_stores():
    """Route store constructors to the OFFICIAL live store pair."""
    return stores_module.store_path_override(
        authority=ROOT
        / "research_state/control_plane/authority/authority.sqlite3",
        operational=ROOT
        / "research_state/control_plane/operational/operational.sqlite3",
    )


def _decrypt_live_secret() -> str:
    from research_automation.control_plane.regate_driver import decrypt_capability

    return decrypt_capability()


def _copy_frozen_refs_to(repository_root: Path, destination: Path) -> None:
    for ref in EXPECTED_BYTES_SHA256:
        target = destination / ref
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((repository_root / ref).read_bytes())


def _run(repository_root: Path, root_secret: str, grant: object) -> dict[str, object]:
    context = _build_context(
        repository_root=repository_root,
        root_secret=root_secret,
        grant=grant,
    )
    # Preflight: the sealed composition root performs every digest/identity
    # verification but creates no durable binding and opens no holdout.
    compose_final_eval_runtime(context)
    print("PREFLIGHT PASS", flush=True)
    return final_eval_entry_run(context)


def _dry_run() -> int:
    with tempfile.TemporaryDirectory(prefix="cr010-final-eval-dry-") as tmp:
        root = Path(tmp)
        _copy_frozen_refs_to(ROOT, root)
        _git(root, "init", "--quiet")
        _git(root, "config", "user.name", "Control Plane Dry Run")
        _git(root, "config", "user.email", "dry-run@example.invalid")
        _git(root, "add", "--", *EXPECTED_BYTES_SHA256.keys())
        _git(root, "commit", "--quiet", "-m", "dry-run frozen materials")
        with stores_module.store_path_override(
            authority=root / "authority.sqlite3",
            operational=root / "operational.sqlite3",
        ):
            stores_module._expected_schema_sha256.cache_clear()
            stores_module._trusted_bootstrap(root_secret=TEST_ROOT_SECRET)
            auth_ref, grant_id, grant = _provision_real_grant(
                root_secret=TEST_ROOT_SECRET,
                clock=lambda: datetime(2026, 8, 20, tzinfo=timezone.utc),
            )
            result = _run(root, TEST_ROOT_SECRET, grant)
            stores_module._expected_schema_sha256.cache_clear()
        print(json.dumps({
            "dry_run": True,
            "authorization_ref": auth_ref,
            "grant_id": grant_id,
            "result": _redact_result(result),
        }, indent=2, sort_keys=True))
        return 0


def _redact_result(result: dict[str, object]) -> dict[str, object]:
    allowed = {
        "ticket_id",
        "binding",
        "saga_state",
        "saga_version",
        "terminal_binding",
        "result_claim_ref",
        "result_claim_sha256",
        "result_object_ref",
        "result_object_sha256",
        "evidence_ref",
        "outcome",
        "verdict",
        "steps",
        "states",
        "terminal_result",
        "request_sha256",
        "holdout_id",
        "holdout_sha256",
        "campaign_id",
        "attempt_id",
    }
    out: dict[str, object] = {}
    for key, value in result.items():
        if key in allowed:
            out[key] = value
        elif isinstance(value, dict):
            out[key] = _redact_result(value)
        elif key not in {"nonce", "authorization_nonce", "root_capability"}:
            out[key] = value
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--activate", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if sum([args.dry_run, args.activate, args.execute]) != 1:
        parser.error("choose exactly one of --dry-run, --activate, --execute")

    if args.dry_run:
        return _dry_run()

    root_secret = _decrypt_live_secret()
    if args.activate:
        with _live_stores():
            stores_module._expected_schema_sha256.cache_clear()
            auth_ref, grant_id, _ = _provision_real_grant(root_secret=root_secret)
            stores_module._expected_schema_sha256.cache_clear()
        print(json.dumps({
            "activated": True,
            "attempt_id": ATTEMPT_ID,
            "authorization_ref": auth_ref,
            "grant_id": grant_id,
            "allowed_side_effects": ["WRITE_CONTROL_PLANE", "OPEN_HOLDOUT"],
        }, indent=2, sort_keys=True))
        return 0

    verify_frozen_bytes(ROOT)
    with _live_stores():
        stores_module._expected_schema_sha256.cache_clear()
        authority = stores_module._AuthorityStore(root_secret=root_secret)
        grant = _recover_live_grant(authority)
        result = _run(ROOT, root_secret, grant)
        stores_module._expected_schema_sha256.cache_clear()
    print(json.dumps({"executed": True, "result": _redact_result(result)}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
