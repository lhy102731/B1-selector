"""CR-010 C0 (Phase B): final evaluation staging tests.

Runs the PRODUCTION composition root against a synthetic staging Holdout
in a disposable root: one consume/open, one worker execution, one fixed
claim, AUTHORITY_TERMINAL/CLOSED, bounded/redacted result; a fresh-process
crash matrix at every durable boundary (recovery with a NEW lease, no
reopen/recompute/reissue); concurrency/identity/replay negatives; and a
full side-effect surface audit.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import (
    Actor,
    canonical_json,
    canonical_sha256,
)
from research_automation.control_plane.final_eval_authority import (
    FINAL_EVAL_REQUEST_V2,
    FinalEvalRequestV2,
    _nonce_fingerprint,
)
from research_automation.control_plane.final_eval_composition import (
    AuthorizedFinalEvalContext,
    SealedMaterialResolver,
    compose_final_eval_runtime,
    compose_holdout_store,
    compose_staging_backend,
)
from research_automation.control_plane.final_eval_entry import run as entry_run
from research_automation.control_plane.final_eval_holdout_store import (
    SqliteHoldoutStore,
)
from research_automation.control_plane.final_eval_request_projection import (
    FinalEvalMaterialBundle,
    adapt_evaluator_request_v1_test_only,
)
from research_automation.control_plane.final_evaluator import (
    AuthorityBroker,
    HoldoutDataBackend,
    TrustedEvaluator,
    TrustedEvaluatorAdapter,
    seal_trusted_data_root,
)
from tests.test_control_plane_campaign_store import (
    P8_GRANT_IDENTITY,
    ROOT_SECRET,
    _claim_campaign_grant,
)
from tests.test_control_plane_final_eval_orchestrator import (
    P8_IDENTITY,
    _make_request,
    _real_publisher_sink,
)
from tests.test_control_plane_final_evaluator import (
    _candidate,
    _candidate_set_digest,
    _execution_spec,
    _roster,
    _sha,
)

NONCE = "0123456789abcdef" * 4
ATTEMPT = "p8-attempt-003"
EVIDENCE_VOLUME = (
    "research_state/control_plane/p8/attempts/p8-attempt-003/evidence"
)
SYNTH_HOLDOUT_REF = "frozen/holdout.parquet"
GRANT_JSON_REF = ".staging-grant.json"
REQUEST_JSON_REF = ".staging-request.json"
CONTEXT_JSON_REF = ".staging-context.json"
WORKER_COUNTER_REF = ".staging-worker-count.txt"
OPEN_COUNTER_REF = ".staging-open-count.txt"
CRASH_HOOK_REF = ".staging-crash-hook.txt"


def _synthetic_holdout_document(holdout_id: str) -> dict[str, object]:
    return {
        "schema_version": "control_plane.synthetic_holdout.v1",
        "holdout_id": holdout_id,
        "metrics": [{"name": "rows", "value": 120}],
        "counts": [{"name": "opened_once", "value": 1}],
        "sha256s": [
            {
                "artifact_id": "synthetic-holdout",
                "sha256": "1" * 64,
            }
        ],
        "evidence_refs": [
            "research_state/control_plane/p8/attempts/p8-attempt-003/"
            "evidence/synthetic_holdout.json"
        ],
    }


def _build_scenario(
    root: Path,
    *,
    campaign_id: str = "campaign-staging-1",
    holdout_id: str = "holdout-staging-1",
    idempotency_key: str = "p8-staging-1",
) -> dict[str, object]:
    """Bootstrap the disposable authority + P8 grant + synthetic holdout
    and return the serialized scenario inputs for child processes."""
    from datetime import datetime, timezone as _timezone

    authority_db = root / "authority.sqlite3"
    operational_db = root / "operational.sqlite3"
    with stores_module.store_path_override(
        authority=authority_db,
        operational=operational_db,
    ):
        stores_module._expected_schema_sha256.cache_clear()
        stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
        actor = Actor("operator-1", "human", "final-eval-op-cr009")
        identity = stores_module.AuthorityIdentity(**P8_GRANT_IDENTITY)
        authority = stores_module._AuthorityStore(
            root_secret=ROOT_SECRET,
            clock=lambda: datetime(2026, 8, 16, tzinfo=_timezone.utc),
        )
        envelope = authority._provision_authorization(
            phase=stores_module.Phase.P8,
            attempt_id=ATTEMPT,
            actor=actor,
            identity=identity,
            expires_at=datetime(2027, 1, 1, tzinfo=_timezone.utc),
            allowed_side_effects=(
                stores_module.SideEffect.READ,
                stores_module.SideEffect.WRITE_CONTROL_PLANE,
            ),
        )
        grant = authority.claim_authorization(
            envelope,
            expected_phase=stores_module.Phase.P8,
            expected_attempt_id=ATTEMPT,
            actor=actor,
            identity=identity,
        )
        stores_module._expected_schema_sha256.cache_clear()
    # synthetic holdout artifact (content-addressed by holdout_sha256)
    document = _synthetic_holdout_document(holdout_id)
    raw = json.dumps(document, sort_keys=True).encode("utf-8")
    holdout_sha256 = hashlib.sha256(raw).hexdigest()
    (root / SYNTH_HOLDOUT_REF).parent.mkdir(parents=True, exist_ok=True)
    (root / SYNTH_HOLDOUT_REF).write_bytes(raw)
    request = _request_for_scenario(holdout_id, holdout_sha256)
    # CR-010 A1 (frozen acceptance): the staging fixture must create and
    # COMMIT real non-empty research materials in the disposable git repo
    # (the synthetic Holdout is NEVER committed).  The committed blob
    # bytes hash to the V2 request identity fields.
    _commit_staging_materials(root, request)
    (root / GRANT_JSON_REF).write_text(
        json.dumps(_serialize_grant(grant)), encoding="utf-8"
    )
    (root / REQUEST_JSON_REF).write_text(
        json.dumps(request.to_payload()), encoding="utf-8"
    )
    scenario = {
        "root": str(root),
        "grant": _serialize_grant(grant),
        "request": request.to_payload(),
        "holdout_id": holdout_id,
        "holdout_sha256": holdout_sha256,
        "campaign_id": campaign_id,
        "idempotency_key": idempotency_key,
    }
    (root / CONTEXT_JSON_REF).write_text(
        json.dumps(_context_payload(scenario)), encoding="utf-8"
    )
    return scenario


def _commit_staging_materials(root: Path, request: FinalEvalRequestV2) -> None:
    """CR-010 A1: write the six ref-backed research materials and COMMIT
    them in the disposable git repo (the synthetic Holdout under
    ``frozen/`` is never added)."""
    from research_automation.control_plane.contracts import canonical_sha256

    def _git(*args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise RuntimeError(f"git {args[0]} failed: {result.stderr[-300:]}")
        return result.stdout.strip()

    if not (root / ".git").exists():
        _git("init", "--quiet")
        _git("config", "user.name", "Staging")
        _git("config", "user.email", "staging@test")
    candidates = (_candidate("candidate-a"), _candidate("candidate-b"))
    freeze = {
        "candidate_set": [
            {"candidate_id": c.candidate_id,
             "candidate_sha256": c.candidate_sha256}
            for c in candidates
        ]
    }
    execution_spec = _execution_spec()
    roster = _roster()
    refs_to_bytes: dict[str, bytes] = {
        request.candidate_freeze_ref: (
            canonical_json(freeze).encode("utf-8")
        ),
        request.code_ref: b"code",
        request.execution_spec_ref: (
            canonical_json(execution_spec.model_dump(mode="json")).encode("utf-8")
        ),
        request.features_ref: b"features",
        request.threshold_ref: b"threshold",
        request.roster_ref: (
            canonical_json(
                {
                    "cycle_id": roster.cycle_id,
                    "members": tuple(m.to_payload() for m in roster.members),
                }
            ).encode("utf-8")
        ),
    }
    for ref, raw in refs_to_bytes.items():
        path = root / ref
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    _git("add", "--", *refs_to_bytes)
    if _git("status", "--porcelain", "--", *refs_to_bytes).strip():
        _git("commit", "-q", "-m", "staging materials (acceptance A1)")
    # the committed blob bytes MUST hash to the V2 request identity
    # (candidate freeze: parse the JSON and recompute the candidate-set
    # digest -- the file bytes hash itself is a different value)
    for ref, raw in refs_to_bytes.items():
        if ref == request.candidate_freeze_ref:
            import json as _json

            from research_automation.control_plane.final_evaluator import (
                CandidateBinding,
            )

            document = _json.loads(raw.decode("utf-8"))
            parsed = tuple(
                CandidateBinding(
                    str(item["candidate_id"]),
                    str(item["candidate_sha256"]),
                )
                for item in document["candidate_set"]
            )
            observed = _candidate_set_digest(parsed)
            declared = _candidate_set_digest(candidates)
        elif ref == request.execution_spec_ref:
            observed = hashlib.sha256(raw).hexdigest()
            declared = canonical_sha256(
                execution_spec.model_dump(mode="json")
            )
        elif ref == request.roster_ref:
            observed = hashlib.sha256(raw).hexdigest()
            declared = roster.manifest_sha256
        else:
            observed = hashlib.sha256(raw).hexdigest()
            declared = observed
        assert observed == declared, f"material {ref} hash drift"


def _request_for_scenario(
    holdout_id: str,
    holdout_sha256: str,
    **overrides,
) -> FinalEvalRequestV2:
    from research_automation.control_plane.contracts import canonical_sha256

    execution_spec = _execution_spec()
    roster = _roster()
    payload = dict(
        schema_version=FINAL_EVAL_REQUEST_V2,
        research_plan_sha256="a" * 64,
        campaign_id="campaign-staging-1",
        campaign_sha256="b" * 64,
        holdout_id=holdout_id,
        holdout_sha256=holdout_sha256,
        nonce_fingerprint=_nonce_fingerprint(ROOT_SECRET, NONCE),
        candidate_freeze_ref=(
            "research_state/control_plane/p8/attempts/p8-attempt-003/"
            "freeze.json"
        ),
        candidate_freeze_sha256=_candidate_set_digest(
            (_candidate("candidate-a"), _candidate("candidate-b"))
        ),
        code_ref="research_automation/control_plane/final_evaluator.py",
        code_sha256=_sha("code"),
        execution_spec_ref="research_state/control_plane/p8/spec.json",
        execution_spec_sha256=canonical_sha256(
            execution_spec.model_dump(mode="json")
        ),
        features_ref="research_state/control_plane/p8/features.json",
        features_sha256=_sha("features"),
        model="model-staging-1",
        model_sha256=_sha("model"),
        threshold="0.5",
        threshold_ref="research_state/control_plane/p8/threshold.json",
        threshold_sha256=_sha("threshold"),
        roster_ref="research_state/control_plane/p8/roster.json",
        roster_sha256=roster.manifest_sha256,
        generation="generation-staging-1",
        generation_sha256=_sha("generation"),
        actor_id="operator-1",
        actor_type="human",
        invocation_id="final-eval-op-cr009",
        authority_plan_hash=P8_IDENTITY["plan_hash"],
        identity_scope_hash=P8_IDENTITY["scope_hash"],
        identity_instruction_policy_hash=P8_IDENTITY[
            "instruction_policy_hash"
        ],
        attempt_id=ATTEMPT,
    )
    payload.update(overrides)
    return FinalEvalRequestV2(**payload)


def _serialize_grant(grant: object) -> dict[str, object]:
    return {
        "grant_id": grant.grant_id,
        "authorization_ref": grant.authorization_ref,
        "phase": grant.phase.value,
        "attempt_id": grant.attempt_id,
        "actor": {
            "actor_id": grant.actor.actor_id,
            "actor_type": grant.actor.actor_type,
            "invocation_id": grant.actor.invocation_id,
        },
        "identity": {
            "plan_hash": grant.identity.plan_hash,
            "scope_hash": grant.identity.scope_hash,
            "instruction_policy_hash": grant.identity.instruction_policy_hash,
        },
        "allowed_side_effects": [
            effect.value for effect in grant.allowed_side_effects
        ],
        "bearer_secret": grant._bearer_secret._reveal_for_authority_check(),
    }


def _rebuild_grant(payload: dict[str, object]) -> stores_module.AuthorityGrant:
    return stores_module.AuthorityGrant(
        grant_id=str(payload["grant_id"]),
        authorization_ref=str(payload["authorization_ref"]),
        phase=stores_module.Phase(str(payload["phase"])),
        attempt_id=str(payload["attempt_id"]),
        actor=stores_module.Actor(
            str(payload["actor"]["actor_id"]),
            str(payload["actor"]["actor_type"]),
            str(payload["actor"]["invocation_id"]),
        ),
        identity=stores_module.AuthorityIdentity(
            str(payload["identity"]["plan_hash"]),
            str(payload["identity"]["scope_hash"]),
            str(payload["identity"]["instruction_policy_hash"]),
        ),
        allowed_side_effects=tuple(
            stores_module.SideEffect(name)
            for name in payload["allowed_side_effects"]
        ),
        _bearer_secret=stores_module._BearerSecret(
            str(payload["bearer_secret"])
        ),
    )


def _context_payload(scenario: dict[str, object]) -> dict[str, object]:
    return {
        "root": scenario["root"],
        "grant": scenario["grant"],
        "request": scenario["request"],
        "holdout_id": scenario["holdout_id"],
        "holdout_sha256": scenario["holdout_sha256"],
        "idempotency_key": scenario["idempotency_key"],
    }


def _material_bundle(scenario: dict[str, object]) -> FinalEvalMaterialBundle:
    request = FinalEvalRequestV2(**scenario["request"])
    execution_spec = _execution_spec()
    roster = _roster()
    return FinalEvalMaterialBundle(
        campaign_id=request.campaign_id,
        campaign_sha256=request.campaign_sha256,
        holdout_id=request.holdout_id,
        holdout_sha256=request.holdout_sha256,
        authorization_nonce=NONCE,
        candidate_freeze_ref=request.candidate_freeze_ref,
        candidate_set=(
            _candidate("candidate-a"),
            _candidate("candidate-b"),
        ),
        code_ref=request.code_ref,
        code_sha256=request.code_sha256,
        execution_spec=execution_spec,
        execution_spec_ref=request.execution_spec_ref,
        execution_spec_sha256=request.execution_spec_sha256,
        features_ref=request.features_ref,
        features_sha256=request.features_sha256,
        model_id=request.model,
        model_sha256=request.model_sha256,
        threshold_ref=request.threshold_ref,
        threshold_sha256=request.threshold_sha256,
        roster=roster,
        roster_ref=request.roster_ref,
        roster_sha256=request.roster_sha256,
        generation_id=request.generation,
        generation_sha256=request.generation_sha256,
        actor=Actor(
            request.actor_id,
            request.actor_type,
            request.invocation_id,
        ),
        identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
        attempt_id=request.attempt_id,
    )


def _staging_sink(root: Path):
    """Create-only evidence sink bound DYNAMICALLY to the real ticket id
    (the orchestrator hands the sink the binding document)."""
    from research_automation.control_plane.final_eval_evidence import (
        FinalEvalResultPublisher,
    )
    from tests.test_control_plane_final_eval_orchestrator import (
        TEST_EVIDENCE_VOLUME,
        _ensure_git,
    )

    _ensure_git(root)
    publisher = FinalEvalResultPublisher(
        repository_root=root,
        evidence_volume=TEST_EVIDENCE_VOLUME,
    )

    def sink(document):
        binding_id = str(document.get("binding_id", ""))
        outcome = str(document.get("outcome", "SUCCEEDED"))
        refs = publisher.publish(
            binding_id,
            binding_id,
            dict(document),
            outcome=outcome,
        )
        return refs.to_payload()

    return sink


def _staging_context(
    scenario: dict[str, object],
) -> AuthorizedFinalEvalContext:
    root = Path(scenario["root"])
    request = FinalEvalRequestV2(**scenario["request"])
    grant = _rebuild_grant(scenario["grant"])

    def launcher() -> int:
        _record_worker(root)
        return 0

    return AuthorizedFinalEvalContext(
        request=request,
        grant=grant,
        nonce=NONCE,
        actor=Actor(
            request.actor_id,
            request.actor_type,
            request.invocation_id,
        ),
        identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
        idempotency_key=str(scenario["idempotency_key"]),
        task_spec_ref="manifest.json",
        task_spec_sha256="1" * 64,
        authority_capability=ROOT_SECRET,
        repository_root=str(root),
        data_root=seal_trusted_data_root(root, (SYNTH_HOLDOUT_REF,)),
        worker_launcher=launcher,
        evidence_sink=_staging_sink(root),
        attempt_id=ATTEMPT,
        material_resolver=_sealed_staging_resolver(root, request),
    )


def _sealed_staging_resolver(
    root: Path,
    request: FinalEvalRequestV2,
) -> SealedMaterialResolver:
    """The manifest-backed sealed resolver for the disposable staging
    root (CR-010 A1): no callable, committed-blob verified materials."""
    from research_automation.control_plane.final_eval_composition import (
        build_sealed_material_resolver,
    )

    return build_sealed_material_resolver(
        request=request,
        bundle=_material_bundle(
            {"root": str(root), "request": request.to_payload()}
        ),
        repository_root=root,
        root_secret=ROOT_SECRET,
    )


@contextlib.contextmanager
def _stores(root: Path):
    """Parent-side store override for the disposable authority pair."""
    with stores_module.store_path_override(
        authority=root / "authority.sqlite3",
        operational=root / "operational.sqlite3",
    ):
        stores_module._expected_schema_sha256.cache_clear()
        try:
            yield
        finally:
            stores_module._expected_schema_sha256.cache_clear()


def _staging_entry_run(scenario: dict[str, object]) -> dict[str, object]:
    with _stores(Path(scenario["root"])):
        return entry_run(_staging_context(scenario))


def _worker_count(root: Path) -> int:
    path = root / WORKER_COUNTER_REF
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


def _record_worker(root: Path) -> None:
    path = root / WORKER_COUNTER_REF
    with path.open("a", encoding="utf-8") as stream:
        stream.write("run\n")
        stream.flush()
        os.fsync(stream.fileno())


def _maintenance_lease(
    authority: stores_module._AuthorityStore,
    identity,
    idempotency_key: str,
):
    import secrets as _secrets
    from datetime import datetime, timezone

    unique = _secrets.token_hex(8)
    maintenance_actor = Actor(
        "final-eval-staging-maintenance",
        "automation",
        f"final-eval-staging-maint-{unique}",
    )
    attempt_id = f"final-eval-staging-maint-{unique}"
    envelope = authority._provision_authorization(
        phase=stores_module.Phase.P0,
        attempt_id=attempt_id,
        actor=maintenance_actor,
        identity=identity,
        expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
        allowed_side_effects=(stores_module.SideEffect.READ,),
    )
    maintenance_grant = authority.claim_authorization(
        envelope,
        expected_phase=stores_module.Phase.P0,
        expected_attempt_id=attempt_id,
        actor=maintenance_actor,
        identity=identity,
    )
    ticket = authority._issue_task_ticket(
        maintenance_grant,
        {
            "task_id": "P8-STAGING-RECONCILER-MAINT",
            "objective": "bounded staging reconciler maintenance",
            "dependencies": [],
            "idempotency_key": idempotency_key + "-maint-" + unique,
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
        allowed_side_effects=(stores_module.SideEffect.READ,),
    )
    return authority._begin_task(ticket)


def _child_first_process(
    scenario: dict[str, object],
    crash_after: str,
) -> int:
    """Child process #1: drive the REAL saga until ``crash_after`` and
    hard-exit AT the real durable boundary (never after the full operation
    has returned).  Runs under the disposable store override."""
    root = Path(scenario["root"])
    with stores_module.store_path_override(
        authority=root / "authority.sqlite3",
        operational=root / "operational.sqlite3",
    ):
        stores_module._expected_schema_sha256.cache_clear()
        grant = _rebuild_grant(scenario["grant"])
        request = FinalEvalRequestV2(**scenario["request"])
        identity = stores_module.AuthorityIdentity(**P8_IDENTITY)
        authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
        from research_automation.control_plane.final_eval_authority import (
            AuthorityFinalEvalBroker,
        )

        broker = AuthorityFinalEvalBroker(
            authority=authority,
            grant=grant,
            attempt_id=ATTEMPT,
            identity=identity,
        )
        binding = broker.bind(
            request=request,
            nonce=NONCE,
            actor=Actor(
                request.actor_id,
                request.actor_type,
                request.invocation_id,
            ),
            idempotency_key=str(scenario["idempotency_key"]),
            task_spec_ref="manifest.json",
            task_spec_sha256="1" * 64,
        )
        if crash_after == "consume":
            os._exit(9)
        consumption = SqliteHoldoutStore(authority=authority).read_consumption(
            binding.ticket_id
        )
        projection = adapt_evaluator_request_v1_test_only(
            _v1_request_aligned(request),
            request,
            root_secret=ROOT_SECRET,
            attempt_id=ATTEMPT,
            identity=identity,
        )

        def launcher() -> int:
            _record_worker(root)
            if crash_after == "open":
                # the artifact handle is ALREADY open; the worker crashes
                os._exit(9)
            return 0

        evaluator = TrustedEvaluator(
            broker=AuthorityBroker(store=compose_holdout_store(_staging_context(scenario))),
            adapter=TrustedEvaluatorAdapter(
                backend=_counting_staging_backend(root)
            ),
        )
        evaluated = evaluator.evaluate_v2(
            projection,
            data_root=seal_trusted_data_root(root, (SYNTH_HOLDOUT_REF,)),
            worker_launcher=launcher,
            consumption=consumption,
            durable_ticket_id=binding.ticket_id,
            durable_request_sha256=binding.request_sha256,
            durable_nonce_fingerprint=binding.nonce_fingerprint,
        )
        if crash_after in ("worker", "publication"):
            os._exit(9)
        from research_automation.control_plane.final_eval_orchestrator import (
            OrchestrationInputs,
            orchestrate,
        )

        def orchestration_crash_hook(boundary: str) -> None:
            if crash_after == "claim" and boundary == "CRASH_AFTER.CLAIM_WRITTEN":
                os._exit(9)
            if (
                crash_after == "result_staged"
                and boundary == "CRASH_AFTER.RESULT_STAGED"
            ):
                os._exit(9)

        staged = orchestrate(
            OrchestrationInputs(
                authority=authority,
                binding_id=binding.ticket_id,
                expected_version=binding.saga_version,
                worker_launcher=lambda: 0,
                evidence_sink=_real_publisher_sink(
                    root, binding.ticket_id
                ),
                repository_root=root,
                crash_hook=orchestration_crash_hook,
            )
        )
        if crash_after in ("publication",):
            os._exit(9)
        from research_automation.control_plane.final_eval_reconciler import (
            reconcile,
        )

        maintenance_lease = _maintenance_lease(
            authority, identity, str(scenario["idempotency_key"])
        )

        def crash_hook(boundary: str) -> None:
            if crash_after == "closed" and boundary == "CRASH_AFTER.CLOSED":
                os._exit(9)
            if (
                crash_after == "terminal"
                and boundary == "CRASH_AFTER.AUTHORITY_TERMINAL"
            ):
                os._exit(9)

        reconcile(
            authority,
            maintenance_lease,
            evidence_ref_for={
                binding.ticket_id: staged.result_claim_ref or ""
            },
            repository_root=root,
            crash_hook=crash_hook,
        )
        return 0


class _CountingStagingBackend(HoldoutDataBackend):
    """Synthetic staging backend that records every artifact open to a
    durable file (the crash harness asserts the open count)."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._counter = root / OPEN_COUNTER_REF

    def read_holdout_summary(
        self,
        *,
        path,
        holdout_id: str,
        holdout_sha256: str,
    ) -> dict[str, object]:
        with self._counter.open("a", encoding="utf-8") as stream:
            stream.write("open\n")
            stream.flush()
            os.fsync(stream.fileno())
        return compose_staging_backend().read_holdout_summary(
            path=path,
            holdout_id=holdout_id,
            holdout_sha256=holdout_sha256,
        )


def _counting_staging_backend(root: Path) -> _CountingStagingBackend:
    return _CountingStagingBackend(root)


def _v1_request_aligned(request: FinalEvalRequestV2):
    from research_automation.control_plane.final_evaluator import (
        CampaignBinding,
        CodeBinding,
        ExecutionSpecBinding,
        FeatureBinding,
        FinalEvalRequest,
        GenerationBinding,
        HoldoutBinding,
        IdentityBinding,
        ModelBinding,
        RosterBinding,
        ThresholdBinding,
    )

    candidates = (_candidate("candidate-a"), _candidate("candidate-b"))
    execution_spec = _execution_spec()
    roster = _roster()
    return FinalEvalRequest(
        campaign=CampaignBinding(
            campaign_id=request.campaign_id,
            campaign_sha256=request.campaign_sha256,
        ),
        candidate_set=candidates,
        candidate_set_sha256=_candidate_set_digest(candidates),
        code=CodeBinding(code_sha256=request.code_sha256),
        execution_spec=ExecutionSpecBinding(
            execution_spec=execution_spec,
            execution_spec_sha256=request.execution_spec_sha256,
        ),
        features=FeatureBinding(features_sha256=request.features_sha256),
        model=ModelBinding(
            model_id=request.model,
            model_sha256=request.model_sha256,
        ),
        threshold=ThresholdBinding(
            threshold_sha256=request.threshold_sha256
        ),
        roster=RosterBinding(
            roster=roster,
            roster_sha256=request.roster_sha256,
        ),
        generation=GenerationBinding(
            generation_id=request.generation,
            generation_sha256=request.generation_sha256,
        ),
        holdout=HoldoutBinding(
            holdout_id=request.holdout_id,
            holdout_sha256=request.holdout_sha256,
            authorization_nonce=NONCE,
        ),
        actor=Actor(
            request.actor_id,
            request.actor_type,
            request.invocation_id,
        ),
        identity_binding=IdentityBinding(
            plan_hash=request.authority_plan_hash,
            scope_hash=request.identity_scope_hash,
            policy_hash=request.identity_instruction_policy_hash,
        ),
    )


def _child_recovery_process(scenario: dict[str, object]) -> dict[str, object]:
    """Child process #2: recover through the PRODUCTION entry using ONLY
    durable state -- never reopen, never recompute, never reissue.

    Reports: the child's own PID/start-time identity, the NEW maintenance
    lease id (a fresh maintenance authorization per recovery), the SQLite
    binding state, the consumption count, the artifact open count, the
    worker count and the terminal claim."""
    import psutil as _psutil

    root = Path(scenario["root"])
    with stores_module.store_path_override(
        authority=root / "authority.sqlite3",
        operational=root / "operational.sqlite3",
    ):
        stores_module._expected_schema_sha256.cache_clear()
        identity = stores_module.AuthorityIdentity(**P8_IDENTITY)
        authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
        durable = SqliteHoldoutStore(authority=authority)
        bindings = authority._scan_final_eval_bindings()
        assert len(bindings) == 1
        binding = bindings[0]
        workers_before = _worker_count(root)
        opens_before = _open_count(root)
        maintenance_ids_before = set(_maintenance_ticket_ids(root))
        started_at_ns = int(_psutil.Process().create_time() * 1_000_000_000)
        result = entry_run(_staging_context(scenario))
        maintenance_ids_after = set(_maintenance_ticket_ids(root))
        new_maintenance = sorted(maintenance_ids_after - maintenance_ids_before)
        final = authority._scan_final_eval_bindings()[0]
        stores_module._expected_schema_sha256.cache_clear()
        return {
            "recovery": {
                "saga_state": final.saga_state,
                "terminal_binding": final.terminal_binding,
                "result_claim_ref": final.result_claim_ref,
                "entry_outcome": result["outcome"],
                "entry_saga_state": result["saga_state"],
            },
            "pid": os.getpid(),
            "started_at_ns": started_at_ns,
            "binding_id": binding.ticket_id,
            "binding_request_sha256": binding.request_sha256,
            "consumption_count": durable.consumption_count(
                binding.request_sha256
            ),
            "workers_before": workers_before,
            "workers_after": _worker_count(root),
            "opens_before": opens_before,
            "opens_after": _open_count(root),
            "maintenance_lease_ids": new_maintenance,
            "maintenance_ticket_states": {
                str(ticket_id): str(
                    stores_module.AuthorityReader().task_ticket_state(
                        str(ticket_id)
                    )
                )
                for ticket_id in new_maintenance
            },
        }


def _open_count(root: Path) -> int:
    path = root / OPEN_COUNTER_REF
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


def _maintenance_ticket_ids(root: Path) -> tuple[str, ...]:
    """The durable maintenance ticket ids (RECONCILER-MAINT and the typed
    FINAL-EVAL-FAILURE-MAINT failure lease) already committed in the
    Authority DB."""
    import sqlite3 as _sqlite3

    connection = _sqlite3.connect(str(root / "authority.sqlite3"))
    try:
        rows = connection.execute(
            "SELECT ticket_id FROM task_tickets_v2 "
            "WHERE task_id LIKE '%RECONCILER-MAINT' "
            "OR task_id = 'P8-FINAL-EVAL-FAILURE-MAINT' "
            "ORDER BY created_at"
        ).fetchall()
    finally:
        connection.close()
    return tuple(str(row[0]) for row in rows)


_CHILD_SCRIPT = """
import json, os, sys
sys.path.insert(0, os.environ["DSH_STAGING_CWD"])
from tests.test_control_plane_final_eval_staging import (
    _child_first_process,
    _child_recovery_process,
)
with open(sys.argv[2], encoding="utf-8") as handle:
    scenario = json.load(handle)
if sys.argv[1] == "first":
    _child_first_process(scenario, sys.argv[3])
    print("CHILD_FIRST_DONE")
else:
    print(json.dumps(_child_recovery_process(scenario), sort_keys=True))
"""


def _run_child(role: str, scenario: dict[str, object], crash_after: str = ""):
    """Spawn one child via Popen and observe its (pid, started_at_ns)
    identity from the PARENT immediately after spawn (CR-010 B-05)."""
    import psutil as _psutil

    root = Path(scenario["root"])
    script_ref = root / ".staging-child-script.py"
    script_ref.write_text(_CHILD_SCRIPT, encoding="utf-8")
    argv = [sys.executable, str(script_ref), role, str(root / CONTEXT_JSON_REF)]
    if role == "first":
        argv.append(crash_after)
    environment = dict(os.environ)
    environment["DSH_STAGING_CWD"] = str(Path(__file__).resolve().parents[1])
    child = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        env=environment,
        cwd=Path(__file__).resolve().parents[1],
    )
    observed = (
        child.pid,
        int(_psutil.Process(child.pid).create_time() * 1_000_000_000),
    )
    stdout_text, stderr_text = child.communicate(timeout=300)
    return child.returncode, stdout_text, stderr_text, observed


class FinalEvalStagingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        subprocess.run(
            ["git", "init", "--quiet"], cwd=self.root, check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Staging"],
            cwd=self.root, check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "staging@test"],
            cwd=self.root, check=True, capture_output=True,
        )
        self.scenario = _build_scenario(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_authorized_composition_root_success(self) -> None:
        """The PRODUCTION composition root runs the full synthetic staging
        chain: exactly one consume/open, one worker execution, one fixed
        claim, AUTHORITY_TERMINAL + CLOSED, bounded/redacted result."""
        root = self.root
        result = _staging_entry_run(self.scenario)
        self.assertEqual(result["saga_state"], "AUTHORITY_TERMINAL")
        self.assertEqual(result["outcome"], "SUCCEEDED")
        self.assertTrue(result["evidence_ref"])
        with _stores(root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            binding = authority._scan_final_eval_bindings()[0]
            self.assertEqual(binding.saga_state, "AUTHORITY_TERMINAL")
            durable = SqliteHoldoutStore(authority=authority)
            self.assertEqual(
                durable.consumption_count(binding.request_sha256), 1
            )
        # exactly one worker execution
        self.assertEqual(_worker_count(root), 1)
        # exactly one fixed claim committed
        claim_path = root / result["evidence_ref"]
        self.assertTrue(claim_path.exists())
        claim = json.loads(claim_path.read_text(encoding="utf-8"))
        self.assertEqual(claim["ticket_id"], binding.ticket_id)
        self.assertEqual(claim["outcome"], "SUCCEEDED")
        # the result is bounded/redacted (no raw nonce, no paths)
        self.assertNotIn(NONCE, json.dumps(result))
        self.assertNotIn(ROOT_SECRET, json.dumps(result))

    def test_fresh_process_crash_matrix(self) -> None:
        """Hard-exit the first process AT every real durable boundary
        (consume/open/worker/claim-write/result-stage/closed/terminal);
        the second process recovers through the PRODUCTION entry with a
        NEW maintenance lease using ONLY durable state.  consume/open/
        worker-count stays one, opens do not increase during recovery,
        no new nonce, no reopen; intermediate crashes end in a DURABLE
        AUTHORITY_TERMINAL/FAILED tombstone (never a reusable
        CONSUMED/EVALUATING binding)."""
        for crash_after in (
            "consume",
            "open",
            "worker",
            "publication",
            "claim",
            "result_staged",
            "closed",
            "terminal",
        ):
            with self.subTest(crash_after=crash_after):
                root = Path(self._tmp.name) / f"crash-{crash_after}"
                root.mkdir()
                scenario = _build_scenario(
                    root,
                    campaign_id="campaign-staging-crash",
                    holdout_id=f"holdout-staging-{crash_after}",
                    idempotency_key=f"p8-staging-{crash_after}",
                )
                first_rc, first_out, first_err, first_observed = _run_child(
                    "first", scenario, crash_after
                )
                self.assertEqual(first_rc, 9, first_err)
                self.assertGreater(first_observed[0], 0)
                self.assertGreater(first_observed[1], 0)
                # durable state at crash time
                with _stores(root):
                    authority = stores_module._AuthorityStore(
                        root_secret=ROOT_SECRET
                    )
                    bindings = authority._scan_final_eval_bindings()
                    self.assertEqual(len(bindings), 1)
                    durable = SqliteHoldoutStore(authority=authority)
                workers_before = _worker_count(root)
                opens_before = _open_count(root)
                second_rc, second_out, second_err, second_observed = (
                    _run_child("recovery", scenario)
                )
                self.assertEqual(second_rc, 0, second_err)
                recovery = json.loads(second_out)
                # the recovery child IS the parent-observed process
                self.assertEqual(
                    (int(recovery["pid"]), int(recovery["started_at_ns"])),
                    second_observed,
                )
                self.assertNotEqual(
                    (int(recovery["pid"]), int(recovery["started_at_ns"])),
                    first_observed,
                )
                self.assertEqual(recovery["consumption_count"], 1)
                self.assertEqual(
                    recovery["workers_after"], recovery["workers_before"]
                )
                # recovery NEVER reopened the artifact and never re-ran
                # the worker
                self.assertEqual(recovery["opens_after"], recovery["opens_before"])
                self.assertEqual(_worker_count(root), workers_before)
                self.assertEqual(_open_count(root), opens_before)
                with _stores(root):
                    authority = stores_module._AuthorityStore(
                        root_secret=ROOT_SECRET
                    )
                    final = authority._scan_final_eval_bindings()[0]
                    if crash_after in (
                        "consume",
                        "open",
                        "worker",
                        "publication",
                        "claim",
                    ):
                        # intermediate crash (consume/open/worker/pre-claim
                        # OR the claim blob written but the binding never
                        # staged) -> DURABLE FAILED tombstone
                        self.assertEqual(
                            recovery["recovery"]["saga_state"],
                            "AUTHORITY_TERMINAL",
                        )
                        self.assertEqual(
                            recovery["recovery"]["terminal_binding"],
                            "FAILED",
                        )
                        self.assertEqual(
                            recovery["recovery"]["entry_outcome"], "FAILED"
                        )
                        self.assertEqual(
                            final.saga_state, "AUTHORITY_TERMINAL"
                        )
                        self.assertEqual(final.terminal_binding, "FAILED")
                        # a FRESH maintenance lease was issued per
                        # recovery and finished FAILED (never IN_PROGRESS)
                        self.assertEqual(
                            len(recovery["maintenance_lease_ids"]), 1
                        )
                        # the maintenance ticket is FAILED, never
                        # IN_PROGRESS (no IN_PROGRESS task may remain)
                        for ticket_id in recovery[
                            "maintenance_lease_ids"
                        ]:
                            self.assertEqual(
                                recovery["maintenance_ticket_states"][
                                    ticket_id
                                ],
                                "FAILED",
                            )
                    elif crash_after == "terminal":
                        # terminal replay: the binding was already
                        # AUTHORITY_TERMINAL; entry_run replays it WITHOUT
                        # any new maintenance lease
                        self.assertEqual(
                            recovery["recovery"]["saga_state"],
                            "AUTHORITY_TERMINAL",
                        )
                        self.assertEqual(
                            recovery["recovery"]["entry_outcome"],
                            "SUCCEEDED",
                        )
                        self.assertEqual(
                            final.saga_state, "AUTHORITY_TERMINAL"
                        )
                        self.assertEqual(
                            len(recovery["maintenance_lease_ids"]), 0
                        )
                    else:
                        # result_staged/closed: reconciler close
                        self.assertEqual(
                            recovery["recovery"]["saga_state"],
                            "AUTHORITY_TERMINAL",
                        )
                        self.assertEqual(
                            final.saga_state, "AUTHORITY_TERMINAL"
                        )
                        # the staged result was CLOSED, never recomputed
                        self.assertIsNotNone(final.result_claim_ref)
                    # no new nonce: the receipt fingerprint is unchanged
                    # (the Authority stores the durable-domain fingerprint)
                    receipt = durable.read_consumption(final.ticket_id)
                self.assertEqual(
                    receipt.nonce_fingerprint,
                    stores_module._final_eval_nonce_fingerprint(
                        ROOT_SECRET, NONCE
                    ),
                )

    def test_concurrency_identity_and_replay_negatives(self) -> None:
        """Every unsafe case fails closed without a partial durable row or
        side effect; a same-identity replay reads the terminal result
        without calling consume/open again."""
        root = self.root
        _staging_entry_run(self.scenario)
        with _stores(root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            binding = authority._scan_final_eval_bindings()[0]
            durable = SqliteHoldoutStore(authority=authority)
            self.assertEqual(
                durable.consumption_count(binding.request_sha256), 1
            )
        workers_after_success = _worker_count(root)
        # same-identity replay: reads the terminal result, no new consume,
        # no worker, no new nonce
        replayed = _staging_entry_run(self.scenario)
        self.assertEqual(replayed["saga_state"], "AUTHORITY_TERMINAL")
        with _stores(root):
            self.assertEqual(
                durable.consumption_count(binding.request_sha256), 1
            )
        self.assertEqual(_worker_count(root), workers_after_success)
        # a DIFFERENT holdout under the same ticket identity context
        # cannot consume (composition rejects the material drift before
        # any open or bind)
        from research_automation.control_plane.final_eval_composition import (
            FinalEvalCompositionRejected,
        )

        other = _request_for_scenario(
            "holdout-other", "1" * 64, campaign_id="campaign-other"
        )
        other_context = _staging_context(self.scenario)
        other_context = AuthorizedFinalEvalContext(
            **{
                **{
                    field: getattr(other_context, field)
                    for field in (
                        "request",
                        "grant",
                        "nonce",
                        "actor",
                        "identity",
                        "idempotency_key",
                        "task_spec_ref",
                        "task_spec_sha256",
                        "authority_capability",
                        "repository_root",
                        "data_root",
                        "worker_launcher",
                        "evidence_sink",
                        "attempt_id",
                        "material_resolver",
                    )
                },
                "request": other,
            }
        )
        with self.assertRaises(FinalEvalCompositionRejected):
            with _stores(root):
                entry_run(other_context)
        # wrong identity context
        forged = AuthorizedFinalEvalContext(
            **{
                field: getattr(_staging_context(self.scenario), field)
                for field in (
                    "request",
                    "grant",
                    "nonce",
                    "actor",
                    "idempotency_key",
                    "task_spec_ref",
                    "task_spec_sha256",
                    "authority_capability",
                    "repository_root",
                    "data_root",
                    "worker_launcher",
                    "evidence_sink",
                    "attempt_id",
                    "material_resolver",
                )
            },
            identity=stores_module.AuthorityIdentity(
                plan_hash="0" * 64,
                scope_hash="0" * 64,
                instruction_policy_hash="0" * 64,
            ),
        )
        with self.assertRaises(FinalEvalCompositionRejected):
            with _stores(root):
                entry_run(forged)
        # missing composition context
        with self.assertRaises(FinalEvalCompositionRejected):
            entry_run(None)  # type: ignore[arg-type]
        # malformed exit code fails closed without a new consume/claim
        malformed = AuthorizedFinalEvalContext(
            **{
                field: getattr(_staging_context(self.scenario), field)
                for field in (
                    "request",
                    "grant",
                    "nonce",
                    "actor",
                    "identity",
                    "idempotency_key",
                    "task_spec_ref",
                    "task_spec_sha256",
                    "authority_capability",
                    "repository_root",
                    "data_root",
                    "evidence_sink",
                    "attempt_id",
                    "material_resolver",
                )
            },
            worker_launcher=lambda: -1,
        )
        # same request -> replay path returns the terminal result even with
        # the malformed launcher (never re-consumes, never re-runs)
        with _stores(root):
            replayed = entry_run(malformed)
        self.assertEqual(replayed["saga_state"], "AUTHORITY_TERMINAL")
        with _stores(root):
            self.assertEqual(
                durable.consumption_count(binding.request_sha256), 1
            )
        self.assertEqual(_worker_count(root), workers_after_success)
        # no partial rows for the rejected paths
        with _stores(root):
            bindings = authority._scan_final_eval_bindings()
        self.assertEqual(len(bindings), 1)

    def test_deleted_synthetic_holdout_fails_before_terminal_success(
        self,
    ) -> None:
        """CR-010 F-01/F-04: deleting the synthetic Holdout artifact makes
        the production entry fail closed BEFORE terminal SUCCESS -- the
        entry returns an explicit DURABLE FAILED terminal (never
        SUCCEEDED, never a claim, never a reusable binding); a
        caller-selected evaluator can never paper over the deletion
        because the entry composes the REAL evaluator from the staged
        backend (which verifies the sealed holdout content)."""
        root = self.root
        (root / SYNTH_HOLDOUT_REF).unlink()
        result = _staging_entry_run(self.scenario)
        self.assertEqual(result["outcome"], "FAILED")
        self.assertEqual(result["saga_state"], "AUTHORITY_TERMINAL")
        self.assertEqual(result["evidence_ref"], "")
        with _stores(root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            bindings = authority._scan_final_eval_bindings()
            self.assertEqual(len(bindings), 1)
            self.assertEqual(bindings[0].saga_state, "AUTHORITY_TERMINAL")
            self.assertEqual(bindings[0].terminal_binding, "FAILED")
            self.assertIsNone(bindings[0].result_claim_ref)
            self.assertEqual(
                stores_module.AuthorityReader().task_ticket_state(
                    bindings[0].ticket_id
                ),
                "FAILED",
            )

    def test_sealed_material_content_rejects_joint_artifact_forgery(
        self,
    ) -> None:
        """CR-010 F-01/F-02: the production resolver verifies sealed
        material CONTENT.  A caller that changes BOTH a request hash AND
        the material bundle hash in the same direction (and even the
        declared strings agree) is still rejected by the composition root
        when the sealed repository artifact file disagrees with the
        declared digest -- and NO durable binding is created."""
        from research_automation.control_plane.final_eval_composition import (
            FinalEvalCompositionRejected,
            SealedMaterialResolver,
        )

        root = self.root
        scenario = self.scenario
        request = FinalEvalRequestV2(**scenario["request"])
        # write the SEALED code artifact the resolver must verify: its
        # content bytes hash to the ORIGINAL declared code_sha256
        code_ref = request.code_ref
        code_path = root / code_ref
        code_path.parent.mkdir(parents=True, exist_ok=True)
        code_path.write_bytes(b"code")  # sha256("code") == _sha("code")
        self.assertEqual(hashlib.sha256(code_path.read_bytes()).hexdigest(),
                         _sha("code"))
        # joint forgery: request AND bundle both declare a tampered digest
        forged_code_sha256 = "9" * 64
        forged_request = _request_for_scenario(
            scenario["holdout_id"],
            str(scenario["holdout_sha256"]),
            code_sha256=forged_code_sha256,
        )
        forged_materials = _material_bundle(
            {
                **scenario,
                "request": forged_request.to_payload(),
            }
        )
        forged_materials = FinalEvalMaterialBundle(
            campaign_id=forged_materials.campaign_id,
            campaign_sha256=forged_materials.campaign_sha256,
            holdout_id=forged_materials.holdout_id,
            holdout_sha256=forged_materials.holdout_sha256,
            authorization_nonce=NONCE,
            candidate_freeze_ref=forged_materials.candidate_freeze_ref,
            candidate_set=forged_materials.candidate_set,
            code_ref=forged_materials.code_ref,
            code_sha256=forged_code_sha256,
            execution_spec=forged_materials.execution_spec,
            execution_spec_ref=forged_materials.execution_spec_ref,
            execution_spec_sha256=forged_materials.execution_spec_sha256,
            features_ref=forged_materials.features_ref,
            features_sha256=forged_materials.features_sha256,
            model_id=forged_materials.model_id,
            model_sha256=forged_materials.model_sha256,
            threshold_ref=forged_materials.threshold_ref,
            threshold_sha256=forged_materials.threshold_sha256,
            roster=forged_materials.roster,
            roster_ref=forged_materials.roster_ref,
            roster_sha256=forged_materials.roster_sha256,
            generation_id=forged_materials.generation_id,
            generation_sha256=forged_materials.generation_sha256,
            actor=forged_materials.actor,
            identity=forged_materials.identity,
            attempt_id=forged_materials.attempt_id,
        )
        from research_automation.control_plane.final_eval_composition import (
            FinalEvalCompositionRejected,
            build_sealed_material_resolver,
        )

        # the manifest factory rejects the joint forgery BEFORE any store/
        # evaluator construction: the sealed committed blob bytes disagree
        # with the tampered request+bundle digest
        with self.assertRaises(FinalEvalCompositionRejected):
            build_sealed_material_resolver(
                request=forged_request,
                bundle=forged_materials,
                repository_root=root,
                root_secret=ROOT_SECRET,
            )
        # and NO durable binding was created
        with _stores(root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            self.assertEqual(len(authority._scan_final_eval_bindings()), 0)

    def test_full_side_effect_surface(self) -> None:
        """Snapshot the full side-effect surface before/after: only the
        named staging evidence delta may differ."""
        root = self.root

        def surface() -> dict[str, object]:
            entries: dict[str, object] = {}
            for path in sorted(root.rglob("*")):
                if path.is_file() and ".git" not in path.parts:
                    entries[str(path.relative_to(root)).replace("\\", "/")] = (
                        hashlib.sha256(path.read_bytes()).hexdigest()
                    )
            return entries

        before = surface()
        _staging_entry_run(self.scenario)
        after = surface()
        new_paths = sorted(set(after) - set(before))
        # only the staging evidence delta + scenario control files may
        # appear; everything else stays byte-identical
        allowed_new = {
            WORKER_COUNTER_REF,
            ".staging-grant.json",
            ".staging-request.json",
            ".staging-context.json",
        }
        evidence_volume_prefix = EVIDENCE_VOLUME + "/"
        for path in new_paths:
            if path in allowed_new:
                continue
            self.assertTrue(
                path.startswith(evidence_volume_prefix),
                f"unexpected new path: {path}",
            )
        for path in set(before) - set(after):
            self.fail(f"path disappeared: {path}")
        for path in sorted(set(before) & set(after)):
            if path.startswith(evidence_volume_prefix):
                continue
            # the Authority/Operational stores legitimately carry the
            # durable staging lineage (verified at row level elsewhere)
            if path in ("authority.sqlite3", "operational.sqlite3"):
                continue
            self.assertEqual(
                before[path],
                after[path],
                f"path mutated: {path}",
            )


if __name__ == "__main__":
    unittest.main()
