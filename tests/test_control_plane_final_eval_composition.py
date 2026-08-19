"""CR-010 C0 (Phase B): the authorized composition root tests.

Ordinary runner/AG2/prompt/memory code can never construct the
composition root: wrong phase/actor/scope/instruction policy/attempt/
candidate/code/execution/features/model/threshold/roster/generation/
holdout all reject BEFORE any store or evaluator is opened.  dry-run
performs zero writes; an attempt id alone -- even one that exists in the
database -- is never an authorization.
"""

from __future__ import annotations

import io
import unittest
from dataclasses import replace

from research_automation.control_plane import cli as cli_module
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
    FinalEvalCompositionRejected,
    SealedMaterialResolver,
    compose_final_eval_runtime,
)
from research_automation.control_plane.final_eval_entry import run as entry_run
from research_automation.control_plane.final_eval_request_projection import (
    FinalEvalMaterialBundle,
)
from research_automation.control_plane.final_evaluator import (
    seal_trusted_data_root,
)
from tests.test_control_plane_campaign_store import (
    _authorized_p8_campaign,
    ROOT_SECRET,
)
from tests.test_control_plane_final_eval_orchestrator import (
    P8_IDENTITY,
    _make_request,
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


def _material_bundle(**overrides) -> FinalEvalMaterialBundle:
    execution_spec = _execution_spec()
    roster = _roster()
    payload = dict(
        campaign_id="campaign-composition-1",
        campaign_sha256="b" * 64,
        holdout_id="holdout-composition-1",
        holdout_sha256="c" * 64,
        authorization_nonce=NONCE,
        candidate_freeze_ref=(
            "research_state/control_plane/p8/attempts/p8-attempt-003/"
            "freeze.json"
        ),
        candidate_set=(_candidate("candidate-a"), _candidate("candidate-b")),
        code_ref="research_automation/control_plane/final_evaluator.py",
        code_sha256=_sha("code"),
        execution_spec=execution_spec,
        execution_spec_ref="research_state/control_plane/p8/spec.json",
        execution_spec_sha256=canonical_sha256(
            execution_spec.model_dump(mode="json")
        ),
        features_ref="research_state/control_plane/p8/features.json",
        features_sha256=_sha("features"),
        model_id="model-composition-1",
        model_sha256=_sha("model"),
        threshold_ref="research_state/control_plane/p8/threshold.json",
        threshold_sha256=_sha("threshold"),
        roster=roster,
        roster_ref="research_state/control_plane/p8/roster.json",
        roster_sha256=roster.manifest_sha256,
        generation_id="generation-composition-1",
        generation_sha256=_sha("generation"),
        actor=Actor("operator-1", "human", "final-eval-op-cr009"),
        identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
        attempt_id=ATTEMPT,
    )
    payload.update(overrides)
    return FinalEvalMaterialBundle(**payload)


def _request(**overrides) -> FinalEvalRequestV2:
    execution_spec = _execution_spec()
    roster = _roster()
    payload = dict(
        schema_version=FINAL_EVAL_REQUEST_V2,
        research_plan_sha256="a" * 64,
        campaign_id="campaign-composition-1",
        campaign_sha256="b" * 64,
        holdout_id="holdout-composition-1",
        holdout_sha256="c" * 64,
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
        model="model-composition-1",
        model_sha256=_sha("model"),
        threshold="0.5",
        threshold_ref="research_state/control_plane/p8/threshold.json",
        threshold_sha256=_sha("threshold"),
        roster_ref="research_state/control_plane/p8/roster.json",
        roster_sha256=roster.manifest_sha256,
        generation="generation-composition-1",
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


def _ensure_committed_materials(root, request) -> None:
    """CR-010 A1: write + COMMIT the six ref-backed research materials in
    the disposable git repo (the synthetic Holdout is never added)."""
    import subprocess as _subprocess

    from research_automation.control_plane.contracts import canonical_sha256

    def _git(*args: str) -> str:
        result = _subprocess.run(
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
        _git("config", "user.name", "Composition")
        _git("config", "user.email", "composition@test")
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
        _git("commit", "-q", "-m", "composition materials (acceptance A1)")


def _sealed_resolver(root, request) -> SealedMaterialResolver:
    from research_automation.control_plane.final_eval_composition import (
        build_sealed_material_resolver,
    )

    _ensure_committed_materials(root, request)
    return build_sealed_material_resolver(
        request=request,
        bundle=_material_bundle(),
        repository_root=root,
        root_secret=ROOT_SECRET,
    )


def _context(root, grant, **overrides) -> AuthorizedFinalEvalContext:
    request = _request()
    payload = dict(
        request=request,
        grant=grant,
        nonce=NONCE,
        actor=Actor("operator-1", "human", "final-eval-op-cr009"),
        identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
        idempotency_key="p8-composition-1",
        task_spec_ref="manifest.json",
        task_spec_sha256="1" * 64,
        authority_capability=ROOT_SECRET,
        repository_root=str(root),
        data_root=seal_trusted_data_root(
            root,
            ("frozen/holdout.parquet",),
        ),
        worker_launcher=lambda: 0,
        evidence_sink=lambda payload: {},
        attempt_id=ATTEMPT,
        material_resolver=_sealed_resolver(root, request),
    )
    payload.update(overrides)
    return AuthorizedFinalEvalContext(**payload)


class FinalEvalCompositionTests(unittest.TestCase):
    def test_ordinary_code_cannot_construct_composition(self) -> None:
        """Wrong phase/actor/scope/instruction policy/attempt all reject
        BEFORE any store or evaluator is opened."""
        from tests.test_control_plane_campaign_store import (
            _authorized_campaign,
        )

        with _authorized_campaign("campaign-composition-p6") as (
            root,
            p6_grant,
            journal,
        ):
            # wrong grant phase (a P6 campaign grant)
            with self.assertRaisesRegex(
                FinalEvalCompositionRejected, "P8 grant"
            ):
                compose_final_eval_runtime(
                    _context(root, p6_grant)
                )
        with _authorized_p8_campaign("campaign-composition-neg") as (
            root,
            grant,
            journal,
        ):
            # wrong actor
            with self.assertRaisesRegex(
                FinalEvalCompositionRejected, "actor"
            ):
                compose_final_eval_runtime(
                    _context(
                        root,
                        grant,
                        actor=Actor(
                            "other-actor", "human", "final-eval-op-cr009"
                        ),
                        request=_request(actor_id="other-actor"),
                    )
                )
            # wrong identity scope
            with self.assertRaisesRegex(
                FinalEvalCompositionRejected, "identity"
            ):
                compose_final_eval_runtime(
                    _context(
                        root,
                        grant,
                        identity=stores_module.AuthorityIdentity(
                            plan_hash=P8_IDENTITY["plan_hash"],
                            scope_hash="0" * 64,
                            instruction_policy_hash=P8_IDENTITY[
                                "instruction_policy_hash"
                            ],
                        ),
                    )
                )
            # wrong attempt
            with self.assertRaisesRegex(
                FinalEvalCompositionRejected, "attempt"
            ):
                compose_final_eval_runtime(
                    _context(
                        root,
                        grant,
                        attempt_id="other-attempt",
                    )
                )

    def test_identity_drift_rejected(self) -> None:
        """Candidate/code/execution/features/model/threshold/roster/
        generation/holdout drifts reject before any open."""
        from research_automation.control_plane.final_eval_composition import (
            build_sealed_material_resolver,
        )

        with _authorized_p8_campaign("campaign-composition-drift") as (
            root,
            grant,
            journal,
        ):
            _ensure_committed_materials(root, _request())
            drifts = {
                "campaign_id": "campaign-other",
                "holdout_id": "holdout-other",
                "code_sha256": "5" * 64,
                "execution_spec_sha256": "6" * 64,
                "features_sha256": "7" * 64,
                "model_id": "model-other",
                "threshold_sha256": "9" * 64,
                "roster_sha256": "a" * 64,
                "generation_id": "generation-other",
                "authorization_nonce": "f" * 64,
            }
            for field_name, value in drifts.items():
                with self.subTest(field=field_name):
                    materials = _material_bundle(**{field_name: value})
                    # the manifest factory rejects the drift BEFORE any
                    # store/evaluator construction (sealed committed blob
                    # bytes disagree with the drifted identity)
                    with self.assertRaises(FinalEvalCompositionRejected):
                        build_sealed_material_resolver(
                            request=_request(),
                            bundle=materials,
                            repository_root=root,
                            root_secret=ROOT_SECRET,
                        )
                    # a drifted REQUEST can never be resolved by a sealed
                    # resolver either (exact request_sha256 lookup); the
                    # bundle-only identities (model_id/generation_id/
                    # authorization_nonce) are covered by the factory check
                    # above because they are not V2 request fields
                    request_field = {
                        "model_id": "model",
                        "generation_id": "generation",
                    }.get(field_name, field_name)
                    if not hasattr(_request(), request_field):
                        continue
                    drift_request = _request(**{request_field: value})
                    drift_context = _context(
                        root,
                        grant,
                        request=drift_request,
                    )
                    with self.assertRaises(FinalEvalCompositionRejected):
                        compose_final_eval_runtime(drift_context)

    def test_non_dry_run_without_context_rejected(self) -> None:
        """A forged/missing context fails BEFORE evaluator/store
        construction."""
        with self.assertRaises(FinalEvalCompositionRejected):
            entry_run(None)  # type: ignore[arg-type]
        with self.assertRaises(FinalEvalCompositionRejected):
            entry_run(object())  # type: ignore[arg-type]

    def test_attempt_id_alone_is_not_authorization(self) -> None:
        """An attempt id alone -- even one that EXISTS in the database --
        is never an authorization: a context with a valid request but no
        verified grant rejects, and NO durable row or evidence appears."""
        with _authorized_p8_campaign("campaign-composition-attempt") as (
            root,
            grant,
            journal,
        ):
            # bind the request so its attempt id exists in the database
            from tests.test_control_plane_final_eval_orchestrator import (
                _make_broker,
            )

            broker = _make_broker(root, grant)
            broker.bind(
                request=_request(),
                nonce=NONCE,
                actor=Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-composition-attempt",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority_db = root / "authority.sqlite3"
            import sqlite3 as _sqlite3

            before = _sqlite3.connect(str(authority_db))
            try:
                tickets_before = before.execute(
                    "SELECT COUNT(*) FROM task_tickets_v2"
                ).fetchone()[0]
            finally:
                before.close()
            with self.assertRaises(FinalEvalCompositionRejected):
                compose_final_eval_runtime(
                    _context(root, object())  # type: ignore[arg-type]
                )
            after = _sqlite3.connect(str(authority_db))
            try:
                tickets_after = after.execute(
                    "SELECT COUNT(*) FROM task_tickets_v2"
                ).fetchone()[0]
            finally:
                after.close()
            self.assertEqual(tickets_before, tickets_after)
            # no evidence was written
            evidence = (
                root
                / "research_state/control_plane/p8/attempts/"
                "p8-attempt-003/evidence"
            )
            self.assertFalse(evidence.exists())

    def test_production_entry_rejects_forged_evaluator_override(self) -> None:
        """CR-010 F-01: the production entry NEVER accepts a caller-selected
        evaluator or evaluator request.  Injecting a fake
        ``SUCCEEDED``-returning evaluator (or a test-only V1 request) into
        the context must fail closed BEFORE the runtime is constructed --
        the composition root and the host entry are the ONLY assembly
        path, and a deleted Holdout can never be papered over."""
        with _authorized_p8_campaign("campaign-composition-forge") as (
            root,
            grant,
            journal,
        ):
            # a caller-selected evaluator/request is NOT an accepted context
            # field -- the context rejects the override outright
            with self.assertRaises((TypeError, FinalEvalCompositionRejected)):
                AuthorizedFinalEvalContext(  # type: ignore[call-arg]
                    **_context_kwargs(root, grant),
                    evaluator=object(),  # type: ignore[arg-type]
                )
            with self.assertRaises((TypeError, FinalEvalCompositionRejected)):
                AuthorizedFinalEvalContext(  # type: ignore[call-arg]
                    **_context_kwargs(root, grant),
                    evaluator_request=object(),  # type: ignore[arg-type]
                )

    def test_repr_contains_no_nonce_or_root_capability(self) -> None:
        """CR-010 F-01: repr(AuthorizedFinalEvalContext) and
        repr(FinalEvalMaterialBundle) must never leak the raw nonce or the
        root capability."""
        with _authorized_p8_campaign("campaign-composition-repr") as (
            root,
            grant,
            journal,
        ):
            context = _context(root, grant)
            context_repr = repr(context)
            self.assertNotIn(NONCE, context_repr)
            self.assertNotIn(ROOT_SECRET, context_repr)
            bundle_repr = repr(_material_bundle())
            self.assertNotIn(NONCE, bundle_repr)
            self.assertNotIn(ROOT_SECRET, bundle_repr)


def _context_kwargs(root, grant) -> dict[str, object]:
    """The valid context keyword arguments (used to prove unknown fields
    like a forged evaluator are rejected)."""
    return dict(
        request=_request(),
        grant=grant,
        nonce=NONCE,
        actor=Actor("operator-1", "human", "final-eval-op-cr009"),
        identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
        idempotency_key="p8-composition-1",
        task_spec_ref="manifest.json",
        task_spec_sha256="1" * 64,
        authority_capability=ROOT_SECRET,
        repository_root=str(root),
        data_root=seal_trusted_data_root(root, ("frozen/holdout.parquet",)),
        worker_launcher=lambda: 0,
        evidence_sink=lambda payload: {},
        attempt_id=ATTEMPT,
        material_resolver=_sealed_resolver(root, _request()),
    )


class FinalEvalCompositionEntryTests(unittest.TestCase):
    def test_dry_run_performs_zero_writes(self) -> None:
        """The ordinary shell final-eval CLI: dry-run is a zero-write
        wiring preview; non-dry-run is ALWAYS rejected (an attempt id can
        never construct authorization)."""
        stdout = io.StringIO()
        stderr = io.StringIO()
        rc = cli_module.main(
            ["final-eval", "--attempt-id", ATTEMPT, "--dry-run"],
            stdout=stdout,
            stderr=stderr,
        )
        self.assertEqual(rc, 0)
        self.assertIn("wired", stdout.getvalue())
        stderr2 = io.StringIO()
        rc2 = cli_module.main(
            ["final-eval", "--attempt-id", ATTEMPT],
            stdout=io.StringIO(),
            stderr=stderr2,
        )
        self.assertEqual(rc2, 2)
        self.assertIn("composition root", stderr2.getvalue())

    def test_reviewed_policy_candidate_validated_and_activated_disposably(
        self,
    ) -> None:
        """CR-010 C0 (Phase B): the updated reviewed policy candidate is
        validated against the REAL inventory scan -- ONLY the authorized
        composition root owns OPEN_HOLDOUT -- and activated ONLY in the
        disposable staging Authority (the real active-policy row is never
        touched)."""
        import hashlib as _hashlib
        import sqlite3 as _sqlite3
        from pathlib import Path as _Path

        from research_automation.control_plane import entry_guard as eg
        from research_automation.control_plane.artifact_semantics import (
            validate_reviewed_entry_policy,
            reviewed_policy_receipt_sha256,
        )
        from research_automation.control_plane.contracts import (
            canonical_json,
            canonical_sha256,
        )
        from research_automation.control_plane.stores import (
            publish_reviewed_entry_policy,
        )

        workspace = _Path(__file__).resolve().parents[1]

        def entry_payload(record) -> dict[str, object]:
            content = record.content_sha256
            if content is None:
                content = _hashlib.sha256(
                    record.entry_id.encode("utf-8")
                ).hexdigest()
            return {
                "actor_type": record.actor_type,
                "callable_name": record.callable_name,
                "content_sha256": content,
                "declared_phase": (
                    record.declared_phase.value
                    if record.declared_phase is not None
                    else None
                ),
                "declared_side_effects": [
                    effect.value for effect in record.declared_side_effects
                ],
                "disposition": record.disposition,
                "entry_id": record.entry_id,
                "external_metadata": dict(record.external_metadata),
                "kind": record.kind,
                "path": record.path,
                "resource_roots": list(record.resource_roots),
                "source": record.source,
                "trust_state": record.trust_state,
            }

        records = eg.EntryInventory.scan(workspace)
        entries = [entry_payload(record) for record in records]
        identity_binding = {
            "plan_hash": P8_IDENTITY["plan_hash"],
            "scope_hash": P8_IDENTITY["scope_hash"],
            "instruction_policy_hash": P8_IDENTITY[
                "instruction_policy_hash"
            ],
        }
        inventory = {
            "schema_version": "control_plane.entry_inventory.v3",
            "plan_version": "V3.4.2-CR010",
            "phase": "P8",
            "attempt_id": ATTEMPT,
            "identity_binding": identity_binding,
            "source_identity_sha256": "1" * 64,
            "entries": entries,
            "entry_count": len(entries),
        }
        inventory_no_hash = {
            key: value
            for key, value in inventory.items()
            if key != "inventory_payload_sha256"
        }
        inventory["inventory_payload_sha256"] = canonical_sha256(
            inventory_no_hash
        )
        policy = {
            "schema_version": "control_plane.entry_policy.v1",
            "plan_version": "V3.4.2-CR010",
            "phase": "P8",
            "attempt_id": ATTEMPT,
            "identity_binding": identity_binding,
            "review_state": "APPROVED",
            "reviewer_id": "independent-reviewer-human",
            "inventory_payload_sha256": inventory[
                "inventory_payload_sha256"
            ],
            "entries": entries,
            "entry_count": len(entries),
        }
        policy["review_receipt_sha256"] = reviewed_policy_receipt_sha256(
            policy
        )
        policy_without_hash = {
            key: value
            for key, value in policy.items()
            if key != "policy_payload_sha256"
        }
        policy["policy_payload_sha256"] = canonical_sha256(
            policy_without_hash
        )
        raw = canonical_json(policy).encode("utf-8")
        validated = validate_reviewed_entry_policy(
            raw,
            expected_plan_version="V3.4.2-CR010",
            expected_phase="P8",
            expected_attempt_id=ATTEMPT,
            expected_identity=identity_binding,
            final_inventory=inventory,
        )
        self.assertEqual(validated["review_state"], "APPROVED")
        # ONLY the authorized composition root owns OPEN_HOLDOUT
        open_holdout = [
            entry
            for entry in entries
            if "OPEN_HOLDOUT" in entry.get("declared_side_effects", [])
        ]
        self.assertEqual(len(open_holdout), 1)
        self.assertEqual(
            open_holdout[0]["entry_id"],
            "callable:research_automation.control_plane."
            "final_eval_composition:compose_final_eval_runtime",
        )
        # publish + activate ONLY in the disposable staging Authority
        with _authorized_p8_campaign("campaign-composition-policy") as (
            root,
            grant,
            journal,
        ):
            (root / "research_state/control_plane").mkdir(
                parents=True, exist_ok=True
            )
            published = publish_reviewed_entry_policy(
                raw,
                repository_root=root,
                expected_plan_version="V3.4.2-CR010",
                expected_phase="P8",
                expected_attempt_id=ATTEMPT,
                expected_identity=identity_binding,
                final_inventory=inventory,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            reviewer = Actor(
                "independent-reviewer", "human", "policy-review-invocation"
            )
            # the activation lease comes from a P0 maintenance grant (a
            # non-P0 ticket would itself require an active policy)
            from datetime import datetime, timezone as _tz

            maintenance_actor = Actor(
                "policy-activator", "automation", "policy-activation-inv"
            )
            envelope = authority._provision_authorization(
                phase=stores_module.Phase.P0,
                attempt_id="p0-policy-activation-staging",
                actor=maintenance_actor,
                identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                expires_at=datetime(2027, 1, 1, tzinfo=_tz.utc),
                allowed_side_effects=(
                    stores_module.SideEffect.WRITE_CONTROL_PLANE,
                ),
            )
            maintenance_grant = authority.claim_authorization(
                envelope,
                expected_phase=stores_module.Phase.P0,
                expected_attempt_id="p0-policy-activation-staging",
                actor=maintenance_actor,
                identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
            )
            ticket = authority._issue_task_ticket(
                maintenance_grant,
                {
                    "task_id": "P0-POLICY-ACTIVATION",
                    "objective": "disposable staging policy activation",
                    "dependencies": [],
                    "idempotency_key": "p0-policy-activation-staging",
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
                allowed_side_effects=(
                    stores_module.SideEffect.WRITE_CONTROL_PLANE,
                ),
            )
            lease = authority._begin_task(ticket)
            activated = authority._activate_reviewed_entry_policy(
                lease,
                reviewer=reviewer,
                policy_sha256=published.file_sha256,
                policy_payload_sha256=published.policy_payload_sha256,
                inventory_payload_sha256=published.inventory_payload_sha256,
                review_receipt_sha256=published.review_receipt_sha256,
                expected_active_sha256=None,
            )
            self.assertEqual(activated.policy_sha256, published.file_sha256)
            connection = _sqlite3.connect(str(root / "authority.sqlite3"))
            try:
                row = connection.execute(
                    "SELECT policy_sha256 FROM active_entry_policy_v1 "
                    "WHERE singleton_id = 1"
                ).fetchone()
            finally:
                connection.close()
            self.assertEqual(row[0], published.file_sha256)


if __name__ == "__main__":
    unittest.main()
