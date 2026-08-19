"""Independent CR-010 P8 acceptance suite (frozen-acceptance plan 4.2/11.1).

Every expected value is computed INDEPENDENTLY from production APIs and the
disposable repository bytes -- this file never imports tests.* fixtures as an
assertion source.  The suite is the contract for A1 (material sealing), A2
(failure-lease authority) and A3 (recovery-only entry); the RED run at Task 0
must reproduce the original bypasses on the pre-fix code.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.campaign_roster import RosterManifest
from research_automation.control_plane.contracts import (
    Actor,
    SideEffect,
    canonical_json,
    canonical_sha256,
)
from research_automation.control_plane.final_eval_authority import (
    FINAL_EVAL_REQUEST_V2,
    AuthorityFinalEvalBroker,
    FinalEvalRequestV2,
    _nonce_fingerprint,
)
from research_automation.control_plane.final_eval_composition import (
    AuthorizedFinalEvalContext,
    FinalEvalCompositionRejected,
    SealedMaterialResolver,
    compose_final_eval_runtime,
)
from research_automation.control_plane.final_eval_holdout_store import (
    SqliteHoldoutStore,
)
from research_automation.control_plane.final_eval_request_projection import (
    FinalEvalMaterialBundle,
)
from research_automation.control_plane.final_evaluator import (
    CandidateBinding,
    TrustedEvaluatorDataRoot,
    seal_trusted_data_root,
)
from research_automation.foundations.artifact_identity import (
    artifact_identity_from_bytes,
)
from research_automation.foundations.protocols import (
    DatasetBinding,
    FeatureBoundary,
    FeatureField,
    FoldSelection,
    FoldSpec,
    LabelDefinition,
    ModelThresholdSpec,
    OutputContract,
    ProtocolApproval,
    ProtocolApprovalStatement,
    ProtocolDefinition,
    ProtocolMetadata,
    RosterMember,
    RunnerSpec,
    compile_execution_spec,
    protocol_sha256,
)

ROOT_SECRET = "acceptance-root-capability-0123456789abcdef"
NONCE = "0123456789abcdef" * 4
ATTEMPT = "p8-acceptance-003"
REPO_SUBDIR = "research_state/control_plane/p8/attempts/p8-acceptance-003"
HOLDOUT_REF = "frozen/holdout.parquet"
IDENTITY = {
    "plan_hash": hashlib.sha256(b"acceptance-plan").hexdigest(),
    "scope_hash": hashlib.sha256(b"acceptance-scope").hexdigest(),
    "instruction_policy_hash": hashlib.sha256(b"acceptance-policy").hexdigest(),
}


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {args[0]} failed: {result.stderr[-300:]}")
    return result.stdout.strip()


def _artifact(
    content: bytes,
    *,
    role: str,
    generation: str = "acceptance-1",
    content_schema: str = "research.source_snapshot.v1",
    kind: str = "source",
    producer: str = "p8-acceptance",
):
    return artifact_identity_from_bytes(
        content,
        content_schema=content_schema,
        producer=producer,
        generation=generation,
        kind=kind,
        logical_role=role,
    )


def _protocol() -> ProtocolDefinition:
    market_data = _artifact(
        b"acceptance-market-data", role="dataset-bars",
        generation="acceptance-generation-1",
        content_schema="research.market_data.v1",
        kind="dataset",
    )
    hyperparameters = _artifact(
        b"acceptance-hyperparameters", role="model-hyperparameters",
        generation="acceptance-generation-1",
        content_schema="research.hyperparameters.v1",
        kind="model-config",
    )
    generation_manifest = _artifact(
        b"acceptance-generation-manifest", role="generation-manifest",
        generation="acceptance-generation-1",
        content_schema="research.generation_manifest.v1",
        kind="manifest",
    )
    code = _artifact(b"acceptance-runner-source", role="runner-source",
                     generation="git-acceptance")
    train = DatasetBinding(
        schema_version="research.dataset_binding.v1",
        dataset_id="acceptance-train",
        role="TRAIN",
        artifact_id=market_data.artifact_id,
        window_start="2020-01-01",
        window_end="2022-12-31",
    )
    validation = DatasetBinding(
        schema_version="research.dataset_binding.v1",
        dataset_id="acceptance-validation",
        role="VALIDATION",
        artifact_id=market_data.artifact_id,
        window_start="2023-01-01",
        window_end="2023-12-31",
    )
    test = DatasetBinding(
        schema_version="research.dataset_binding.v1",
        dataset_id="acceptance-test",
        role="FOLD_TEST",
        artifact_id=market_data.artifact_id,
        window_start="2024-01-01",
        window_end="2024-12-31",
    )
    return ProtocolDefinition(
        schema_version="research.protocol_definition.v1",
        protocol_id="acceptance-protocol-1",
        metadata=ProtocolMetadata(
            schema_version="research.protocol_metadata.v1",
            display_name="Acceptance forward validation",
            notes="independent acceptance fixture",
        ),
        generation_id="acceptance-generation-1",
        generation_manifest_artifact_id=generation_manifest.artifact_id,
        universe_id="a-share-point-in-time-v1",
        calendar_id="sse-szse-trading-v1",
        adjustment_scheme_id="hfq-v1",
        validation_design="ROLLING_FORWARD",
        fold_window_policy_id="train3y-validate1y-test1y-v1",
        label=LabelDefinition(
            schema_version="research.label_definition.v1",
            label_id="acceptance-label",
            entry_rule_id="signal-close-next-open",
            exit_rule_id="fixed-horizon-or-stop",
            horizon_trading_days=5,
        ),
        datasets=tuple(
            sorted(
                (train, validation, test),
                key=lambda item: item.dataset_id,
            )
        ),
        folds=(
            FoldSpec(
                schema_version="research.fold_spec.v1",
                fold_id="acceptance-fold",
                train_dataset_id="acceptance-train",
                validation_dataset_id="acceptance-validation",
                test_dataset_id="acceptance-test",
                purge_trading_days=5,
                embargo_trading_days=2,
            ),
        ),
        feature_boundary=FeatureBoundary(
            schema_version="research.feature_boundary.v1",
            boundary_id="acceptance-boundary-v1",
            feature_fields=(
                FeatureField(
                    schema_version="research.feature_field.v1",
                    name="white_line",
                    availability="SIGNAL_DAY_CLOSE",
                    reference_fields=("signal_day_close",),
                ),
            ),
            forbidden_feature_names=(
                "entry_date_close",
                "entry_date_high",
                "entry_date_low",
                "exit_date",
                "exit_price",
                "hold_days",
                "return_pct",
                "t1_close",
            ),
        ),
        code_artifacts=(code,),
        input_artifacts=tuple(
            sorted(
                (generation_manifest, market_data, hyperparameters),
                key=lambda item: item.artifact_id,
            )
        ),
        runner=RunnerSpec(
            schema_version="research.runner_spec.v1",
            runner_id="acceptance-runner",
            entrypoint="research.acceptance.runner:main",
            code_artifact_ids=(code.artifact_id,),
            argument_schema_sha256="a" * 64,
            compute_backend="CPU",
            backend_version="python-3.13",
        ),
        model=ModelThresholdSpec(
            schema_version="research.model_threshold_spec.v1",
            model_mode="TRAIN_NEW",
            model_family="ranker",
            model_artifact_id=None,
            hyperparameter_artifact_id=hyperparameters.artifact_id,
            selection_by_fold=(
                FoldSelection(
                    schema_version="research.fold_selection.v1",
                    fold_id="acceptance-fold",
                    training_dataset_id="acceptance-train",
                    threshold_source="VALIDATION_SELECTED",
                    threshold_dataset_ids=("acceptance-validation",),
                    threshold_value=0.5,
                ),
            ),
            promotion_gate_id="acceptance-gate-v1",
        ),
        roster=(
            RosterMember(
                schema_version="research.roster_member.v1",
                role="factor_engineer",
                provider_profile_id="offline-local",
                model_id="deterministic-reviewer",
                public_identity_sha256="b" * 64,
                redacted=True,
            ),
        ),
        output_contracts=(
            OutputContract(
                schema_version="research.output_contract.v1",
                logical_role="fold-report",
                output_schema_id="research.fold_report.v1",
                destination_class="STAGING_ONLY",
            ),
        ),
        allowed_side_effects=(
            SideEffect.READ,
            SideEffect.RUN_RESEARCH,
            SideEffect.START_SUBPROCESS,
            SideEffect.WRITE_STAGING,
        ),
    )


def _approval(protocol: ProtocolDefinition) -> ProtocolApproval:
    approver = Actor("acceptance-reviewer", "human", "acceptance-review-1")
    statement = ProtocolApprovalStatement(
        schema_version="research.protocol_approval_statement.v1",
        approved_protocol_sha256=protocol_sha256(protocol),
        decision="APPROVED",
        approver=approver,
    )
    evidence = _artifact(
        canonical_json(statement.model_dump(mode="json")).encode("utf-8"),
        role="protocol-approval",
        generation="approval-1",
        content_schema="research.protocol_approval_statement.v1",
        kind="review",
        producer=approver.actor_id,
    )
    return ProtocolApproval(
        schema_version="research.protocol_approval.v1",
        statement=statement,
        approval_evidence=evidence,
        evidence_trust="UNVERIFIED_EXTERNAL_STATEMENT",
    )


def _execution_spec():
    protocol = _protocol()
    return compile_execution_spec(
        protocol,
        approved_protocol=protocol,
        approval=_approval(protocol),
        amendment=None,
    )


def _roster() -> RosterManifest:
    from research_automation.control_plane.campaign_roster import (
        RosterMember as CampaignRosterMember,
    )

    member = CampaignRosterMember(
        member_id="acceptance-member",
        provider="fake-provider",
        profile="offline-local",
        model="deterministic-reviewer",
        role="factor_engineer",
        prompt_sha256=_sha("prompt"),
        config_sha256=_sha("config"),
        capability_sha256=_sha("capability"),
    )
    return RosterManifest(
        cycle_id="acceptance-cycle-1",
        members=(member,),
        manifest_sha256=canonical_sha256(
            {
                "cycle_id": "acceptance-cycle-1",
                "members": (member.to_payload(),),
            }
        ),
    )


def _candidate_set() -> tuple[CandidateBinding, ...]:
    return (
        CandidateBinding("candidate-a", _sha("candidate-a")),
        CandidateBinding("candidate-b", _sha("candidate-b")),
    )


def _candidate_set_sha256(candidates: tuple[CandidateBinding, ...]) -> str:
    return canonical_sha256(
        tuple((c.candidate_id, c.candidate_sha256) for c in candidates)
    )


def _build_disposable_repo(root: Path) -> dict[str, str]:
    """git init + write + COMMIT every research material; returns the
    ref -> sha256 map.  The synthetic Holdout is NEVER committed."""
    base = root / REPO_SUBDIR
    base.mkdir(parents=True)
    candidates = _candidate_set()
    freeze = {
        "candidate_set": [
            {"candidate_id": c.candidate_id,
             "candidate_sha256": c.candidate_sha256}
            for c in candidates
        ]
    }
    execution_spec = _execution_spec()
    roster = _roster()
    materials: dict[str, tuple[Path, bytes, str]] = {
        "candidate_freeze_ref": (
            base / "freeze.json",
            canonical_json(freeze).encode("utf-8"),
            _candidate_set_sha256(candidates),
        ),
        "code_ref": (
            base / "code.py",
            b"# acceptance frozen runner\n",
            _sha_bytes(b"# acceptance frozen runner\n"),
        ),
        "execution_spec_ref": (
            base / "spec.json",
            canonical_json(execution_spec.model_dump(mode="json")).encode("utf-8"),
            canonical_sha256(execution_spec.model_dump(mode="json")),
        ),
        "features_ref": (
            base / "features.json",
            b'{"features": ["volume", "price_shape"]}\n',
            _sha_bytes(b'{"features": ["volume", "price_shape"]}\n'),
        ),
        "threshold_ref": (
            base / "threshold.json",
            b'{"metric": "accuracy", "value": 0.5}\n',
            _sha_bytes(b'{"metric": "accuracy", "value": 0.5}\n'),
        ),
        "roster_ref": (
            base / "roster.json",
            canonical_json(
                {
                    "cycle_id": "acceptance-cycle-1",
                    "members": tuple(m.to_payload() for m in roster.members),
                }
            ).encode("utf-8"),
            roster.manifest_sha256,
        ),
    }
    for _, (path, raw, _) in materials.items():
        path.write_bytes(raw)
    _git(root, "init", "--quiet")
    _git(root, "config", "user.name", "Acceptance")
    _git(root, "config", "user.email", "acceptance@test")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "acceptance materials")
    return {ref: sha for ref, (_, _, sha) in materials.items()}


def _synthetic_holdout(data_root: Path, holdout_id: str) -> tuple[str, str]:
    document = {
        "schema_version": "control_plane.synthetic_holdout.v1",
        "holdout_id": holdout_id,
        "metrics": [{"name": "rows", "value": 120}],
        "counts": [{"name": "opened_once", "value": 1}],
        "sha256s": [{"artifact_id": "synthetic-holdout", "sha256": "1" * 64}],
        "evidence_refs": [REPO_SUBDIR + "/evidence/synthetic_holdout.json"],
    }
    raw = json.dumps(document, sort_keys=True).encode("utf-8")
    (data_root / HOLDOUT_REF).parent.mkdir(parents=True, exist_ok=True)
    (data_root / HOLDOUT_REF).write_bytes(raw)
    return holdout_id, _sha_bytes(raw)


def _bootstrap_authority(root: Path):
    """Disposable Authority + ACTIVE P8 grant; returns (grant, identity)."""
    with _stores(root):
        stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
        identity = stores_module.AuthorityIdentity(**IDENTITY)
        actor = Actor("operator-acceptance", "human", "invocation-acceptance-003")
        authority = stores_module._AuthorityStore(
            root_secret=ROOT_SECRET,
            clock=lambda: datetime(2026, 8, 17, tzinfo=timezone.utc),
        )
        envelope = authority._provision_authorization(
            phase=stores_module.Phase.P8,
            attempt_id=ATTEMPT,
            actor=actor,
            identity=identity,
            expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
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
        return grant, identity


def _v2_request(
    *,
    holdout_id: str,
    holdout_sha256: str,
    material_sha256: dict[str, str],
    **overrides,
) -> FinalEvalRequestV2:
    payload = dict(
        schema_version=FINAL_EVAL_REQUEST_V2,
        research_plan_sha256=IDENTITY["plan_hash"],
        campaign_id="campaign-acceptance-1",
        campaign_sha256=_sha("campaign-acceptance"),
        holdout_id=holdout_id,
        holdout_sha256=holdout_sha256,
        nonce_fingerprint=_nonce_fingerprint(ROOT_SECRET, NONCE),
        candidate_freeze_ref=REPO_SUBDIR + "/freeze.json",
        candidate_freeze_sha256=material_sha256["candidate_freeze_ref"],
        code_ref=REPO_SUBDIR + "/code.py",
        code_sha256=material_sha256["code_ref"],
        execution_spec_ref=REPO_SUBDIR + "/spec.json",
        execution_spec_sha256=material_sha256["execution_spec_ref"],
        features_ref=REPO_SUBDIR + "/features.json",
        features_sha256=material_sha256["features_ref"],
        model="model-acceptance-1",
        model_sha256=_sha("model-acceptance"),
        threshold="0.5",
        threshold_ref=REPO_SUBDIR + "/threshold.json",
        threshold_sha256=material_sha256["threshold_ref"],
        roster_ref=REPO_SUBDIR + "/roster.json",
        roster_sha256=material_sha256["roster_ref"],
        generation="generation-acceptance-1",
        generation_sha256=_sha("generation-acceptance"),
        actor_id="operator-acceptance",
        actor_type="human",
        invocation_id="invocation-acceptance-003",
        authority_plan_hash=IDENTITY["plan_hash"],
        identity_scope_hash=IDENTITY["scope_hash"],
        identity_instruction_policy_hash=IDENTITY["instruction_policy_hash"],
        attempt_id=ATTEMPT,
    )
    payload.update(overrides)
    return FinalEvalRequestV2(**payload)


def _material_bundle(request: FinalEvalRequestV2) -> FinalEvalMaterialBundle:
    return FinalEvalMaterialBundle(
        campaign_id=request.campaign_id,
        campaign_sha256=request.campaign_sha256,
        holdout_id=request.holdout_id,
        holdout_sha256=request.holdout_sha256,
        authorization_nonce=NONCE,
        candidate_freeze_ref=request.candidate_freeze_ref,
        candidate_set=_candidate_set(),
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
        actor=Actor(request.actor_id, request.actor_type, request.invocation_id),
        identity=stores_module.AuthorityIdentity(
            request.authority_plan_hash,
            request.identity_scope_hash,
            request.identity_instruction_policy_hash,
        ),
        attempt_id=request.attempt_id,
    )


@contextlib.contextmanager
def _stores(root: Path):
    with stores_module.store_path_override(
        authority=root / "authority.sqlite3",
        operational=root / "operational.sqlite3",
    ):
        stores_module._expected_schema_sha256.cache_clear()
        try:
            yield
        finally:
            stores_module._expected_schema_sha256.cache_clear()


class _P8AcceptanceHarness(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.material_sha256 = _build_disposable_repo(self.root)
        self.data_root = self.root / "data-root"
        self.data_root.mkdir()
        self.holdout_id = "holdout-acceptance-1"
        self.holdout_sha256 = _synthetic_holdout(
            self.data_root, self.holdout_id
        )[1]
        self.request = _v2_request(
            holdout_id=self.holdout_id,
            holdout_sha256=self.holdout_sha256,
            material_sha256=self.material_sha256,
        )
        self.grant, self.identity = _bootstrap_authority(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _actor(self) -> Actor:
        return Actor(
            "operator-acceptance", "human", "invocation-acceptance-003"
        )

    def _context(self, **overrides) -> AuthorizedFinalEvalContext:
        payload = dict(
            request=self.request,
            grant=self.grant,
            nonce=NONCE,
            actor=self._actor(),
            identity=self.identity,
            idempotency_key="p8-acceptance-1",
            task_spec_ref="manifest.json",
            task_spec_sha256="1" * 64,
            authority_capability=ROOT_SECRET,
            repository_root=str(self.root),
            data_root=seal_trusted_data_root(self.data_root, (HOLDOUT_REF,)),
            worker_launcher=lambda: 0,
            evidence_sink=lambda payload: {},
            attempt_id=ATTEMPT,
            material_resolver=_sealed_resolver(self.request, self.root),
        )
        payload.update(overrides)
        return AuthorizedFinalEvalContext(**payload)

    def _bind(self, request=None, idempotency_key="p8-acceptance-bind", nonce=NONCE):
        with _stores(self.root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            broker = AuthorityFinalEvalBroker(
                authority=authority,
                grant=self.grant,
                attempt_id=ATTEMPT,
                identity=self.identity,
            )
            return broker.bind(
                request=request or self.request,
                nonce=nonce,
                actor=self._actor(),
                idempotency_key=idempotency_key,
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )


def _sealed_resolver(
    request: FinalEvalRequestV2,
    repository_root: Path,
) -> SealedMaterialResolver:
    """The sealed resolver the production manifest factory MUST build
    (A1: record-backed, no callable, exact-request lookup).

    Task 1 switches this to ``build_sealed_material_resolver``; the RED
    run uses the legacy callable wrapper so the A1 bypasses reproduce on
    the pre-fix code.
    """
    try:
        from research_automation.control_plane.final_eval_composition import (
            build_sealed_material_resolver,
        )
    except ImportError:
        build_sealed_material_resolver = None  # type: ignore[assignment]
    if build_sealed_material_resolver is not None:
        return build_sealed_material_resolver(
            request=request,
            bundle=_material_bundle(request),
            repository_root=repository_root,
            root_secret=ROOT_SECRET,
        )
    return SealedMaterialResolver(
        lambda r: _material_bundle(r)
    )


class A1MaterialSealingTests(_P8AcceptanceHarness):
    """A1: every ref-backed research material must exist, be non-empty, be
    a committed regular blob and match the recomputed bytes hash."""

    def _compose(self, context):
        with _stores(self.root):
            return compose_final_eval_runtime(context)

    def test_a1_missing_material_ref_is_rejected(self) -> None:
        for ref_field in (
            "candidate_freeze_ref",
            "code_ref",
            "execution_spec_ref",
            "features_ref",
            "threshold_ref",
            "roster_ref",
        ):
            with self.subTest(ref=ref_field):
                request = replace(
                    self.request,
                    **{ref_field: REPO_SUBDIR + "/does-not-exist.bin"},
                )
                context = self._context(request=request)
                with self.assertRaises(FinalEvalCompositionRejected):
                    self._compose(context)

    def test_a1_empty_material_ref_is_rejected(self) -> None:
        for ref_field in (
            "candidate_freeze_ref",
            "code_ref",
            "execution_spec_ref",
            "features_ref",
            "threshold_ref",
            "roster_ref",
        ):
            with self.subTest(ref=ref_field):
                request = replace(self.request, **{ref_field: ""})
                context = self._context(request=request)
                with self.assertRaises(FinalEvalCompositionRejected):
                    self._compose(context)

    def test_a1_uncommitted_or_symlink_material_is_rejected(self) -> None:
        # worktree-only tamper: the committed blob differs from the
        # working-tree file -> the sealed resolver must use committed bytes
        code_path = self.root / REPO_SUBDIR / "code.py"
        original = code_path.read_bytes()
        code_path.write_bytes(b"# uncommitted tamper\n")
        request = replace(
            self.request,
            code_sha256=_sha_bytes(b"# uncommitted tamper\n"),
        )
        context = self._context(request=request)
        with self.assertRaises(FinalEvalCompositionRejected):
            self._compose(context)
        code_path.write_bytes(original)
        # symlink ref must be rejected (only committed regular blobs)
        if hasattr(os, "symlink"):
            link = self.root / REPO_SUBDIR / "code-link.py"
            try:
                os.symlink(code_path, link)
            except OSError:
                self.skipTest("symlink unavailable on this platform")
            request = replace(
                self.request,
                code_ref=str(link.relative_to(self.root)).replace("\\", "/"),
                code_sha256=self.material_sha256["code_ref"],
            )
            context = self._context(request=request)
            with self.assertRaises(FinalEvalCompositionRejected):
                self._compose(context)

    def test_a1_arbitrary_callable_cannot_construct_production_resolver(
        self,
    ) -> None:
        # a raw callable is never a sealed resolver
        with self.assertRaises((TypeError, FinalEvalCompositionRejected)):
            SealedMaterialResolver(  # type: ignore[call-arg]
                lambda request: _material_bundle(request)
            )
        context = self._context(
            material_resolver=lambda request: _material_bundle(request)  # type: ignore[assignment]
        )
        with self.assertRaises(FinalEvalCompositionRejected):
            self._compose(context)

    def test_a1_valid_committed_materials_compose(self) -> None:
        runtime = self._compose(self._context())
        self.assertIsNotNone(runtime)

    def test_directly_forged_sealed_resolver_rejected_before_store(self) -> None:
        """F-01: a caller-built ``SealedMaterialResolver`` must be rejected
        by the production composition BEFORE any store/evaluator is
        created.  Direct dataclass construction always raises; an
        ``object.__new__``-stuffed forged instance (no valid factory
        token) is rejected at composition."""
        from research_automation.control_plane.final_eval_composition import (
            MaterialRecord,
            SealedMaterialResolver,
        )

        with self.assertRaises(FinalEvalCompositionRejected):
            SealedMaterialResolver(
                records=(
                    MaterialRecord(
                        ref="forged/ref.bin",
                        content_sha256="1" * 64,
                        blob_sha256="2" * 64,
                        frozen_commit="forged-commit",
                    ),
                ),
                repository_root=str(self.root),
                frozen_commit="forged-commit",
                frozen_tree="forged-tree",
                manifest_digest="3" * 64,
                request_sha256=self.request.request_sha256,
                _bundle=object(),
            )
        # object.__new__ forged instance with a WRONG factory token
        forged = object.__new__(SealedMaterialResolver)
        forged._records = (
            MaterialRecord(
                ref="forged/ref.bin",
                content_sha256="1" * 64,
                blob_sha256="2" * 64,
                frozen_commit="forged-commit",
            ),
        )
        forged._repository_root = str(self.root)
        forged._frozen_commit = "forged-commit"
        forged._frozen_tree = "forged-tree"
        forged._manifest_digest = "3" * 64
        forged._request_sha256 = self.request.request_sha256
        forged._bundle = _material_bundle(self.request)
        forged._factory_token = "FORGED-TOKEN"
        context = self._context(material_resolver=forged)
        with self.assertRaises(FinalEvalCompositionRejected):
            self._compose(context)

    def test_forged_resolver_missing_ref_rejected(self) -> None:
        """F-01: a same-direction forged request+bundle whose material file
        does NOT exist must be rejected.  A forged
        ``object.__new__``-stuffed resolver is rejected by factory
        provenance; missing committed materials are rejected by the
        sealed-content verification."""
        from research_automation.control_plane.final_eval_composition import (
            MaterialRecord,
            SealedMaterialResolver,
        )

        forged_request = _v2_request(
            holdout_id=self.holdout_id,
            holdout_sha256=self.holdout_sha256,
            material_sha256=self.material_sha256,
            code_ref=REPO_SUBDIR + "/does-not-exist.py",
            code_sha256=_sha("forged-code"),
        )
        forged_bundle = _material_bundle(forged_request)
        forged = object.__new__(SealedMaterialResolver)
        forged._records = (
            MaterialRecord(
                ref=REPO_SUBDIR + "/does-not-exist.py",
                content_sha256=_sha("forged-code"),
                blob_sha256=_sha("forged-code"),
                frozen_commit="forged-commit",
            ),
        )
        forged._repository_root = str(self.root)
        forged._frozen_commit = "forged-commit"
        forged._frozen_tree = "forged-tree"
        forged._manifest_digest = "1" * 64
        forged._request_sha256 = forged_request.request_sha256
        forged._bundle = forged_bundle
        forged._factory_token = "FORGED-TOKEN"
        context = self._context(
            request=forged_request,
            material_resolver=forged,
        )
        # provenance fails first (wrong token); even with a valid token the
        # MISSING committed material must fail the sealed-content check
        with self.assertRaises(FinalEvalCompositionRejected):
            self._compose(context)



    def test_no_casting_api_for_forged_records(self) -> None:
        """F-01 (git-native run005): the sealed resolver class exposes NO
        casting API (`_create`/`_mint`) a caller could invoke with forged
        records/manifest: minting happens only inside the module-private
        factory, which demands the sealed-identity root capability.
        `object.__new__`-stuffed forgery is covered separately by
        test_directly_forged_sealed_resolver_rejected_before_store."""
        from research_automation.control_plane.final_eval_composition import (
            SealedMaterialResolver,
        )

        self.assertFalse(hasattr(SealedMaterialResolver, "_create"))
        self.assertFalse(hasattr(SealedMaterialResolver, "_mint"))

    def test_resolver_slot_mutation_rejected(self) -> None:
        """F-01 (git-native run003): a sealed resolver is IMMUTABLE -- any
        attempt to mutate _records/_bundle/frozen_commit/frozen_tree after
        creation is rejected."""
        from research_automation.control_plane.final_eval_composition import (
            FinalEvalCompositionRejected,
        )

        resolver = _sealed_resolver(self.request, self.root)
        for field, value in (
            ("_records", ()),
            ("_bundle", object()),
            ("_frozen_commit", "forged-commit"),
            ("_frozen_tree", "forged-tree"),
        ):
            with self.subTest(field=field):
                with self.assertRaises(FinalEvalCompositionRejected):
                    setattr(resolver, field, value)

    def test_compose_rejects_head_drift_after_freeze(self) -> None:
        """F-01 (git-native run003): compose verifies the CURRENT Git
        HEAD/tree equals the resolver's frozen snapshot; a commit made
        AFTER the resolver was frozen (HEAD drift) is rejected before any
        store/evaluator."""
        from research_automation.control_plane.final_eval_composition import (
            FinalEvalCompositionRejected,
        )

        resolver = _sealed_resolver(self.request, self.root)
        context = self._context(material_resolver=resolver)
        runtime = self._compose(context)
        self.assertIsNotNone(runtime)
        (self.root / "drift-after-freeze.txt").write_text(
            "head moved", encoding="utf-8"
        )
        _git(self.root, "add", "-A")
        _git(self.root, "commit", "-q", "-m", "drift after freeze")
        with self.assertRaises(FinalEvalCompositionRejected):
            self._compose(context)

class A2FailureLeaseTests(_P8AcceptanceHarness):
    """A2: only an exact, typed, one-shot FINAL_EVAL_FAIL_BINDING
    WRITE_CONTROL_PLANE lease may terminalize a P8 binding as FAILED."""

    def _fail_with_lease(self, lease, binding, expected_version, *, clock=None):
        with _stores(self.root):
            authority = stores_module._AuthorityStore(
                root_secret=ROOT_SECRET, clock=clock
            )
            return authority._fail_final_eval_binding(
                lease,
                binding_id=binding.ticket_id,
                expected_version=expected_version,
                failure_reason="acceptance probe",
            )

    def _snapshot(self, ticket_id):
        with _stores(self.root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            return authority.final_eval_binding_snapshot(ticket_id)

    def test_a2_unrelated_read_lease_cannot_fail_binding(self) -> None:
        binding = self._bind()
        read_lease = self._p0_lease(
            binding, side_effects=(stores_module.SideEffect.READ,)
        )
        with self.assertRaises((TypeError, ValueError, RuntimeError)):
            self._fail_with_lease(read_lease, binding, binding.saga_version)
        self.assertEqual(self._snapshot(binding.ticket_id).saga_state, "CONSUMED")

    def test_a2_write_lease_for_other_binding_cannot_fail_binding(self) -> None:
        other_request = replace(
            self.request,
            holdout_id="holdout-other",
            holdout_sha256=_sha("holdout-other"),
            nonce_fingerprint=_nonce_fingerprint(ROOT_SECRET, NONCE + "x"),
        )
        other_binding = self._bind(
            request=other_request,
            idempotency_key="p8-acceptance-other",
            nonce=NONCE + "x",
        )
        binding = self._bind(idempotency_key="p8-acceptance-target")
        lease = self._p0_lease(
            other_binding,
            side_effects=(stores_module.SideEffect.WRITE_CONTROL_PLANE,),
        )
        with self.assertRaises((TypeError, ValueError, RuntimeError)):
            self._fail_with_lease(lease, binding, binding.saga_version)
        self.assertEqual(self._snapshot(binding.ticket_id).saga_state, "CONSUMED")

    def test_a2_expired_failure_lease_is_rejected_by_store_clock(self) -> None:
        binding = self._bind()
        lease = self._p0_lease(
            binding,
            side_effects=(stores_module.SideEffect.WRITE_CONTROL_PLANE,),
            expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
        )
        # the lease was issued while valid; the STORE clock later passes
        # the expiry -- the mutation must be rejected by the store clock
        with self.assertRaises((TypeError, ValueError, RuntimeError)):
            self._fail_with_lease(
                lease,
                binding,
                binding.saga_version,
                clock=lambda: datetime(2028, 1, 1, tzinfo=timezone.utc),
            )
        self.assertEqual(self._snapshot(binding.ticket_id).saga_state, "CONSUMED")

    def test_a2_recovery_lease_cannot_fail_binding(self) -> None:
        from research_automation.control_plane.stores import (
            FinalEvalRecoveryLease,
        )

        binding = self._bind()
        with self.assertRaises((TypeError, ValueError, RuntimeError)):
            self._fail_with_lease(
                object(), binding, binding.saga_version
            )
        with self.assertRaises((TypeError, ValueError, RuntimeError)):
            self._fail_with_lease(
                FinalEvalRecoveryLease, binding, binding.saga_version
            )
        self.assertEqual(self._snapshot(binding.ticket_id).saga_state, "CONSUMED")

    def test_a2_failure_lease_is_one_shot_and_cas_bound(self) -> None:
        binding = self._bind()
        with _stores(self.root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            failure_lease = self._issue_failure_lease(authority, binding)
            first = authority._fail_final_eval_binding(
                failure_lease,
                binding_id=binding.ticket_id,
                expected_version=binding.saga_version,
                failure_reason="acceptance one-shot",
            )
            self.assertEqual(first.saga_state, "AUTHORITY_TERMINAL")
            self.assertEqual(first.terminal_binding, "FAILED")
            # the same lease cannot be reused
            with self.assertRaises(RuntimeError):
                authority._fail_final_eval_binding(
                    failure_lease,
                    binding_id=binding.ticket_id,
                    expected_version=binding.saga_version,
                    failure_reason="acceptance replay",
                )
            # stale CAS version cannot write
            with self.assertRaises(RuntimeError):
                authority._fail_final_eval_binding(
                    self._issue_failure_lease(authority, binding),
                    binding_id=binding.ticket_id,
                    expected_version=binding.saga_version + 99,
                    failure_reason="acceptance stale",
                )
            snapshot = authority.final_eval_binding_snapshot(binding.ticket_id)
            self.assertEqual(snapshot.terminal_binding, "FAILED")

    def _p0_lease(self, binding, *, side_effects, expires_at=None):
        """An ordinary P0 maintenance TaskExecutionLease (the ORIGINAL
        bypass instrument: unrelated READ leases could mutate the binding)."""
        import secrets as _secrets

        with _stores(self.root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            unique = _secrets.token_hex(8)
            maintenance_actor = Actor(
                "p0-acceptance", "automation", "p0-acceptance-inv-" + unique
            )
            envelope = authority._provision_authorization(
                phase=stores_module.Phase.P0,
                attempt_id="p0-acceptance-" + unique,
                actor=maintenance_actor,
                identity=self.identity,
                expires_at=expires_at
                or datetime(2027, 1, 1, tzinfo=timezone.utc),
                allowed_side_effects=side_effects,
            )
            grant = authority.claim_authorization(
                envelope,
                expected_phase=stores_module.Phase.P0,
                expected_attempt_id="p0-acceptance-" + unique,
                actor=maintenance_actor,
                identity=self.identity,
            )
            ticket = authority._issue_task_ticket(
                grant,
                {
                    "task_id": "P0-ACCEPTANCE-MAINT",
                    "objective": "acceptance probe lease",
                    "dependencies": [],
                    "idempotency_key": "p0-acceptance-" + unique,
                    "task_spec_ref": "manifest.json",
                    "task_spec_sha256": "1" * 64,
                    "requirements": {
                        "required_test_receipt_ids": [],
                        "required_review_receipt_ids": [],
                        "required_evidence_ids": [],
                    },
                    "allowed_files": [],
                    "forbidden_files": [],
                    "baseline_ref": "manifest.json",
                    "baseline_sha256": "1" * 64,
                    "input_evidence_refs": [],
                },
                allowed_side_effects=side_effects,
            )
            return authority._begin_task(ticket)

    def _issue_failure_lease(self, authority, binding):
        """The typed FINAL_EVAL_FAIL_BINDING lease (A2 contract)."""
        from research_automation.control_plane.stores import (
            FinalEvalFailureLease,
        )

        return FinalEvalFailureLease.issue(
            authority=authority,
            binding_id=binding.ticket_id,
            expected_saga_version=binding.saga_version,
            identity=self.identity,
        )


class A3RecoveryEntryTests(_P8AcceptanceHarness):
    """A3: recovery is a dedicated entry that receives ONLY capability,
    binding id, a controlled store locator and exactly one durable lease."""

    def test_a3_recovery_context_contains_no_evaluation_secrets(self) -> None:
        from research_automation.control_plane.final_eval_recovery_entry import (
            RecoveryContext,
            RecoveryContextRejected,
        )

        for kwargs in (
            {"nonce": NONCE},
            {"grant": self.grant},
            {"worker_launcher": lambda: 0},
            {"material_resolver": object()},
            {"evaluator": object()},
            {"holdout_backend": object()},
        ):
            with self.subTest(field=next(iter(kwargs))):
                with self.assertRaises((TypeError, RecoveryContextRejected)):
                    RecoveryContext(  # type: ignore[call-arg]
                        authority_capability=ROOT_SECRET,
                        binding_id="binding-x",
                        repository_root=str(self.root),
                        **kwargs,
                    )
        context = RecoveryContext(
            authority_capability=ROOT_SECRET,
            binding_id="binding-x",
            repository_root=str(self.root),
        )
        rendered = repr(context)
        self.assertNotIn(NONCE, rendered)
        self.assertNotIn(ROOT_SECRET, rendered)

    def test_a3_each_crash_boundary_recovers_without_rebuilding_evaluation_context(
        self,
    ) -> None:
        from research_automation.control_plane.final_eval_recovery_entry import (
            run_recovery,
        )

        for boundary in (
            "consume",
            "open",
            "worker",
            "object",
            "claim",
            "result_staged",
            "closed",
            "terminal",
        ):
            with self.subTest(boundary=boundary):
                root = Path(self._tmp.name) / f"a3-{boundary}"
                root.mkdir()
                binding_id = self._crash_child(root, boundary)
                outcome = run_recovery(
                    authority_capability=ROOT_SECRET,
                    binding_id=binding_id,
                    repository_root=str(root),
                )
                self.assertEqual(outcome["saga_state"], "AUTHORITY_TERMINAL")
                self.assertIn(
                    outcome["terminal_binding"], ("SUCCEEDED", "FAILED")
                )

    def test_a3_recovery_never_reopens_or_recomputes(self) -> None:
        from research_automation.control_plane.final_eval_recovery_entry import (
            run_recovery,
        )

        root = Path(self._tmp.name) / "a3-reopen"
        root.mkdir()
        binding_id = self._crash_child(root, "worker")
        with _stores(root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            durable = SqliteHoldoutStore(authority=authority)
            before = durable.consumption_count(
                authority.final_eval_binding_snapshot(
                    binding_id
                ).request_sha256
            )
        outcome = run_recovery(
            authority_capability=ROOT_SECRET,
            binding_id=binding_id,
            repository_root=str(root),
        )
        self.assertEqual(outcome["saga_state"], "AUTHORITY_TERMINAL")
        with _stores(root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            durable = SqliteHoldoutStore(authority=authority)
            after = durable.consumption_count(
                authority.final_eval_binding_snapshot(
                    binding_id
                ).request_sha256
            )
        self.assertEqual(before, after)
        self.assertEqual(before, 1)

    # -- fresh-process crash harness (independent; the child drives the
    # saga through PRODUCTION APIs and hard-exits at each boundary) --

    def _crash_child(self, root: Path, boundary: str) -> str:
        # each crash root is a FULL disposable staging root (materials
        # committed, synthetic Holdout in a sealed data root, P8 grant)
        material_sha256 = _build_disposable_repo(root)
        (root / "data-root").mkdir()
        holdout_id, holdout_sha256 = _synthetic_holdout(
            root / "data-root", "holdout-acceptance-1"
        )
        grant, identity = _bootstrap_authority(root)
        request = _v2_request(
            holdout_id=holdout_id,
            holdout_sha256=holdout_sha256,
            material_sha256=material_sha256,
        )
        scenario = {
            "root": str(root),
            "boundary": boundary,
            "request": request.to_payload(),
            "grant": _serialize_grant(grant),
        }
        (root / ".a3-scenario.json").write_text(
            json.dumps(scenario), encoding="utf-8"
        )
        script_ref = root / ".a3-child.py"
        script_ref.write_text(_A3_CHILD_SCRIPT, encoding="utf-8")
        environment = dict(os.environ)
        environment["DSH_ACCEPTANCE_CWD"] = str(
            Path(__file__).resolve().parents[1]
        )
        environment["A3_SCENARIO"] = str(root / ".a3-scenario.json")
        child = subprocess.run(
            [sys.executable, str(script_ref)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=900,
            env=environment,
            cwd=Path(__file__).resolve().parents[1],
        )
        self.assertEqual(child.returncode, 9, child.stderr[-2000:])
        with _stores(root):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            bindings = authority._scan_final_eval_bindings()
            self.assertEqual(len(bindings), 1)
            return bindings[0].ticket_id


def _serialize_grant(grant) -> dict[str, object]:
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


def _v1_request_aligned(request: FinalEvalRequestV2):
    """The V1 adapter request aligned to ONE V2 request (independent
    construction from production bindings; never a tests fixture)."""
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

    return FinalEvalRequest(
        campaign=CampaignBinding(
            campaign_id=request.campaign_id,
            campaign_sha256=request.campaign_sha256,
        ),
        candidate_set=_candidate_set(),
        candidate_set_sha256=_candidate_set_sha256(_candidate_set()),
        code=CodeBinding(code_sha256=request.code_sha256),
        execution_spec=ExecutionSpecBinding(
            execution_spec=_execution_spec(),
            execution_spec_sha256=request.execution_spec_sha256,
        ),
        features=FeatureBinding(features_sha256=request.features_sha256),
        model=ModelBinding(
            model_id=request.model,
            model_sha256=request.model_sha256,
        ),
        threshold=ThresholdBinding(threshold_sha256=request.threshold_sha256),
        roster=RosterBinding(
            roster=_roster(),
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
        actor=Actor(request.actor_id, request.actor_type, request.invocation_id),
        identity_binding=IdentityBinding(
            plan_hash=request.authority_plan_hash,
            scope_hash=request.identity_scope_hash,
            policy_hash=request.identity_instruction_policy_hash,
        ),
    )


def _maintenance_lease_probe(authority, identity):
    """A P0 READ maintenance lease for the crash harness reconciler."""
    import secrets as _secrets

    unique = _secrets.token_hex(8)
    maintenance_actor = Actor(
        "p0-acceptance", "automation", "p0-acceptance-maint-" + unique
    )
    envelope = authority._provision_authorization(
        phase=stores_module.Phase.P0,
        attempt_id="p0-acceptance-maint-" + unique,
        actor=maintenance_actor,
        identity=identity,
        expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
        allowed_side_effects=(stores_module.SideEffect.READ,),
    )
    grant = authority.claim_authorization(
        envelope,
        expected_phase=stores_module.Phase.P0,
        expected_attempt_id="p0-acceptance-maint-" + unique,
        actor=maintenance_actor,
        identity=identity,
    )
    ticket = authority._issue_task_ticket(
        grant,
        {
            "task_id": "P0-ACCEPTANCE-RECONCILER-MAINT",
            "objective": "acceptance crash harness maintenance",
            "dependencies": [],
            "idempotency_key": "p0-acceptance-maint-" + unique,
            "task_spec_ref": "manifest.json",
            "task_spec_sha256": "1" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": [],
            "forbidden_files": [],
            "baseline_ref": "manifest.json",
            "baseline_sha256": "1" * 64,
            "input_evidence_refs": [],
        },
        allowed_side_effects=(stores_module.SideEffect.READ,),
    )
    return authority._begin_task(ticket)


_A3_CHILD_SCRIPT = r"""
import json, os, sys
sys.path.insert(0, os.environ["DSH_ACCEPTANCE_CWD"])
from pathlib import Path
from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import Actor
from research_automation.control_plane.final_eval_authority import (
    AuthorityFinalEvalBroker,
    FinalEvalRequestV2,
)

ROOT_SECRET = "acceptance-root-capability-0123456789abcdef"
NONCE = "0123456789abcdef" * 4
HOLDOUT_REF = "frozen/holdout.parquet"

with open(os.environ.get("A3_SCENARIO", "x"), encoding="utf-8") as handle:
    scenario = json.load(handle)
root = Path(scenario["root"])
boundary = scenario["boundary"]
with stores_module.store_path_override(
    authority=root / "authority.sqlite3",
    operational=root / "operational.sqlite3",
):
    stores_module._expected_schema_sha256.cache_clear()
    grant = stores_module.AuthorityGrant(
        grant_id=scenario["grant"]["grant_id"],
        authorization_ref=scenario["grant"]["authorization_ref"],
        phase=stores_module.Phase(scenario["grant"]["phase"]),
        attempt_id=scenario["grant"]["attempt_id"],
        actor=stores_module.Actor(
            scenario["grant"]["actor"]["actor_id"],
            scenario["grant"]["actor"]["actor_type"],
            scenario["grant"]["actor"]["invocation_id"],
        ),
        identity=stores_module.AuthorityIdentity(
            scenario["grant"]["identity"]["plan_hash"],
            scenario["grant"]["identity"]["scope_hash"],
            scenario["grant"]["identity"]["instruction_policy_hash"],
        ),
        allowed_side_effects=tuple(
            stores_module.SideEffect(name)
            for name in scenario["grant"]["allowed_side_effects"]
        ),
        _bearer_secret=stores_module._BearerSecret(
            scenario["grant"]["bearer_secret"]
        ),
    )
    request = FinalEvalRequestV2(**scenario["request"])
    identity = stores_module.AuthorityIdentity(
        request.authority_plan_hash,
        request.identity_scope_hash,
        request.identity_instruction_policy_hash,
    )
    authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
    broker = AuthorityFinalEvalBroker(
        authority=authority,
        grant=grant,
        attempt_id=request.attempt_id,
        identity=identity,
    )
    binding = broker.bind(
        request=request,
        nonce=NONCE,
        actor=Actor(request.actor_id, request.actor_type, request.invocation_id),
        idempotency_key="p8-acceptance-a3",
        task_spec_ref="manifest.json",
        task_spec_sha256="1" * 64,
    )
    if boundary == "consume":
        os._exit(9)
    from research_automation.control_plane.final_eval_holdout_store import (
        SqliteHoldoutStore,
    )
    from research_automation.control_plane.final_eval_request_projection import (
        adapt_evaluator_request_v1_test_only,
    )
    from tests.test_control_plane_cr010_p8_acceptance import (
        _material_bundle,
        _v1_request_aligned,
    )

    consumption = SqliteHoldoutStore(authority=authority).read_consumption(
        binding.ticket_id
    )
    projection = adapt_evaluator_request_v1_test_only(
        _v1_request_aligned(request),
        request,
        root_secret=ROOT_SECRET,
        attempt_id=request.attempt_id,
        identity=identity,
    )
    from research_automation.control_plane.final_evaluator import (
        AuthorityBroker,
        HoldoutDataBackend,
        TrustedEvaluator,
        TrustedEvaluatorAdapter,
        seal_trusted_data_root,
    )
    from research_automation.control_plane.final_eval_holdout_store import (
        SqliteHoldoutStore,
    )
    from research_automation.control_plane.final_eval_composition import (
        compose_holdout_store,
        compose_staging_backend,
    )
    from research_automation.control_plane.final_eval_evidence import (
        FinalEvalResultPublisher,
    )
    from research_automation.control_plane.final_eval_orchestrator import (
        OrchestrationInputs,
        orchestrate,
    )
    from research_automation.control_plane.final_eval_reconciler import (
        reconcile,
    )
    from research_automation.control_plane.final_eval_runtime import (
        FinalEvalRootCapability,
        FinalEvalRuntime,
        FinalEvalRuntimeInputs,
    )

    data_root = seal_trusted_data_root(root / "data-root", (HOLDOUT_REF,))
    opened = [0]

    class _CountingBackend(HoldoutDataBackend):
        def read_holdout_summary(self, *, path, holdout_id, holdout_sha256):
            opened[0] += 1
            return compose_staging_backend().read_holdout_summary(
                path=path, holdout_id=holdout_id, holdout_sha256=holdout_sha256
            )

    evaluator = TrustedEvaluator(
        broker=AuthorityBroker(
            store=SqliteHoldoutStore(authority=authority)
        ),
        adapter=TrustedEvaluatorAdapter(backend=_CountingBackend()),
    )
    evaluated = evaluator.evaluate_v2(
        projection,
        data_root=data_root,
        worker_launcher=lambda: 0,
        consumption=consumption,
        durable_ticket_id=binding.ticket_id,
        durable_request_sha256=binding.request_sha256,
        durable_nonce_fingerprint=binding.nonce_fingerprint,
    )
    if boundary in ("open", "worker"):
        os._exit(9)

    def evidence_crash_hook(state):
        if boundary == "object" and state == "CRASH_AFTER.OBJECT_WRITTEN":
            os._exit(9)
        if boundary == "claim" and state == "CRASH_AFTER.CLAIM_WRITTEN":
            os._exit(9)

    publisher = FinalEvalResultPublisher(
        repository_root=root,
        evidence_volume="research_state/control_plane/p8/evidence",
        crash_hook=evidence_crash_hook,
    )
    staged = orchestrate(
        OrchestrationInputs(
            authority=authority,
            binding_id=binding.ticket_id,
            expected_version=binding.saga_version,
            worker_launcher=lambda: 0,
            evidence_sink=lambda document: publisher.publish(
                str(document.get("binding_id", "")),
                str(document.get("ticket_id", "")),
                dict(document),
                outcome=str(document.get("outcome", "SUCCEEDED")),
            ).to_payload(),
            repository_root=root,
        )
    )
    if boundary == "result_staged":
        os._exit(9)
    from tests.test_control_plane_cr010_p8_acceptance import _maintenance_lease_probe

    maintenance_lease = _maintenance_lease_probe(authority, identity)
    reconcile(
        authority,
        maintenance_lease,
        evidence_ref_for={
            binding.ticket_id: staged.result_claim_ref or ""
        },
        repository_root=root,
        crash_hook=(
            (lambda state: os._exit(9))
            if boundary in ("closed", "terminal")
            else None
        ),
    )
    os._exit(9)
"""
