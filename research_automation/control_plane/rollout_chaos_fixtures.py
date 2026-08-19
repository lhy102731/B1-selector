"""Production-owned deterministic offline chaos fixtures (C0R2 T1).

Provides the deterministic clock, PID/process identity, protocol/member,
synthetic Authority-bound evidence and store bootstrap used by the C0
rollout chaos simulation — without importing ``tests.*`` or relying on
``unittest.mock``.  The fake provider is the P6 production-owned
``CampaignOfflineProvider`` (never a second provider copy).
"""

from __future__ import annotations

import hashlib
import json
import random
import secrets
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from .campaign_offline_provider import CampaignOfflineProvider
from .campaign import (
    InvalidModelResponseError,
    ProviderResponse,
)
from .campaign_controller import (
    CampaignBudgetLimits,
    CycleReservationLimits,
    OperationalModelCallLimits,
)
from . import stores as stores_module
from .campaign_store import OperationalCampaignJournal
from .stores import Phase
from .contracts import canonical_json
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from tempfile import TemporaryDirectory

from research_automation.control_plane.campaign_controller import (
    operational_prompt_sha256,
)
from research_automation.control_plane.campaign_store import (
    campaign_scope_sha256,
)
from research_automation.control_plane.contracts import (
    Actor,
    SideEffect,
    canonical_json,
)
from research_automation.control_plane.contracts import canonical_sha256
from research_automation.control_plane.contracts import canonical_json as _canonical_json
def canonical_bytes(value):
    return _canonical_json(value).encode("utf-8")

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

from .memory import ClaimScope


@dataclass(frozen=True, slots=True)
class OfflineChaosIdentity:
    """Deterministic fake clock/PID/process identity for one run."""

    seed: int
    pid: int
    process_started_at_ns: int
    host_id: str = "offline-host"

    def process_identity(self):
        from .campaign_lease import ProcessIdentity

        return ProcessIdentity(
            host_id=self.host_id,
            pid=self.pid,
            process_started_at_ns=self.process_started_at_ns,
        )


class SequentialMonotonicClock:
    """Deterministic monotonic clock yielding seeded ns values."""

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)
        self._value = 0

    def __call__(self) -> int:
        self._value += 1 + self._rng.randrange(0, 1000)
        return self._value


class FakeProcessIdentityProvider:
    """Deterministic fake ProcessIdentityProvider for offline runs.

    Duck-typed: accepts either an ``OfflineChaosIdentity`` (which carries a
    ``process_identity()`` factory), a raw ``ProcessIdentity`` object, or
    the campaign-lease call style ``current=..., process_starts=...``.
    """

    def __init__(
        self,
        identity: object = None,
        *,
        current: object = None,
        process_starts: dict[tuple[str, int], int | None] | None = None,
        probe=None,
    ) -> None:
        self._identity = identity if identity is not None else current
        self.current_calls = 0
        self.probe_calls: list[tuple[str, int]] = []
        self._process_starts = dict(process_starts or {})
        self._probe = probe
        if self._identity is not None:
            host_id = getattr(self._identity, "host_id", None)
            pid = getattr(self._identity, "pid", None)
            started = getattr(self._identity, "process_started_at_ns", None)
            if host_id is not None and pid is not None:
                self._process_starts.setdefault(
                    (host_id, pid), started
                )

    def set_current(self, current: object) -> None:
        self._identity = current
        host_id = getattr(current, "host_id", None)
        pid = getattr(current, "pid", None)
        started = getattr(current, "process_started_at_ns", None)
        if host_id is not None and pid is not None:
            self._process_starts[(host_id, pid)] = started

    def current(self):
        self.current_calls += 1
        factory = getattr(self._identity, "process_identity", None)
        if callable(factory):
            return factory()
        return self._identity

    def probe(self, host_id: str, pid: int) -> int | None:
        self.probe_calls.append((host_id, pid))
        if self._probe is not None:
            return self._probe(host_id, pid)
        if self._identity is not None:
            if (host_id, pid) == (
                getattr(self._identity, "host_id", None),
                getattr(self._identity, "pid", None),
            ):
                return getattr(
                    self._identity, "process_started_at_ns", None
                )
        return self._process_starts.get((host_id, pid))


def deterministic_scope(*, generation: str = "c0-generation-1") -> dict[str, object]:
    """Canonical claim scope for one offline run."""
    return {
        "mechanisms": ["volume-contraction-rebound"],
        "usage_modes": ["factor-candidate"],
        "market_regimes": ["all"],
        "time_windows": [{"start": "2020-01-01", "end": "2026-12-31"}],
        "universes": ["a-share"],
        "liquidity_buckets": ["production-minimum"],
        "label_protocol_families": ["rolling-forward-v1"],
        "generation_families": [generation],
    }


def deterministic_protocol():
    """Build the canonical offline protocol via the repository builder.

    Delegates to the campaign fixture path so the protocol structure is
    production-owned and identical to real campaign runs.
    """
    from research_automation.foundations.protocols import (
        ProtocolDefinition,
    )
    from research_automation.foundations.protocols import RosterMember

    member = RosterMember(
        role="factor_engineer",
        provider_profile_id="offline-local",
        model_id="deterministic-reviewer",
    )
    return ProtocolDefinition(
        version="1.0.0",
        name="c0-offline-protocol",
        roster=(member,),
        execution_flow=("generate", "verify", "record"),
        evidence_schema="control_plane.evidence.v1",
    )


def deterministic_member(*, prompt_sha256: str):
    """Build a canonical RosterMember for the offline run."""
    from .campaign_roster import RosterMember

    return RosterMember(
        member_id="member-001",
        provider="fake-provider",
        profile="offline-local",
        model="deterministic-reviewer",
        role="factor_engineer",
        prompt_sha256=prompt_sha256,
        config_sha256="2" * 64,
        capability_sha256="3" * 64,
    )


def claim_campaign_grant(
    *,
    campaign_id: str,
    namespace: str,
    attempt_id: str,
    plan_sha256: str,
    instruction_sha256: str,
):
    """Provision + claim a campaign grant in the fixture store."""
    from .campaign_store import campaign_scope_sha256
    from .stores import Actor, AuthorityIdentity, Phase, SideEffect

    actor = Actor("p6-runner", "automation", f"{campaign_id}-fixture")
    identity = AuthorityIdentity(
        plan_sha256,
        campaign_scope_sha256(
            namespace=namespace,
            campaign_id=campaign_id,
        ),
        instruction_sha256,
    )
    authority = stores_module._AuthorityStore(root_secret="0" * 64)
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    authorization = authority._provision_authorization(
        phase=Phase.P6,
        attempt_id=attempt_id,
        actor=actor,
        identity=identity,
        expires_at=now + timedelta(days=1),
        allowed_side_effects=(
            SideEffect.READ,
            SideEffect.WRITE_CONTROL_PLANE,
        ),
    )
    return authority.claim_authorization(
        authorization,
        expected_phase=Phase.P6,
        expected_attempt_id=attempt_id,
        actor=actor,
        identity=identity,
    )


def bootstrap_fixture_stores(root: Path, *, root_secret: str = "0" * 64):
    """Bootstrap authority+operational fixture stores under ``root``."""
    stores_module._expected_schema_sha256.cache_clear()
    stores_module._trusted_bootstrap(root_secret=root_secret)


__all__ = [
    "CampaignOfflineProvider",
    "FakeProcessIdentityProvider",
    "OfflineChaosIdentity",
    "SequentialMonotonicClock",
    "bootstrap_fixture_stores",
    "claim_campaign_grant",
    "deterministic_member",
    "deterministic_protocol",
    "deterministic_scope",
]


# ---------------------------------------------------------------------------
# CR010-R05a: production-owned offline fixtures ported from tests.* -- the
# production path must never import tests.* private fixtures.
# ---------------------------------------------------------------------------

FIXTURE_ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"
FIXTURE_NOW = datetime(2026, 8, 11, 1, 0, tzinfo=timezone.utc)

def fixture_artifact(
    content: bytes,
    *,
    content_schema: str,
    generation: str,
    kind: str,
    logical_role: str,
    producer: str = "unit-test",
) -> ArtifactIdentity:
    return artifact_identity_from_bytes(
        content,
        content_schema=content_schema,
        producer=producer,
        generation=generation,
        kind=kind,
        logical_role=logical_role,
    )

def fixture_protocol() -> ProtocolDefinition:
    generation_manifest = fixture_artifact(
        b"generation-1-manifest",
        content_schema="research.generation_manifest.v1",
        generation="generation-1",
        kind="manifest",
        logical_role="generation-manifest",
    )
    market_data = fixture_artifact(
        b"market-data-generation-1",
        content_schema="research.market_data.v1",
        generation="generation-1",
        kind="dataset",
        logical_role="dataset-bars",
    )
    hyperparameters = fixture_artifact(
        b"hyperparameters-v1",
        content_schema="research.hyperparameters.v1",
        generation="generation-1",
        kind="model-config",
        logical_role="model-hyperparameters",
    )
    code = fixture_artifact(
        b"runner-source-v1",
        content_schema="research.source_snapshot.v1",
        generation="git-abc123",
        kind="source",
        logical_role="runner-source",
    )
    inputs = tuple(
        sorted(
            (generation_manifest, market_data, hyperparameters),
            key=lambda item: item.artifact_id,
        )
    )
    code_artifacts = (code,)
    datasets = tuple(
        sorted(
            (
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="test-1",
                    role="FOLD_TEST",
                    artifact_id=market_data.artifact_id,
                    window_start="2024-01-01",
                    window_end="2024-12-31",
                ),
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="test-2",
                    role="FOLD_TEST",
                    artifact_id=market_data.artifact_id,
                    window_start="2025-01-01",
                    window_end="2025-12-31",
                ),
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="train-1",
                    role="TRAIN",
                    artifact_id=market_data.artifact_id,
                    window_start="2020-01-01",
                    window_end="2022-12-31",
                ),
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="train-2",
                    role="TRAIN",
                    artifact_id=market_data.artifact_id,
                    window_start="2021-01-01",
                    window_end="2023-12-31",
                ),
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="validation-1",
                    role="VALIDATION",
                    artifact_id=market_data.artifact_id,
                    window_start="2023-01-01",
                    window_end="2023-12-31",
                ),
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="validation-2",
                    role="VALIDATION",
                    artifact_id=market_data.artifact_id,
                    window_start="2024-01-01",
                    window_end="2024-12-31",
                ),
            ),
            key=lambda item: item.dataset_id,
        )
    )
    return ProtocolDefinition(
        schema_version="research.protocol_definition.v1",
        protocol_id="brick-forward-v1",
        metadata=ProtocolMetadata(
            schema_version="research.protocol_metadata.v1",
            display_name="Brick forward validation",
            notes="fixture",
        ),
        generation_id="generation-1",
        generation_manifest_artifact_id=generation_manifest.artifact_id,
        universe_id="a-share-point-in-time-v1",
        calendar_id="sse-szse-trading-v1",
        adjustment_scheme_id="hfq-v1",
        validation_design="ROLLING_FORWARD",
        fold_window_policy_id="train3y-validate1y-test1y-v1",
        label=LabelDefinition(
            schema_version="research.label_definition.v1",
            label_id="return-after-entry-v1",
            entry_rule_id="signal-close-next-open",
            exit_rule_id="fixed-horizon-or-stop",
            horizon_trading_days=5,
        ),
        datasets=datasets,
        folds=(
            FoldSpec(
                schema_version="research.fold_spec.v1",
                fold_id="fold-1",
                train_dataset_id="train-1",
                validation_dataset_id="validation-1",
                test_dataset_id="test-1",
                purge_trading_days=5,
                embargo_trading_days=2,
            ),
            FoldSpec(
                schema_version="research.fold_spec.v1",
                fold_id="fold-2",
                train_dataset_id="train-2",
                validation_dataset_id="validation-2",
                test_dataset_id="test-2",
                purge_trading_days=5,
                embargo_trading_days=2,
            ),
        ),
        feature_boundary=FeatureBoundary(
            schema_version="research.feature_boundary.v1",
            boundary_id="brick-v2-0925-v1",
            feature_fields=(
                FeatureField(
                    schema_version="research.feature_field.v1",
                    name="entry_open_to_ma5_pct",
                    availability="ENTRY_DATE_OPEN",
                    reference_fields=("signal_day_ma5",),
                ),
                FeatureField(
                    schema_version="research.feature_field.v1",
                    name="entry_open_to_yellow_pct",
                    availability="ENTRY_DATE_OPEN",
                    reference_fields=("signal_day_yellow",),
                ),
                FeatureField(
                    schema_version="research.feature_field.v1",
                    name="overnight_gap_pct",
                    availability="ENTRY_DATE_OPEN",
                    reference_fields=("signal_day_close",),
                ),
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
        code_artifacts=code_artifacts,
        input_artifacts=inputs,
        runner=RunnerSpec(
            schema_version="research.runner_spec.v1",
            runner_id="brick-v2-research",
            entrypoint="research.brick.runner:main",
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
                    fold_id="fold-1",
                    training_dataset_id="train-1",
                    threshold_source="VALIDATION_SELECTED",
                    threshold_dataset_ids=("validation-1",),
                    threshold_value=0.5,
                ),
                FoldSelection(
                    schema_version="research.fold_selection.v1",
                    fold_id="fold-2",
                    training_dataset_id="train-2",
                    threshold_source="VALIDATION_SELECTED",
                    threshold_dataset_ids=("validation-2",),
                    threshold_value=0.5,
                ),
            ),
            promotion_gate_id="strict-forward-v1",
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

def fixture_approval(protocol: ProtocolDefinition) -> ProtocolApproval:
    approver = Actor(
        actor_id="independent-reviewer",
        actor_type="human",
        invocation_id="review-1",
    )
    statement = ProtocolApprovalStatement(
        schema_version="research.protocol_approval_statement.v1",
        approved_protocol_sha256=protocol_sha256(protocol),
        decision="APPROVED",
        approver=approver,
    )
    evidence = fixture_artifact(
        canonical_json(statement.model_dump(mode="json")).encode("utf-8"),
        content_schema="research.protocol_approval_statement.v1",
        generation="approval-1",
        kind="review",
        logical_role="protocol-approval",
        producer=approver.actor_id,
    )
    return ProtocolApproval(
        schema_version="research.protocol_approval.v1",
        statement=statement,
        approval_evidence=evidence,
        evidence_trust="UNVERIFIED_EXTERNAL_STATEMENT",
    )

def fixture_member() -> RosterMember:
    from research_automation.control_plane.campaign_roster import (
        RosterMember as _CampaignRosterMember,
    )

    return _CampaignRosterMember(
        member_id="factor-engineer",
        provider="fake-provider",
        profile="offline-local",
        model="deterministic-reviewer",
        role="factor_engineer",
        prompt_sha256="1" * 64,
        config_sha256="2" * 64,
        capability_sha256="3" * 64,
    )

def fixture_execution_spec_and_member(prompt: object):
    protocol = fixture_protocol()
    execution_spec = compile_execution_spec(
        protocol,
        approved_protocol=protocol,
        approval=fixture_approval(protocol),
        amendment=None,
    )
    member = replace(
        fixture_member(),
        prompt_sha256=operational_prompt_sha256(prompt),
    )
    return execution_spec, member

def fixture_claim_campaign_grant(
    *,
    campaign_id: str,
    namespace: str,
    actor_id: str,
    invocation_id: str,
    attempt_id: str,
    plan_sha256: str,
    instruction_sha256: str,
) -> stores_module.AuthorityGrant:
    actor = Actor(actor_id, "automation", invocation_id)
    identity = stores_module.AuthorityIdentity(
        plan_sha256,
        campaign_scope_sha256(
            namespace=namespace,
            campaign_id=campaign_id,
        ),
        instruction_sha256,
    )
    authority = stores_module._AuthorityStore(
        root_secret=FIXTURE_ROOT_SECRET,
        clock=lambda: FIXTURE_NOW,
    )
    authorization = authority._provision_authorization(
        phase=Phase.P6,
        attempt_id=attempt_id,
        actor=actor,
        identity=identity,
        expires_at=FIXTURE_NOW.replace(year=2027),
        allowed_side_effects=(
            SideEffect.READ,
            SideEffect.WRITE_CONTROL_PLANE,
        ),
    )
    return authority.claim_authorization(
        authorization,
        expected_phase=Phase.P6,
        expected_attempt_id=attempt_id,
        actor=actor,
        identity=identity,
    )


@contextmanager
def _authorized_campaign_context(
    root: Path,
    campaign_id: str,
    namespace: str,
):
    with stores_module.store_path_override(
        authority=root / "authority.sqlite3",
        operational=root / "operational.sqlite3",
    ):
        stores_module._expected_schema_sha256.cache_clear()
        stores_module._trusted_bootstrap(root_secret=FIXTURE_ROOT_SECRET)
        grant = fixture_claim_campaign_grant(
            campaign_id=campaign_id,
            namespace=namespace,
            actor_id="p6-runner",
            invocation_id=f"{campaign_id}-test",
            attempt_id=f"{campaign_id}-attempt",
            plan_sha256="a" * 64,
            instruction_sha256="c" * 64,
        )
        try:
            yield root, grant, OperationalCampaignJournal(
                root_secret=FIXTURE_ROOT_SECRET,
                grant=grant,
                namespace=namespace,
                campaign_id=campaign_id,
                clock=lambda: FIXTURE_NOW,
            )
        finally:
            stores_module._expected_schema_sha256.cache_clear()


@contextmanager

def fixture_authorized_campaign(
    campaign_id: str,
    *,
    namespace: str = "formal",
    root: Path | None = None,
):
    """Fixture campaign context; ``root`` redirects the disposable fixture
    root (CR-010 C0: negative-scenario roots are routed into their own
    disposable roots, never system temp)."""
    if root is not None:
        with _authorized_campaign_context(root, campaign_id, namespace) as ctx:
            yield ctx
        return
    with TemporaryDirectory() as temporary:
        with _authorized_campaign_context(
            Path(temporary), campaign_id, namespace
        ) as ctx:
            yield ctx


class FixtureAuthorityReader:
    """Production-owned fixture Authority reader (CR-010 F-10).

    Serves the pre-computed per-cycle bindings the C0 campaign commits
    against -- WITHOUT ``unittest.mock``.  The controller and the Learning
    ledger resolve task reports through this reader instead of a patched
    class method; an unknown ticket fails closed exactly like the real
    AuthorityReader.
    """

    __slots__ = ("_bindings",)

    def __init__(self, bindings_by_ticket: dict[str, object]) -> None:
        self._bindings = bindings_by_ticket

    def verify_task_report_binding(self, report: object) -> object:
        if not isinstance(report, dict):
            raise TypeError("task report must be a mapping")
        ticket_id = str(report.get("ticket_id", ""))
        binding = self._bindings.get(ticket_id)
        if binding is None:
            raise RuntimeError(f"no fixture binding for ticket {ticket_id}")
        return binding

def fixture_write_json(root, ref, payload):
    raw = canonical_bytes(payload)
    path = root / ref
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return {
        "evidence_ref": ref,
        "evidence_sha256": hashlib.sha256(raw).hexdigest(),
        "status": "VERIFIED",
    }

def fixture_authority_fixture(root, *, claim=None, protocol=None):
    from research_automation.control_plane.contracts import SideEffect
    from research_automation.control_plane.evidence_learning import EvidenceAdapter
    from research_automation.control_plane import evidence_learning as module
    from research_automation.control_plane.task_reports import build_task_report_v2

    if claim is None:
        claim = {
            "kind": "NEGATIVE",
            "scope": canonical_bytes(
                {
                    "mechanisms": ["volume-contraction-rebound"],
                    "usage_modes": ["factor-candidate"],
                    "market_regimes": ["all"],
                    "time_windows": [
                        {"start": "2020-01-01", "end": "2026-12-31"}
                    ],
                    "universes": ["a-share"],
                    "liquidity_buckets": ["production-minimum"],
                    "label_protocol_families": ["rolling-forward-v1"],
                    "generation_families": ["generation-1"],
                }
            ).decode("utf-8"),
        }
    protocol = (
        {"label": "signal-day", "embargo_days": 5}
        if protocol is None
        else protocol
    )
    artifact = {
        "schema_version": "runner.artifact.v1",
        "runner": "fixture-runner",
        "runner_version": "1.0.0",
        "status": "COMPLETED",
        "claim": claim,
        "protocol_conformance": "CONFORMING",
        "executed_protocol": protocol,
        "artifact_refs": [
            {"ref": "fixtures/result.json", "sha256": "e" * 64}
        ],
        "access_event_ids": ["event:fixture-001"],
        "taint_refs": [],
    }
    source_ref = "research_automation/control_plane/evidence_learning.py"
    source_raw = Path(module.__file__).read_bytes()
    source_path = root / source_ref
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(source_raw)
    adapter = {
        "schema_version": "control_plane.runner_adapter.v1",
        "adapter_id": "EvidenceAdapter.v1",
        "source_ref": source_ref,
        "source_sha256": hashlib.sha256(source_raw).hexdigest(),
        "runners": {"fixture-runner": "1.0.0"},
    }
    base = "research_state/control_plane/p4/fixtures"
    refs = {
        "approved-claim": fixture_write_json(root, f"{base}/claim.json", claim),
        "approved-protocol": fixture_write_json(
            root, f"{base}/protocol.json", protocol
        ),
        "runner-adapter": fixture_write_json(
            root, f"{base}/adapter.json", adapter
        ),
        "runner-artifact": fixture_write_json(
            root, f"{base}/artifact.json", artifact
        ),
    }
    evidence = EvidenceAdapter(
        known_runners=adapter["runners"],
        approved_protocol=protocol,
        approved_claim=claim,
    ).evaluate(artifact)
    decision = {
        "schema_version": "control_plane.evidence_decision.v1",
        "bindings": {
            name: refs[name]["evidence_sha256"]
            for name in sorted(refs)
        },
        "claim": claim,
        "evidence": {
            "verdict": evidence.verdict,
            "protocol_conformance": evidence.protocol_conformance,
            "audit_grade": evidence.audit_grade,
            "scientific_outcome": evidence.scientific_outcome,
            "promotion_eligible": evidence.promotion_eligible,
            "evidence_refs": list(evidence.evidence_refs),
            "access_event_ids": list(evidence.access_event_ids),
            "taint_refs": list(evidence.taint_refs),
            "invalidation_codes": list(evidence.invalidation_codes),
        },
    }
    refs["learning-decision"] = fixture_write_json(
        root, f"{base}/decision.json", decision
    )
    baseline_ref = "research_state/control_plane/p3/baseline.json"
    baseline_raw = canonical_bytes(
        {
            "phase": "P3",
            "repository_root": str(root.resolve()),
            "status": "PASS",
        }
    )
    baseline_path = root / baseline_ref
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_bytes(baseline_raw)
    input_refs = []
    for evidence_id in sorted(refs):
        input_refs.append({"evidence_id": evidence_id, **refs[evidence_id]})
    report = build_task_report_v2({
        "plan_version": "V3.4.2-P0R2",
        "phase": "P4",
        "task_id": "P4-LEARNING-COMMIT",
        "attempt_id": "p4-fixture",
        "authorization_ref": "auth-learning-001",
        "ticket_id": "ticket-learning-001",
        "identity_binding": {
            "plan_hash": "a" * 64,
            "scope_hash": "b" * 64,
            "instruction_policy_hash": "c" * 64,
        },
        "objective": "Project one independently reviewed Learning decision.",
        "dependencies": [],
        "idempotency_key": "p4-learning-commit-001",
        "task_spec_ref": "research_state/control_plane/p4/task_specs/learning.json",
        "task_spec_sha256": "d" * 64,
        "requirements": {
            "required_test_receipt_ids": [],
            "required_review_receipt_ids": [],
            "required_evidence_ids": sorted(refs),
        },
        "ticket_state": "SUCCEEDED",
        "allowed_files": [
            "research_state/control_plane/learning_commit.sqlite3",
            "research_state/control_plane/learning_packets/",
        ],
        "forbidden_files": ["data/", "knowledge/"],
        "baseline_ref": baseline_ref,
        "baseline_sha256": hashlib.sha256(baseline_raw).hexdigest(),
        "input_evidence_refs": input_refs,
        "test_receipts": [],
        "review_receipts": [],
        "review_findings": [],
        "changed_files": [],
        "external_invocations": [],
        "side_effect_summary": {"observed": [], "unauthorized": []},
        "started_at": "2026-07-30T08:00:00Z",
        "completed_at": "2026-07-30T08:01:00Z",
    })
    binding = SimpleNamespace(
        ticket_id="ticket-learning-001",
        report_payload_sha256=report["report_payload_sha256"],
        actor_id="independent-evidence-reviewer",
        allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
        ticket_state="SUCCEEDED",
        terminal_evidence_ref=refs["learning-decision"]["evidence_ref"],
    )
    return report, binding, artifact, evidence, refs


# ---------------------------------------------------------------------------
# CR-010 F-07: C0 shared step-execution components (moved from rollout_chaos
# so the one-step WORKER subprocess can execute REAL controller transitions
# without importing the supervisor module).
# ---------------------------------------------------------------------------

C0_CALL_LIMITS = OperationalModelCallLimits(
    currency="USD",
    max_input_tokens=20,
    max_output_tokens=10,
    max_cost="0.1",
    max_wall_time_ms=5_000,
    max_attempts=2,
)

C0_RESERVATION_LIMITS = CycleReservationLimits(
    currency="USD",
    max_input_tokens=20,
    max_output_tokens=10,
    max_cost="0.1",
    max_wall_time_ms=5_000,
    max_tool_attempts=2,
)


def campaign_limits(cycles: int) -> CampaignBudgetLimits:
    return CampaignBudgetLimits(
        max_cycles=cycles,
        currency="USD",
        max_input_tokens=cycles * 20 + 100,
        max_output_tokens=cycles * 10 + 100,
        max_cost=str(cycles + 2),
        max_wall_time_ms=cycles * 5_000 + 60_000,
        max_tool_attempts=cycles * 2 + 8,
    )


class FixtureSequentialClock:
    """Deterministic increasing monotonic clock (ns)."""

    def __init__(self, start_ns: int = 100, step_ns: int = 1_000_000) -> None:
        self._next = start_ns
        self._step = step_ns

    def __call__(self) -> int:
        value = self._next
        self._next += self._step
        return value


def c0_claim_for_cycle(cycle_number: int) -> dict[str, object]:
    # Byte-identical to the supervisor's per-cycle claim: the learning
    # packet content hash must not depend on which process generated it.
    return {
        "kind": "NEGATIVE",
        "summary": f"Synthetic scoped finding from C0 cycle {cycle_number}",
        "scope": json.dumps(
            deterministic_scope(generation="generation-1"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "parent_lineage": [],
        "reopen_predicate": "[]",
        "future_usage_guidance": '{"conclusion":"AVOID","directional_status":"avoid"}',
    }


class C0ChaosProvider:
    """Offline fake provider matching the roster member binding.

    The controller executes providers inside a spawned subprocess, so the
    call counter is persisted in a shared temp file; the timeout-once mode
    therefore survives subprocess pickling and retry re-spawns.
    """

    provider_name = "fake-provider"
    profile = "offline-local"
    model = "deterministic-reviewer"
    config_sha256 = "2" * 64
    capability_sha256 = "3" * 64

    def __init__(
        self,
        artifact: dict[str, object],
        *,
        timeout_first: bool = False,
        counter_path: str | None = None,
        crash_on_call: bool = False,
    ) -> None:
        self._artifact = dict(artifact)
        self._timeout_first = timeout_first
        self._crash_on_call = crash_on_call
        if counter_path is None:
            handle = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                delete=False,
                prefix="c0-provider-counter-",
            )
            handle.write("0")
            handle.close()
            counter_path = handle.name
        self._counter_path = str(counter_path)
        Path(self._counter_path).write_text("0", encoding="utf-8")

    @property
    def call_count(self) -> int:
        try:
            raw = Path(self._counter_path).read_text(encoding="utf-8").strip()
            return int(raw or "0")
        except FileNotFoundError:
            return 0

    def _next_call_number(self) -> int:
        counter_path = Path(self._counter_path)
        counter_path.parent.mkdir(parents=True, exist_ok=True)
        if not counter_path.exists():
            # first invocation may create the counter file when the parent
            # provider executor routes a caller-visible path (CR-010 A4)
            counter_path.write_text("0", encoding="utf-8")
        with counter_path.open("r+", encoding="utf-8") as stream:
            raw = stream.read().strip() or "0"
            value = int(raw) + 1
            stream.seek(0)
            stream.write(str(value))
            stream.truncate()
        return value

    def invoke(self, request: object) -> ProviderResponse:
        number = self._next_call_number()
        if self._timeout_first and number == 1:
            raise TimeoutError("synthetic provider timeout")
        # CR-010 F-10: a REAL provider-level crash (raised inside the
        # spawned provider worker, propagated as a provider error) -- no
        # unittest.mock replacement of the invocation layer.  Every
        # attempt crashes, so the invocation cannot silently succeed on a
        # retry.
        if self._crash_on_call:
            raise RuntimeError("synthetic mid-call crash")
        return ProviderResponse(
            output_text=json.dumps(
                self._artifact,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
            request_model=self.model,
            response_model=self.model,
            raw_usage={
                "input_tokens": 7,
                "output_tokens": 3,
                "total_tokens": 10,
                "reported_cost": "0.02",
                "currency": "USD",
            },
        )


@contextmanager
def deterministic_secrets(seed: int):
    """Deterministic secrets token source so cross-process replays are stable.

    The durable controller/journal/lease layers generate lease ids, nonces,
    and grant ids through the stdlib ``secrets`` module. For the offline C0
    simulation only, we substitute a seeded token generator so identical seeds
    produce byte-identical event payloads and digests across processes.

    CR-010 F-10: the substitution saves/restores the module functions
    directly -- no ``unittest.mock`` on the production path.
    """
    rng = random.Random((seed * 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF)

    def token_hex(nbytes=None):
        return rng.randbytes(int(nbytes or 16)).hex()

    def token_urlsafe(nbytes=None):
        raw = rng.randbytes(int(nbytes or 32))
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    original_hex = secrets.token_hex
    original_urlsafe = secrets.token_urlsafe
    secrets.token_hex = token_hex
    secrets.token_urlsafe = token_urlsafe
    try:
        yield
    finally:
        secrets.token_hex = original_hex
        secrets.token_urlsafe = original_urlsafe
