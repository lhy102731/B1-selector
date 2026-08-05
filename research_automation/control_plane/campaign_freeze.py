"""Atomic P6 Cycle input freezes for proposal, protocol, roster, and generation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import hashlib
import hmac
import json
import re

from research_automation.foundations.protocols import ExecutionSpec

from . import stores
from .campaign_context import (
    CycleContextError,
    CycleContextIntegrityError,
    OperationalCycleContextJournal,
    _campaign_proposal_sha256,
    campaign_target_scope_sha256,
    canonical_campaign_proposal,
)
from .campaign_lifecycle import (
    CampaignLifecycleError,
    CampaignStatus,
    CycleStatus,
    OperationalCampaignLifecycle,
    _CYCLE_AGGREGATE_TYPE,
    _CYCLE_FREEZE_POLICY_AGGREGATE_TYPE,
    _CYCLE_TRANSITIONED,
)
from .campaign_preflight import run_campaign_preflight
from .campaign_roster import (
    OperationalRosterJournal,
    RosterIntegrityError,
    RosterManifest,
)
from .campaign_store import (
    CampaignEvent,
    CampaignEventConflictError,
    CampaignJournalError,
    OperationalCampaignJournal,
    _event_domain_payload,
    _event_from_row,
    _identifier,
    _payload,
)
from .sqlite_uow import _SqliteUnitOfWork


_FREEZE_POLICY_CONFIGURED = "CYCLE_FREEZE_POLICY_CONFIGURED"
_CYCLE_FREEZE_AGGREGATE_TYPE = "CYCLE_INPUT_FREEZE"
_CYCLE_INPUTS_FROZEN = "CYCLE_INPUTS_FROZEN"
_CYCLE_FREEZE_SCHEMA_VERSION = "control_plane.campaign_cycle_freeze.v2"
_CYCLE_FREEZE_MANIFEST_DOMAIN = b"control_plane.campaign_cycle_freeze.v2"
_MAX_FROZEN_INPUT_BYTES = 64 * 1024
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_PRE_FREEZE_STATUSES = frozenset(
    {
        CycleStatus.CREATED,
        CycleStatus.BUDGET_RESERVED,
        CycleStatus.CONTEXT_READY,
    }
)
_POST_FREEZE_STATUS_VALUES = frozenset(
    status.value for status in CycleStatus if status not in _PRE_FREEZE_STATUSES
)


class CycleFreezeError(RuntimeError):
    """Base error for durable Cycle input freezes."""


class CycleFreezeConflictError(CycleFreezeError):
    """Raised when frozen inputs or lifecycle ordering conflict."""


class CycleFreezeIntegrityError(CycleFreezeError):
    """Raised when persisted freeze events fail strict replay."""


def _sha256(value: object, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _canonical_bytes(value: object, name: str) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as error:
        raise ValueError(f"{name} must be bounded canonical JSON") from error
    if len(encoded) > _MAX_FROZEN_INPUT_BYTES:
        raise ValueError(f"{name} exceeds the bounded freeze size")
    return encoded


def _content_sha256(domain: bytes, value: object, name: str) -> str:
    return hashlib.sha256(domain + b"\0" + _canonical_bytes(value, name)).hexdigest()


def _execution_spec_from_json(value: object, name: str) -> ExecutionSpec:
    if type(value) is not str:
        raise ValueError(f"{name} must be canonical JSON")
    try:
        parsed = json.loads(value)
        execution_spec = ExecutionSpec.model_validate_json(value, strict=True)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a valid ExecutionSpec") from error
    canonical = _canonical_bytes(
        execution_spec.model_dump(mode="json", warnings=False),
        name,
    ).decode("utf-8")
    if (
        not isinstance(parsed, dict)
        or _canonical_bytes(parsed, name).decode("utf-8") != value
        or canonical != value
    ):
        raise ValueError(f"{name} must be canonical JSON")
    return execution_spec


def _event_id(
    *,
    namespace: str,
    campaign_id: str,
    aggregate_type: str,
    aggregate_id: str,
    role: str,
) -> str:
    return hashlib.sha256(
        b"control_plane.campaign_cycle_freeze_event.v1\0"
        + "\0".join(
            (namespace, campaign_id, aggregate_type, aggregate_id, role)
        ).encode("ascii")
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class FrozenCycleInputs:
    schema_version: str
    cycle_id: str
    proposal_sha256: str
    execution_spec_id: str
    execution_spec_json: str
    executed_protocol_sha256: str
    generation_id: str
    generation_manifest_artifact_id: str
    roster_manifest_sha256: str
    context_manifest_sha256: str
    preflight_json: str
    preflight_sha256: str
    manifest_sha256: str
    event_id: str
    sequence: int

    def identity_payload(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "cycle_id": self.cycle_id,
            "proposal_sha256": self.proposal_sha256,
            "execution_spec_id": self.execution_spec_id,
            "execution_spec_json": self.execution_spec_json,
            "executed_protocol_sha256": self.executed_protocol_sha256,
            "generation_id": self.generation_id,
            "generation_manifest_artifact_id": (
                self.generation_manifest_artifact_id
            ),
            "roster_manifest_sha256": self.roster_manifest_sha256,
            "context_manifest_sha256": self.context_manifest_sha256,
            "preflight_json": self.preflight_json,
            "preflight_sha256": self.preflight_sha256,
            "manifest_sha256": self.manifest_sha256,
        }


class OperationalCycleFreezeJournal:
    """Freeze all execution identities before one Cycle becomes FROZEN."""

    __slots__ = ("_journal", "_lifecycle", "_roster", "_context")

    def __init__(
        self,
        *,
        journal: OperationalCampaignJournal,
        lifecycle: OperationalCampaignLifecycle,
        roster: OperationalRosterJournal,
        context: OperationalCycleContextJournal,
    ) -> None:
        if not isinstance(journal, OperationalCampaignJournal) or not isinstance(
            lifecycle,
            OperationalCampaignLifecycle,
        ):
            raise TypeError("journal and lifecycle must be operational P6 objects")
        if not isinstance(roster, OperationalRosterJournal):
            raise TypeError("roster must be an OperationalRosterJournal")
        if not isinstance(context, OperationalCycleContextJournal):
            raise TypeError("context must be an OperationalCycleContextJournal")
        journal._authorize()
        if (
            lifecycle._journal is not journal
            or roster._journal is not journal
            or context._journal is not journal
        ):
            raise ValueError("freeze components must use the same Campaign journal")
        if roster._lifecycle is not lifecycle or context._lifecycle is not lifecycle:
            raise ValueError("freeze components must use the same lifecycle")
        self._journal = journal
        self._lifecycle = lifecycle
        self._roster = roster
        self._context = context

        def configure(connection) -> None:
            events = self._policy_events(connection)
            if events:
                policy = self._replay_policy(events)
                self._require_policy_precedes_freeze_history(
                    connection,
                    policy.sequence,
                )
                return
            for opened in self._lifecycle._opened_cycles(connection):
                cycle = self._lifecycle._replay_cycle(
                    self._lifecycle._cycle_events(connection, opened.cycle_id)
                )
                if cycle.status not in _PRE_FREEZE_STATUSES:
                    raise CycleFreezeConflictError(
                        "freeze policy cannot adopt post-freeze Cycle history"
                    )
            event = self._journal._append_in_transaction(
                connection,
                event_id=self._policy_event_id(),
                cycle_id=None,
                aggregate_type=_CYCLE_FREEZE_POLICY_AGGREGATE_TYPE,
                aggregate_id=self._journal._campaign_id,
                event_type=_FREEZE_POLICY_CONFIGURED,
                payload={
                    "campaign_id": self._journal._campaign_id,
                    "schema_version": "control_plane.cycle_freeze_policy.v1",
                },
            )
            self._require_policy_precedes_freeze_history(
                connection,
                event.sequence,
            )

        _SqliteUnitOfWork(stores._operational_spec())._write(configure)

    def _validated_expected_payload(
        self,
        *,
        cycle_id: str,
        proposal: Mapping[str, object],
        execution_spec: ExecutionSpec,
        expected_roster: RosterManifest,
        context_roles: tuple[str, ...],
        context_target_scope_sha256: str,
        context_manifest_sha256: str,
        preflight: Mapping[str, object],
    ) -> tuple[dict[str, object], dict[str, object], str]:
        proposal_bytes = _canonical_bytes(dict(proposal), "proposal")
        frozen_proposal = json.loads(proposal_bytes)
        frozen_preflight = dict(preflight)
        if frozen_preflight.get("verdict") != "WOULD_ACCEPT":
            raise CycleFreezeConflictError(
                "Campaign preflight rejected frozen inputs"
            )
        proposal_sha256 = _content_sha256(
            b"control_plane.campaign_proposal.v1",
            frozen_proposal,
            "proposal",
        )
        expected_context_proposal_sha256 = _campaign_proposal_sha256(
            frozen_proposal
        )
        preflight_sha256 = _content_sha256(
            b"control_plane.campaign_preflight.v1",
            frozen_preflight,
            "preflight",
        )
        preflight_json = _canonical_bytes(
            frozen_preflight,
            "preflight",
        ).decode("utf-8")
        execution_spec_json = _canonical_bytes(
            execution_spec.model_dump(mode="json", warnings=False),
            "execution_spec",
        ).decode("utf-8")
        protocol = execution_spec.protocol
        expected_context_roles = tuple(
            sorted({member.role for member in expected_roster.members})
        )
        if context_roles != expected_context_roles:
            raise CycleFreezeConflictError(
                "safe context roles conflict with the frozen roster"
            )
        target_scope_sha256 = campaign_target_scope_sha256(
            frozen_proposal.get("scope")
        )
        if not hmac.compare_digest(
            _sha256(
                context_target_scope_sha256,
                "context_target_scope_sha256",
            ),
            target_scope_sha256,
        ):
            raise CycleFreezeConflictError(
                "safe context target scope conflicts with the proposal"
            )
        identity = {
            "schema_version": _CYCLE_FREEZE_SCHEMA_VERSION,
            "cycle_id": cycle_id,
            "proposal_sha256": proposal_sha256,
            "execution_spec_id": execution_spec.execution_spec_id,
            "execution_spec_json": execution_spec_json,
            "executed_protocol_sha256": execution_spec.executed_protocol_sha256,
            "generation_id": protocol.generation_id,
            "generation_manifest_artifact_id": (
                protocol.generation_manifest_artifact_id
            ),
            "roster_manifest_sha256": expected_roster.manifest_sha256,
            "context_manifest_sha256": _sha256(
                context_manifest_sha256,
                "context_manifest_sha256",
            ),
            "preflight_json": preflight_json,
            "preflight_sha256": preflight_sha256,
        }
        manifest_sha256 = _content_sha256(
            _CYCLE_FREEZE_MANIFEST_DOMAIN,
            identity,
            "Cycle freeze identity",
        )
        expected_payload = {**identity, "manifest_sha256": manifest_sha256}
        try:
            _payload(
                {
                    **expected_payload,
                    "_authority_grant_id": self._journal._grant.grant_id,
                }
            )
        except ValueError as error:
            raise CycleFreezeConflictError(
                "Cycle freeze durable event payload exceeds the bounded size"
            ) from error
        return (
            expected_payload,
            frozen_proposal,
            expected_context_proposal_sha256,
        )

    def freeze(
        self,
        *,
        cycle_id: str,
        proposal: Mapping[str, object],
        execution_spec: ExecutionSpec,
        expected_roster: RosterManifest,
        _transaction_guard: Callable[[object], None] | None = None,
        _prevalidated_payload: Mapping[str, object] | None = None,
    ) -> FrozenCycleInputs:
        self._journal._authorize()
        if _transaction_guard is not None and not callable(_transaction_guard):
            raise TypeError("_transaction_guard must be callable")
        cycle_id = _identifier(cycle_id, "cycle_id")
        if not isinstance(execution_spec, ExecutionSpec):
            raise TypeError("execution_spec must be an ExecutionSpec")
        if not isinstance(expected_roster, RosterManifest):
            raise TypeError("expected_roster must be a RosterManifest")
        if expected_roster.cycle_id != cycle_id:
            raise ValueError("expected_roster must belong to cycle_id")
        if not isinstance(proposal, Mapping):
            raise ValueError("proposal must be a mapping")
        proposal_bytes = _canonical_bytes(dict(proposal), "proposal")
        frozen_proposal = json.loads(proposal_bytes)
        try:
            context_receipt = self._context.snapshot(cycle_id=cycle_id)
            projection_input, projection_input_sha256 = (
                self._context._verified_projection_input()
            )
        except (CycleContextError, TypeError, ValueError) as error:
            raise CycleFreezeConflictError(
                "Cycle safe context is unavailable or invalid"
            ) from error
        if (
            projection_input_sha256
            != context_receipt.projection_input_sha256
        ):
            raise CycleFreezeConflictError(
                "committed Learning changed after safe context preparation"
            )
        preflight = run_campaign_preflight(
            execution_spec=execution_spec,
            proposal=frozen_proposal,
            committed_claims=projection_input["claims"],
        )
        (
            expected_payload,
            frozen_proposal,
            context_proposal_sha256,
        ) = self._validated_expected_payload(
            cycle_id=cycle_id,
            proposal=frozen_proposal,
            execution_spec=execution_spec,
            expected_roster=expected_roster,
            context_roles=context_receipt.roles,
            context_target_scope_sha256=context_receipt.target_scope_sha256,
            context_manifest_sha256=context_receipt.manifest_sha256,
            preflight=preflight,
        )
        if _prevalidated_payload is not None:
            if (
                not isinstance(_prevalidated_payload, Mapping)
                or dict(_prevalidated_payload) != expected_payload
            ):
                raise CycleFreezeConflictError(
                    "prevalidated Cycle freeze payload conflicts"
                )
            expected_payload = dict(_prevalidated_payload)
        protocol = execution_spec.protocol

        def freeze_inputs(
            connection,
        ) -> tuple[FrozenCycleInputs | None, str | None]:
            if _transaction_guard is not None:
                _transaction_guard(connection)
            policy = self._replay_policy(self._policy_events(connection))
            self._require_policy_precedes_freeze_history(
                connection,
                policy.sequence,
            )
            campaign = self._lifecycle._replay_campaign(
                self._lifecycle._campaign_events(connection)
            )
            if campaign.status is not CampaignStatus.ACTIVE:
                raise CycleFreezeConflictError("Campaign is not ACTIVE")
            roster_events = self._roster._events(connection, cycle_id)
            try:
                frozen_roster = self._roster._replay(roster_events)
            except RosterIntegrityError:
                self._roster._block_invalid_history(connection, roster_events)
                return None, "ROSTER_JOURNAL_INVALID"
            if frozen_roster != expected_roster:
                raise CycleFreezeConflictError("frozen roster identity conflicts")
            protocol_roster = tuple(
                sorted(
                    (
                        member.role,
                        member.provider_profile_id,
                        member.model_id,
                    )
                    for member in protocol.roster
                )
            )
            operational_roster = tuple(
                sorted(
                    (member.role, member.profile, member.model)
                    for member in frozen_roster.members
                )
            )
            if operational_roster != protocol_roster:
                raise CycleFreezeConflictError(
                    "ExecutionSpec roster conflicts with frozen roster"
                )
            events = self._freeze_events(connection, cycle_id)
            if events:
                frozen = self._replay_freeze(events)
                if frozen.identity_payload() != expected_payload:
                    raise CycleFreezeConflictError("Cycle inputs are already frozen")
                self._require_complete_freeze_order(connection, policy, frozen)
                return frozen, None
            cycle = self._lifecycle._replay_cycle(
                self._lifecycle._cycle_events(connection, cycle_id)
            )
            if cycle.status is not CycleStatus.CONTEXT_READY:
                raise CycleFreezeConflictError(
                    "Cycle inputs may freeze only after safe context is ready"
                )
            persisted_context = self._context._replay_context(
                self._context._context_events(connection, cycle_id)
            )
            self._context._require_complete_context_order(
                connection,
                self._context._replay_policy(
                    self._context._policy_events(connection)
                ),
                persisted_context,
            )
            if persisted_context != context_receipt:
                raise CycleFreezeConflictError(
                    "safe context changed before the Cycle freeze"
                )
            persisted_assembly = (
                self._context._verified_stored_assembly_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                )
            )
            if (
                persisted_assembly is None
                or not hmac.compare_digest(
                    persisted_assembly.preview.proposal_sha256,
                    context_proposal_sha256,
                )
            ):
                raise CycleFreezeConflictError(
                    "safe context proposal conflicts with the proposal"
                )
            freeze_event_id = self._freeze_event_id(cycle_id)
            try:
                event = self._journal._append_in_transaction(
                    connection,
                    event_id=freeze_event_id,
                    cycle_id=cycle_id,
                    aggregate_type=_CYCLE_FREEZE_AGGREGATE_TYPE,
                    aggregate_id=cycle_id,
                    event_type=_CYCLE_INPUTS_FROZEN,
                    payload=expected_payload,
                )
            except CampaignEventConflictError:
                self._lifecycle._block_in_transaction(
                    connection,
                    reason_code="CYCLE_FREEZE_JOURNAL_INVALID",
                    source_ref=freeze_event_id,
                )
                return None, "CYCLE_FREEZE_JOURNAL_INVALID"
            self._lifecycle._advance_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                expected_status=CycleStatus.CONTEXT_READY,
                next_status=CycleStatus.FROZEN,
            )
            frozen = FrozenCycleInputs(
                **expected_payload,
                event_id=event.event_id,
                sequence=event.sequence,
            )
            self._require_complete_freeze_order(connection, policy, frozen)
            return frozen, None

        frozen, failure_reason = _SqliteUnitOfWork(
            stores._operational_spec()
        )._write(
            freeze_inputs
        )
        if frozen is None:
            raise CycleFreezeConflictError(
                f"{failure_reason or 'freeze integrity failure'} blocked the Campaign"
            )
        return frozen

    def snapshot(self, *, cycle_id: str) -> FrozenCycleInputs:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._snapshot_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
        )

    def _snapshot_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ) -> FrozenCycleInputs:
        policy = self._replay_policy(self._policy_events(connection))
        frozen = self._replay_freeze(self._freeze_events(connection, cycle_id))
        self._require_complete_freeze_order(connection, policy, frozen)
        return frozen

    def _policy_event_id(self) -> str:
        return _event_id(
            namespace=self._journal._namespace,
            campaign_id=self._journal._campaign_id,
            aggregate_type=_CYCLE_FREEZE_POLICY_AGGREGATE_TYPE,
            aggregate_id=self._journal._campaign_id,
            role="configure",
        )

    def _freeze_event_id(self, cycle_id: str) -> str:
        return _event_id(
            namespace=self._journal._namespace,
            campaign_id=self._journal._campaign_id,
            aggregate_type=_CYCLE_FREEZE_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
            role="freeze",
        )

    def _policy_events(self, connection) -> tuple[CampaignEvent, ...]:
        return self._journal._list_in_transaction(
            connection,
            cycle_id=None,
            aggregate_type=_CYCLE_FREEZE_POLICY_AGGREGATE_TYPE,
            aggregate_id=self._journal._campaign_id,
        )

    def _freeze_events(
        self,
        connection,
        cycle_id: str,
    ) -> tuple[CampaignEvent, ...]:
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_CYCLE_FREEZE_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _replay_policy(self, events: tuple[CampaignEvent, ...]) -> CampaignEvent:
        if len(events) != 1:
            raise CycleFreezeIntegrityError("Cycle freeze policy history is invalid")
        event = events[0]
        expected_payload = {
            "campaign_id": self._journal._campaign_id,
            "schema_version": "control_plane.cycle_freeze_policy.v1",
        }
        if (
            event.event_id != self._policy_event_id()
            or event.event_type != _FREEZE_POLICY_CONFIGURED
            or _event_domain_payload(event) != expected_payload
        ):
            raise CycleFreezeIntegrityError("Cycle freeze policy is invalid")
        return event

    def _replay_freeze(
        self,
        events: tuple[CampaignEvent, ...],
    ) -> FrozenCycleInputs:
        if len(events) != 1:
            raise CycleFreezeIntegrityError("Cycle freeze history is invalid")
        event = events[0]
        payload = _event_domain_payload(event)
        expected_fields = {
            "schema_version",
            "cycle_id",
            "proposal_sha256",
            "execution_spec_id",
            "execution_spec_json",
            "executed_protocol_sha256",
            "generation_id",
            "generation_manifest_artifact_id",
            "roster_manifest_sha256",
            "context_manifest_sha256",
            "preflight_json",
            "preflight_sha256",
            "manifest_sha256",
        }
        if set(payload) != expected_fields:
            raise CycleFreezeIntegrityError("Cycle freeze payload is invalid")
        try:
            cycle_id = _identifier(payload["cycle_id"], "stored cycle_id")
            for field_name in expected_fields - {
                "schema_version",
                "cycle_id",
                "generation_id",
                "execution_spec_json",
                "preflight_json",
            }:
                _sha256(payload[field_name], f"stored {field_name}")
            if payload["schema_version"] != _CYCLE_FREEZE_SCHEMA_VERSION:
                raise ValueError("stored freeze schema is unsupported")
            execution_spec = _execution_spec_from_json(
                payload["execution_spec_json"],
                "stored execution_spec",
            )
            generation_id = payload["generation_id"]
            if (
                not isinstance(generation_id, str)
                or not generation_id
                or generation_id != generation_id.strip()
            ):
                raise ValueError("stored generation_id must be canonical")
            preflight_json = payload["preflight_json"]
            if type(preflight_json) is not str:
                raise ValueError("stored preflight_json must be canonical JSON")
            preflight = json.loads(preflight_json)
            if (
                not isinstance(preflight, dict)
                or _canonical_bytes(preflight, "stored preflight").decode("utf-8")
                != preflight_json
                or set(preflight)
                != {
                    "schema_version",
                    "verdict",
                    "execution_spec_id",
                    "protocol_conformance",
                    "proposal_identity",
                    "learning_verdict",
                    "rejection_codes",
                }
                or preflight["schema_version"]
                != "control_plane.campaign_preflight.v1"
                or preflight["verdict"] != "WOULD_ACCEPT"
                or preflight["execution_spec_id"] != payload["execution_spec_id"]
                or execution_spec.execution_spec_id
                != payload["execution_spec_id"]
                or execution_spec.executed_protocol_sha256
                != payload["executed_protocol_sha256"]
                or execution_spec.protocol.generation_id != generation_id
                or execution_spec.protocol.generation_manifest_artifact_id
                != payload["generation_manifest_artifact_id"]
                or preflight["protocol_conformance"]
                not in {
                    "IDENTICAL",
                    "IMMATERIAL_ALLOWLISTED",
                    "APPROVED_AMENDMENT",
                }
                or preflight["protocol_conformance"]
                != execution_spec.conformance
                or preflight["rejection_codes"] != []
                or not hmac.compare_digest(
                    payload["preflight_sha256"],
                    _content_sha256(
                        b"control_plane.campaign_preflight.v1",
                        preflight,
                        "stored preflight",
                    ),
                )
            ):
                raise ValueError("stored preflight is invalid")
        except (TypeError, ValueError) as error:
            raise CycleFreezeIntegrityError(
                "Cycle freeze identity or preflight is invalid"
            ) from error
        expected_manifest = _content_sha256(
            _CYCLE_FREEZE_MANIFEST_DOMAIN,
            {key: payload[key] for key in payload if key != "manifest_sha256"},
            "stored Cycle freeze identity",
        )
        expected_envelope = (
            self._journal._namespace,
            self._journal._campaign_id,
            cycle_id,
            _CYCLE_FREEZE_AGGREGATE_TYPE,
            cycle_id,
            _CYCLE_INPUTS_FROZEN,
            self._freeze_event_id(cycle_id),
        )
        observed_envelope = (
            event.namespace,
            event.campaign_id,
            event.cycle_id,
            event.aggregate_type,
            event.aggregate_id,
            event.event_type,
            event.event_id,
        )
        if (
            observed_envelope != expected_envelope
            or not hmac.compare_digest(payload["manifest_sha256"], expected_manifest)
        ):
            raise CycleFreezeIntegrityError("Cycle freeze integrity is invalid")
        return FrozenCycleInputs(
            **payload,
            event_id=event.event_id,
            sequence=event.sequence,
        )

    def _require_policy_precedes_freeze_history(
        self,
        connection,
        policy_sequence: int,
    ) -> None:
        rows = connection.execute(
            "SELECT * FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? AND event_type = ? "
            "ORDER BY sequence",
            (
                self._journal._namespace,
                self._journal._campaign_id,
                _CYCLE_AGGREGATE_TYPE,
                _CYCLE_TRANSITIONED,
            ),
        ).fetchall()
        for row in rows:
            event = _event_from_row(row)
            payload = _event_domain_payload(event)
            to_status = payload.get("to_status")
            if not isinstance(to_status, str):
                raise CampaignJournalError("Cycle transition status is invalid")
            if (
                to_status in _POST_FREEZE_STATUS_VALUES
                and event.sequence <= policy_sequence
            ):
                raise CycleFreezeConflictError(
                    "freeze policy cannot retroactively adopt Cycle history"
                )

    def _require_complete_freeze_order(
        self,
        connection,
        policy: CampaignEvent,
        frozen: FrozenCycleInputs,
    ) -> None:
        if policy.sequence >= frozen.sequence:
            raise CycleFreezeIntegrityError(
                "Cycle freeze must follow its Campaign freeze policy"
            )
        roster_events = self._roster._events(connection, frozen.cycle_id)
        try:
            roster = self._roster._replay(roster_events)
        except RosterIntegrityError as error:
            raise CycleFreezeIntegrityError(
                "Cycle freeze roster binding is invalid"
            ) from error
        if (
            roster.manifest_sha256 != frozen.roster_manifest_sha256
            or roster_events[0].sequence >= frozen.sequence
        ):
            raise CycleFreezeIntegrityError(
                "Cycle freeze roster ordering is invalid"
            )
        try:
            context_events = self._context._context_events(
                connection,
                frozen.cycle_id,
            )
            context = self._context._replay_context(
                context_events
            )
            context_policy = self._context._replay_policy(
                self._context._policy_events(connection)
            )
            self._context._require_complete_context_order(
                connection,
                context_policy,
                context,
            )
            context_payload = _event_domain_payload(context_events[0])
            context_proposal = canonical_campaign_proposal(
                context_payload["proposal"]
            )
            context_proposal_sha256 = _content_sha256(
                b"control_plane.campaign_proposal.v1",
                context_proposal,
                "durable context proposal",
            )
            projection_input = context_payload["projection_input"]
            if not isinstance(projection_input, dict) or not isinstance(
                projection_input.get("claims"),
                list,
            ):
                raise ValueError("durable context projection is invalid")
            execution_spec = _execution_spec_from_json(
                frozen.execution_spec_json,
                "frozen execution_spec",
            )
            protocol_roster = tuple(
                sorted(
                    (
                        member.role,
                        member.provider_profile_id,
                        member.model_id,
                    )
                    for member in execution_spec.protocol.roster
                )
            )
            operational_roster = tuple(
                sorted(
                    (member.role, member.profile, member.model)
                    for member in roster.members
                )
            )
        except (CycleContextIntegrityError, TypeError, ValueError) as error:
            raise CycleFreezeIntegrityError(
                "Cycle freeze context binding is invalid"
            ) from error
        if protocol_roster != operational_roster:
            raise CycleFreezeIntegrityError(
                "Cycle freeze ExecutionSpec roster binding is invalid"
            )
        if context.roles != tuple(
            sorted({member.role for member in roster.members})
        ):
            raise CycleFreezeIntegrityError(
                "Cycle freeze context roles are not bound to the frozen roster"
            )
        if not hmac.compare_digest(
            context_proposal_sha256,
            frozen.proposal_sha256,
        ):
            raise CycleFreezeIntegrityError(
                "Cycle freeze proposal binding is invalid"
            )
        if (
            context.manifest_sha256 != frozen.context_manifest_sha256
            or context.sequence >= frozen.sequence
        ):
            raise CycleFreezeIntegrityError(
                "Cycle freeze context ordering is invalid"
            )
        cycle_events = self._lifecycle._cycle_events(
            connection,
            frozen.cycle_id,
        )
        try:
            cycle = self._lifecycle._replay_cycle(cycle_events)
        except CampaignLifecycleError as error:
            raise CycleFreezeIntegrityError(
                "Cycle freeze lifecycle binding is invalid"
            ) from error
        frozen_transitions = tuple(
            event
            for event in cycle_events
            if event.event_type == _CYCLE_TRANSITIONED
            and _event_domain_payload(event).get("to_status")
            == CycleStatus.FROZEN.value
        )
        context_ready_transitions = tuple(
            event
            for event in cycle_events
            if event.event_type == _CYCLE_TRANSITIONED
            and _event_domain_payload(event).get("to_status")
            == CycleStatus.CONTEXT_READY.value
        )
        if (
            cycle.status in _PRE_FREEZE_STATUSES
            or len(context_ready_transitions) != 1
            or len(frozen_transitions) != 1
            or not (
                context_ready_transitions[0].sequence
                < roster_events[0].sequence
                < frozen.sequence
                < frozen_transitions[0].sequence
            )
        ):
            raise CycleFreezeIntegrityError(
                "Cycle freeze lifecycle ordering is invalid"
            )
        try:
            durable_preflight = run_campaign_preflight(
                execution_spec=execution_spec,
                proposal=context_proposal,
                committed_claims=projection_input["claims"],
            )
            durable_preflight_json = _canonical_bytes(
                durable_preflight,
                "durable preflight",
            ).decode("utf-8")
        except (TypeError, ValueError) as error:
            raise CycleFreezeIntegrityError(
                "Cycle freeze preflight binding is invalid"
            ) from error
        if (
            durable_preflight["verdict"] != "WOULD_ACCEPT"
            or durable_preflight_json != frozen.preflight_json
            or not hmac.compare_digest(
                frozen.preflight_sha256,
                _content_sha256(
                    b"control_plane.campaign_preflight.v1",
                    durable_preflight,
                    "durable preflight",
                ),
            )
        ):
            raise CycleFreezeIntegrityError(
                "Cycle freeze preflight binding is invalid"
            )


__all__ = [
    "CycleFreezeConflictError",
    "CycleFreezeError",
    "CycleFreezeIntegrityError",
    "FrozenCycleInputs",
    "OperationalCycleFreezeJournal",
]
