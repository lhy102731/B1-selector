"""Frozen P6 Campaign roster manifests and drift checks."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Callable
from dataclasses import dataclass

from . import stores
from .campaign import InvocationOutcome
from .campaign_lifecycle import (
    CampaignStatus,
    CycleStatus,
    OperationalCampaignLifecycle,
)
from .campaign_store import (
    CampaignEvent,
    CampaignEventConflictError,
    CampaignJournalError,
    OperationalCampaignJournal,
    OperationalUsageJournal,
    RecordedModelAttempt,
    _attempt_id,
    _event_domain_payload,
    _identifier,
)
from .sqlite_uow import _SqliteUnitOfWork


_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_ROSTER_AGGREGATE_TYPE = "CYCLE_ROSTER"
_ROSTER_FROZEN = "ROSTER_FROZEN"
_ROSTER_RESPONSE_VERIFIED = "ROSTER_RESPONSE_VERIFIED"
_ROSTER_DRIFT_DETECTED = "ROSTER_DRIFT_DETECTED"
_ROSTER_RESPONSES_COMPLETED = "ROSTER_RESPONSES_COMPLETED"
_MAX_ROSTER_MEMBERS = 64


class RosterError(RuntimeError):
    """Base error for frozen Campaign roster operations."""


class RosterConflictError(RosterError):
    """Raised when a frozen roster is replayed with different content."""


class RosterIntegrityError(RosterError):
    """Raised when persisted roster events fail canonical replay."""


class RosterDriftError(RosterError):
    """Raised after roster drift and Campaign blocking commit atomically."""


def _sha256(value: str, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class RosterMember:
    member_id: str
    provider: str
    profile: str
    model: str
    role: str
    prompt_sha256: str
    config_sha256: str
    capability_sha256: str

    def __post_init__(self) -> None:
        for field_name in ("member_id", "provider", "profile", "model", "role"):
            object.__setattr__(
                self,
                field_name,
                _identifier(getattr(self, field_name), field_name),
            )
        for field_name in (
            "prompt_sha256",
            "config_sha256",
            "capability_sha256",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(getattr(self, field_name), field_name),
            )

    def to_payload(self) -> dict[str, str]:
        return {
            "member_id": self.member_id,
            "provider": self.provider,
            "profile": self.profile,
            "model": self.model,
            "role": self.role,
            "prompt_sha256": self.prompt_sha256,
            "config_sha256": self.config_sha256,
            "capability_sha256": self.capability_sha256,
        }


@dataclass(frozen=True, slots=True)
class RosterManifest:
    cycle_id: str
    members: tuple[RosterMember, ...]
    manifest_sha256: str


@dataclass(frozen=True, slots=True)
class VerifiedRosterResponse:
    member_id: str
    response_model: str
    event_id: str


@dataclass(frozen=True, slots=True)
class RosterCompletion:
    cycle_id: str
    member_ids: tuple[str, ...]
    event_id: str


@dataclass(frozen=True, slots=True)
class RosterSnapshot:
    cycle_id: str
    manifest_sha256: str
    member_ids: tuple[str, ...]
    verified_member_ids: tuple[str, ...]
    terminal_event_type: str | None
    terminal_event_id: str | None


@dataclass(frozen=True, slots=True)
class _RosterHistory:
    manifest: RosterManifest
    verified_member_ids: frozenset[str]
    terminal_event_type: str | None
    terminal_event_id: str | None


def _roster_manifest(
    cycle_id: str,
    members: tuple[RosterMember, ...],
) -> RosterManifest:
    cycle_id = _identifier(cycle_id, "cycle_id")
    if (
        not isinstance(members, tuple)
        or not 1 <= len(members) <= _MAX_ROSTER_MEMBERS
        or not all(isinstance(member, RosterMember) for member in members)
    ):
        raise ValueError("members must be a bounded tuple of RosterMember values")
    ordered = tuple(sorted(members, key=lambda member: member.member_id))
    member_ids = tuple(member.member_id for member in ordered)
    if len(set(member_ids)) != len(member_ids):
        raise ValueError("roster member_id values must be unique")
    payload = json.dumps(
        {
            "cycle_id": cycle_id,
            "members": [member.to_payload() for member in ordered],
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha256(b"control_plane.campaign_roster.v1\0" + payload).hexdigest()
    return RosterManifest(cycle_id, ordered, digest)


def _roster_event_id(
    *,
    namespace: str,
    campaign_id: str,
    cycle_id: str,
    role: str,
) -> str:
    return hashlib.sha256(
        b"control_plane.campaign_roster_event.v1\0"
        + "\0".join((namespace, campaign_id, cycle_id, role)).encode("ascii")
    ).hexdigest()


class OperationalRosterJournal:
    """Freeze and replay one roster per Cycle in OperationalJournal."""

    __slots__ = ("_journal", "_lifecycle")

    def __init__(
        self,
        *,
        journal: OperationalCampaignJournal,
        lifecycle: OperationalCampaignLifecycle,
    ) -> None:
        if not isinstance(journal, OperationalCampaignJournal) or not isinstance(
            lifecycle,
            OperationalCampaignLifecycle,
        ):
            raise TypeError("journal and lifecycle must be operational P6 objects")
        journal._authorize()
        if lifecycle._journal is not journal:
            raise ValueError("lifecycle must use the same Campaign journal")
        self._journal = journal
        self._lifecycle = lifecycle

    def freeze(
        self,
        *,
        cycle_id: str,
        members: tuple[RosterMember, ...],
        _transaction_guard: Callable[[object], None] | None = None,
    ) -> RosterManifest:
        self._journal._authorize()
        if _transaction_guard is not None and not callable(_transaction_guard):
            raise TypeError("_transaction_guard must be callable")
        manifest = _roster_manifest(cycle_id, members)

        def freeze_roster(connection) -> RosterManifest | None:
            if _transaction_guard is not None:
                _transaction_guard(connection)
            campaign = self._lifecycle._replay_campaign(
                self._lifecycle._campaign_events(connection)
            )
            if campaign.status is not CampaignStatus.ACTIVE:
                raise RosterConflictError("Campaign is not ACTIVE")
            events = self._events(connection, manifest.cycle_id)
            if events:
                try:
                    frozen = self._replay_history(
                        connection,
                        events,
                    ).manifest
                except RosterIntegrityError:
                    self._block_invalid_history(connection, events)
                    return None
                if frozen != manifest:
                    raise RosterConflictError("Cycle roster is already frozen")
                return frozen
            cycle = self._lifecycle._replay_cycle(
                self._lifecycle._cycle_events(connection, manifest.cycle_id)
            )
            if cycle.status is not CycleStatus.CONTEXT_READY:
                raise RosterConflictError(
                    "roster may freeze only after safe context is ready"
                )
            event = self._append_event_in_transaction(
                connection,
                cycle_id=manifest.cycle_id,
                role="freeze",
                event_type=_ROSTER_FROZEN,
                payload={
                    "cycle_id": manifest.cycle_id,
                    "manifest_sha256": manifest.manifest_sha256,
                    "members": [
                        member.to_payload() for member in manifest.members
                    ],
                },
            )
            if event is None:
                return None
            return manifest

        frozen = _SqliteUnitOfWork(stores._operational_spec())._write(
            freeze_roster
        )
        if frozen is None:
            raise RosterDriftError(
                "roster drift blocked Campaign: ROSTER_JOURNAL_INVALID"
            )
        return frozen

    def verify_response(
        self,
        *,
        cycle_id: str,
        member_id: str,
        usage_journal: OperationalUsageJournal,
        call_id: str,
        attempt_id: str,
        _transaction_guard: Callable[[object], None] | None = None,
    ) -> VerifiedRosterResponse:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")
        member_id = _identifier(member_id, "member_id")
        call_id = _identifier(call_id, "call_id")
        attempt_id = _identifier(attempt_id, "attempt_id")
        if not isinstance(usage_journal, OperationalUsageJournal):
            raise TypeError("usage_journal must be an OperationalUsageJournal")
        if (
            usage_journal._cycle_id != cycle_id
            or usage_journal._journal._namespace != self._journal._namespace
            or usage_journal._journal._campaign_id != self._journal._campaign_id
        ):
            raise RosterConflictError(
                "usage attempt does not belong to this Campaign Cycle"
            )
        if _transaction_guard is not None and not callable(_transaction_guard):
            raise TypeError("_transaction_guard must be callable")

        def verify(connection):
            if _transaction_guard is not None:
                _transaction_guard(connection)
            campaign = self._lifecycle._replay_campaign(
                self._lifecycle._campaign_events(connection)
            )
            if campaign.status is not CampaignStatus.ACTIVE:
                raise RosterConflictError("Campaign is not ACTIVE")
            cycle = self._lifecycle._replay_cycle(
                self._lifecycle._cycle_events(connection, cycle_id)
            )
            if cycle.status is not CycleStatus.EXECUTING:
                raise RosterConflictError(
                    "roster responses require an EXECUTING Cycle"
                )
            events = self._events(connection, cycle_id)
            try:
                history = self._replay_history(connection, events)
            except RosterIntegrityError:
                self._block_invalid_history(connection, events)
                return None, "ROSTER_JOURNAL_INVALID"
            if history.terminal_event_type is not None:
                raise RosterConflictError("roster response journal is terminal")
            manifest = history.manifest
            expected = next(
                (
                    frozen_member
                    for frozen_member in manifest.members
                    if frozen_member.member_id == member_id
                ),
                None,
            )
            attempt_events = usage_journal._events_in_transaction(
                connection,
                _attempt_id(cycle_id, call_id, attempt_id),
            )
            try:
                recorded = usage_journal._read_attempt_in_transaction(
                    connection,
                    call_id=call_id,
                    attempt_id=attempt_id,
                )
            except CampaignJournalError as error:
                if not attempt_events:
                    raise RosterConflictError(
                        "persisted usage attempt does not exist"
                    ) from error
                source_ref = attempt_events[-1].event_id
                self._lifecycle._block_in_transaction(
                    connection,
                    reason_code="ROSTER_JOURNAL_INVALID",
                    source_ref=source_ref,
                )
                return None, "ROSTER_JOURNAL_INVALID"
            envelope = recorded.envelope
            reason_code = self._drift_reason(expected, recorded)
            if reason_code is not None:
                drift = self._append_event_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    role="drift",
                    event_type=_ROSTER_DRIFT_DETECTED,
                    payload={
                        "cycle_id": cycle_id,
                        "manifest_sha256": manifest.manifest_sha256,
                        "reason_code": reason_code,
                        "member_id": member_id,
                        "expected_member": (
                            None if expected is None else expected.to_payload()
                        ),
                        "observed_attempt": self._attempt_payload(recorded),
                    },
                )
                if drift is None:
                    return None, "ROSTER_JOURNAL_INVALID"
                self._lifecycle._block_in_transaction(
                    connection,
                    reason_code=reason_code,
                    source_ref=drift.event_id,
                )
                return None, reason_code
            event_id = self._event_id(
                cycle_id,
                f"verified:{member_id}",
            )
            verified_payload = self._verified_payload(
                manifest,
                expected,
                recorded,
            )
            existing = next(
                (event for event in events if event.event_id == event_id),
                None,
            )
            if existing is None:
                verified_event = self._append_event_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    role=f"verified:{member_id}",
                    event_type=_ROSTER_RESPONSE_VERIFIED,
                    payload=verified_payload,
                )
                if verified_event is None:
                    return None, "ROSTER_JOURNAL_INVALID"
            elif _event_domain_payload(existing) != verified_payload:
                raise RosterConflictError(
                    "roster member is already verified by another attempt"
                )
            return VerifiedRosterResponse(
                member_id,
                envelope.response_model,
                event_id,
            ), None

        response, drift_reason = _SqliteUnitOfWork(
            stores._operational_spec()
        )._write(verify)
        if drift_reason is not None:
            raise RosterDriftError(
                f"roster drift blocked Campaign: {drift_reason}"
            )
        if not isinstance(response, VerifiedRosterResponse):
            raise RosterIntegrityError("verified roster response is missing")
        return response

    def complete_responses(
        self,
        *,
        cycle_id: str,
        _transaction_guard: Callable[[object], None] | None = None,
    ) -> RosterCompletion:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")
        if _transaction_guard is not None and not callable(_transaction_guard):
            raise TypeError("_transaction_guard must be callable")

        def complete(connection):
            if _transaction_guard is not None:
                _transaction_guard(connection)
            campaign = self._lifecycle._replay_campaign(
                self._lifecycle._campaign_events(connection)
            )
            if campaign.status is not CampaignStatus.ACTIVE:
                raise RosterConflictError("Campaign is not ACTIVE")
            cycle = self._lifecycle._replay_cycle(
                self._lifecycle._cycle_events(connection, cycle_id)
            )
            if cycle.status is not CycleStatus.EXECUTING:
                raise RosterConflictError(
                    "roster responses require an EXECUTING Cycle"
                )
            events = self._events(connection, cycle_id)
            try:
                history = self._replay_history(connection, events)
            except RosterIntegrityError:
                self._block_invalid_history(connection, events)
                return None, "ROSTER_JOURNAL_INVALID"
            if history.terminal_event_type == _ROSTER_RESPONSES_COMPLETED:
                return RosterCompletion(
                    cycle_id,
                    tuple(sorted(history.verified_member_ids)),
                    history.terminal_event_id,
                ), None
            if history.terminal_event_type is not None:
                raise RosterConflictError("roster response journal is terminal")
            manifest = history.manifest
            verified = set(history.verified_member_ids)
            required = {member.member_id for member in manifest.members}
            missing = tuple(sorted(required - verified))
            if missing:
                reason_code = "REQUIRED_MEMBER_MISSING"
                drift = self._append_event_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    role="drift",
                    event_type=_ROSTER_DRIFT_DETECTED,
                    payload={
                        "cycle_id": cycle_id,
                        "manifest_sha256": manifest.manifest_sha256,
                        "reason_code": reason_code,
                        "missing_member_ids": list(missing),
                    },
                )
                if drift is None:
                    return None, "ROSTER_JOURNAL_INVALID"
                self._lifecycle._block_in_transaction(
                    connection,
                    reason_code=reason_code,
                    source_ref=drift.event_id,
                )
                return None, reason_code
            member_ids = tuple(sorted(verified))
            event_id = self._event_id(cycle_id, "complete")
            if not any(event.event_id == event_id for event in events):
                completion_event = self._append_event_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    role="complete",
                    event_type=_ROSTER_RESPONSES_COMPLETED,
                    payload={
                        "cycle_id": cycle_id,
                        "manifest_sha256": manifest.manifest_sha256,
                        "member_ids": list(member_ids),
                    },
                )
                if completion_event is None:
                    return None, "ROSTER_JOURNAL_INVALID"
            return RosterCompletion(cycle_id, member_ids, event_id), None

        completion, drift_reason = _SqliteUnitOfWork(
            stores._operational_spec()
        )._write(complete)
        if drift_reason is not None:
            raise RosterDriftError(
                f"roster drift blocked Campaign: {drift_reason}"
            )
        if not isinstance(completion, RosterCompletion):
            raise RosterIntegrityError("roster completion is missing")
        return completion

    def snapshot(self, *, cycle_id: str) -> RosterSnapshot:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")

        def load_snapshot(connection) -> RosterSnapshot | None:
            events = self._events(connection, cycle_id)
            try:
                history = self._replay_history(connection, events)
            except RosterIntegrityError:
                self._block_invalid_history(connection, events)
                return None
            if history.manifest.cycle_id != cycle_id:
                raise RosterIntegrityError("roster snapshot Cycle is invalid")
            return RosterSnapshot(
                cycle_id,
                history.manifest.manifest_sha256,
                tuple(
                    member.member_id for member in history.manifest.members
                ),
                tuple(sorted(history.verified_member_ids)),
                history.terminal_event_type,
                history.terminal_event_id,
            )

        snapshot = _SqliteUnitOfWork(stores._operational_spec())._write(
            load_snapshot
        )
        if snapshot is None:
            raise RosterDriftError(
                "roster drift blocked Campaign: ROSTER_JOURNAL_INVALID"
            )
        return snapshot

    def _events(
        self,
        connection,
        cycle_id: str,
    ) -> tuple[CampaignEvent, ...]:
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_ROSTER_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _event_id(self, cycle_id: str, role: str) -> str:
        return _roster_event_id(
            namespace=self._journal._namespace,
            campaign_id=self._journal._campaign_id,
            cycle_id=cycle_id,
            role=role,
        )

    def _append_event_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        role: str,
        event_type: str,
        payload: dict[str, object],
    ) -> CampaignEvent | None:
        event_id = self._event_id(cycle_id, role)
        try:
            return self._journal._append_in_transaction(
                connection,
                event_id=event_id,
                cycle_id=cycle_id,
                aggregate_type=_ROSTER_AGGREGATE_TYPE,
                aggregate_id=cycle_id,
                event_type=event_type,
                payload=payload,
            )
        except CampaignEventConflictError:
            self._lifecycle._block_in_transaction(
                connection,
                reason_code="ROSTER_JOURNAL_INVALID",
                source_ref=event_id,
            )
            return None

    def _replay(self, events: tuple[CampaignEvent, ...]) -> RosterManifest:
        if not events:
            raise RosterIntegrityError("Cycle roster has not been frozen")
        event = events[0]
        payload = _event_domain_payload(event)
        if set(payload) != {"cycle_id", "manifest_sha256", "members"}:
            raise RosterIntegrityError("roster freeze payload is invalid")
        try:
            cycle_id = _identifier(payload["cycle_id"], "stored cycle_id")
            manifest_sha256 = _sha256(
                payload["manifest_sha256"],
                "stored manifest_sha256",
            )
        except ValueError as error:
            raise RosterIntegrityError("roster freeze identity is invalid") from error
        expected_envelope = (
            self._journal._namespace,
            self._journal._campaign_id,
            cycle_id,
            _ROSTER_AGGREGATE_TYPE,
            cycle_id,
            _ROSTER_FROZEN,
            self._event_id(cycle_id, "freeze"),
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
        if observed_envelope != expected_envelope:
            raise RosterIntegrityError("roster freeze envelope is invalid")
        raw_members = payload["members"]
        if (
            not isinstance(raw_members, list)
            or not 1 <= len(raw_members) <= _MAX_ROSTER_MEMBERS
        ):
            raise RosterIntegrityError("stored roster members are invalid")
        expected_member_fields = {
            "member_id",
            "provider",
            "profile",
            "model",
            "role",
            "prompt_sha256",
            "config_sha256",
            "capability_sha256",
        }
        try:
            members = tuple(
                RosterMember(**member)
                for member in raw_members
                if isinstance(member, dict) and set(member) == expected_member_fields
            )
        except (TypeError, ValueError) as error:
            raise RosterIntegrityError("stored roster member is invalid") from error
        if len(members) != len(raw_members):
            raise RosterIntegrityError("stored roster member is invalid")
        try:
            manifest = _roster_manifest(cycle_id, members)
        except ValueError as error:
            raise RosterIntegrityError("stored roster manifest is invalid") from error
        if not hmac.compare_digest(manifest.manifest_sha256, manifest_sha256):
            raise RosterIntegrityError("stored roster manifest hash is invalid")
        expected_payload = {
            "cycle_id": manifest.cycle_id,
            "manifest_sha256": manifest.manifest_sha256,
            "members": [member.to_payload() for member in manifest.members],
        }
        if payload != expected_payload:
            raise RosterIntegrityError("stored roster manifest is not canonical")
        return manifest

    def _block_invalid_history(
        self,
        connection,
        events: tuple[CampaignEvent, ...],
    ) -> None:
        if not events:
            raise RosterIntegrityError("roster journal is empty")
        self._lifecycle._block_in_transaction(
            connection,
            reason_code="ROSTER_JOURNAL_INVALID",
            source_ref=events[-1].event_id,
        )

    def _replay_history(
        self,
        connection,
        events: tuple[CampaignEvent, ...],
    ) -> _RosterHistory:
        manifest = self._replay(events)
        verified_events: list[CampaignEvent] = []
        terminal: CampaignEvent | None = None
        for event in events[1:]:
            if terminal is not None:
                raise RosterIntegrityError(
                    "roster journal contains events after terminal state"
                )
            if event.event_type == _ROSTER_RESPONSE_VERIFIED:
                verified_events.append(event)
            elif event.event_type in {
                _ROSTER_DRIFT_DETECTED,
                _ROSTER_RESPONSES_COMPLETED,
            }:
                terminal = event
            else:
                raise RosterIntegrityError("roster journal event type is invalid")
        verified = self._verified_member_ids(
            connection,
            (events[0], *verified_events),
            manifest,
        )
        if terminal is not None:
            if terminal.event_type == _ROSTER_RESPONSES_COMPLETED:
                self._validate_completion(terminal, manifest, verified)
            else:
                self._validate_drift(
                    connection,
                    terminal,
                    manifest,
                    verified,
                )
        return _RosterHistory(
            manifest,
            frozenset(verified),
            None if terminal is None else terminal.event_type,
            None if terminal is None else terminal.event_id,
        )

    def _expected_envelope(
        self,
        *,
        cycle_id: str,
        event_type: str,
        event_id: str,
    ) -> tuple[str, str, str, str, str, str, str]:
        return (
            self._journal._namespace,
            self._journal._campaign_id,
            cycle_id,
            _ROSTER_AGGREGATE_TYPE,
            cycle_id,
            event_type,
            event_id,
        )

    @staticmethod
    def _observed_envelope(
        event: CampaignEvent,
    ) -> tuple[str, str, str | None, str, str, str, str]:
        return (
            event.namespace,
            event.campaign_id,
            event.cycle_id,
            event.aggregate_type,
            event.aggregate_id,
            event.event_type,
            event.event_id,
        )

    def _validate_completion(
        self,
        event: CampaignEvent,
        manifest: RosterManifest,
        verified: set[str],
    ) -> None:
        member_ids = tuple(member.member_id for member in manifest.members)
        expected_payload = {
            "cycle_id": manifest.cycle_id,
            "manifest_sha256": manifest.manifest_sha256,
            "member_ids": list(member_ids),
        }
        payload = _event_domain_payload(event)
        if (
            payload != expected_payload
            or verified != set(member_ids)
            or self._observed_envelope(event)
            != self._expected_envelope(
                cycle_id=manifest.cycle_id,
                event_type=_ROSTER_RESPONSES_COMPLETED,
                event_id=self._event_id(manifest.cycle_id, "complete"),
            )
        ):
            raise RosterIntegrityError("roster completion event is invalid")

    def _drift_reason(
        self,
        expected: RosterMember | None,
        recorded: RecordedModelAttempt,
    ) -> str | None:
        envelope = recorded.envelope
        if expected is None:
            return "ROSTER_IDENTITY_DRIFT"
        if (
            envelope.provider != expected.provider
            or envelope.profile != expected.profile
            or envelope.request_model != expected.model
        ):
            return "ROSTER_IDENTITY_DRIFT"
        if recorded.final_outcome is not InvocationOutcome.SUCCESS:
            return "REQUIRED_MEMBER_RESPONSE_INVALID"
        if envelope.response_model != expected.model or envelope.fallback:
            return "RESPONSE_MODEL_DRIFT"
        return None

    def _validate_drift(
        self,
        connection,
        event: CampaignEvent,
        manifest: RosterManifest,
        verified: set[str],
    ) -> None:
        payload = _event_domain_payload(event)
        required_ids = {member.member_id for member in manifest.members}
        expected_members = {
            member.member_id: member for member in manifest.members
        }
        if set(payload) == {
            "cycle_id",
            "manifest_sha256",
            "reason_code",
            "missing_member_ids",
        }:
            missing = tuple(sorted(required_ids - verified))
            expected_payload: dict[str, object] = {
                "cycle_id": manifest.cycle_id,
                "manifest_sha256": manifest.manifest_sha256,
                "reason_code": "REQUIRED_MEMBER_MISSING",
                "missing_member_ids": list(missing),
            }
            reason_code = "REQUIRED_MEMBER_MISSING"
            if not missing:
                raise RosterIntegrityError("roster drift has no missing member")
        elif set(payload) == {
            "cycle_id",
            "manifest_sha256",
            "reason_code",
            "member_id",
            "expected_member",
            "observed_attempt",
        }:
            try:
                member_id = _identifier(payload["member_id"], "stored member_id")
                observed_attempt = payload["observed_attempt"]
                if not isinstance(observed_attempt, dict):
                    raise ValueError("observed_attempt must be an object")
                call_id = _identifier(
                    observed_attempt.get("call_id"),
                    "stored call_id",
                )
                attempt_id = _identifier(
                    observed_attempt.get("attempt_id"),
                    "stored attempt_id",
                )
            except ValueError as error:
                raise RosterIntegrityError("roster drift binding is invalid") from error
            usage = OperationalUsageJournal(
                journal=self._journal,
                cycle_id=manifest.cycle_id,
            )
            try:
                recorded = usage._read_attempt_in_transaction(
                    connection,
                    call_id=call_id,
                    attempt_id=attempt_id,
                )
            except (CampaignJournalError, KeyError, TypeError, ValueError) as error:
                raise RosterIntegrityError(
                    "roster drift usage attempt is invalid"
                ) from error
            expected = expected_members.get(member_id)
            reason_code = self._drift_reason(expected, recorded)
            if reason_code is None:
                raise RosterIntegrityError("roster drift is not supported by usage")
            expected_payload = {
                "cycle_id": manifest.cycle_id,
                "manifest_sha256": manifest.manifest_sha256,
                "reason_code": reason_code,
                "member_id": member_id,
                "expected_member": (
                    None if expected is None else expected.to_payload()
                ),
                "observed_attempt": self._attempt_payload(recorded),
            }
        else:
            raise RosterIntegrityError("roster drift payload is invalid")
        campaign = self._lifecycle._replay_campaign(
            self._lifecycle._campaign_events(connection)
        )
        if (
            payload != expected_payload
            or self._observed_envelope(event)
            != self._expected_envelope(
                cycle_id=manifest.cycle_id,
                event_type=_ROSTER_DRIFT_DETECTED,
                event_id=self._event_id(manifest.cycle_id, "drift"),
            )
            or campaign.status is not CampaignStatus.BLOCKED
            or campaign.block_reason_code != reason_code
            or campaign.block_source_ref != event.event_id
        ):
            raise RosterIntegrityError("roster drift event is invalid")

    @staticmethod
    def _attempt_payload(recorded: RecordedModelAttempt) -> dict[str, object]:
        envelope = recorded.envelope
        return {
            "provider": envelope.provider,
            "profile": envelope.profile,
            "request_model": envelope.request_model,
            "response_model": envelope.response_model,
            "call_id": envelope.call_id,
            "attempt_id": envelope.attempt_id,
            "fallback": envelope.fallback,
            "streamed": envelope.streamed,
            "final_outcome": recorded.final_outcome.value,
            "raw_usage_sha256": envelope.raw_usage_sha256,
        }

    def _verified_payload(
        self,
        manifest: RosterManifest,
        member: RosterMember | None,
        recorded: RecordedModelAttempt,
    ) -> dict[str, object]:
        if not isinstance(member, RosterMember):
            raise RosterIntegrityError("verified roster member is missing")
        envelope = recorded.envelope
        if (
            envelope.provider != member.provider
            or envelope.profile != member.profile
            or envelope.request_model != member.model
            or envelope.response_model != member.model
            or envelope.fallback
            or envelope.streamed
            or recorded.final_outcome is not InvocationOutcome.SUCCESS
        ):
            raise RosterIntegrityError(
                "verified usage attempt does not match frozen roster"
            )
        return {
            "cycle_id": manifest.cycle_id,
            "manifest_sha256": manifest.manifest_sha256,
            "member_id": member.member_id,
            "attempt": self._attempt_payload(recorded),
        }

    def _verified_member_ids(
        self,
        connection,
        events: tuple[CampaignEvent, ...],
        manifest: RosterManifest,
    ) -> set[str]:
        expected_members = {
            member.member_id: member for member in manifest.members
        }
        verified: set[str] = set()
        usage = OperationalUsageJournal(
            journal=self._journal,
            cycle_id=manifest.cycle_id,
        )
        for event in events[1:]:
            if event.event_type != _ROSTER_RESPONSE_VERIFIED:
                continue
            payload = _event_domain_payload(event)
            if set(payload) != {
                "cycle_id",
                "manifest_sha256",
                "member_id",
                "attempt",
            }:
                raise RosterIntegrityError("verified roster response is invalid")
            try:
                cycle_id = _identifier(payload["cycle_id"], "stored cycle_id")
                member_id = _identifier(payload["member_id"], "stored member_id")
                manifest_sha256 = _sha256(
                    payload["manifest_sha256"],
                    "stored manifest_sha256",
                )
                attempt = payload["attempt"]
                if not isinstance(attempt, dict):
                    raise ValueError("stored attempt must be an object")
                call_id = _identifier(attempt.get("call_id"), "stored call_id")
                attempt_id = _identifier(
                    attempt.get("attempt_id"),
                    "stored attempt_id",
                )
            except ValueError as error:
                raise RosterIntegrityError(
                    "verified roster response binding is invalid"
                ) from error
            expected = expected_members.get(member_id)
            try:
                recorded = usage._read_attempt_in_transaction(
                    connection,
                    call_id=call_id,
                    attempt_id=attempt_id,
                )
            except (CampaignJournalError, KeyError, TypeError, ValueError) as error:
                raise RosterIntegrityError(
                    "verified roster usage attempt is invalid"
                ) from error
            expected_envelope = (
                self._journal._namespace,
                self._journal._campaign_id,
                manifest.cycle_id,
                _ROSTER_AGGREGATE_TYPE,
                manifest.cycle_id,
                _ROSTER_RESPONSE_VERIFIED,
                self._event_id(
                    manifest.cycle_id,
                    f"verified:{member_id}",
                ),
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
                cycle_id != manifest.cycle_id
                or not hmac.compare_digest(
                    manifest_sha256,
                    manifest.manifest_sha256,
                )
                or expected is None
                or payload != self._verified_payload(manifest, expected, recorded)
                or observed_envelope != expected_envelope
                or member_id in verified
            ):
                raise RosterIntegrityError(
                    "verified roster response does not match frozen roster"
                )
            verified.add(member_id)
        return verified


__all__ = [
    "OperationalRosterJournal",
    "RosterConflictError",
    "RosterDriftError",
    "RosterError",
    "RosterIntegrityError",
    "RosterCompletion",
    "RosterManifest",
    "RosterMember",
    "RosterSnapshot",
    "VerifiedRosterResponse",
]
