"""Durable P3 access, lineage, taint, and one-time fold-test controls."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from enum import Enum
import hashlib
import hmac
import json
from pathlib import Path
import re
from typing import Callable, Mapping

from .contracts import Actor, Phase, SideEffect, canonical_json
from .sqlite_uow import _SqliteUnitOfWork
from . import stores


class DatasetRole(str, Enum):
    TRAIN = "TRAIN"
    VALIDATION = "VALIDATION"
    FOLD_TEST = "FOLD_TEST"
    FINAL_HOLDOUT = "FINAL_HOLDOUT"
    LIVE_FORWARD = "LIVE_FORWARD"


class AccessOperation(str, Enum):
    READ = "READ"
    MATERIALIZE = "MATERIALIZE"
    DERIVE = "DERIVE"
    DISPLAY = "DISPLAY"
    CONSUME = "CONSUME"
    EXPORT = "EXPORT"


class Taint(str, Enum):
    CLEAN = "CLEAN"
    TEST_LABEL = "TEST_LABEL"
    TEST_DERIVED = "TEST_DERIVED"
    FINAL_HOLDOUT = "FINAL_HOLDOUT"
    INVALID = "INVALID"


class AccessError(RuntimeError):
    pass


class AccessConflictError(AccessError):
    pass


class FinalHoldoutUnavailable(AccessError):
    pass


class LineageError(AccessError):
    pass


class InvalidTaintError(AccessError):
    pass


class FoldTestAlreadyConsumed(AccessError):
    pass


_MAX_EVENT_BYTES = 64 * 1024
_MAX_STRING = 1024
_TAINT_ORDER = {
    Taint.CLEAN: 0,
    Taint.TEST_LABEL: 1,
    Taint.TEST_DERIVED: 2,
    Taint.FINAL_HOLDOUT: 3,
    Taint.INVALID: 4,
}
_FORBIDDEN_METADATA = ("raw_", "dataframe", "rawlog", "secret")
_CAPABILITY_SEAL = object()
_REGISTRY_SEAL = object()
_ALLOWED_METADATA_KEYS = frozenset({
    "algorithm", "artifact_sha256", "backend", "candidate_id", "classification",
    "count", "field_count", "fold_id", "generation_id", "protocol_id",
    "reason_code", "row_count", "schema_version", "source_kind", "status",
})
_SHA256_KEYS = frozenset({"artifact_sha256", "payload_sha256"})
_COUNT_KEYS = frozenset({"count", "field_count", "row_count"})
_OPAQUE_ID_KEYS = frozenset({"candidate_id", "fold_id", "generation_id", "protocol_id"})
_REF_PATTERN = re.compile(r"(?:artifact|claim|dataset|export|metric|prompt):[A-Za-z0-9_.:/#-]{1,959}\Z")
_FIELD_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]{0,255}\Z")
_EVENT_ID_PATTERN = re.compile(r"(?:consume|cycle|derive|display|event|export|evt|missing|protected|seed|train)(?:[-:][A-Za-z0-9_.:-]+)+\Z")
_CLASSIFICATION_VALUES = frozenset({
    "APPROVED", "BLOCKED", "CLEAN", "FAIL", "FAILED", "INVALID",
    "PASS", "PENDING", "REJECTED", "RESEARCH_ONLY", "SUCCEEDED",
    "TEST_DERIVED", "TEST_LABEL", "VERIFIED",
})


def _nonempty(value: str, name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > _MAX_STRING:
        raise ValueError(f"{name} must be a bounded non-empty string")
    return value


def _registered_id(value: str, name: str, prefix: str) -> str:
    _nonempty(value, name)
    if len(value) > 128 or re.fullmatch(
        rf"{re.escape(prefix)}(?:[-:][A-Za-z0-9_.:-]+)?", value
    ) is None:
        raise ValueError(f"{name} must be a registered opaque identifier")
    return value


def _refs(values: tuple[str, ...], name: str, *, fields: bool = False) -> tuple[str, ...]:
    pattern = _FIELD_PATTERN if fields else _REF_PATTERN
    if not isinstance(values, tuple) or any(
        not isinstance(value, str) or pattern.fullmatch(value) is None
        for value in values
    ) or len(set(values)) != len(values):
        raise ValueError(f"{name} must contain unique registered references")
    return values


def _optional_date(value: str | None, name: str) -> str | None:
    if value is None:
        return None
    _nonempty(value, name)
    try:
        date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an ISO date") from error
    return value


def _taints(values: tuple[Taint, ...], name: str) -> tuple[Taint, ...]:
    if not isinstance(values, tuple) or any(not isinstance(value, Taint) for value in values):
        raise TypeError(f"{name} must contain Taint values")
    return tuple(sorted(set(values), key=lambda value: _TAINT_ORDER[value]))


@dataclass(frozen=True, slots=True)
class AccessEvent:
    event_id: str
    operation: AccessOperation
    actor_id: str
    actor_type: str
    invocation_id: str
    run_id: str
    dataset_role: DatasetRole
    generation_id: str | None = None
    fields: tuple[str, ...] = ()
    date_start: str | None = None
    date_end: str | None = None
    input_artifact_refs: tuple[str, ...] = ()
    output_artifact_refs: tuple[str, ...] = ()
    taint_in: tuple[Taint, ...] = ()
    taint_out: tuple[Taint, ...] = (Taint.CLEAN,)
    metadata: tuple[tuple[str, str], ...] = ()
    sequence: int | None = None
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        _nonempty(self.event_id, "event_id")
        if len(self.event_id) > 128 or _EVENT_ID_PATTERN.fullmatch(self.event_id) is None:
            raise ValueError("event_id must be a registered opaque identifier")
        for field in ("actor_id", "actor_type", "invocation_id"):
            _nonempty(getattr(self, field), field)
        _registered_id(self.run_id, "run_id", "run")
        Actor(self.actor_id, self.actor_type, self.invocation_id)
        if not isinstance(self.operation, AccessOperation):
            raise TypeError("operation must be an AccessOperation")
        if not isinstance(self.dataset_role, DatasetRole):
            raise TypeError("dataset_role must be a DatasetRole")
        if self.generation_id is not None:
            _registered_id(self.generation_id, "generation_id", "generation")
        _refs(self.fields, "fields", fields=True)
        _refs(self.input_artifact_refs, "input_artifact_refs")
        _refs(self.output_artifact_refs, "output_artifact_refs")
        _optional_date(self.date_start, "date_start")
        _optional_date(self.date_end, "date_end")
        if self.date_start is not None and self.date_end is not None and self.date_start > self.date_end:
            raise ValueError("date_start must not be later than date_end")
        object.__setattr__(self, "taint_in", _taints(self.taint_in, "taint_in"))
        object.__setattr__(self, "taint_out", _taints(self.taint_out, "taint_out"))
        if Taint.FINAL_HOLDOUT in self.taint_in or Taint.FINAL_HOLDOUT in self.taint_out:
            raise FinalHoldoutUnavailable("FINAL_HOLDOUT taint is unavailable in P3")
        if self.operation in (AccessOperation.READ, AccessOperation.MATERIALIZE) and not self.output_artifact_refs:
            raise ValueError("READ and MATERIALIZE require an output artifact")
        if self.operation is AccessOperation.DERIVE and (
            not self.input_artifact_refs or not self.output_artifact_refs
        ):
            raise ValueError("DERIVE requires input and output artifacts")
        if self.operation in (AccessOperation.DISPLAY, AccessOperation.CONSUME, AccessOperation.EXPORT) and not self.input_artifact_refs:
            raise ValueError("DISPLAY, CONSUME, and EXPORT require an input artifact")
        if self.output_artifact_refs and not self.taint_out:
            raise ValueError("published artifacts require a taint projection")
        if not isinstance(self.metadata, tuple):
            raise TypeError("metadata must be a tuple of key/value pairs")
        metadata_keys: list[str] = []
        for key, value in self.metadata:
            _nonempty(key, "metadata key")
            _nonempty(value, "metadata value")
            metadata_keys.append(key)
            if key not in _ALLOWED_METADATA_KEYS:
                raise ValueError("metadata key is not in the bounded allowlist")
            if any(token in key.lower() for token in _FORBIDDEN_METADATA):
                raise ValueError("raw payloads, logs, labels, and secrets are forbidden")
            if key in _SHA256_KEYS and re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError("hash metadata must be a SHA-256 digest")
            if key in _COUNT_KEYS and (not value.isdigit() or len(value) > 12):
                raise ValueError("count metadata must be a bounded decimal integer")
            if key in _OPAQUE_ID_KEYS and re.fullmatch(r"[A-Za-z0-9_.:/-]{1,256}", value) is None:
                raise ValueError("identifier metadata must be an opaque bounded reference")
            if key not in _SHA256_KEYS and key not in _COUNT_KEYS and key not in _OPAQUE_ID_KEYS:
                if value not in _CLASSIFICATION_VALUES:
                    raise ValueError("classification metadata must use a closed value")
        if len(metadata_keys) != len(set(metadata_keys)):
            raise ValueError("metadata keys must be unique")
        if self.sequence is not None and (type(self.sequence) is not int or self.sequence < 1):
            raise ValueError("sequence must be a positive integer")
        if self.occurred_at is not None and (
            not isinstance(self.occurred_at, datetime) or self.occurred_at.tzinfo is None
        ):
            raise ValueError("occurred_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class FoldTestHandle:
    artifact_ref: str
    event_id: str
    consumed_at: datetime


@dataclass(frozen=True, slots=True)
class FoldTestCapability:
    candidate_id: str
    protocol_id: str
    fold_id: str
    artifact_ref: str
    run_id: str
    actor_id: str
    invocation_id: str
    _token: str
    _seal: object = field(default=None, repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class AccessRootCapability:
    artifact_ref: str
    dataset_role: DatasetRole
    taint: Taint
    actor_id: str
    invocation_id: str
    _token: str
    _seal: object = field(default=None, repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class FrozenAccessRegistration:
    registry_ref: str
    registry_sha256: str
    artifact_ref: str
    dataset_role: DatasetRole
    taint: Taint
    fold_attempts: tuple[tuple[str, str, str, str], ...]
    grant_id: str
    actor: Actor
    _seal: object = field(default=None, repr=False, compare=False)


def load_frozen_access_registration(
    *, authority_lease: object, repository_root: str, registry_ref: str,
    artifact_ref: str,
) -> FrozenAccessRegistration:
    if (
        not isinstance(authority_lease, stores.TaskExecutionLease)
        or authority_lease.phase is not Phase.P3
        or not {SideEffect.READ, SideEffect.WRITE_CONTROL_PLANE}.issubset(
            authority_lease.allowed_side_effects
        )
    ):
        raise PermissionError("an active P3 READ+WRITE task lease is required")
    _nonempty(registry_ref, "registry_ref")
    if ".." in registry_ref or re.fullmatch(r"[A-Za-z0-9_./-]{1,512}", registry_ref) is None:
        raise ValueError("registry_ref must be a safe repository-relative path")
    _refs((artifact_ref,), "artifact_ref")

    def authorize(connection):
        row = stores._AuthorityStore._require_task_lease(connection, authority_lease)
        task_spec = json.loads(str(row["task_spec_payload_json"]))
        matches = [
            item for item in task_spec.get("input_evidence_refs", ())
            if item.get("evidence_ref") == registry_ref
        ]
        if len(matches) != 1 or matches[0].get("status") != "VERIFIED":
            raise PermissionError("access registry is not frozen by the task spec")
        return str(matches[0].get("evidence_sha256", ""))

    expected_sha = _SqliteUnitOfWork(stores._authority_spec())._read(authorize)
    root = Path(repository_root).resolve()
    path = (root / registry_ref).resolve()
    if root not in path.parents or not path.is_file():
        raise PermissionError("access registry path is unavailable or unsafe")
    raw = path.read_bytes()
    observed_sha = hashlib.sha256(raw).hexdigest()
    if not hmac.compare_digest(expected_sha, observed_sha):
        raise PermissionError("access registry hash does not match the task spec")
    try:
        registry = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("access registry must be canonical JSON") from error
    if registry.get("schema_version") != "control_plane.access_registry.v1":
        raise ValueError("unsupported access registry schema")
    entries = registry.get("artifacts")
    if not isinstance(entries, list) or len(entries) > 4096:
        raise ValueError("access registry artifacts are invalid")
    selected = [entry for entry in entries if entry.get("artifact_ref") == artifact_ref]
    if len(selected) != 1:
        raise PermissionError("artifact is absent or ambiguous in the frozen registry")
    entry = selected[0]
    if set(entry) != {"artifact_ref", "dataset_role", "taint", "fold_attempts"}:
        raise ValueError("access registry artifact has unexpected fields")
    role = DatasetRole(entry["dataset_role"])
    taint = Taint(entry["taint"])
    if role is DatasetRole.FINAL_HOLDOUT or taint is Taint.FINAL_HOLDOUT:
        raise FinalHoldoutUnavailable("FINAL_HOLDOUT is unavailable in P3")
    attempts = []
    if not isinstance(entry["fold_attempts"], list) or len(entry["fold_attempts"]) > 1024:
        raise ValueError("frozen fold attempts are invalid")
    for attempt in entry["fold_attempts"]:
        if not isinstance(attempt, dict) or set(attempt) != {
            "candidate_id", "protocol_id", "fold_id", "run_id"
        }:
            raise ValueError("frozen fold attempt has unexpected fields")
        values = (
            _registered_id(attempt["candidate_id"], "candidate_id", "candidate"),
            _registered_id(attempt["protocol_id"], "protocol_id", "protocol"),
            _registered_id(attempt["fold_id"], "fold_id", "fold"),
            _registered_id(attempt["run_id"], "run_id", "run"),
        )
        attempts.append(values)
    if role is DatasetRole.FOLD_TEST and not attempts:
        raise ValueError("FOLD_TEST registration requires frozen attempts")
    if role is not DatasetRole.FOLD_TEST and attempts:
        raise ValueError("only FOLD_TEST registrations may contain attempts")
    return FrozenAccessRegistration(
        registry_ref, observed_sha, artifact_ref, role, taint, tuple(attempts),
        authority_lease.grant_id, authority_lease.actor, _REGISTRY_SEAL,
    )


def _grant_snapshot_is_active(grant: object) -> None:
    if not isinstance(grant, stores.AuthorityGrant) or grant.phase is not Phase.P3:
        raise PermissionError("an active P3 AuthorityGrant is required")
    required = {SideEffect.READ, SideEffect.WRITE_CONTROL_PLANE}
    if not required.issubset(grant.allowed_side_effects):
        raise PermissionError("P3 access requires READ and WRITE_CONTROL_PLANE authority")
    try:
        _SqliteUnitOfWork(stores._authority_spec())._read(
            lambda connection: stores._AuthorityStore._require_active_grant(
                connection, grant
            )
        )
    except stores.AuthorizationRejectedError as error:
        raise PermissionError("the P3 AuthorityGrant is invalid or inactive") from error


def issue_root_capability(*, grant: object,
                          registration: FrozenAccessRegistration,
                          actor: Actor) -> AccessRootCapability:
    _grant_snapshot_is_active(grant)
    if grant.actor != actor:
        raise PermissionError("capability actor does not match the AuthorityGrant")
    if (
        not isinstance(registration, FrozenAccessRegistration)
        or registration._seal is not _REGISTRY_SEAL
        or registration.grant_id != grant.grant_id
        or registration.actor != actor
    ):
        raise PermissionError("a matching frozen access registration is required")
    artifact_ref = registration.artifact_ref
    dataset_role = registration.dataset_role
    taint = registration.taint
    payload = canonical_json({
        "grant_id": grant.grant_id, "artifact_ref": artifact_ref,
        "dataset_role": dataset_role.value, "taint": taint.value,
        "actor_id": actor.actor_id, "actor_type": actor.actor_type,
        "invocation_id": actor.invocation_id,
    }).encode()
    token = hmac.new(
        grant._bearer_secret._reveal_for_authority_check().encode(), payload, hashlib.sha256
    ).hexdigest()
    return AccessRootCapability(artifact_ref, dataset_role, taint, actor.actor_id,
                                actor.invocation_id, token, _CAPABILITY_SEAL)


def issue_fold_test_capability(*, grant: object, candidate_id: str, protocol_id: str,
                               fold_id: str, artifact_ref: str, run_id: str,
                               actor: Actor,
                               registration: FrozenAccessRegistration) -> FoldTestCapability:
    """Create a one-fold capability from an active trusted P3 AuthorityGrant."""
    _grant_snapshot_is_active(grant)
    if grant.actor != actor:
        raise PermissionError("capability actor does not match the AuthorityGrant")
    for name, value in (("candidate_id", candidate_id), ("protocol_id", protocol_id),
                        ("fold_id", fold_id), ("artifact_ref", artifact_ref), ("run_id", run_id)):
        _nonempty(value, name)
    _registered_id(candidate_id, "candidate_id", "candidate")
    _registered_id(protocol_id, "protocol_id", "protocol")
    _registered_id(fold_id, "fold_id", "fold")
    _registered_id(run_id, "run_id", "run")
    _refs((artifact_ref,), "artifact_ref")
    if (
        not isinstance(registration, FrozenAccessRegistration)
        or registration._seal is not _REGISTRY_SEAL
        or registration.grant_id != grant.grant_id
        or registration.actor != actor
        or registration.artifact_ref != artifact_ref
        or registration.dataset_role is not DatasetRole.FOLD_TEST
        or (candidate_id, protocol_id, fold_id, run_id) not in registration.fold_attempts
    ):
        raise PermissionError("fold-test tuple is absent from the frozen access registry")
    payload = canonical_json({
        "grant_id": grant.grant_id, "candidate_id": candidate_id,
        "protocol_id": protocol_id, "fold_id": fold_id,
        "artifact_ref": artifact_ref, "run_id": run_id,
        "actor_id": actor.actor_id, "actor_type": actor.actor_type,
        "invocation_id": actor.invocation_id,
    }).encode()
    token = hmac.new(
        grant._bearer_secret._reveal_for_authority_check().encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return FoldTestCapability(candidate_id, protocol_id, fold_id, artifact_ref,
                              run_id, actor.actor_id, actor.invocation_id, token,
                              _CAPABILITY_SEAL)


def _canonical_event(event: AccessEvent) -> tuple[str, str]:
    payload = {
        "event_id": event.event_id,
        "operation": event.operation.value,
        "actor_id": event.actor_id,
        "actor_type": event.actor_type,
        "invocation_id": event.invocation_id,
        "run_id": event.run_id,
        "generation_id": event.generation_id,
        "dataset_role": event.dataset_role.value,
        "fields": list(event.fields),
        "date_start": event.date_start,
        "date_end": event.date_end,
        "input_artifact_refs": list(event.input_artifact_refs),
        "output_artifact_refs": list(event.output_artifact_refs),
        "taint_in": [value.value for value in event.taint_in],
        "taint_out": [value.value for value in event.taint_out],
        "metadata": [list(pair) for pair in event.metadata],
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    digest = hashlib.sha256(raw.encode()).hexdigest()
    if len(raw.encode()) > _MAX_EVENT_BYTES:
        raise ValueError("access event exceeds bounded metadata limit")
    return raw, digest


def _utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _parse_event(row: Mapping[str, object]) -> AccessEvent:
    return AccessEvent(
        event_id=str(row["event_id"]),
        operation=AccessOperation(str(row["operation"])),
        actor_id=str(row["actor_id"]),
        actor_type=str(row["actor_type"]),
        invocation_id=str(row["invocation_id"]),
        run_id=str(row["run_id"]),
        generation_id=None if row["generation_id"] is None else str(row["generation_id"]),
        dataset_role=DatasetRole(str(row["dataset_role"])),
        fields=tuple(json.loads(str(row["fields_json"]))),
        date_start=None if row["date_start"] is None else str(row["date_start"]),
        date_end=None if row["date_end"] is None else str(row["date_end"]),
        input_artifact_refs=tuple(json.loads(str(row["input_refs_json"]))),
        output_artifact_refs=tuple(json.loads(str(row["output_refs_json"]))),
        taint_in=tuple(Taint(value) for value in json.loads(str(row["taint_in_json"]))),
        taint_out=tuple(Taint(value) for value in json.loads(str(row["taint_out_json"]))),
        metadata=tuple(tuple(pair) for pair in json.loads(str(row["metadata_json"]))),
        sequence=int(row["sequence"]),
        occurred_at=datetime.fromisoformat(str(row["occurred_at"])),
    )


def _union_taints(values: list[tuple[Taint, ...]]) -> tuple[Taint, ...]:
    merged = {taint for group in values for taint in group}
    if len(merged) > 1:
        merged.discard(Taint.CLEAN)
    return tuple(sorted(merged or {Taint.CLEAN}, key=lambda value: _TAINT_ORDER[value]))


class AccessJournal:
    __slots__ = ("_clock", "_grant")

    def __init__(self, *, root_secret: str, grant: object,
                 clock: Callable[[], datetime] | None = None) -> None:
        stores._migrate_operational_journal_v4(root_secret=root_secret)
        stores._require_store_root(stores._operational_spec(), root_secret)
        _grant_snapshot_is_active(grant)
        self._grant = grant
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _now(self) -> datetime:
        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise ValueError("clock result must be timezone-aware")
        return now

    def append(self, event: AccessEvent) -> AccessEvent:
        if not isinstance(event, AccessEvent):
            raise TypeError("event must be an AccessEvent")
        if event.dataset_role is DatasetRole.FINAL_HOLDOUT:
            raise FinalHoldoutUnavailable("FINAL_HOLDOUT is unavailable in P3")
        if event.dataset_role is DatasetRole.FOLD_TEST and event.operation in (
            AccessOperation.READ,
            AccessOperation.MATERIALIZE,
        ):
            raise FinalHoldoutUnavailable(
                "FOLD_TEST reads must be preceded by a FoldTestBroker consumption"
            )
        _grant_snapshot_is_active(self._grant)
        self._require_grant_actor(event)
        if event.operation in (AccessOperation.READ, AccessOperation.MATERIALIZE) and not event.input_artifact_refs:
            raise PermissionError("root access requires a trusted AccessRootCapability")
        _, payload_sha = _canonical_event(event)
        occurred = event.occurred_at or self._now()
        def write(connection):
            return self._insert_event(connection, event, payload_sha, occurred)
        return _SqliteUnitOfWork(stores._operational_spec())._write(write)

    def append_root(self, event: AccessEvent, capability: AccessRootCapability) -> AccessEvent:
        if not isinstance(capability, AccessRootCapability) or capability._seal is not _CAPABILITY_SEAL:
            raise TypeError("a trusted AccessRootCapability is required")
        _grant_snapshot_is_active(self._grant)
        self._require_grant_actor(event)
        if event.operation not in (AccessOperation.READ, AccessOperation.MATERIALIZE) or event.input_artifact_refs:
            raise ValueError("root capability can only authorize input-less READ/MATERIALIZE")
        if event.output_artifact_refs != (capability.artifact_ref,) or event.dataset_role is not capability.dataset_role or event.taint_out != (capability.taint,):
            raise PermissionError("root event does not match its capability")
        payload = canonical_json({
            "grant_id": self._grant.grant_id, "artifact_ref": capability.artifact_ref,
            "dataset_role": capability.dataset_role.value, "taint": capability.taint.value,
            "actor_id": capability.actor_id, "actor_type": self._grant.actor.actor_type,
            "invocation_id": capability.invocation_id,
        }).encode()
        expected = hmac.new(self._grant._bearer_secret._reveal_for_authority_check().encode(), payload, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, capability._token) or (event.actor_id, event.actor_type, event.invocation_id) != (capability.actor_id, self._grant.actor.actor_type, capability.invocation_id):
            raise PermissionError("root capability proof is invalid")
        _, payload_sha = _canonical_event(event)
        occurred = event.occurred_at or self._now()
        return _SqliteUnitOfWork(stores._operational_spec())._write(
            lambda connection: self._insert_event(connection, event, payload_sha, occurred)
        )

    def _require_grant_actor(self, event: AccessEvent) -> None:
        if (event.actor_id, event.actor_type, event.invocation_id) != (
            self._grant.actor.actor_id,
            self._grant.actor.actor_type,
            self._grant.actor.invocation_id,
        ):
            raise PermissionError("access event actor does not match the AuthorityGrant")

    def _insert_event(self, connection, event: AccessEvent, payload_sha: str, occurred: datetime,
                      *, allow_fold_consume: bool = False) -> AccessEvent:
        existing = connection.execute(
            "SELECT * FROM access_events WHERE event_id = ?", (event.event_id,)
        ).fetchone()
        if existing is not None:
            observed = _parse_event(existing)
            if existing["payload_sha256"] != payload_sha or (
                event.sequence is not None and int(existing["sequence"]) != event.sequence
            ) or (
                event.occurred_at is not None
                and str(existing["occurred_at"]) != _utc(event.occurred_at)
            ):
                raise AccessConflictError("event_id content conflict")
            return observed
        projected_operation = event.operation in (
            AccessOperation.DERIVE,
            AccessOperation.DISPLAY,
            AccessOperation.CONSUME,
            AccessOperation.EXPORT,
        ) or (
            event.operation is AccessOperation.MATERIALIZE
            and bool(event.input_artifact_refs)
        )
        if projected_operation:
            self._validate_projection_inputs(connection, event)
            expected_in = _union_taints([
                self._taints_for_connection(connection, ref) for ref in event.input_artifact_refs
            ])
            if event.operation is AccessOperation.DERIVE and (
                event.dataset_role is DatasetRole.FOLD_TEST or Taint.TEST_LABEL in expected_in
            ):
                expected_out = _union_taints([expected_in, (Taint.TEST_DERIVED,)])
            else:
                expected_out = expected_in
            if event.operation in (AccessOperation.CONSUME, AccessOperation.EXPORT) and Taint.INVALID in expected_in:
                raise InvalidTaintError("INVALID taint cannot be consumed or exported")
            if event.taint_in != expected_in or event.taint_out != expected_out:
                raise InvalidTaintError("declared taint does not match projected input taint")
        self._validate_fold_provenance(connection, event, allow_fold_consume=allow_fold_consume)
        if event.sequence is not None:
            raise AccessConflictError("new events cannot preassign a sequence")
        _canonical_event(event)
        connection.execute(
            """INSERT INTO access_events
            (event_id, operation, actor_id, actor_type, invocation_id, run_id,
             generation_id, dataset_role, fields_json, date_start, date_end,
             input_refs_json, output_refs_json, taint_in_json, taint_out_json,
             metadata_json, payload_sha256, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (event.event_id, event.operation.value, event.actor_id, event.actor_type,
             event.invocation_id, event.run_id, event.generation_id, event.dataset_role.value,
             json.dumps(list(event.fields), separators=(",", ":")), event.date_start, event.date_end,
             json.dumps(list(event.input_artifact_refs), separators=(",", ":")),
             json.dumps(list(event.output_artifact_refs), separators=(",", ":")),
             json.dumps([value.value for value in event.taint_in], separators=(",", ":")),
             json.dumps([value.value for value in event.taint_out], separators=(",", ":")),
             json.dumps([list(pair) for pair in event.metadata], ensure_ascii=False, separators=(",", ":")),
             payload_sha, _utc(occurred)),
        )
        sequence = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        stored = replace(event, sequence=sequence, occurred_at=occurred)
        integrity_row = connection.execute(
            "SELECT * FROM access_events WHERE sequence = ?", (sequence,)
        ).fetchone()
        row_sha256 = stores._access_row_sha256(
            {field: integrity_row[field] for field in stores._ACCESS_ROW_FIELDS}
        )
        previous = connection.execute(
            "SELECT prefix_sha256 FROM ops_access_event_integrity "
            "ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        previous_prefix = (
            stores._ACCESS_EMPTY_CHAIN_ROOT
            if previous is None
            else str(previous["prefix_sha256"])
        )
        prefix = stores._access_prefix_sha256(previous_prefix, row_sha256)
        connection.execute(
            """INSERT INTO ops_access_event_integrity
            (sequence, event_id, row_sha256, prefix_sha256, occurred_at)
            VALUES (?, ?, ?, ?, ?)""",
            (
                sequence,
                str(integrity_row["event_id"]),
                row_sha256,
                prefix,
                _utc(occurred),
            ),
        )
        connection.execute(
            "UPDATE operational_meta SET value = ? "
            "WHERE key = 'access_integrity_root'",
            (prefix,),
        )
        for output in event.output_artifact_refs:
            for taint in event.taint_out:
                try:
                    connection.execute(
                        "INSERT INTO taint_projection(artifact_ref, taint, source_event_id) VALUES (?, ?, ?)",
                        (output, taint.value, event.event_id),
                    )
                except Exception as error:
                    raise LineageError("artifact lineage root is already fixed") from error
        if projected_operation and event.output_artifact_refs:
            for input_ref in event.input_artifact_refs:
                if input_ref in event.output_artifact_refs or self._would_cycle(connection, input_ref, event.output_artifact_refs):
                    raise LineageError("derivation cycle detected")
                for output in event.output_artifact_refs:
                    connection.execute(
                        "INSERT INTO derivation_edges(input_ref, output_ref, event_id) VALUES (?, ?, ?)",
                        (input_ref, output, event.event_id),
                    )
        return stored

    def _taints_for_connection(self, connection, ref: str) -> tuple[Taint, ...]:
        rows = connection.execute(
            "SELECT taint FROM taint_projection WHERE artifact_ref = ? ORDER BY taint",
            (ref,),
        ).fetchall()
        if not rows:
            raise LineageError(f"unknown lineage root: {ref}")
        return tuple(sorted((Taint(str(row[0])) for row in rows), key=lambda value: _TAINT_ORDER[value]))

    def _roles_for_connection(self, connection, ref: str) -> set[DatasetRole]:
        pending = [ref]
        seen: set[str] = set()
        roles: set[DatasetRole] = set()
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            rows = connection.execute(
                """SELECT DISTINCT e.dataset_role
                   FROM access_events e
                   JOIN taint_projection p ON p.source_event_id = e.event_id
                   WHERE p.artifact_ref = ?""",
                (current,),
            ).fetchall()
            roles.update(DatasetRole(str(row[0])) for row in rows)
            pending.extend(
                row[0] for row in connection.execute(
                    "SELECT input_ref FROM derivation_edges WHERE output_ref = ?",
                    (current,),
                )
            )
        return roles

    def _has_fold_provenance(self, connection, refs: tuple[str, ...]) -> bool:
        return any(
            DatasetRole.FOLD_TEST in self._roles_for_connection(connection, ref)
            for ref in refs
        )

    def _fold_roots_for_ref(self, connection, ref: str) -> set[str]:
        pending = [ref]
        seen: set[str] = set()
        roots: set[str] = set()
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            parents = [
                str(row[0])
                for row in connection.execute(
                    "SELECT input_ref FROM derivation_edges WHERE output_ref = ?",
                    (current,),
                )
            ]
            roles = self._roles_for_connection(connection, current)
            if DatasetRole.FOLD_TEST in roles and not parents:
                roots.add(current)
            pending.extend(parents)
        return roots

    def _validate_fold_provenance(self, connection, event: AccessEvent,
                                  *, allow_fold_consume: bool = False) -> None:
        if not self._has_fold_provenance(connection, event.input_artifact_refs):
            return
        if allow_fold_consume and event.operation is AccessOperation.CONSUME:
            return
        protected_roots = set().union(*(
            self._fold_roots_for_ref(connection, ref)
            for ref in event.input_artifact_refs
        ))
        covered_roots = self._fold_attempts_for_event(connection, event)
        if not protected_roots.issubset(covered_roots):
            raise PermissionError("FOLD_TEST provenance requires a consumed capability")

    def _fold_attempts_for_event(self, connection, event: AccessEvent) -> set[str]:
        covered: set[str] = set()
        for row in connection.execute(
            """SELECT a.actor_id, a.invocation_id, a.run_id, e.input_refs_json
               FROM fold_test_attempts a
               JOIN access_events e ON e.event_id = a.event_id"""
        ):
            refs = tuple(json.loads(str(row["input_refs_json"])))
            if (
                str(row["actor_id"]), str(row["invocation_id"]), str(row["run_id"])
            ) == (event.actor_id, event.invocation_id, event.run_id):
                covered.update(refs)
        return covered

    def _validate_projection_inputs(self, connection, event: AccessEvent) -> None:
        for ref in event.input_artifact_refs:
            self._taints_for_connection(connection, ref)

    def _validate_projected_taint(self, connection, event: AccessEvent) -> None:
        projected = event.operation in (
            AccessOperation.DERIVE, AccessOperation.DISPLAY,
            AccessOperation.CONSUME, AccessOperation.EXPORT,
        ) or (event.operation is AccessOperation.MATERIALIZE and bool(event.input_artifact_refs))
        if not projected:
            return
        self._validate_projection_inputs(connection, event)
        expected_in = _union_taints([
            self._taints_for_connection(connection, ref) for ref in event.input_artifact_refs
        ])
        expected_out = expected_in
        if event.operation is AccessOperation.DERIVE and (
            event.dataset_role is DatasetRole.FOLD_TEST or Taint.TEST_LABEL in expected_in
        ):
            expected_out = _union_taints([expected_in, (Taint.TEST_DERIVED,)])
        if event.operation in (AccessOperation.CONSUME, AccessOperation.EXPORT) and Taint.INVALID in expected_in:
            raise InvalidTaintError("INVALID taint cannot be consumed or exported")
        if event.taint_in != expected_in or event.taint_out != expected_out:
            raise InvalidTaintError("declared taint does not match projected input taint")

    def _would_cycle(self, connection, input_ref: str, outputs: tuple[str, ...]) -> bool:
        pending = list(outputs)
        seen: set[str] = set()
        while pending:
            current = pending.pop()
            if current == input_ref:
                return True
            if current in seen:
                continue
            seen.add(current)
            pending.extend(row[0] for row in connection.execute(
                "SELECT output_ref FROM derivation_edges WHERE input_ref = ?", (current,)
            ))
        return False

    def list_for_run(self, run_id: str) -> tuple[AccessEvent, ...]:
        _nonempty(run_id, "run_id")
        return tuple(self._read_events("SELECT * FROM access_events WHERE run_id = ? ORDER BY sequence", (run_id,)))

    def replay(self) -> tuple[AccessEvent, ...]:
        return tuple(self._read_events("SELECT * FROM access_events ORDER BY sequence", ()))

    def _read_events(self, query: str, params: tuple[object, ...]) -> list[AccessEvent]:
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: [_parse_event(row) for row in connection.execute(query, params).fetchall()]
        )

    def taint_for(self, artifact_ref: str) -> tuple[Taint, ...]:
        _nonempty(artifact_ref, "artifact_ref")
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._taints_for_connection(connection, artifact_ref)
        )

    def rebuild_taint_projection(self) -> None:
        def rebuild(connection):
            events = [_parse_event(row) for row in connection.execute(
                "SELECT * FROM access_events ORDER BY sequence"
            ).fetchall()]
            connection.execute("DELETE FROM taint_projection")
            connection.execute("DELETE FROM derivation_edges")
            for event in events:
                payload_sha = _canonical_event(event)[1]
                row = connection.execute(
                    "SELECT payload_sha256 FROM access_events WHERE event_id = ?", (event.event_id,)
                ).fetchone()
                if row is None or row[0] != payload_sha:
                    raise AccessConflictError("access event integrity mismatch")
                self._validate_fold_provenance(connection, event)
                self._validate_projected_taint(connection, event)
                for output in event.output_artifact_refs:
                    for taint in event.taint_out:
                        connection.execute(
                            "INSERT INTO taint_projection(artifact_ref, taint, source_event_id) VALUES (?, ?, ?)",
                            (output, taint.value, event.event_id),
                        )
                projected_operation = event.operation in (
                    AccessOperation.DERIVE,
                    AccessOperation.DISPLAY,
                    AccessOperation.CONSUME,
                    AccessOperation.EXPORT,
                ) or (
                    event.operation is AccessOperation.MATERIALIZE
                    and bool(event.input_artifact_refs)
                )
                if projected_operation and event.output_artifact_refs:
                    for input_ref in event.input_artifact_refs:
                        for output in event.output_artifact_refs:
                            connection.execute(
                                "INSERT INTO derivation_edges(input_ref, output_ref, event_id) VALUES (?, ?, ?)",
                                (input_ref, output, event.event_id),
                            )
        _SqliteUnitOfWork(stores._operational_spec())._write(rebuild)


class TaintGraph:
    def __init__(self, journal: AccessJournal) -> None:
        if not isinstance(journal, AccessJournal):
            raise TypeError("journal must be an AccessJournal")
        self._journal = journal

    def taint_for(self, artifact_ref: str) -> tuple[Taint, ...]:
        return self._journal.taint_for(artifact_ref)

    def derive(self, *, inputs: tuple[str, ...], output: str, event_id: str,
               actor: Actor, run_id: str, dataset_role: DatasetRole,
               generation_id: str | None = None) -> AccessEvent:
        taint_in = _union_taints([self.taint_for(ref) for ref in inputs])
        taint_out = _union_taints([taint_in, (Taint.TEST_DERIVED,)] if (
            dataset_role is DatasetRole.FOLD_TEST or Taint.TEST_LABEL in taint_in
        ) else [taint_in])
        return self._journal.append(AccessEvent(
            event_id=event_id, operation=AccessOperation.DERIVE,
            actor_id=actor.actor_id, actor_type=actor.actor_type, invocation_id=actor.invocation_id,
            run_id=run_id, dataset_role=dataset_role, generation_id=generation_id,
            input_artifact_refs=inputs, output_artifact_refs=(output,),
            taint_in=taint_in, taint_out=taint_out,
        ))

    def expose(self, *, operation: AccessOperation, inputs: tuple[str, ...], outputs: tuple[str, ...],
               event_id: str, actor: Actor, run_id: str, dataset_role: DatasetRole,
               generation_id: str | None = None) -> AccessEvent:
        if operation not in (AccessOperation.DISPLAY, AccessOperation.CONSUME, AccessOperation.EXPORT):
            raise ValueError("expose supports DISPLAY, CONSUME, or EXPORT")
        taint = _union_taints([self.taint_for(ref) for ref in inputs])
        return self._journal.append(AccessEvent(
            event_id=event_id, operation=operation, actor_id=actor.actor_id,
            actor_type=actor.actor_type, invocation_id=actor.invocation_id, run_id=run_id,
            dataset_role=dataset_role, generation_id=generation_id,
            input_artifact_refs=inputs, output_artifact_refs=outputs,
            taint_in=taint, taint_out=taint,
        ))


class FoldTestBroker:
    def __init__(self, journal: AccessJournal, capability: FoldTestCapability, grant: object) -> None:
        if not isinstance(capability, FoldTestCapability) or capability._seal is not _CAPABILITY_SEAL:
            raise TypeError("a trusted frozen FoldTestCapability is required")
        _grant_snapshot_is_active(grant)
        if grant != journal._grant:
            raise PermissionError("fold-test grant does not match the AccessJournal grant")
        payload = canonical_json({
            "grant_id": grant.grant_id, "candidate_id": capability.candidate_id,
            "protocol_id": capability.protocol_id, "fold_id": capability.fold_id,
            "artifact_ref": capability.artifact_ref, "run_id": capability.run_id,
            "actor_id": capability.actor_id, "actor_type": grant.actor.actor_type,
            "invocation_id": capability.invocation_id,
        }).encode()
        expected = hmac.new(
            grant._bearer_secret._reveal_for_authority_check().encode(), payload, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, capability._token):
            raise PermissionError("fold-test capability proof is invalid")
        self._journal = journal
        self._capability = capability
        self._grant = grant

    def consume_once(self, *, candidate_id: str, protocol_id: str, fold_id: str,
                     artifact_ref: str, event_id: str, actor: Actor,
                     run_id: str) -> FoldTestHandle:
        for name, value in (("candidate_id", candidate_id), ("protocol_id", protocol_id), ("fold_id", fold_id), ("artifact_ref", artifact_ref), ("run_id", run_id)):
            _nonempty(value, name)
        _registered_id(candidate_id, "candidate_id", "candidate")
        _registered_id(protocol_id, "protocol_id", "protocol")
        _registered_id(fold_id, "fold_id", "fold")
        _registered_id(run_id, "run_id", "run")
        _refs((artifact_ref,), "artifact_ref")
        capability = self._capability
        if (candidate_id, protocol_id, fold_id, artifact_ref, run_id) != (
            capability.candidate_id, capability.protocol_id, capability.fold_id,
            capability.artifact_ref, capability.run_id,
        ) or (actor.actor_id, actor.actor_type, actor.invocation_id) != (
            capability.actor_id, self._grant.actor.actor_type, capability.invocation_id
        ):
            raise PermissionError("fold-test request does not match its frozen capability")
        _grant_snapshot_is_active(self._grant)
        occurred = self._journal._now()
        def consume(connection):
            existing = connection.execute(
                "SELECT 1 FROM fold_test_attempts WHERE candidate_id = ? AND protocol_id = ? AND fold_id = ?",
                (candidate_id, protocol_id, fold_id),
            ).fetchone()
            if existing is not None:
                raise FoldTestAlreadyConsumed("FOLD_TEST attempt already consumed")
            for prior in connection.execute(
                """SELECT e.input_refs_json
                   FROM fold_test_attempts a
                   JOIN access_events e ON e.event_id = a.event_id"""
            ):
                if artifact_ref in tuple(json.loads(str(prior[0]))):
                    raise FoldTestAlreadyConsumed(
                        "FOLD_TEST artifact was already consumed by a frozen attempt"
                    )
            roles = self._journal._roles_for_connection(connection, artifact_ref)
            if roles != {DatasetRole.FOLD_TEST}:
                raise PermissionError("artifact is not registered as a FOLD_TEST source")
            taint = self._journal._taints_for_connection(connection, artifact_ref)
            if Taint.INVALID in taint:
                raise InvalidTaintError("INVALID taint cannot be consumed")
            event = AccessEvent(
                event_id=event_id, operation=AccessOperation.CONSUME,
                actor_id=actor.actor_id, actor_type=actor.actor_type,
                invocation_id=actor.invocation_id, run_id=run_id,
                dataset_role=DatasetRole.FOLD_TEST,
                input_artifact_refs=(artifact_ref,), taint_in=taint, taint_out=taint,
            )
            stored = self._journal._insert_event(
                connection, event, _canonical_event(event)[1], occurred,
                allow_fold_consume=True,
            )
            connection.execute(
                "INSERT INTO fold_test_attempts(candidate_id, protocol_id, fold_id, event_id, actor_id, invocation_id, run_id, consumed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (candidate_id, protocol_id, fold_id, stored.event_id, actor.actor_id,
                 actor.invocation_id, run_id, _utc(occurred)),
            )
            return FoldTestHandle(artifact_ref, stored.event_id, occurred)
        return _SqliteUnitOfWork(stores._operational_spec())._write(consume)


__all__ = [
    "AccessConflictError", "AccessError", "AccessEvent", "AccessJournal",
    "AccessOperation", "DatasetRole", "FinalHoldoutUnavailable", "FoldTestAlreadyConsumed",
    "FoldTestBroker", "FoldTestCapability", "FoldTestHandle", "InvalidTaintError",
    "LineageError", "Taint", "TaintGraph", "issue_fold_test_capability",
    "AccessRootCapability", "issue_root_capability",
    "FrozenAccessRegistration", "load_frozen_access_registration",
]
