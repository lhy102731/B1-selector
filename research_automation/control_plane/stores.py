"""Physically isolated stores owned by the research control plane."""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
import os
from pathlib import Path
import re
import secrets
from typing import Callable

from .contracts import Actor, Phase, SideEffect
from .sqlite_uow import (
    _SqliteUnitOfWork,
    _StoreSpec,
    _schema_sha256_for_statements,
)


_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_AUTHORITY_STORE_PATH = (
    _REPOSITORY_ROOT
    / "research_state"
    / "control_plane"
    / "authority"
    / "authority.sqlite3"
)
_OPERATIONAL_STORE_PATH = (
    _REPOSITORY_ROOT
    / "research_state"
    / "control_plane"
    / "operational"
    / "operational.sqlite3"
)
_TASK_SPEC_FIELDS = frozenset(
    {
        "task_id",
        "objective",
        "dependencies",
        "idempotency_key",
        "task_spec_ref",
        "task_spec_sha256",
        "requirements",
        "allowed_files",
        "forbidden_files",
        "baseline_ref",
        "baseline_sha256",
        "input_evidence_refs",
    }
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_STORE_SCHEMA_VERSION = 1
_AUTHORITY_SCHEMA = (
    """
    CREATE TABLE authorizations_v2 (
        authorization_ref TEXT PRIMARY KEY,
        phase TEXT NOT NULL,
        attempt_id TEXT NOT NULL,
        actor_id TEXT NOT NULL,
        actor_type TEXT NOT NULL,
        invocation_id TEXT NOT NULL,
        plan_hash TEXT NOT NULL,
        scope_hash TEXT NOT NULL,
        instruction_policy_hash TEXT NOT NULL,
        secret_sha256 TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        allowed_effects_json TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('PENDING', 'CLAIMED', 'EXPIRED')),
        created_at TEXT NOT NULL,
        claimed_at TEXT
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE phase_grants_v2 (
        grant_id TEXT PRIMARY KEY,
        authorization_ref TEXT NOT NULL UNIQUE
            REFERENCES authorizations_v2(authorization_ref),
        phase TEXT NOT NULL,
        attempt_id TEXT NOT NULL,
        actor_id TEXT NOT NULL,
        actor_type TEXT NOT NULL,
        invocation_id TEXT NOT NULL,
        plan_hash TEXT NOT NULL,
        scope_hash TEXT NOT NULL,
        instruction_policy_hash TEXT NOT NULL,
        secret_sha256 TEXT NOT NULL,
        allowed_effects_json TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('ACTIVE', 'CLOSED', 'REVOKED')),
        created_at TEXT NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE task_tickets_v2 (
        ticket_id TEXT PRIMARY KEY,
        grant_id TEXT NOT NULL REFERENCES phase_grants_v2(grant_id),
        phase TEXT NOT NULL,
        attempt_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        task_spec_ref TEXT NOT NULL,
        task_spec_sha256 TEXT NOT NULL,
        task_spec_payload_json TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        allowed_effects_json TEXT NOT NULL,
        secret_sha256 TEXT NOT NULL,
        lease_id TEXT,
        lease_secret_sha256 TEXT,
        state TEXT NOT NULL CHECK(
            state IN ('ISSUED', 'IN_PROGRESS', 'SUCCEEDED', 'FAILED', 'IN_DOUBT')
        ),
        created_at TEXT NOT NULL,
        started_at TEXT,
        completed_at TEXT,
        evidence_ref TEXT,
        UNIQUE(grant_id, idempotency_key)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE trusted_task_receipts_v2 (
        ticket_id TEXT NOT NULL REFERENCES task_tickets_v2(ticket_id),
        receipt_kind TEXT NOT NULL CHECK(
            receipt_kind IN ('TEST', 'REVIEW', 'EVIDENCE')
        ),
        receipt_id TEXT NOT NULL,
        issuer_actor_id TEXT NOT NULL,
        issuer_actor_type TEXT NOT NULL,
        issuer_invocation_id TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
        attestation_sha256 TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY(ticket_id, receipt_kind, receipt_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE authority_outbox (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE,
        event_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
        event_sha256 TEXT NOT NULL,
        created_at TEXT NOT NULL,
        mirrored_at TEXT
    )
    """,
)
_OPERATIONAL_SCHEMA = (
    """
    CREATE TABLE journal_events (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        authority_sequence INTEGER NOT NULL UNIQUE,
        event_id TEXT NOT NULL UNIQUE,
        event_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
        event_sha256 TEXT NOT NULL,
        created_at TEXT NOT NULL,
        mirrored_at TEXT NOT NULL
    )
    """,
)


class StoreError(RuntimeError):
    """Base error for trusted control-plane storage."""


class StoreConfigurationError(StoreError):
    """Raised when fixed store locations violate isolation rules."""


class StoreBootstrapError(StoreError):
    """Raised when trusted first-time provisioning cannot complete."""


class StoreAlreadyBootstrappedError(StoreBootstrapError):
    """Raised when a complete store pair already exists."""


class StoreBootstrapIncompleteError(StoreBootstrapError):
    """Raised when only one fixed store is present."""


class StoreBootstrapInProgressError(StoreBootstrapError):
    """Raised when another trusted bootstrap owns the publication lock."""


class AuthorityRootError(StoreError):
    """Raised when a trusted writer lacks the installation root capability."""


class AuthorizationError(StoreError):
    """Base error for V2 authority-envelope operations."""


class AuthorizationRejectedError(AuthorizationError):
    """Raised when an envelope binding or bearer proof is invalid."""


class AuthorizationReplayError(AuthorizationError):
    """Raised when an authentic one-time envelope was already claimed."""


class AuthorizationExpiredError(AuthorizationError):
    """Raised when an envelope is no longer valid."""


class OutboxConflictError(StoreError):
    """Raised when one event_id is associated with different event content."""


class PendingOutboxError(StoreError):
    """Raised when phase close is attempted before authority mirroring drains."""


class TaskTicketError(StoreError):
    """Base error for trusted task-ticket operations."""


class TaskTicketIdempotencyError(TaskTicketError):
    """Raised when one idempotency key is reused for different semantics."""


class TaskTicketStateError(TaskTicketError):
    """Raised when a ticket transition does not match its expected state."""


class TrustedReceiptConflictError(TaskTicketError):
    """Raised when one trusted receipt identity changes content."""


class TaskReportAuthorityError(StoreError):
    """Raised when TaskReport evidence disagrees with trusted authority facts."""


class _BearerSecret:
    """Opaque in-memory secret whose normal renderings are always redacted."""

    __slots__ = ("__value",)

    def __init__(self, value: str) -> None:
        self.__value = _require_nonempty(value, "bearer secret")

    def __repr__(self) -> str:
        return "<redacted bearer secret>"

    __str__ = __repr__

    def __deepcopy__(self, _memo: dict[int, object]) -> _BearerSecret:
        return self

    def _reveal_for_authority_check(self) -> str:
        return self.__value


def _require_nonempty(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty canonical string")
    return value


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return value


def _root_secret_sha256(value: object) -> str:
    try:
        secret = _require_nonempty(value, "authority root capability")
    except ValueError as error:
        raise AuthorityRootError("authority root capability is invalid") from error
    if len(secret) < 32:
        raise AuthorityRootError("authority root capability is invalid")
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _require_aware_datetime(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return _require_aware_datetime(value, "timestamp").isoformat().replace(
        "+00:00",
        "Z",
    )


def _parse_utc_text(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _require_aware_datetime(parsed, "stored timestamp")


@dataclass(frozen=True, slots=True)
class AuthorityIdentity:
    """Exact P0R2 authority identity; never aliases legacy policy_hash."""

    plan_hash: str
    scope_hash: str
    instruction_policy_hash: str

    def __post_init__(self) -> None:
        for field_name in (
            "plan_hash",
            "scope_hash",
            "instruction_policy_hash",
        ):
            object.__setattr__(
                self,
                field_name,
                _require_sha256(getattr(self, field_name), field_name),
            )


@dataclass(frozen=True, slots=True)
class AuthorizationEnvelope:
    """One-time bearer envelope issued only by the trusted authority broker."""

    authorization_ref: str
    phase: Phase
    attempt_id: str
    actor: Actor
    identity: AuthorityIdentity
    expires_at: datetime
    allowed_side_effects: tuple[SideEffect, ...]
    _bearer_secret: _BearerSecret = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        _require_nonempty(self.authorization_ref, "authorization_ref")
        _require_nonempty(self.attempt_id, "attempt_id")
        if not isinstance(self._bearer_secret, _BearerSecret):
            raise ValueError("bearer secret must be an opaque capability")
        if not isinstance(self.phase, Phase):
            raise ValueError("phase must be a Phase")
        if not isinstance(self.actor, Actor):
            raise ValueError("actor must be an Actor")
        if not isinstance(self.identity, AuthorityIdentity):
            raise ValueError("identity must be an AuthorityIdentity")
        object.__setattr__(
            self,
            "expires_at",
            _require_aware_datetime(self.expires_at, "expires_at"),
        )
        if (
            not self.allowed_side_effects
            or not all(
                isinstance(effect, SideEffect)
                for effect in self.allowed_side_effects
            )
            or len(set(self.allowed_side_effects)) != len(self.allowed_side_effects)
        ):
            raise ValueError("allowed_side_effects must be unique SideEffect values")

    def to_public_dict(self) -> dict[str, object]:
        return {
            "authorization_ref": self.authorization_ref,
            "phase": self.phase.value,
            "attempt_id": self.attempt_id,
            "actor_id": self.actor.actor_id,
            "actor_type": self.actor.actor_type,
            "invocation_id": self.actor.invocation_id,
            "identity": {
                "plan_hash": self.identity.plan_hash,
                "scope_hash": self.identity.scope_hash,
                "instruction_policy_hash": (
                    self.identity.instruction_policy_hash
                ),
            },
            "expires_at": _utc_text(self.expires_at),
            "allowed_side_effects": [
                effect.value for effect in self.allowed_side_effects
            ],
        }


@dataclass(frozen=True, slots=True)
class AuthorityGrant:
    """In-memory grant produced by one successful V2 envelope claim."""

    grant_id: str
    authorization_ref: str
    phase: Phase
    attempt_id: str
    actor: Actor
    identity: AuthorityIdentity
    allowed_side_effects: tuple[SideEffect, ...]
    _bearer_secret: _BearerSecret = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class TaskAuthorityTicket:
    ticket_id: str
    grant_id: str
    authorization_ref: str
    phase: Phase
    attempt_id: str
    task_id: str
    idempotency_key: str
    task_spec_ref: str
    task_spec_sha256: str
    allowed_side_effects: tuple[SideEffect, ...]
    actor: Actor
    identity: AuthorityIdentity
    _bearer_secret: _BearerSecret = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class TaskExecutionLease:
    lease_id: str
    ticket_id: str
    grant_id: str
    authorization_ref: str
    phase: Phase
    attempt_id: str
    task_id: str
    allowed_side_effects: tuple[SideEffect, ...]
    actor: Actor
    identity: AuthorityIdentity
    _bearer_secret: _BearerSecret = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class TaskTicketSnapshot:
    ticket_id: str
    state: str
    evidence_ref: str
    started_at: datetime
    completed_at: datetime


@dataclass(frozen=True, slots=True)
class TrustedReceiptAttestation:
    ticket_id: str
    receipt_kind: str
    receipt_id: str
    issuer: Actor
    payload_json: str
    payload_sha256: str
    attestation_sha256: str


@dataclass(frozen=True)
class StoreBootstrapReceipt:
    """Resolved locations created by one trusted bootstrap operation."""

    authority_path: Path
    operational_path: Path
    installation_id: str


@dataclass(frozen=True)
class StorePairDescriptor:
    """Narrow read-only view of a provisioned store pair."""

    installation_id: str
    authority_kind: str
    operational_kind: str


@dataclass(frozen=True)
class StoreIdentity:
    """Safe identity fields returned by an ordinary store reader."""

    installation_id: str
    store_kind: str
    schema_version: int


@dataclass(frozen=True, slots=True)
class AuthorityOutboxEvent:
    sequence: int
    event_id: str
    event_type: str
    aggregate_id: str
    payload_json: str
    payload_sha256: str
    event_sha256: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class OutboxMirrorResult:
    scanned_events: int
    inserted_events: int
    acknowledged_events: int


@dataclass(frozen=True, slots=True)
class TaskReportAuthorityBinding:
    ticket_id: str
    grant_id: str
    authorization_ref: str
    actor_id: str
    actor_type: str
    invocation_id: str
    identity: AuthorityIdentity
    allowed_side_effects: tuple[SideEffect, ...]
    ticket_state: str
    report_payload_sha256: str


def _path_identity(path: str | Path) -> str:
    return os.path.normcase(str(Path(path).resolve(strict=False)))


def _metadata_schema_statement(metadata_table: str) -> str:
    return f"""
        CREATE TABLE {metadata_table} (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID
        """


def _store_schema_statements(
    *,
    store_kind: str,
    metadata_table: str,
) -> tuple[str, ...]:
    domain_schema = (
        _AUTHORITY_SCHEMA
        if store_kind == "AUTHORITY_STORE"
        else _OPERATIONAL_SCHEMA
    )
    return (_metadata_schema_statement(metadata_table),) + domain_schema


@lru_cache(maxsize=2)
def _expected_schema_sha256(store_kind: str, metadata_table: str) -> str:
    return _schema_sha256_for_statements(
        _store_schema_statements(
            store_kind=store_kind,
            metadata_table=metadata_table,
        )
    )


def _provision_store(
    path: Path,
    *,
    store_kind: str,
    metadata_table: str,
    installation_id: str,
    root_capability_sha256: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("BEGIN IMMEDIATE")
        schema = _store_schema_statements(
            store_kind=store_kind,
            metadata_table=metadata_table,
        )
        for statement in schema:
            connection.execute(statement)
        connection.executemany(
            f"INSERT INTO {metadata_table}(key, value) VALUES (?, ?)",
            (
                ("installation_id", installation_id),
                ("root_capability_sha256", root_capability_sha256),
                ("schema_version", str(_STORE_SCHEMA_VERSION)),
                ("store_kind", store_kind),
            ),
        )
        connection.execute(f"PRAGMA user_version = {_STORE_SCHEMA_VERSION}")
        connection.commit()
    finally:
        connection.close()


def _remove_owned_sqlite_artifacts(path: Path) -> None:
    for candidate in (
        path,
        Path(f"{path}-journal"),
        Path(f"{path}-wal"),
        Path(f"{path}-shm"),
    ):
        candidate.unlink(missing_ok=True)


def _cleanup_bootstrap_artifacts(paths: tuple[Path, ...]) -> None:
    for path in paths:
        _remove_owned_sqlite_artifacts(path)


def _require_unprovisioned(authority_path: Path, operational_path: Path) -> None:
    authority_exists = os.path.lexists(authority_path)
    operational_exists = os.path.lexists(operational_path)
    if authority_exists and operational_exists:
        raise StoreAlreadyBootstrappedError(
            "control-plane stores are already provisioned"
        )
    if authority_exists or operational_exists:
        raise StoreBootstrapIncompleteError(
            "control-plane store bootstrap is incomplete"
        )


def _provision_store_pair_under_lock(
    resolved_authority: Path,
    resolved_operational: Path,
    *,
    root_capability_sha256: str,
) -> StoreBootstrapReceipt:
    installation_id = secrets.token_hex(32)
    staging_id = secrets.token_hex(16)
    authority_staging = resolved_authority.with_name(
        f".{resolved_authority.name}.{staging_id}.bootstrap"
    )
    operational_staging = resolved_operational.with_name(
        f".{resolved_operational.name}.{staging_id}.bootstrap"
    )
    staged = (authority_staging, operational_staging)
    published: list[Path] = []
    try:
        _provision_store(
            authority_staging,
            store_kind="AUTHORITY_STORE",
            metadata_table="authority_meta",
            installation_id=installation_id,
            root_capability_sha256=root_capability_sha256,
        )
        _provision_store(
            operational_staging,
            store_kind="OPERATIONAL_JOURNAL",
            metadata_table="operational_meta",
            installation_id=installation_id,
            root_capability_sha256=root_capability_sha256,
        )
        if os.path.samefile(authority_staging, operational_staging):
            raise StoreConfigurationError(
                "authority and operational stores must use different SQLite files"
            )
        _require_unprovisioned(resolved_authority, resolved_operational)
        os.replace(authority_staging, resolved_authority)
        published.append(resolved_authority)
        os.replace(operational_staging, resolved_operational)
        published.append(resolved_operational)
    except StoreError:
        _cleanup_bootstrap_artifacts(staged + tuple(published))
        raise
    except (OSError, sqlite3.DatabaseError) as error:
        _cleanup_bootstrap_artifacts(staged + tuple(published))
        raise StoreBootstrapError("control-plane store bootstrap failed") from error

    if os.path.samefile(resolved_authority, resolved_operational):
        raise StoreConfigurationError(
            "authority and operational stores must use different SQLite files"
        )
    return StoreBootstrapReceipt(
        authority_path=resolved_authority,
        operational_path=resolved_operational,
        installation_id=installation_id,
    )


def _acquire_bootstrap_lock(
    authority_path: Path,
    operational_path: Path,
) -> Path:
    try:
        common_root = Path(
            os.path.commonpath(
                (str(authority_path.parent), str(operational_path.parent))
            )
        )
    except ValueError as error:
        raise StoreConfigurationError(
            "control-plane stores must share one local bootstrap root"
        ) from error
    try:
        common_root.mkdir(parents=True, exist_ok=True)
        lock_path = common_root / ".control-plane-bootstrap.lock"
        descriptor = os.open(
            lock_path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
        os.close(descriptor)
        return lock_path
    except FileExistsError as error:
        raise StoreBootstrapInProgressError(
            "control-plane store bootstrap is already in progress"
        ) from error
    except OSError as error:
        raise StoreBootstrapError(
            "control-plane bootstrap lock is unavailable"
        ) from error


def _trusted_bootstrap(*, root_secret: str) -> StoreBootstrapReceipt:
    """Provision the fixed store pair from the trusted composition root."""

    root_capability_sha256 = _root_secret_sha256(root_secret)
    resolved_authority = Path(_AUTHORITY_STORE_PATH).resolve(strict=False)
    resolved_operational = Path(_OPERATIONAL_STORE_PATH).resolve(strict=False)
    if _path_identity(resolved_authority) == _path_identity(resolved_operational):
        raise StoreConfigurationError(
            "authority and operational stores must use different SQLite files"
        )
    _require_unprovisioned(resolved_authority, resolved_operational)
    lock_path = _acquire_bootstrap_lock(
        resolved_authority,
        resolved_operational,
    )
    try:
        _require_unprovisioned(resolved_authority, resolved_operational)
        return _provision_store_pair_under_lock(
            resolved_authority,
            resolved_operational,
            root_capability_sha256=root_capability_sha256,
        )
    finally:
        lock_path.unlink(missing_ok=True)


def _read_store_identity(spec: _StoreSpec) -> StoreIdentity:
    metadata = _SqliteUnitOfWork(spec)._read(
        lambda connection: {
            str(row["key"]): str(row["value"])
            for row in connection.execute(
                f"SELECT key, value FROM {spec.metadata_table}"
            )
        }
    )
    installation_id = metadata.get("installation_id", "")
    if (
        len(installation_id) != 64
        or any(character not in "0123456789abcdef" for character in installation_id)
    ):
        raise StoreBootstrapError("control-plane store identity is invalid")
    return StoreIdentity(
        installation_id=installation_id,
        store_kind=spec.store_kind,
        schema_version=spec.schema_version,
    )


def _require_store_root(spec: _StoreSpec, root_secret: str) -> None:
    supplied_sha256 = _root_secret_sha256(root_secret)

    def read_root(connection: sqlite3.Connection) -> str | None:
        row = connection.execute(
            f"SELECT value FROM {spec.metadata_table} WHERE key = ?",
            ("root_capability_sha256",),
        ).fetchone()
        return None if row is None else str(row["value"])

    stored_sha256 = _SqliteUnitOfWork(spec)._read(read_root)
    if stored_sha256 is None or not hmac.compare_digest(
        stored_sha256,
        supplied_sha256,
    ):
        raise AuthorityRootError("authority root capability is invalid")


class AuthorityReader:
    """Read-only authority queries with no generic SQL surface."""

    __slots__ = ()

    def read_identity(self) -> StoreIdentity:
        return _read_store_identity(_authority_spec())

    def pending_outbox_count(self) -> int:
        return int(
            _SqliteUnitOfWork(_authority_spec())._read(
                lambda connection: connection.execute(
                    """
                    SELECT COUNT(*) FROM authority_outbox
                    WHERE mirrored_at IS NULL
                    """
                ).fetchone()[0]
            )
        )

    def authorization_state(self, authorization_ref: str) -> str | None:
        _require_nonempty(authorization_ref, "authorization_ref")

        def read_state(connection: sqlite3.Connection) -> str | None:
            row = connection.execute(
                """
                SELECT state FROM authorizations_v2
                WHERE authorization_ref = ?
                """,
                (authorization_ref,),
            ).fetchone()
            return None if row is None else str(row["state"])

        return _SqliteUnitOfWork(_authority_spec())._read(read_state)

    def task_ticket_state(self, ticket_id: str) -> str | None:
        _require_nonempty(ticket_id, "ticket_id")

        def read_state(connection: sqlite3.Connection) -> str | None:
            row = connection.execute(
                "SELECT state FROM task_tickets_v2 WHERE ticket_id = ?",
                (ticket_id,),
            ).fetchone()
            return None if row is None else str(row["state"])

        return _SqliteUnitOfWork(_authority_spec())._read(read_state)

    def trusted_receipt_count(self, ticket_id: str) -> int:
        _require_nonempty(ticket_id, "ticket_id")
        return int(
            _SqliteUnitOfWork(_authority_spec())._read(
                lambda connection: connection.execute(
                    """
                    SELECT COUNT(*) FROM trusted_task_receipts_v2
                    WHERE ticket_id = ?
                    """,
                    (ticket_id,),
                ).fetchone()[0]
            )
        )

    def verify_task_report_binding(
        self,
        report: Mapping[str, object],
    ) -> TaskReportAuthorityBinding:
        from .task_reports import (
            TaskReportValidationError,
            parse_task_report_v2_bytes,
        )

        try:
            frozen_report = parse_task_report_v2_bytes(
                json.dumps(
                    report,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            )
        except (TypeError, ValueError, TaskReportValidationError) as error:
            raise TaskReportAuthorityError("TaskReport V2 is invalid") from error
        return _SqliteUnitOfWork(_authority_spec())._read(
            lambda connection: _verify_task_report_authority(
                connection,
                frozen_report,
            )
        )


class OperationalReader:
    """Read-only journal queries with no generic SQL surface."""

    __slots__ = ()

    def read_identity(self) -> StoreIdentity:
        return _read_store_identity(_operational_spec())

    def event_count(self) -> int:
        return int(
            _SqliteUnitOfWork(_operational_spec())._read(
                lambda connection: connection.execute(
                    "SELECT COUNT(*) FROM journal_events"
                ).fetchone()[0]
            )
        )


def _authority_spec() -> _StoreSpec:
    return _StoreSpec(
        path=_AUTHORITY_STORE_PATH,
        store_kind="AUTHORITY_STORE",
        metadata_table="authority_meta",
        schema_version=_STORE_SCHEMA_VERSION,
        expected_schema_sha256=_expected_schema_sha256(
            "AUTHORITY_STORE",
            "authority_meta",
        ),
    )


def _operational_spec() -> _StoreSpec:
    return _StoreSpec(
        path=_OPERATIONAL_STORE_PATH,
        store_kind="OPERATIONAL_JOURNAL",
        metadata_table="operational_meta",
        schema_version=_STORE_SCHEMA_VERSION,
        expected_schema_sha256=_expected_schema_sha256(
            "OPERATIONAL_JOURNAL",
            "operational_meta",
        ),
    )


def _canonical_payload(payload: dict[str, object]) -> tuple[str, str]:
    payload_json = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return payload_json, hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def _event_envelope_sha256(
    *,
    authority_sequence: int,
    event_id: str,
    event_type: str,
    aggregate_id: str,
    payload_sha256: str,
    created_at: str,
) -> str:
    if type(authority_sequence) is not int or authority_sequence < 1:
        raise ValueError("authority_sequence must be a positive integer")
    envelope = json.dumps(
        {
            "aggregate_id": _require_nonempty(aggregate_id, "aggregate_id"),
            "authority_sequence": authority_sequence,
            "created_at": _require_nonempty(created_at, "created_at"),
            "event_id": _require_nonempty(event_id, "event_id"),
            "event_type": _require_nonempty(event_type, "event_type"),
            "payload_sha256": _require_sha256(
                payload_sha256,
                "payload_sha256",
            ),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(
        b"control_plane.authority_event.v1\0" + envelope
    ).hexdigest()


def _insert_authority_outbox(
    connection: sqlite3.Connection,
    *,
    event_type: str,
    aggregate_id: str,
    payload: dict[str, object],
    created_at: datetime,
) -> None:
    payload_json, payload_sha256 = _canonical_payload(payload)
    event_id = f"evt_{secrets.token_hex(16)}"
    created_at_text = _utc_text(created_at)
    placeholder_sha256 = "0" * 64
    insert = connection.execute(
        """
        INSERT INTO authority_outbox
        (event_id, event_type, aggregate_id, payload_json, payload_sha256,
         event_sha256, created_at, mirrored_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            event_id,
            event_type,
            aggregate_id,
            payload_json,
            payload_sha256,
            placeholder_sha256,
            created_at_text,
        ),
    )
    authority_sequence = int(insert.lastrowid)
    event_sha256 = _event_envelope_sha256(
        authority_sequence=authority_sequence,
        event_id=event_id,
        event_type=event_type,
        aggregate_id=aggregate_id,
        payload_sha256=payload_sha256,
        created_at=created_at_text,
    )
    update = connection.execute(
        """
        UPDATE authority_outbox
        SET event_sha256 = ?
        WHERE sequence = ? AND event_sha256 = ?
        """,
        (event_sha256, authority_sequence, placeholder_sha256),
    )
    if update.rowcount != 1:
        raise StoreError("authority outbox sequence allocation failed")


def _effects_json(effects: tuple[SideEffect, ...]) -> str:
    return json.dumps(
        [effect.value for effect in effects],
        separators=(",", ":"),
    )


def _require_side_effects(
    value: object,
    field_name: str,
) -> tuple[SideEffect, ...]:
    if (
        not isinstance(value, tuple)
        or not value
        or not all(isinstance(effect, SideEffect) for effect in value)
        or len(set(value)) != len(value)
    ):
        raise TaskTicketError(
            f"{field_name} must be unique SideEffect values"
        )
    return value


def _effects_from_json(value: object) -> tuple[SideEffect, ...]:
    try:
        raw_effects = json.loads(str(value))
        if not isinstance(raw_effects, list):
            raise ValueError("effects must be an array")
        effects = tuple(SideEffect(raw_effect) for raw_effect in raw_effects)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise TaskTicketError("stored task side effects are invalid") from error
    return _require_side_effects(effects, "stored task side effects")


def _require_unique_string_array(value: object, field_name: str) -> None:
    if not isinstance(value, list):
        raise TaskTicketError(f"{field_name} must be an array")
    if any(
        not isinstance(item, str) or not item or item != item.strip()
        for item in value
    ):
        raise TaskTicketError(
            f"{field_name} must contain canonical non-empty strings"
        )
    if len(set(value)) != len(value):
        raise TaskTicketError(f"{field_name} must not contain duplicates")


def _canonical_task_spec(task_spec: Mapping[str, object]) -> str:
    if not isinstance(task_spec, Mapping) or set(task_spec) != _TASK_SPEC_FIELDS:
        raise TaskTicketError("task spec has an invalid field contract")
    for field_name in (
        "task_id",
        "objective",
        "idempotency_key",
        "task_spec_ref",
        "baseline_ref",
    ):
        try:
            _require_nonempty(task_spec[field_name], field_name)
        except ValueError as error:
            raise TaskTicketError(str(error)) from error
    for field_name in ("task_spec_sha256", "baseline_sha256"):
        try:
            _require_sha256(task_spec[field_name], field_name)
        except ValueError as error:
            raise TaskTicketError(str(error)) from error
    for field_name in (
        "dependencies",
        "allowed_files",
        "forbidden_files",
    ):
        _require_unique_string_array(task_spec[field_name], field_name)
    try:
        from .task_reports import (
            TaskReportValidationError,
            _validate_evidence_refs,
        )

        _validate_evidence_refs(task_spec["input_evidence_refs"])
    except TaskReportValidationError as error:
        raise TaskTicketError(str(error)) from error
    requirements = task_spec["requirements"]
    required_fields = {
        "required_test_receipt_ids",
        "required_review_receipt_ids",
        "required_evidence_ids",
    }
    if not isinstance(requirements, Mapping) or set(requirements) != required_fields:
        raise TaskTicketError("task spec requirements contract is invalid")
    for field_name in sorted(required_fields):
        _require_unique_string_array(requirements[field_name], field_name)
    try:
        return json.dumps(
            task_spec,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise TaskTicketError("task spec is not canonical JSON") from error


def _canonical_trusted_receipt(
    receipt_kind: str,
    payload: Mapping[str, object],
) -> tuple[str, str, str, str]:
    kind = _require_nonempty(receipt_kind, "receipt_kind").upper()
    if kind not in {"TEST", "REVIEW", "EVIDENCE"}:
        raise TaskTicketError("receipt_kind is invalid")
    if not isinstance(payload, Mapping):
        raise TaskTicketError("trusted receipt payload must be a mapping")
    from .task_reports import (
        TaskReportValidationError,
        _receipt_results,
        _validate_evidence_refs,
    )

    try:
        if kind == "TEST":
            _receipt_results([payload], "test_receipts")
            identity_field = "receipt_id"
        elif kind == "REVIEW":
            _receipt_results([payload], "review_receipts")
            identity_field = "receipt_id"
        else:
            _validate_evidence_refs([payload])
            identity_field = "evidence_id"
        receipt_id = _require_nonempty(payload.get(identity_field), identity_field)
        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TaskReportValidationError, TypeError, ValueError) as error:
        raise TaskTicketError("trusted receipt payload is invalid") from error
    payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    return kind, receipt_id, payload_json, payload_sha256


def _receipt_attestation_sha256(
    root_secret: _BearerSecret,
    *,
    ticket_id: str,
    receipt_kind: str,
    receipt_id: str,
    issuer: Actor,
    payload_sha256: str,
) -> str:
    if not isinstance(root_secret, _BearerSecret) or not isinstance(issuer, Actor):
        raise TaskTicketError("trusted receipt issuer is invalid")
    message = json.dumps(
        {
            "issuer_actor_id": issuer.actor_id,
            "issuer_actor_type": issuer.actor_type,
            "issuer_invocation_id": issuer.invocation_id,
            "payload_sha256": _require_sha256(
                payload_sha256,
                "payload_sha256",
            ),
            "receipt_id": _require_nonempty(receipt_id, "receipt_id"),
            "receipt_kind": _require_nonempty(receipt_kind, "receipt_kind"),
            "ticket_id": _require_nonempty(ticket_id, "ticket_id"),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hmac.new(
        root_secret._reveal_for_authority_check().encode("utf-8"),
        b"control_plane.trusted_receipt_attestation.v1\0" + message,
        hashlib.sha256,
    ).hexdigest()


def _derive_root_capability_secret(
    root_secret: _BearerSecret,
    *,
    domain: bytes,
    payload: Mapping[str, object],
) -> str:
    if not isinstance(root_secret, _BearerSecret) or not isinstance(
        payload,
        Mapping,
    ):
        raise AuthorizationRejectedError("authority capability derivation failed")
    canonical_payload = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hmac.new(
        root_secret._reveal_for_authority_check().encode("utf-8"),
        domain + b"\0" + canonical_payload,
        hashlib.sha256,
    ).hexdigest()


def _authorization_secret_payload(
    *,
    authorization_ref: str,
    phase: Phase,
    attempt_id: str,
    actor: Actor,
    identity: AuthorityIdentity,
    expires_at: datetime,
    allowed_side_effects: tuple[SideEffect, ...],
) -> dict[str, object]:
    return {
        "authorization_ref": _require_nonempty(
            authorization_ref,
            "authorization_ref",
        ),
        "phase": phase.value,
        "attempt_id": _require_nonempty(attempt_id, "attempt_id"),
        "actor_id": actor.actor_id,
        "actor_type": actor.actor_type,
        "invocation_id": actor.invocation_id,
        "plan_hash": identity.plan_hash,
        "scope_hash": identity.scope_hash,
        "instruction_policy_hash": identity.instruction_policy_hash,
        "expires_at": _utc_text(expires_at),
        "allowed_side_effects": [
            effect.value for effect in allowed_side_effects
        ],
    }


def _grant_secret_payload(
    *,
    grant_id: str,
    authorization_ref: str,
    phase: Phase,
    attempt_id: str,
    actor: Actor,
    identity: AuthorityIdentity,
    allowed_side_effects: tuple[SideEffect, ...],
) -> dict[str, object]:
    return {
        "grant_id": _require_nonempty(grant_id, "grant_id"),
        "authorization_ref": _require_nonempty(
            authorization_ref,
            "authorization_ref",
        ),
        "phase": phase.value,
        "attempt_id": _require_nonempty(attempt_id, "attempt_id"),
        "actor_id": actor.actor_id,
        "actor_type": actor.actor_type,
        "invocation_id": actor.invocation_id,
        "plan_hash": identity.plan_hash,
        "scope_hash": identity.scope_hash,
        "instruction_policy_hash": identity.instruction_policy_hash,
        "allowed_side_effects": [
            effect.value for effect in allowed_side_effects
        ],
    }


def _require_independent_receipt_issuer(
    receipt_kind: str,
    issuer: Actor,
    task_actor: Actor,
) -> None:
    allowed_actor_types = {
        "TEST": frozenset({"automation"}),
        "REVIEW": frozenset({"human", "llm"}),
        "EVIDENCE": frozenset({"human", "automation"}),
    }
    if (
        not isinstance(issuer, Actor)
        or issuer.actor_type not in allowed_actor_types[receipt_kind]
    ):
        raise TaskTicketError("trusted receipt issuer role is invalid")
    if issuer == task_actor:
        raise TaskTicketError("task actor cannot attest its own receipt")


def _report_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise TaskReportAuthorityError("TaskReport timestamp is invalid")
    try:
        return _require_aware_datetime(
            datetime.fromisoformat(value.replace("Z", "+00:00")),
            "TaskReport timestamp",
        )
    except ValueError as error:
        raise TaskReportAuthorityError("TaskReport timestamp is invalid") from error


def _trusted_receipts_from_report(
    report: Mapping[str, object],
) -> dict[tuple[str, str], tuple[str, str]]:
    result: dict[tuple[str, str], tuple[str, str]] = {}
    groups = (
        ("TEST", "test_receipts", "receipt_id"),
        ("REVIEW", "review_receipts", "receipt_id"),
        ("EVIDENCE", "input_evidence_refs", "evidence_id"),
    )
    for kind, field_name, identity_field in groups:
        values = report[field_name]
        if not isinstance(values, list):
            raise TaskReportAuthorityError("TaskReport receipt arrays are invalid")
        for payload in values:
            if not isinstance(payload, Mapping):
                raise TaskReportAuthorityError("TaskReport receipt is invalid")
            receipt_id = str(payload[identity_field])
            payload_json = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            result[(kind, receipt_id)] = (
                payload_json,
                hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
            )
    return result


def _verify_task_report_authority(
    connection: sqlite3.Connection,
    report: Mapping[str, object],
) -> TaskReportAuthorityBinding:
    row = connection.execute(
        """
        SELECT ticket.*,
               grant.authorization_ref AS grant_authorization_ref,
               grant.actor_id AS grant_actor_id,
               grant.actor_type AS grant_actor_type,
               grant.invocation_id AS grant_invocation_id,
               grant.plan_hash AS grant_plan_hash,
               grant.scope_hash AS grant_scope_hash,
               grant.instruction_policy_hash AS grant_instruction_policy_hash
        FROM task_tickets_v2 AS ticket
        JOIN phase_grants_v2 AS grant ON grant.grant_id = ticket.grant_id
        WHERE ticket.ticket_id = ?
        """,
        (str(report["ticket_id"]),),
    ).fetchone()
    if row is None:
        raise TaskReportAuthorityError("TaskReport ticket is unknown")
    identity = report["identity_binding"]
    if not isinstance(identity, Mapping):
        raise TaskReportAuthorityError("TaskReport identity binding is invalid")
    report_binding = (
        report["ticket_id"],
        report["authorization_ref"],
        report["phase"],
        report["attempt_id"],
        report["task_id"],
        report["idempotency_key"],
        report["task_spec_ref"],
        report["task_spec_sha256"],
        report["ticket_state"],
        identity["plan_hash"],
        identity["scope_hash"],
        identity["instruction_policy_hash"],
    )
    trusted_binding = (
        row["ticket_id"],
        row["grant_authorization_ref"],
        row["phase"],
        row["attempt_id"],
        row["task_id"],
        row["idempotency_key"],
        row["task_spec_ref"],
        row["task_spec_sha256"],
        row["state"],
        row["grant_plan_hash"],
        row["grant_scope_hash"],
        row["grant_instruction_policy_hash"],
    )
    if report_binding != trusted_binding:
        raise TaskReportAuthorityError("TaskReport authority binding mismatch")
    task_spec_payload = {
        field_name: report[field_name] for field_name in _TASK_SPEC_FIELDS
    }
    if _canonical_task_spec(task_spec_payload) != row["task_spec_payload_json"]:
        raise TaskReportAuthorityError("TaskReport task spec mismatch")
    if row["started_at"] is None or row["completed_at"] is None:
        raise TaskReportAuthorityError("TaskReport ticket is not terminal")
    if (
        _report_timestamp(report["started_at"])
        != _parse_utc_text(str(row["started_at"]))
        or _report_timestamp(report["completed_at"])
        != _parse_utc_text(str(row["completed_at"]))
    ):
        raise TaskReportAuthorityError("TaskReport ticket timestamps mismatch")

    expected_receipts = _trusted_receipts_from_report(report)
    trusted_receipts: dict[tuple[str, str], tuple[str, str]] = {}
    for receipt in connection.execute(
        """
        SELECT receipt_kind, receipt_id, payload_json, payload_sha256
        FROM trusted_task_receipts_v2
        WHERE ticket_id = ?
        """,
        (row["ticket_id"],),
    ):
        key = (str(receipt["receipt_kind"]), str(receipt["receipt_id"]))
        payload_json = str(receipt["payload_json"])
        payload_sha256 = str(receipt["payload_sha256"])
        if not hmac.compare_digest(
            hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
            payload_sha256,
        ):
            raise TaskReportAuthorityError("trusted receipt integrity mismatch")
        trusted_receipts[key] = (payload_json, payload_sha256)
    if trusted_receipts != expected_receipts:
        raise TaskReportAuthorityError("TaskReport trusted receipts mismatch")
    return TaskReportAuthorityBinding(
        ticket_id=str(row["ticket_id"]),
        grant_id=str(row["grant_id"]),
        authorization_ref=str(row["grant_authorization_ref"]),
        actor_id=str(row["grant_actor_id"]),
        actor_type=str(row["grant_actor_type"]),
        invocation_id=str(row["grant_invocation_id"]),
        identity=AuthorityIdentity(
            plan_hash=str(row["grant_plan_hash"]),
            scope_hash=str(row["grant_scope_hash"]),
            instruction_policy_hash=str(
                row["grant_instruction_policy_hash"]
            ),
        ),
        allowed_side_effects=_effects_from_json(row["allowed_effects_json"]),
        ticket_state=str(row["state"]),
        report_payload_sha256=str(report["report_payload_sha256"]),
    )


class _AuthorityStore:
    """Trusted V2 authority mutations; never exported to ordinary workers."""

    __slots__ = ("_clock", "_root_secret")

    def __init__(
        self,
        *,
        root_secret: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        _require_store_root(_authority_spec(), root_secret)
        self._root_secret = _BearerSecret(root_secret)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _now(self) -> datetime:
        return _require_aware_datetime(self._clock(), "clock result")

    def _assert_outbox_drained_for_phase_close(self) -> None:
        if AuthorityReader().pending_outbox_count() != 0:
            raise PendingOutboxError(
                "pending authority outbox blocks phase closure"
            )

    def _read_pending_outbox(
        self,
        *,
        limit: int,
    ) -> tuple[AuthorityOutboxEvent, ...]:
        if type(limit) is not int or not 1 <= limit <= 1_000:
            raise ValueError("outbox limit must be between 1 and 1000")

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[AuthorityOutboxEvent, ...]:
            rows = connection.execute(
                """
                SELECT sequence, event_id, event_type, aggregate_id,
                       payload_json, payload_sha256, event_sha256, created_at
                FROM authority_outbox
                WHERE mirrored_at IS NULL
                ORDER BY sequence
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            events: list[AuthorityOutboxEvent] = []
            for row in rows:
                payload_json = str(row["payload_json"])
                payload_sha256 = str(row["payload_sha256"])
                try:
                    payload = json.loads(payload_json)
                    if not isinstance(payload, dict):
                        raise ValueError("outbox payload must be an object")
                    canonical_json, canonical_sha256 = _canonical_payload(payload)
                    created_at = _parse_utc_text(str(row["created_at"]))
                    canonical_created_at = _utc_text(created_at)
                    event_sha256 = _event_envelope_sha256(
                        authority_sequence=int(row["sequence"]),
                        event_id=str(row["event_id"]),
                        event_type=str(row["event_type"]),
                        aggregate_id=str(row["aggregate_id"]),
                        payload_sha256=payload_sha256,
                        created_at=canonical_created_at,
                    )
                except (TypeError, ValueError) as error:
                    raise OutboxConflictError(
                        "authority outbox event is invalid"
                    ) from error
                if (
                    canonical_json != payload_json
                    or canonical_created_at != str(row["created_at"])
                    or not hmac.compare_digest(
                        canonical_sha256,
                        payload_sha256,
                    )
                    or not hmac.compare_digest(
                        event_sha256,
                        str(row["event_sha256"]),
                    )
                ):
                    raise OutboxConflictError(
                        "authority outbox event integrity mismatch"
                    )
                events.append(
                    AuthorityOutboxEvent(
                        sequence=int(row["sequence"]),
                        event_id=str(row["event_id"]),
                        event_type=str(row["event_type"]),
                        aggregate_id=str(row["aggregate_id"]),
                        payload_json=payload_json,
                        payload_sha256=payload_sha256,
                        event_sha256=event_sha256,
                        created_at=created_at,
                    )
                )
            return tuple(events)

        return _SqliteUnitOfWork(_authority_spec())._read(read)

    def _acknowledge_outbox(self, event_id: str) -> bool:
        _require_nonempty(event_id, "event_id")
        mirrored_at = _utc_text(self._now())

        def acknowledge(connection: sqlite3.Connection) -> bool:
            update = connection.execute(
                """
                UPDATE authority_outbox
                SET mirrored_at = ?
                WHERE event_id = ? AND mirrored_at IS NULL
                """,
                (mirrored_at, event_id),
            )
            if update.rowcount == 1:
                return True
            existing = connection.execute(
                "SELECT mirrored_at FROM authority_outbox WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if existing is None:
                raise OutboxConflictError("authority outbox event is missing")
            return False

        return _SqliteUnitOfWork(_authority_spec())._write(acknowledge)

    def _provision_authorization(
        self,
        *,
        phase: Phase,
        attempt_id: str,
        actor: Actor,
        identity: AuthorityIdentity,
        expires_at: datetime,
        allowed_side_effects: tuple[SideEffect, ...],
    ) -> AuthorizationEnvelope:
        now = self._now()
        expiry = _require_aware_datetime(expires_at, "expires_at")
        if expiry <= now:
            raise AuthorizationExpiredError(
                "authorization expiry must be later than issuance"
        )
        authorization_ref = f"auth_{secrets.token_hex(16)}"
        envelope_draft = AuthorizationEnvelope(
            authorization_ref=authorization_ref,
            phase=phase,
            attempt_id=attempt_id,
            actor=actor,
            identity=identity,
            expires_at=expiry,
            allowed_side_effects=allowed_side_effects,
            _bearer_secret=_BearerSecret("validation-placeholder"),
        )
        bearer_secret = _derive_root_capability_secret(
            self._root_secret,
            domain=b"control_plane.authorization_envelope.v2",
            payload=_authorization_secret_payload(
                authorization_ref=envelope_draft.authorization_ref,
                phase=envelope_draft.phase,
                attempt_id=envelope_draft.attempt_id,
                actor=envelope_draft.actor,
                identity=envelope_draft.identity,
                expires_at=envelope_draft.expires_at,
                allowed_side_effects=envelope_draft.allowed_side_effects,
            ),
        )
        envelope = AuthorizationEnvelope(
            authorization_ref=envelope_draft.authorization_ref,
            phase=envelope_draft.phase,
            attempt_id=envelope_draft.attempt_id,
            actor=envelope_draft.actor,
            identity=envelope_draft.identity,
            expires_at=envelope_draft.expires_at,
            allowed_side_effects=envelope_draft.allowed_side_effects,
            _bearer_secret=_BearerSecret(bearer_secret),
        )
        allowed_effects_json = _effects_json(envelope.allowed_side_effects)
        secret_sha256 = hashlib.sha256(
            bearer_secret.encode("utf-8")
        ).hexdigest()

        def provision(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO authorizations_v2
                (authorization_ref, phase, attempt_id, actor_id, actor_type,
                 invocation_id, plan_hash, scope_hash, instruction_policy_hash,
                 secret_sha256, expires_at, allowed_effects_json, state,
                 created_at, claimed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, NULL)
                """,
                (
                    envelope.authorization_ref,
                    envelope.phase.value,
                    envelope.attempt_id,
                    envelope.actor.actor_id,
                    envelope.actor.actor_type,
                    envelope.actor.invocation_id,
                    envelope.identity.plan_hash,
                    envelope.identity.scope_hash,
                    envelope.identity.instruction_policy_hash,
                    secret_sha256,
                    _utc_text(envelope.expires_at),
                    allowed_effects_json,
                    _utc_text(now),
                ),
            )
            _insert_authority_outbox(
                connection,
                event_type="AUTHORIZATION_PROVISIONED",
                aggregate_id=envelope.authorization_ref,
                payload=envelope.to_public_dict(),
                created_at=now,
            )

        _SqliteUnitOfWork(_authority_spec())._write(provision)
        return envelope

    def _recover_pending_authorization(
        self,
        authorization_ref: str,
    ) -> AuthorizationEnvelope:
        reference = _require_nonempty(
            authorization_ref,
            "authorization_ref",
        )

        def read(connection: sqlite3.Connection) -> sqlite3.Row | None:
            return connection.execute(
                "SELECT * FROM authorizations_v2 WHERE authorization_ref = ?",
                (reference,),
            ).fetchone()

        row = _SqliteUnitOfWork(_authority_spec())._read(read)
        if row is None:
            raise AuthorizationRejectedError("authorization envelope is unknown")
        if row["state"] != "PENDING":
            raise AuthorizationReplayError(
                "only a pending authorization can be recovered"
            )
        try:
            allowed_side_effects = _effects_from_json(
                row["allowed_effects_json"]
            )
            phase = Phase(str(row["phase"]))
            actor = Actor(
                actor_id=str(row["actor_id"]),
                actor_type=str(row["actor_type"]),
                invocation_id=str(row["invocation_id"]),
            )
            identity = AuthorityIdentity(
                plan_hash=str(row["plan_hash"]),
                scope_hash=str(row["scope_hash"]),
                instruction_policy_hash=str(row["instruction_policy_hash"]),
            )
            expires_at = _parse_utc_text(str(row["expires_at"]))
            bearer_secret = _derive_root_capability_secret(
                self._root_secret,
                domain=b"control_plane.authorization_envelope.v2",
                payload=_authorization_secret_payload(
                    authorization_ref=reference,
                    phase=phase,
                    attempt_id=str(row["attempt_id"]),
                    actor=actor,
                    identity=identity,
                    expires_at=expires_at,
                    allowed_side_effects=allowed_side_effects,
                ),
            )
        except (TaskTicketError, TypeError, ValueError) as error:
            raise AuthorizationRejectedError(
                "stored authorization envelope is invalid"
            ) from error
        if not hmac.compare_digest(
            str(row["secret_sha256"]),
            hashlib.sha256(bearer_secret.encode("utf-8")).hexdigest(),
        ):
            raise AuthorizationRejectedError(
                "stored authorization envelope integrity mismatch"
            )
        return AuthorizationEnvelope(
            authorization_ref=reference,
            phase=phase,
            attempt_id=str(row["attempt_id"]),
            actor=actor,
            identity=identity,
            expires_at=expires_at,
            allowed_side_effects=allowed_side_effects,
            _bearer_secret=_BearerSecret(bearer_secret),
        )

    def _recover_claimed_grant(
        self,
        authorization_ref: str,
    ) -> AuthorityGrant:
        reference = _require_nonempty(
            authorization_ref,
            "authorization_ref",
        )

        def read(connection: sqlite3.Connection) -> sqlite3.Row | None:
            return connection.execute(
                """
                SELECT grant.*, authorization.state AS authorization_state
                FROM phase_grants_v2 AS grant
                JOIN authorizations_v2 AS authorization
                  ON authorization.authorization_ref = grant.authorization_ref
                WHERE grant.authorization_ref = ?
                """,
                (reference,),
            ).fetchone()

        row = _SqliteUnitOfWork(_authority_spec())._read(read)
        if (
            row is None
            or row["authorization_state"] != "CLAIMED"
            or row["state"] != "ACTIVE"
        ):
            raise AuthorizationRejectedError(
                "active claimed authority grant is unavailable"
            )
        try:
            allowed_side_effects = _effects_from_json(
                row["allowed_effects_json"]
            )
            phase = Phase(str(row["phase"]))
            actor = Actor(
                actor_id=str(row["actor_id"]),
                actor_type=str(row["actor_type"]),
                invocation_id=str(row["invocation_id"]),
            )
            identity = AuthorityIdentity(
                plan_hash=str(row["plan_hash"]),
                scope_hash=str(row["scope_hash"]),
                instruction_policy_hash=str(row["instruction_policy_hash"]),
            )
            grant_secret = _derive_root_capability_secret(
                self._root_secret,
                domain=b"control_plane.authority_grant.v2",
                payload=_grant_secret_payload(
                    grant_id=str(row["grant_id"]),
                    authorization_ref=reference,
                    phase=phase,
                    attempt_id=str(row["attempt_id"]),
                    actor=actor,
                    identity=identity,
                    allowed_side_effects=allowed_side_effects,
                ),
            )
        except (TaskTicketError, TypeError, ValueError) as error:
            raise AuthorizationRejectedError(
                "stored authority grant is invalid"
            ) from error
        if not hmac.compare_digest(
            str(row["secret_sha256"]),
            hashlib.sha256(grant_secret.encode("utf-8")).hexdigest(),
        ):
            raise AuthorizationRejectedError(
                "stored authority grant integrity mismatch"
            )
        return AuthorityGrant(
            grant_id=str(row["grant_id"]),
            authorization_ref=reference,
            phase=phase,
            attempt_id=str(row["attempt_id"]),
            actor=actor,
            identity=identity,
            allowed_side_effects=allowed_side_effects,
            _bearer_secret=_BearerSecret(grant_secret),
        )

    @staticmethod
    def _require_active_grant(
        connection: sqlite3.Connection,
        grant: AuthorityGrant,
    ) -> sqlite3.Row:
        if not isinstance(grant, AuthorityGrant):
            raise AuthorizationRejectedError("authority grant is invalid")
        row = connection.execute(
            "SELECT * FROM phase_grants_v2 WHERE grant_id = ?",
            (grant.grant_id,),
        ).fetchone()
        supplied_secret_sha256 = hashlib.sha256(
            grant._bearer_secret._reveal_for_authority_check().encode("utf-8")
        ).hexdigest()
        if row is None or row["state"] != "ACTIVE":
            raise AuthorizationRejectedError("authority grant is not active")
        stored_binding = (
            row["authorization_ref"],
            row["phase"],
            row["attempt_id"],
            row["actor_id"],
            row["actor_type"],
            row["invocation_id"],
            row["plan_hash"],
            row["scope_hash"],
            row["instruction_policy_hash"],
            row["allowed_effects_json"],
        )
        supplied_binding = (
            grant.authorization_ref,
            grant.phase.value,
            grant.attempt_id,
            grant.actor.actor_id,
            grant.actor.actor_type,
            grant.actor.invocation_id,
            grant.identity.plan_hash,
            grant.identity.scope_hash,
            grant.identity.instruction_policy_hash,
            _effects_json(grant.allowed_side_effects),
        )
        if stored_binding != supplied_binding or not hmac.compare_digest(
            str(row["secret_sha256"]),
            supplied_secret_sha256,
        ):
            raise AuthorizationRejectedError("authority grant is invalid")
        return row

    def _issue_task_ticket(
        self,
        grant: AuthorityGrant,
        task_spec: Mapping[str, object],
        *,
        allowed_side_effects: tuple[SideEffect, ...],
    ) -> TaskAuthorityTicket:
        requested_effects = _require_side_effects(
            allowed_side_effects,
            "allowed_side_effects",
        )
        payload_json = _canonical_task_spec(task_spec)
        allowed_effects_json = _effects_json(requested_effects)
        request_sha256 = hashlib.sha256(
            b"control_plane.task_spec_binding.v2\0"
            + payload_json.encode("utf-8")
            + b"\0"
            + allowed_effects_json.encode("utf-8")
        ).hexdigest()
        idempotency_key = str(task_spec["idempotency_key"])
        ticket_id = hashlib.sha256(
            b"control_plane.task_ticket.v2\0"
            + grant.grant_id.encode("utf-8")
            + b"\0"
            + idempotency_key.encode("utf-8")
        ).hexdigest()
        ticket_secret_value = hmac.new(
            grant._bearer_secret._reveal_for_authority_check().encode("utf-8"),
            request_sha256.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()
        ticket_secret_sha256 = hashlib.sha256(
            ticket_secret_value.encode("utf-8")
        ).hexdigest()
        now = self._now()

        def issue(connection: sqlite3.Connection) -> None:
            self._require_active_grant(connection, grant)
            if not set(requested_effects).issubset(grant.allowed_side_effects):
                raise TaskTicketError(
                    "task side effects exceed the active phase grant"
                )
            existing = connection.execute(
                """
                SELECT request_sha256 FROM task_tickets_v2
                WHERE grant_id = ? AND idempotency_key = ?
                """,
                (grant.grant_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                if not hmac.compare_digest(
                    str(existing["request_sha256"]),
                    request_sha256,
                ):
                    raise TaskTicketIdempotencyError(
                        "task-ticket idempotency key changed semantics"
                    )
                return
            connection.execute(
                """
                INSERT INTO task_tickets_v2
                (ticket_id, grant_id, phase, attempt_id, task_id,
                 idempotency_key, task_spec_ref, task_spec_sha256,
                  task_spec_payload_json, request_sha256, allowed_effects_json,
                  secret_sha256,
                  state, created_at, started_at, completed_at, evidence_ref)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ISSUED', ?,
                        NULL, NULL, NULL)
                """,
                (
                    ticket_id,
                    grant.grant_id,
                    grant.phase.value,
                    grant.attempt_id,
                    str(task_spec["task_id"]),
                    idempotency_key,
                    str(task_spec["task_spec_ref"]),
                    str(task_spec["task_spec_sha256"]),
                    payload_json,
                    request_sha256,
                    allowed_effects_json,
                    ticket_secret_sha256,
                    _utc_text(now),
                ),
            )
            _insert_authority_outbox(
                connection,
                event_type="TASK_TICKET_ISSUED",
                aggregate_id=ticket_id,
                payload={
                    "ticket_id": ticket_id,
                    "grant_id": grant.grant_id,
                    "authorization_ref": grant.authorization_ref,
                    "phase": grant.phase.value,
                    "attempt_id": grant.attempt_id,
                    "task_id": task_spec["task_id"],
                    "task_spec_ref": task_spec["task_spec_ref"],
                    "task_spec_sha256": task_spec["task_spec_sha256"],
                    "request_sha256": request_sha256,
                    "allowed_side_effects": [
                        effect.value for effect in requested_effects
                    ],
                },
                created_at=now,
            )

        _SqliteUnitOfWork(_authority_spec())._write(issue)
        return TaskAuthorityTicket(
            ticket_id=ticket_id,
            grant_id=grant.grant_id,
            authorization_ref=grant.authorization_ref,
            phase=grant.phase,
            attempt_id=grant.attempt_id,
            task_id=str(task_spec["task_id"]),
            idempotency_key=idempotency_key,
            task_spec_ref=str(task_spec["task_spec_ref"]),
            task_spec_sha256=str(task_spec["task_spec_sha256"]),
            allowed_side_effects=requested_effects,
            actor=grant.actor,
            identity=grant.identity,
            _bearer_secret=_BearerSecret(ticket_secret_value),
        )

    @staticmethod
    def _require_task_ticket(
        connection: sqlite3.Connection,
        ticket: TaskAuthorityTicket,
    ) -> sqlite3.Row:
        if not isinstance(ticket, TaskAuthorityTicket):
            raise TaskTicketError("task ticket capability is invalid")
        row = connection.execute(
            """
            SELECT ticket.*,
                   grant.authorization_ref AS grant_authorization_ref,
                   grant.actor_id AS grant_actor_id,
                   grant.actor_type AS grant_actor_type,
                   grant.invocation_id AS grant_invocation_id,
                   grant.plan_hash AS grant_plan_hash,
                   grant.scope_hash AS grant_scope_hash,
                   grant.instruction_policy_hash AS grant_instruction_policy_hash,
                   grant.state AS grant_state
            FROM task_tickets_v2 AS ticket
            JOIN phase_grants_v2 AS grant ON grant.grant_id = ticket.grant_id
            WHERE ticket.ticket_id = ?
            """,
            (ticket.ticket_id,),
        ).fetchone()
        supplied_secret_sha256 = hashlib.sha256(
            ticket._bearer_secret._reveal_for_authority_check().encode("utf-8")
        ).hexdigest()
        if row is None or row["grant_state"] != "ACTIVE":
            raise TaskTicketError("task ticket is unavailable")
        stored_binding = (
            row["grant_id"],
            row["grant_authorization_ref"],
            row["phase"],
            row["attempt_id"],
            row["task_id"],
            row["idempotency_key"],
            row["task_spec_ref"],
            row["task_spec_sha256"],
            row["allowed_effects_json"],
            row["grant_actor_id"],
            row["grant_actor_type"],
            row["grant_invocation_id"],
            row["grant_plan_hash"],
            row["grant_scope_hash"],
            row["grant_instruction_policy_hash"],
        )
        supplied_binding = (
            ticket.grant_id,
            ticket.authorization_ref,
            ticket.phase.value,
            ticket.attempt_id,
            ticket.task_id,
            ticket.idempotency_key,
            ticket.task_spec_ref,
            ticket.task_spec_sha256,
            _effects_json(ticket.allowed_side_effects),
            ticket.actor.actor_id,
            ticket.actor.actor_type,
            ticket.actor.invocation_id,
            ticket.identity.plan_hash,
            ticket.identity.scope_hash,
            ticket.identity.instruction_policy_hash,
        )
        if stored_binding != supplied_binding or not hmac.compare_digest(
            str(row["secret_sha256"]),
            supplied_secret_sha256,
        ):
            raise TaskTicketError("task ticket capability is invalid")
        return row

    def _begin_task(self, ticket: TaskAuthorityTicket) -> TaskExecutionLease:
        now = self._now()
        lease_id = f"lease_{secrets.token_hex(16)}"
        lease_secret_value = secrets.token_urlsafe(32)
        lease_secret_sha256 = hashlib.sha256(
            lease_secret_value.encode("utf-8")
        ).hexdigest()

        def begin(connection: sqlite3.Connection) -> None:
            row = self._require_task_ticket(connection, ticket)
            if row["state"] != "ISSUED":
                raise TaskTicketStateError("task ticket is not ISSUED")
            update = connection.execute(
                """
                UPDATE task_tickets_v2
                SET state = 'IN_PROGRESS', started_at = ?, lease_id = ?,
                    lease_secret_sha256 = ?
                WHERE ticket_id = ? AND state = 'ISSUED'
                """,
                (
                    _utc_text(now),
                    lease_id,
                    lease_secret_sha256,
                    ticket.ticket_id,
                ),
            )
            if update.rowcount != 1:
                raise TaskTicketStateError("task begin lost a concurrent race")
            _insert_authority_outbox(
                connection,
                event_type="TASK_STARTED",
                aggregate_id=ticket.ticket_id,
                payload={
                    "ticket_id": ticket.ticket_id,
                    "lease_id": lease_id,
                    "task_id": ticket.task_id,
                    "attempt_id": ticket.attempt_id,
                    "started_at": _utc_text(now),
                },
                created_at=now,
            )

        _SqliteUnitOfWork(_authority_spec())._write(begin)
        return TaskExecutionLease(
            lease_id=lease_id,
            ticket_id=ticket.ticket_id,
            grant_id=ticket.grant_id,
            authorization_ref=ticket.authorization_ref,
            phase=ticket.phase,
            attempt_id=ticket.attempt_id,
            task_id=ticket.task_id,
            allowed_side_effects=ticket.allowed_side_effects,
            actor=ticket.actor,
            identity=ticket.identity,
            _bearer_secret=_BearerSecret(lease_secret_value),
        )

    @staticmethod
    def _require_task_lease(
        connection: sqlite3.Connection,
        lease: TaskExecutionLease,
    ) -> sqlite3.Row:
        if not isinstance(lease, TaskExecutionLease):
            raise TaskTicketError("task execution lease is invalid")
        row = connection.execute(
            """
            SELECT ticket.*,
                   grant.authorization_ref AS grant_authorization_ref,
                   grant.actor_id AS grant_actor_id,
                   grant.actor_type AS grant_actor_type,
                   grant.invocation_id AS grant_invocation_id,
                   grant.plan_hash AS grant_plan_hash,
                   grant.scope_hash AS grant_scope_hash,
                   grant.instruction_policy_hash AS grant_instruction_policy_hash,
                   grant.state AS grant_state
            FROM task_tickets_v2 AS ticket
            JOIN phase_grants_v2 AS grant ON grant.grant_id = ticket.grant_id
            WHERE ticket.ticket_id = ?
            """,
            (lease.ticket_id,),
        ).fetchone()
        if row is None or row["state"] != "IN_PROGRESS":
            raise TaskTicketStateError("task ticket is not IN_PROGRESS")
        supplied_secret_sha256 = hashlib.sha256(
            lease._bearer_secret._reveal_for_authority_check().encode("utf-8")
        ).hexdigest()
        stored_binding = (
            row["lease_id"],
            row["grant_id"],
            row["grant_authorization_ref"],
            row["phase"],
            row["attempt_id"],
            row["task_id"],
            row["allowed_effects_json"],
            row["grant_actor_id"],
            row["grant_actor_type"],
            row["grant_invocation_id"],
            row["grant_plan_hash"],
            row["grant_scope_hash"],
            row["grant_instruction_policy_hash"],
            row["grant_state"],
        )
        supplied_binding = (
            lease.lease_id,
            lease.grant_id,
            lease.authorization_ref,
            lease.phase.value,
            lease.attempt_id,
            lease.task_id,
            _effects_json(lease.allowed_side_effects),
            lease.actor.actor_id,
            lease.actor.actor_type,
            lease.actor.invocation_id,
            lease.identity.plan_hash,
            lease.identity.scope_hash,
            lease.identity.instruction_policy_hash,
            "ACTIVE",
        )
        if stored_binding != supplied_binding or not hmac.compare_digest(
            str(row["lease_secret_sha256"]),
            supplied_secret_sha256,
        ):
            raise TaskTicketError("task execution lease is invalid")
        return row

    def _attest_task_receipt(
        self,
        lease: TaskExecutionLease,
        *,
        receipt_kind: str,
        issuer: Actor,
        payload: Mapping[str, object],
    ) -> TrustedReceiptAttestation:
        kind, receipt_id, payload_json, payload_sha256 = (
            _canonical_trusted_receipt(receipt_kind, payload)
        )

        def read_task_actor(connection: sqlite3.Connection) -> Actor:
            row = self._require_task_lease(connection, lease)
            return Actor(
                actor_id=str(row["grant_actor_id"]),
                actor_type=str(row["grant_actor_type"]),
                invocation_id=str(row["grant_invocation_id"]),
            )

        task_actor = _SqliteUnitOfWork(_authority_spec())._read(read_task_actor)
        _require_independent_receipt_issuer(kind, issuer, task_actor)
        if kind == "REVIEW" and payload.get("reviewer_id") != issuer.actor_id:
            raise TaskTicketError("review receipt issuer does not match reviewer_id")
        attestation_sha256 = _receipt_attestation_sha256(
            self._root_secret,
            ticket_id=lease.ticket_id,
            receipt_kind=kind,
            receipt_id=receipt_id,
            issuer=issuer,
            payload_sha256=payload_sha256,
        )
        return TrustedReceiptAttestation(
            ticket_id=lease.ticket_id,
            receipt_kind=kind,
            receipt_id=receipt_id,
            issuer=issuer,
            payload_json=payload_json,
            payload_sha256=payload_sha256,
            attestation_sha256=attestation_sha256,
        )

    def _record_task_receipt(
        self,
        lease: TaskExecutionLease,
        *,
        attestation: TrustedReceiptAttestation,
    ) -> bool:
        if not isinstance(attestation, TrustedReceiptAttestation):
            raise TaskTicketError("trusted receipt attestation is invalid")
        try:
            payload = json.loads(attestation.payload_json)
            if not isinstance(payload, Mapping):
                raise ValueError("receipt payload must be an object")
            kind, receipt_id, payload_json, payload_sha256 = (
                _canonical_trusted_receipt(attestation.receipt_kind, payload)
            )
        except (TaskTicketError, TypeError, ValueError) as error:
            raise TaskTicketError("trusted receipt attestation is invalid") from error
        if (
            attestation.ticket_id != lease.ticket_id
            or attestation.receipt_id != receipt_id
            or attestation.payload_json != payload_json
            or not hmac.compare_digest(
                attestation.payload_sha256,
                payload_sha256,
            )
        ):
            raise TaskTicketError("trusted receipt attestation is invalid")

        def record(connection: sqlite3.Connection) -> bool:
            row = self._require_task_lease(connection, lease)
            task_actor = Actor(
                actor_id=str(row["grant_actor_id"]),
                actor_type=str(row["grant_actor_type"]),
                invocation_id=str(row["grant_invocation_id"]),
            )
            _require_independent_receipt_issuer(
                kind,
                attestation.issuer,
                task_actor,
            )
            if (
                kind == "REVIEW"
                and payload.get("reviewer_id") != attestation.issuer.actor_id
            ):
                raise TaskTicketError(
                    "review receipt issuer does not match reviewer_id"
                )
            expected_attestation_sha256 = _receipt_attestation_sha256(
                self._root_secret,
                ticket_id=lease.ticket_id,
                receipt_kind=kind,
                receipt_id=receipt_id,
                issuer=attestation.issuer,
                payload_sha256=payload_sha256,
            )
            if not hmac.compare_digest(
                attestation.attestation_sha256,
                expected_attestation_sha256,
            ):
                raise TaskTicketError("trusted receipt attestation is invalid")
            existing = connection.execute(
                """
                SELECT issuer_actor_id, issuer_actor_type,
                       issuer_invocation_id, payload_json, payload_sha256,
                       attestation_sha256
                FROM trusted_task_receipts_v2
                WHERE ticket_id = ? AND receipt_kind = ? AND receipt_id = ?
                """,
                (lease.ticket_id, kind, receipt_id),
            ).fetchone()
            if existing is not None:
                expected_existing = (
                    attestation.issuer.actor_id,
                    attestation.issuer.actor_type,
                    attestation.issuer.invocation_id,
                    payload_json,
                    payload_sha256,
                    expected_attestation_sha256,
                )
                if tuple(str(value) for value in existing) != expected_existing:
                    raise TrustedReceiptConflictError(
                        "trusted receipt identity changed content"
                    )
                return False
            now = self._now()
            connection.execute(
                """
                INSERT INTO trusted_task_receipts_v2
                (ticket_id, receipt_kind, receipt_id, issuer_actor_id,
                 issuer_actor_type, issuer_invocation_id, payload_json,
                 payload_sha256, attestation_sha256, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease.ticket_id,
                    kind,
                    receipt_id,
                    attestation.issuer.actor_id,
                    attestation.issuer.actor_type,
                    attestation.issuer.invocation_id,
                    payload_json,
                    payload_sha256,
                    expected_attestation_sha256,
                    _utc_text(now),
                ),
            )
            _insert_authority_outbox(
                connection,
                event_type="TRUSTED_RECEIPT_RECORDED",
                aggregate_id=lease.ticket_id,
                payload={
                    "ticket_id": lease.ticket_id,
                    "receipt_kind": kind,
                    "receipt_id": receipt_id,
                    "issuer_actor_id": attestation.issuer.actor_id,
                    "issuer_actor_type": attestation.issuer.actor_type,
                    "issuer_invocation_id": attestation.issuer.invocation_id,
                    "payload_sha256": payload_sha256,
                    "attestation_sha256": expected_attestation_sha256,
                },
                created_at=now,
            )
            return True

        return _SqliteUnitOfWork(_authority_spec())._write(record)

    def _mark_task_in_doubt(
        self,
        ticket_id: str,
        *,
        evidence_ref: str,
    ) -> TaskTicketSnapshot:
        trusted_ticket_id = _require_nonempty(ticket_id, "ticket_id")
        evidence = _require_nonempty(evidence_ref, "evidence_ref")

        def mark(
            connection: sqlite3.Connection,
        ) -> tuple[datetime, datetime]:
            row = connection.execute(
                """
                SELECT task_id, attempt_id, state, started_at
                FROM task_tickets_v2
                WHERE ticket_id = ?
                """,
                (trusted_ticket_id,),
            ).fetchone()
            if (
                row is None
                or row["state"] != "IN_PROGRESS"
                or row["started_at"] is None
            ):
                raise TaskTicketStateError("task ticket is not IN_PROGRESS")
            started_at = _parse_utc_text(str(row["started_at"]))
            completed_at = self._now()
            if completed_at < started_at:
                raise TaskTicketStateError(
                    "task reconciliation time precedes its start"
                )
            update = connection.execute(
                """
                UPDATE task_tickets_v2
                SET state = 'IN_DOUBT', completed_at = ?, evidence_ref = ?
                WHERE ticket_id = ? AND state = 'IN_PROGRESS'
                """,
                (
                    _utc_text(completed_at),
                    evidence,
                    trusted_ticket_id,
                ),
            )
            if update.rowcount != 1:
                raise TaskTicketStateError(
                    "task reconciliation lost a concurrent race"
                )
            _insert_authority_outbox(
                connection,
                event_type="TASK_IN_DOUBT",
                aggregate_id=trusted_ticket_id,
                payload={
                    "ticket_id": trusted_ticket_id,
                    "task_id": str(row["task_id"]),
                    "attempt_id": str(row["attempt_id"]),
                    "state": "IN_DOUBT",
                    "evidence_ref": evidence,
                    "completed_at": _utc_text(completed_at),
                },
                created_at=completed_at,
            )
            return started_at, completed_at

        started_at, completed_at = _SqliteUnitOfWork(_authority_spec())._write(
            mark
        )
        return TaskTicketSnapshot(
            ticket_id=trusted_ticket_id,
            state="IN_DOUBT",
            evidence_ref=evidence,
            started_at=started_at,
            completed_at=completed_at,
        )

    def _finish_task(
        self,
        lease: TaskExecutionLease,
        *,
        outcome: str,
        evidence_ref: str,
    ) -> TaskTicketSnapshot:
        if not isinstance(lease, TaskExecutionLease):
            raise TaskTicketError("task execution lease is invalid")
        terminal_state = _require_nonempty(outcome, "outcome").upper()
        if terminal_state not in {"SUCCEEDED", "FAILED", "IN_DOUBT"}:
            raise TaskTicketError("task outcome is invalid")
        evidence = _require_nonempty(evidence_ref, "evidence_ref")
        lease_secret_sha256 = hashlib.sha256(
            lease._bearer_secret._reveal_for_authority_check().encode("utf-8")
        ).hexdigest()

        def finish(
            connection: sqlite3.Connection,
        ) -> tuple[datetime, datetime]:
            row = connection.execute(
                """
                SELECT ticket.*,
                       grant.authorization_ref AS grant_authorization_ref,
                       grant.actor_id AS grant_actor_id,
                       grant.actor_type AS grant_actor_type,
                       grant.invocation_id AS grant_invocation_id,
                       grant.plan_hash AS grant_plan_hash,
                       grant.scope_hash AS grant_scope_hash,
                       grant.instruction_policy_hash AS grant_instruction_policy_hash,
                       grant.state AS grant_state
                FROM task_tickets_v2 AS ticket
                JOIN phase_grants_v2 AS grant ON grant.grant_id = ticket.grant_id
                WHERE ticket.ticket_id = ?
                """,
                (lease.ticket_id,),
            ).fetchone()
            if row is None or row["state"] != "IN_PROGRESS":
                raise TaskTicketStateError("task ticket is not IN_PROGRESS")
            stored_binding = (
                row["lease_id"],
                row["grant_id"],
                row["grant_authorization_ref"],
                row["phase"],
                row["attempt_id"],
                row["task_id"],
                row["allowed_effects_json"],
                row["grant_actor_id"],
                row["grant_actor_type"],
                row["grant_invocation_id"],
                row["grant_plan_hash"],
                row["grant_scope_hash"],
                row["grant_instruction_policy_hash"],
                row["grant_state"],
            )
            supplied_binding = (
                lease.lease_id,
                lease.grant_id,
                lease.authorization_ref,
                lease.phase.value,
                lease.attempt_id,
                lease.task_id,
                _effects_json(lease.allowed_side_effects),
                lease.actor.actor_id,
                lease.actor.actor_type,
                lease.actor.invocation_id,
                lease.identity.plan_hash,
                lease.identity.scope_hash,
                lease.identity.instruction_policy_hash,
                "ACTIVE",
            )
            if stored_binding != supplied_binding or not hmac.compare_digest(
                str(row["lease_secret_sha256"]),
                lease_secret_sha256,
            ):
                raise TaskTicketError("task execution lease is invalid")
            started_at = _parse_utc_text(str(row["started_at"]))
            completed_at = self._now()
            if completed_at < started_at:
                raise TaskTicketStateError(
                    "task completion time precedes its start"
                )
            update = connection.execute(
                """
                UPDATE task_tickets_v2
                SET state = ?, completed_at = ?, evidence_ref = ?
                WHERE ticket_id = ? AND state = 'IN_PROGRESS'
                """,
                (
                    terminal_state,
                    _utc_text(completed_at),
                    evidence,
                    lease.ticket_id,
                ),
            )
            if update.rowcount != 1:
                raise TaskTicketStateError("task finish lost a concurrent race")
            _insert_authority_outbox(
                connection,
                event_type=f"TASK_{terminal_state}",
                aggregate_id=lease.ticket_id,
                payload={
                    "ticket_id": lease.ticket_id,
                    "task_id": lease.task_id,
                    "attempt_id": lease.attempt_id,
                    "state": terminal_state,
                    "evidence_ref": evidence,
                    "completed_at": _utc_text(completed_at),
                },
                created_at=completed_at,
            )
            return started_at, completed_at

        started_at, completed_at = _SqliteUnitOfWork(_authority_spec())._write(
            finish
        )
        return TaskTicketSnapshot(
            ticket_id=lease.ticket_id,
            state=terminal_state,
            evidence_ref=evidence,
            started_at=started_at,
            completed_at=completed_at,
        )

    def claim_authorization(
        self,
        envelope: AuthorizationEnvelope,
        *,
        expected_phase: Phase,
        expected_attempt_id: str,
        actor: Actor,
        identity: AuthorityIdentity,
    ) -> AuthorityGrant:
        if not isinstance(envelope, AuthorizationEnvelope):
            raise AuthorizationRejectedError("authorization envelope is invalid")
        _require_nonempty(expected_attempt_id, "expected_attempt_id")
        if (
            not isinstance(expected_phase, Phase)
            or not isinstance(actor, Actor)
            or not isinstance(identity, AuthorityIdentity)
        ):
            raise AuthorizationRejectedError("authorization binding is invalid")
        grant_id = f"grant_{secrets.token_hex(16)}"
        grant_secret = _derive_root_capability_secret(
            self._root_secret,
            domain=b"control_plane.authority_grant.v2",
            payload=_grant_secret_payload(
                grant_id=grant_id,
                authorization_ref=envelope.authorization_ref,
                phase=expected_phase,
                attempt_id=expected_attempt_id,
                actor=actor,
                identity=identity,
                allowed_side_effects=envelope.allowed_side_effects,
            ),
        )
        grant_secret_sha256 = hashlib.sha256(
            grant_secret.encode("utf-8")
        ).hexdigest()
        envelope_secret_sha256 = hashlib.sha256(
            envelope._bearer_secret._reveal_for_authority_check().encode("utf-8")
        ).hexdigest()

        def claim(connection: sqlite3.Connection) -> str:
            now = self._now()
            row = connection.execute(
                """
                SELECT * FROM authorizations_v2
                WHERE authorization_ref = ?
                """,
                (envelope.authorization_ref,),
            ).fetchone()
            expected_effects_json = _effects_json(envelope.allowed_side_effects)
            if row is None or not hmac.compare_digest(
                str(row["secret_sha256"]),
                envelope_secret_sha256,
            ):
                raise AuthorizationRejectedError(
                    "authorization envelope is invalid"
                )
            stored_binding = (
                row["phase"],
                row["attempt_id"],
                row["actor_id"],
                row["actor_type"],
                row["invocation_id"],
                row["plan_hash"],
                row["scope_hash"],
                row["instruction_policy_hash"],
                row["expires_at"],
                row["allowed_effects_json"],
            )
            requested_binding = (
                expected_phase.value,
                expected_attempt_id,
                actor.actor_id,
                actor.actor_type,
                actor.invocation_id,
                identity.plan_hash,
                identity.scope_hash,
                identity.instruction_policy_hash,
                _utc_text(envelope.expires_at),
                expected_effects_json,
            )
            envelope_binding = (
                envelope.phase,
                envelope.attempt_id,
                envelope.actor,
                envelope.identity,
            )
            if (
                stored_binding != requested_binding
                or envelope_binding
                != (expected_phase, expected_attempt_id, actor, identity)
            ):
                raise AuthorizationRejectedError(
                    "authorization envelope is invalid"
                )
            if row["state"] != "PENDING":
                raise AuthorizationReplayError(
                    "authorization envelope was already claimed"
                )
            if now >= _parse_utc_text(str(row["expires_at"])):
                expired = connection.execute(
                    """
                    UPDATE authorizations_v2
                    SET state = 'EXPIRED'
                    WHERE authorization_ref = ? AND state = 'PENDING'
                    """,
                    (envelope.authorization_ref,),
                )
                if expired.rowcount != 1:
                    raise AuthorizationReplayError(
                        "authorization envelope was already claimed"
                    )
                _insert_authority_outbox(
                    connection,
                    event_type="AUTHORIZATION_EXPIRED",
                    aggregate_id=envelope.authorization_ref,
                    payload={
                        "authorization_ref": envelope.authorization_ref,
                        "phase": expected_phase.value,
                        "attempt_id": expected_attempt_id,
                        "expired_at": _utc_text(now),
                    },
                    created_at=now,
                )
                return "EXPIRED"
            update = connection.execute(
                """
                UPDATE authorizations_v2
                SET state = 'CLAIMED', claimed_at = ?
                WHERE authorization_ref = ? AND state = 'PENDING'
                """,
                (_utc_text(now), envelope.authorization_ref),
            )
            if update.rowcount != 1:
                raise AuthorizationReplayError(
                    "authorization envelope was already claimed"
                )
            connection.execute(
                """
                INSERT INTO phase_grants_v2
                (grant_id, authorization_ref, phase, attempt_id, actor_id,
                 actor_type, invocation_id, plan_hash, scope_hash,
                 instruction_policy_hash, secret_sha256, allowed_effects_json,
                 state, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)
                """,
                (
                    grant_id,
                    envelope.authorization_ref,
                    expected_phase.value,
                    expected_attempt_id,
                    actor.actor_id,
                    actor.actor_type,
                    actor.invocation_id,
                    identity.plan_hash,
                    identity.scope_hash,
                    identity.instruction_policy_hash,
                    grant_secret_sha256,
                    expected_effects_json,
                    _utc_text(now),
                ),
            )
            _insert_authority_outbox(
                connection,
                event_type="AUTHORIZATION_CLAIMED",
                aggregate_id=envelope.authorization_ref,
                payload={
                    "authorization_ref": envelope.authorization_ref,
                    "grant_id": grant_id,
                    "phase": expected_phase.value,
                    "attempt_id": expected_attempt_id,
                    "actor_id": actor.actor_id,
                    "invocation_id": actor.invocation_id,
                    "identity": {
                        "plan_hash": identity.plan_hash,
                        "scope_hash": identity.scope_hash,
                        "instruction_policy_hash": (
                            identity.instruction_policy_hash
                        ),
                    },
                },
                created_at=now,
            )
            return "CLAIMED"

        outcome = _SqliteUnitOfWork(_authority_spec())._write(claim)
        if outcome == "EXPIRED":
            raise AuthorizationExpiredError("authorization envelope expired")
        return AuthorityGrant(
            grant_id=grant_id,
            authorization_ref=envelope.authorization_ref,
            phase=expected_phase,
            attempt_id=expected_attempt_id,
            actor=actor,
            identity=identity,
            allowed_side_effects=envelope.allowed_side_effects,
            _bearer_secret=_BearerSecret(grant_secret),
        )


class _OperationalJournal:
    """Typed OperationalJournal writer with event_id idempotency."""

    __slots__ = ("_clock",)

    def __init__(
        self,
        *,
        root_secret: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        _require_store_root(_operational_spec(), root_secret)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _now(self) -> datetime:
        return _require_aware_datetime(self._clock(), "clock result")

    def _mirror_event(self, event: AuthorityOutboxEvent) -> bool:
        if not isinstance(event, AuthorityOutboxEvent):
            raise TypeError("event must be an AuthorityOutboxEvent")
        expected_event_sha256 = _event_envelope_sha256(
            authority_sequence=event.sequence,
            event_id=event.event_id,
            event_type=event.event_type,
            aggregate_id=event.aggregate_id,
            payload_sha256=event.payload_sha256,
            created_at=_utc_text(event.created_at),
        )
        if not hmac.compare_digest(event.event_sha256, expected_event_sha256):
            raise OutboxConflictError("authority outbox event integrity mismatch")
        mirrored_at = _utc_text(self._now())

        def mirror(connection: sqlite3.Connection) -> bool:
            existing = connection.execute(
                """
                SELECT authority_sequence, event_type, aggregate_id,
                       payload_json, payload_sha256, event_sha256, created_at
                FROM journal_events
                WHERE event_id = ?
                """,
                (event.event_id,),
            ).fetchone()
            expected = (
                str(event.sequence),
                event.event_type,
                event.aggregate_id,
                event.payload_json,
                event.payload_sha256,
                event.event_sha256,
                _utc_text(event.created_at),
            )
            if existing is not None:
                observed = tuple(str(value) for value in existing)
                if observed != expected:
                    raise OutboxConflictError(
                        "journal event_id content conflict"
                    )
                return False
            connection.execute(
                """
                INSERT INTO journal_events
                (authority_sequence, event_id, event_type, aggregate_id, payload_json,
                 payload_sha256, event_sha256, created_at, mirrored_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.sequence,
                    event.event_id,
                    event.event_type,
                    event.aggregate_id,
                    event.payload_json,
                    event.payload_sha256,
                    event.event_sha256,
                    _utc_text(event.created_at),
                    mirrored_at,
                ),
            )
            return True

        return _SqliteUnitOfWork(_operational_spec())._write(mirror)


def _mirror_authority_outbox(
    authority: _AuthorityStore,
    journal: _OperationalJournal,
    *,
    limit: int,
) -> OutboxMirrorResult:
    if not isinstance(authority, _AuthorityStore) or not isinstance(
        journal,
        _OperationalJournal,
    ):
        raise TypeError("trusted authority and operational journal are required")
    read_store_pair_descriptor()
    events = authority._read_pending_outbox(limit=limit)
    inserted = 0
    acknowledged = 0
    for event in events:
        if journal._mirror_event(event):
            inserted += 1
        if authority._acknowledge_outbox(event.event_id):
            acknowledged += 1
    return OutboxMirrorResult(
        scanned_events=len(events),
        inserted_events=inserted,
        acknowledged_events=acknowledged,
    )


def read_store_pair_descriptor() -> StorePairDescriptor:
    """Read only the fixed pair identity; never return a SQL connection."""

    authority = AuthorityReader().read_identity()
    operational = OperationalReader().read_identity()
    if operational.installation_id != authority.installation_id:
        raise StoreBootstrapError("control-plane store pair identity mismatch")
    return StorePairDescriptor(
        installation_id=authority.installation_id,
        authority_kind=authority.store_kind,
        operational_kind=operational.store_kind,
    )


__all__ = [
    "AuthorityReader",
    "AuthorityGrant",
    "AuthorityIdentity",
    "AuthorityRootError",
    "AuthorityOutboxEvent",
    "AuthorizationEnvelope",
    "AuthorizationError",
    "AuthorizationExpiredError",
    "AuthorizationRejectedError",
    "AuthorizationReplayError",
    "OperationalReader",
    "OutboxConflictError",
    "OutboxMirrorResult",
    "PendingOutboxError",
    "StoreAlreadyBootstrappedError",
    "StoreBootstrapError",
    "StoreBootstrapIncompleteError",
    "StoreBootstrapInProgressError",
    "StoreBootstrapReceipt",
    "StoreConfigurationError",
    "StoreError",
    "StorePairDescriptor",
    "StoreIdentity",
    "TaskAuthorityTicket",
    "TaskExecutionLease",
    "TaskReportAuthorityBinding",
    "TaskReportAuthorityError",
    "TaskTicketSnapshot",
    "TaskTicketError",
    "TaskTicketIdempotencyError",
    "TaskTicketStateError",
    "TrustedReceiptAttestation",
    "TrustedReceiptConflictError",
    "read_store_pair_descriptor",
]
