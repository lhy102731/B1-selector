"""Physically isolated stores owned by the research control plane."""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
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
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
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
    CREATE TABLE authority_outbox (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE,
        event_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
        created_at TEXT NOT NULL,
        mirrored_at TEXT
    )
    """,
)
_OPERATIONAL_SCHEMA = (
    """
    CREATE TABLE journal_events (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE,
        event_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
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
    created_at: datetime


@dataclass(frozen=True, slots=True)
class OutboxMirrorResult:
    scanned_events: int
    inserted_events: int
    acknowledged_events: int


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
                ("schema_version", "1"),
                ("store_kind", store_kind),
            ),
        )
        connection.execute("PRAGMA user_version = 1")
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
        )
        _provision_store(
            operational_staging,
            store_kind="OPERATIONAL_JOURNAL",
            metadata_table="operational_meta",
            installation_id=installation_id,
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


def _trusted_bootstrap_at_paths(
    *,
    authority_path: str | Path,
    operational_path: str | Path,
) -> StoreBootstrapReceipt:
    """Private test seam for provisioning fixed production locations."""

    resolved_authority = Path(authority_path).resolve(strict=False)
    resolved_operational = Path(operational_path).resolve(strict=False)
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
        )
    finally:
        lock_path.unlink(missing_ok=True)


def _trusted_bootstrap() -> StoreBootstrapReceipt:
    """Provision the two fixed-path stores from a trusted entrypoint."""

    return _trusted_bootstrap_at_paths(
        authority_path=_AUTHORITY_STORE_PATH,
        operational_path=_OPERATIONAL_STORE_PATH,
    )


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
        schema_version=1,
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
        schema_version=1,
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


def _insert_authority_outbox(
    connection: sqlite3.Connection,
    *,
    event_type: str,
    aggregate_id: str,
    payload: dict[str, object],
    created_at: datetime,
) -> None:
    payload_json, payload_sha256 = _canonical_payload(payload)
    connection.execute(
        """
        INSERT INTO authority_outbox
        (event_id, event_type, aggregate_id, payload_json, payload_sha256,
         created_at, mirrored_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            f"evt_{secrets.token_hex(16)}",
            event_type,
            aggregate_id,
            payload_json,
            payload_sha256,
            _utc_text(created_at),
        ),
    )


def _effects_json(effects: tuple[SideEffect, ...]) -> str:
    return json.dumps(
        [effect.value for effect in effects],
        separators=(",", ":"),
    )


class _AuthorityStore:
    """Trusted V2 authority mutations; never exported to ordinary workers."""

    __slots__ = ("_clock",)

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _now(self) -> datetime:
        return _require_aware_datetime(self._clock(), "clock result")

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
                       payload_json, payload_sha256, created_at
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
                except (TypeError, ValueError) as error:
                    raise OutboxConflictError(
                        "authority outbox payload is invalid"
                    ) from error
                if (
                    canonical_json != payload_json
                    or not hmac.compare_digest(
                        canonical_sha256,
                        payload_sha256,
                    )
                ):
                    raise OutboxConflictError(
                        "authority outbox payload integrity mismatch"
                    )
                events.append(
                    AuthorityOutboxEvent(
                        sequence=int(row["sequence"]),
                        event_id=str(row["event_id"]),
                        event_type=str(row["event_type"]),
                        aggregate_id=str(row["aggregate_id"]),
                        payload_json=payload_json,
                        payload_sha256=payload_sha256,
                        created_at=_parse_utc_text(str(row["created_at"])),
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
        bearer_secret = secrets.token_urlsafe(32)
        envelope = AuthorizationEnvelope(
            authorization_ref=authorization_ref,
            phase=phase,
            attempt_id=attempt_id,
            actor=actor,
            identity=identity,
            expires_at=expiry,
            allowed_side_effects=allowed_side_effects,
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
        now = self._now()
        grant_id = f"grant_{secrets.token_hex(16)}"
        grant_secret = secrets.token_urlsafe(32)
        grant_secret_sha256 = hashlib.sha256(
            grant_secret.encode("utf-8")
        ).hexdigest()
        envelope_secret_sha256 = hashlib.sha256(
            envelope._bearer_secret._reveal_for_authority_check().encode("utf-8")
        ).hexdigest()

        def claim(connection: sqlite3.Connection) -> None:
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
                raise AuthorizationExpiredError("authorization envelope expired")
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

        _SqliteUnitOfWork(_authority_spec())._write(claim)
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
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _now(self) -> datetime:
        return _require_aware_datetime(self._clock(), "clock result")

    def _mirror_event(self, event: AuthorityOutboxEvent) -> bool:
        if not isinstance(event, AuthorityOutboxEvent):
            raise TypeError("event must be an AuthorityOutboxEvent")
        mirrored_at = _utc_text(self._now())

        def mirror(connection: sqlite3.Connection) -> bool:
            existing = connection.execute(
                """
                SELECT event_type, aggregate_id, payload_json, payload_sha256,
                       created_at
                FROM journal_events
                WHERE event_id = ?
                """,
                (event.event_id,),
            ).fetchone()
            expected = (
                event.event_type,
                event.aggregate_id,
                event.payload_json,
                event.payload_sha256,
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
                (event_id, event_type, aggregate_id, payload_json,
                 payload_sha256, created_at, mirrored_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.event_type,
                    event.aggregate_id,
                    event.payload_json,
                    event.payload_sha256,
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
    "AuthorityOutboxEvent",
    "AuthorizationEnvelope",
    "AuthorizationError",
    "AuthorizationExpiredError",
    "AuthorizationRejectedError",
    "AuthorizationReplayError",
    "OperationalReader",
    "OutboxConflictError",
    "OutboxMirrorResult",
    "StoreAlreadyBootstrappedError",
    "StoreBootstrapError",
    "StoreBootstrapIncompleteError",
    "StoreBootstrapInProgressError",
    "StoreBootstrapReceipt",
    "StoreConfigurationError",
    "StoreError",
    "StorePairDescriptor",
    "StoreIdentity",
    "read_store_pair_descriptor",
]
