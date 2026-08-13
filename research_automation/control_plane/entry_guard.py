"""Deterministic entry and authorization primitives for the research control plane."""
from __future__ import annotations

import re
import os
import json
import hashlib
import hmac
import secrets
import sqlite3
import stat
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .contracts import (
    ACTOR_TYPES,
    Actor,
    IdentityBinding,
    Phase,
    PhaseGrant,
    SideEffect,
    SideEffectLease,
    TaskTicket,
    TicketSnapshot,
    canonical_json,
    canonical_sha256,
)


_SCRIPT_SUFFIXES = frozenset({".py", ".bat", ".ps1", ".sh"})
_ENTRY_DISPOSITIONS = frozenset(
    {
        "CONTROLLED_RESEARCH",
        "LEGACY_UNAUDITED",
        "PRODUCTION_DAILY",
        "ADMIN_ONLY",
        "TEST_ONLY",
        "DENIED_WEB",
    }
)
_ENTRY_RECORD_FIELDS = frozenset(
    {
        "entry_id",
        "path",
        "kind",
        "callable_name",
        "actor_type",
        "content_sha256",
        "disposition",
        "trust_state",
        "declared_side_effects",
        "declared_phase",
        "resource_roots",
        "external_metadata",
        "source",
    }
)
_BOUNDED_ROOTS = (
    ".",
    "apps",
    "tools",
    "research_automation",
    "ag2_research",
    "research",
    "l2",
    "strategy",
    "utils",
    "tests",
)
_EXCLUDED_DIRECTORY_NAMES = frozenset(
    {
        ".git",
        ".agents",
        ".claude",
        ".codex_pydeps",
        ".venv",
        "venv",
        "node_modules",
        "__pycache__",
        ".pytest_cache",
        ".idea",
        ".cache",
        ".vscode",
        "artifacts",
        "archive",
        "cache",
        "caches",
        "tmp",
        "_output",
        "output",
        "outputs",
        "research_state",
        "dist",
        "build",
    }
)
_EXCLUDED_RELATIVE_DIRECTORIES = frozenset(
    {"research_automation/_output"}
)
_PRODUCTION_DAILY_PATHS = frozenset(
    {
        "run_select.bat",
        "daily_run.py",
        "daily_select.py",
        "main.py",
        "build_daily_ret_cache.py",
        "build_indicators_cache.py",
        "backtest_brick_v2.py",
        "filter_exec_reduce.py",
        "run_b1_v3.py",
        "run_b3.py",
        "tools/backfill_daily_pcf_baostock.py",
        "tools/select_etf_candidates.py",
        "tools/ths_yuanhang_bridge/build.ps1",
        "tools/update_ths_market_assets.py",
        "tools/update_today_ths.py",
    }
)
_ADMIN_ONLY_PATHS = frozenset({"run_select1.bat"})
_THS_BRIDGE_MARKER_PATH = "utils/ths_yuanhang_bridge.py"
_THS_BRIDGE_RUNTIME_PATHS = (
    "tools/ths_yuanhang_bridge/build.ps1",
    "tools/ths_yuanhang_bridge/YuanhangBridge.cs",
    "tools/ths_yuanhang_bridge/YuanhangBridge.dll",
    "tools/ths_yuanhang_bridge/YuanhangBridge.runtimeconfig.json",
    "tools/ths_yuanhang_bridge/workspace/datacenter.xml",
    "tools/ths_yuanhang_bridge/workspace/DNSTest.xml",
)
_PRODUCTION_DAILY_SCHEDULER_PATH = "\\A\u80a1\u9009\u80a1"
_CONTROL_PLANE_DB_PATH = (
    Path(__file__).resolve().parents[2]
    / "research_state"
    / "control_plane"
    / "p0"
    / "control_plane.sqlite3"
)
_REQUIRED_CONTROL_PLANE_TABLES = frozenset(
    {
        "control_plane_meta",
        "authorizations",
        "phase_grants",
        "entry_permissions",
        "task_tickets",
        "side_effect_events",
    }
)
_REQUIRED_CONTROL_PLANE_COLUMNS = {
    "control_plane_meta": frozenset({"key", "value"}),
    "authorizations": frozenset(
        {
            "authorization_ref",
            "phase",
            "actor_id",
            "actor_type",
            "invocation_id",
            "plan_hash",
            "scope_hash",
            "policy_hash",
            "secret_hash",
            "state",
        }
    ),
    "phase_grants": frozenset(
        {
            "grant_id",
            "authorization_ref",
            "phase",
            "actor_id",
            "actor_type",
            "invocation_id",
            "plan_hash",
            "scope_hash",
            "policy_hash",
            "secret_hash",
            "allowed_effects",
            "state",
        }
    ),
    "entry_permissions": frozenset(
        {"entry_id", "phase", "effect", "resource_root"}
    ),
    "task_tickets": frozenset(
        {
            "ticket_id",
            "grant_id",
            "entry_id",
            "effect",
            "resource_scope",
            "idempotency_key",
            "request_hash",
            "secret_hash",
            "lease_id",
            "lease_secret_hash",
            "state",
        }
    ),
    "side_effect_events": frozenset(
        {"event_id", "ticket_id", "event_type", "evidence_ref"}
    ),
}
_REQUIRED_CONTROL_PLANE_UNIQUE_KEYS = {
    "control_plane_meta": frozenset({("key",)}),
    "authorizations": frozenset({("authorization_ref",)}),
    "phase_grants": frozenset({("grant_id",), ("authorization_ref",)}),
    "entry_permissions": frozenset(
        {("entry_id", "phase", "effect", "resource_root")}
    ),
    "task_tickets": frozenset(
        {("ticket_id",), ("grant_id", "idempotency_key")}
    ),
    "side_effect_events": frozenset(),
}
_REQUIRED_CONTROL_PLANE_PRIMARY_KEYS = {
    "control_plane_meta": ("key",),
    "authorizations": ("authorization_ref",),
    "phase_grants": ("grant_id",),
    "entry_permissions": ("entry_id", "phase", "effect", "resource_root"),
    "task_tickets": ("ticket_id",),
    "side_effect_events": ("event_id",),
}
_REQUIRED_IMPORT_SEAMS = (
    (
        "research_automation.autonomous_runner",
        "AutonomousRunnerV1.run",
        "research_automation/autonomous_runner.py",
        (
            SideEffect.READ,
            SideEffect.WRITE_STAGING,
            SideEffect.RUN_RESEARCH,
            SideEffect.WRITE_KBASE,
            SideEffect.GIT_MUTATION,
        ),
    ),
    (
        "research_automation.kbase_ag2_full_cycle",
        "run_kbase_ag2_full_cycle",
        "research_automation/kbase_ag2_full_cycle.py",
        (
            SideEffect.READ,
            SideEffect.WRITE_STAGING,
            SideEffect.RUN_RESEARCH,
            SideEffect.GIT_MUTATION,
        ),
    ),
    (
        "research_automation.discovery_execution_bridge",
        "execute_plan",
        "research_automation/discovery_execution_bridge.py",
        (SideEffect.WRITE_STAGING, SideEffect.RUN_RESEARCH),
    ),
    # P8 CR-009 (GPT F-03): the TrustedEvaluator is the ONLY entry that
    # declares OPEN_HOLDOUT; every other seam and runner stays deny-by-
    # default for the holdout effect.  The inventory scan emits this seam
    # so the reviewed entry policy declares the effect explicitly.
    (
        "research_automation.control_plane.final_evaluator",
        "TrustedEvaluator.evaluate_v2",
        "research_automation/control_plane/final_evaluator.py",
        (SideEffect.OPEN_HOLDOUT,),
    ),
)


class EntryNotDeclaredError(RuntimeError):
    """Raised when an executable/import entry is absent from the manifest."""


class AuthorizationError(RuntimeError):
    """Raised whenever a phase or side-effect authorization is invalid."""


def _connect_control_plane_store() -> sqlite3.Connection:
    """Open only an existing control-plane database; never create one."""
    try:
        database_uri = _CONTROL_PLANE_DB_PATH.resolve(strict=False).as_uri()
        return sqlite3.connect(
            f"{database_uri}?mode=rw",
            uri=True,
            timeout=30,
            isolation_level=None,
        )
    except (OSError, ValueError, sqlite3.DatabaseError) as error:
        raise AuthorizationError(
            "pre-provisioned control-plane store is required"
        ) from error


def _begin_immediate(connection: sqlite3.Connection) -> None:
    try:
        connection.execute("BEGIN IMMEDIATE")
    except sqlite3.DatabaseError as error:
        raise AuthorizationError("control-plane store is unavailable") from error


def _migrate_legacy_control_plane_store_if_needed() -> None:
    """Quarantine the legacy token schema without upgrading any authority."""
    legacy_tables = {"phase_tokens", "phase_gates", "authorizer_meta"}
    connection = _connect_control_plane_store()

    with closing(connection):
        connection.row_factory = sqlite3.Row
        try:
            _begin_immediate(connection)
            tables = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            if "control_plane_meta" in tables:
                connection.commit()
                return
            if not legacy_tables.issubset(tables):
                raise AuthorizationError("unsupported control-plane schema version")

            connection.execute(
                "ALTER TABLE phase_tokens RENAME TO legacy_phase_tokens"
            )
            connection.execute(
                "ALTER TABLE phase_gates RENAME TO legacy_phase_gates"
            )
            connection.execute(
                "ALTER TABLE authorizer_meta RENAME TO legacy_authorizer_meta"
            )
            for table in (
                "legacy_phase_tokens",
                "legacy_phase_gates",
                "legacy_authorizer_meta",
            ):
                connection.execute(
                    f"""
                    ALTER TABLE {table}
                    ADD COLUMN trust_state TEXT NOT NULL
                    DEFAULT 'LEGACY_UNTRUSTED'
                    """
                )

            connection.execute(
                """
                CREATE TABLE control_plane_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            # Legacy hashes were caller-supplied identifiers, not credentials.
            # A trusted broker must provision the new binding and envelope later.
            connection.executemany(
                "INSERT INTO control_plane_meta(key, value) VALUES (?, ?)",
                (
                    ("schema_version", "2"),
                    ("migration_state", "LEGACY_QUARANTINED"),
                ),
            )
            connection.execute(
                """
                CREATE TABLE authorizations (
                    authorization_ref TEXT PRIMARY KEY,
                    phase TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_type TEXT NOT NULL,
                    invocation_id TEXT NOT NULL,
                    plan_hash TEXT NOT NULL,
                    scope_hash TEXT NOT NULL,
                    policy_hash TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    state TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE phase_grants (
                    grant_id TEXT PRIMARY KEY,
                    authorization_ref TEXT NOT NULL UNIQUE,
                    phase TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_type TEXT NOT NULL,
                    invocation_id TEXT NOT NULL,
                    plan_hash TEXT NOT NULL,
                    scope_hash TEXT NOT NULL,
                    policy_hash TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    allowed_effects TEXT NOT NULL,
                    state TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE entry_permissions (
                    entry_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    effect TEXT NOT NULL,
                    resource_root TEXT NOT NULL,
                    PRIMARY KEY(entry_id, phase, effect, resource_root)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE task_tickets (
                    ticket_id TEXT PRIMARY KEY,
                    grant_id TEXT NOT NULL,
                    entry_id TEXT NOT NULL,
                    effect TEXT NOT NULL,
                    resource_scope TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    lease_id TEXT,
                    lease_secret_hash TEXT,
                    state TEXT NOT NULL,
                    UNIQUE(grant_id, idempotency_key)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE side_effect_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    evidence_ref TEXT
                )
                """
            )
            connection.commit()
        except AuthorizationError:
            connection.rollback()
            raise
        except sqlite3.DatabaseError as error:
            connection.rollback()
            raise AuthorizationError("legacy schema migration failed") from error
        except Exception:
            connection.rollback()
            raise


def _require_control_plane_schema(connection: sqlite3.Connection) -> None:
    try:
        schema = connection.execute(
            "SELECT value FROM control_plane_meta WHERE key = 'schema_version'"
        ).fetchone()
        tables = {
            table["name"]
            for table in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        table_info = {
            table: tuple(
                connection.execute(f'PRAGMA table_info("{table}")')
            )
            for table in _REQUIRED_CONTROL_PLANE_TABLES
        }
        columns = {
            table: frozenset(row["name"] for row in rows)
            for table, rows in table_info.items()
        }
        primary_keys = {
            table: tuple(
                row["name"]
                for row in sorted(
                    (row for row in rows if row["pk"]),
                    key=lambda row: row["pk"],
                )
            )
            for table, rows in table_info.items()
        }
        unique_keys: dict[str, frozenset[tuple[str, ...]]] = {}
        for table in _REQUIRED_CONTROL_PLANE_TABLES:
            table_keys: set[tuple[str, ...]] = set()
            for index in connection.execute(f'PRAGMA index_list("{table}")'):
                if not index["unique"] or index["partial"]:
                    continue
                index_name = str(index["name"]).replace('"', '""')
                table_keys.add(
                    tuple(
                        column["name"]
                        for column in connection.execute(
                            f'PRAGMA index_info("{index_name}")'
                        )
                    )
                )
            unique_keys[table] = frozenset(table_keys)
    except sqlite3.DatabaseError as error:
        raise AuthorizationError(
            "unsupported control-plane schema version"
        ) from error
    if schema is None or schema["value"] != "2":
        raise AuthorizationError("unsupported control-plane schema version")
    if not _REQUIRED_CONTROL_PLANE_TABLES.issubset(tables):
        raise AuthorizationError("incomplete control-plane schema")
    if any(
        columns[table] != expected_columns
        for table, expected_columns in _REQUIRED_CONTROL_PLANE_COLUMNS.items()
    ):
        raise AuthorizationError("incomplete control-plane schema")
    if any(
        unique_keys[table] != expected_keys
        for table, expected_keys in _REQUIRED_CONTROL_PLANE_UNIQUE_KEYS.items()
    ):
        raise AuthorizationError("incomplete control-plane schema")
    if any(
        primary_keys[table] != expected_key
        for table, expected_key in _REQUIRED_CONTROL_PLANE_PRIMARY_KEYS.items()
    ):
        raise AuthorizationError("incomplete control-plane schema")


def _require_control_plane_identity(
    connection: sqlite3.Connection,
    identity_binding: IdentityBinding,
) -> None:
    rows = {
        row["key"]: row["value"]
        for row in connection.execute(
            """
            SELECT key, value FROM control_plane_meta
            WHERE key IN ('plan_hash', 'scope_hash', 'policy_hash')
            """
        )
    }
    expected = {
        "plan_hash": identity_binding.plan_hash,
        "scope_hash": identity_binding.scope_hash,
        "policy_hash": identity_binding.policy_hash,
    }
    if rows != expected:
        raise AuthorizationError(
            "approved control-plane identity binding mismatch"
        )


def _validate_resource_path_lexically(resource: str | Path) -> str:
    """Reject unsafe path spellings without touching the filesystem."""
    raw_path = os.fspath(resource)
    if os.pardir in Path(raw_path).parts:
        raise AuthorizationError(
            "unsafe resource path: parent traversal is forbidden"
        )
    if os.name == "nt":
        classified_path = raw_path.replace("/", "\\")
        if classified_path.startswith(("\\\\?\\", "\\\\.\\")):
            raise AuthorizationError(
                "unsafe Windows resource path: device namespace aliases are forbidden"
            )
        if classified_path.startswith("\\\\"):
            raise AuthorizationError(
                "unsafe Windows resource path: UNC paths are not approved in P0"
            )
        if os.path.isreserved(classified_path):
            raise AuthorizationError(
                "unsafe Windows resource path: reserved Windows path component"
            )
        drive, tail = os.path.splitdrive(classified_path)
        if not drive:
            raise AuthorizationError(
                "unsafe Windows resource path: root-relative or relative paths are forbidden"
            )
        if not os.path.isabs(classified_path):
            raise AuthorizationError(
                "unsafe Windows resource path: drive-relative paths are forbidden"
            )
        if ":" in tail:
            raise AuthorizationError(
                "unsafe Windows resource path: alternate data streams are forbidden"
            )
    return raw_path


def _resolve_validated_resource(raw_path: str) -> Path:
    resolved = Path(raw_path).resolve()
    if os.name == "nt":
        return Path(os.path.normcase(str(resolved)))
    return resolved


def _resource_path_has_reparse_point(path: Path) -> bool:
    """Inspect from the filesystem anchor toward the leaf without following it."""
    for component in reversed((path, *path.parents)):
        try:
            if _path_is_reparse_point(component):
                return True
        except OSError as error:
            raise AuthorizationError(
                "resource path reparse inspection failed"
            ) from error
    return False


def _path_is_reparse_point(path: Path) -> bool:
    """Recognize any Windows reparse point, not only links and junctions."""
    if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
        return True
    try:
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except FileNotFoundError:
        return False
    return bool(
        attributes
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x00000400)
    )


def _resolve_authorized_resource(resource: str | Path) -> Path:
    """Resolve a resource path after lexical namespace validation."""
    return _resolve_validated_resource(_validate_resource_path_lexically(resource))


def _resolve_side_effect_resource(resource: str | Path) -> Path:
    """Resolve an effect target only after rejecting existing reparse parents."""
    raw_path = _validate_resource_path_lexically(resource)
    if _resource_path_has_reparse_point(Path(raw_path)):
        raise AuthorizationError("resource path contains a reparse point")
    return _resolve_validated_resource(raw_path)


def _resolve_approved_resource_root(resource_root: str | Path) -> Path:
    raw_root = _validate_resource_path_lexically(resource_root)
    root = Path(raw_root)
    if _resource_path_has_reparse_point(root) or not root.is_dir():
        raise AuthorizationError(
            "approved resource root is not stable or is a reparse point"
        )
    return _resolve_validated_resource(raw_root)


def claim_phase(
    authorization_ref: str,
    bearer_secret: str,
    actor: Actor,
    identity_binding: IdentityBinding,
) -> PhaseGrant:
    """Claim only authority that was provisioned in the fixed P0 store."""
    if not isinstance(actor, Actor) or not isinstance(identity_binding, IdentityBinding):
        raise AuthorizationError("typed actor and identity binding are required")
    if not str(authorization_ref or "").strip() or not str(bearer_secret or ""):
        raise AuthorizationError("authorization reference and bearer secret are required")
    if not _CONTROL_PLANE_DB_PATH.is_file():
        raise AuthorizationError("pre-provisioned control-plane store is required")
    _migrate_legacy_control_plane_store_if_needed()
    reference = str(authorization_ref).strip()
    supplied_secret_hash = hashlib.sha256(bearer_secret.encode("utf-8")).hexdigest()
    with closing(_connect_control_plane_store()) as connection:
        connection.row_factory = sqlite3.Row
        _begin_immediate(connection)
        try:
            _require_control_plane_schema(connection)
            _require_control_plane_identity(connection, identity_binding)
            row = connection.execute(
                "SELECT * FROM authorizations WHERE authorization_ref = ?",
                (reference,),
            ).fetchone()
            if row is None:
                raise AuthorizationError("unknown authorization reference")
            if row["state"] != "PENDING":
                raise AuthorizationError("authorization was already claimed")
            if row["phase"] != Phase.P0.value:
                raise AuthorizationError("authorization is not for P0")
            if (
                row["actor_id"] != actor.actor_id
                or row["actor_type"] != actor.actor_type
                or row["invocation_id"] != actor.invocation_id
                or row["plan_hash"] != identity_binding.plan_hash
                or row["scope_hash"] != identity_binding.scope_hash
                or row["policy_hash"] != identity_binding.policy_hash
                or not hmac.compare_digest(row["secret_hash"], supplied_secret_hash)
            ):
                raise AuthorizationError("authorization identity or bearer secret mismatch")

            grant_id = secrets.token_urlsafe(24)
            grant_secret = secrets.token_urlsafe(32)
            allowed_effects = (SideEffect.READ, SideEffect.WRITE_CONTROL_PLANE)
            updated = connection.execute(
                """
                UPDATE authorizations SET state = 'CLAIMED'
                WHERE authorization_ref = ? AND state = 'PENDING'
                """,
                (reference,),
            )
            if updated.rowcount != 1:
                raise AuthorizationError("authorization claim lost a concurrent race")
            connection.execute(
                """
                INSERT INTO phase_grants
                (grant_id, authorization_ref, phase, actor_id, actor_type, invocation_id,
                 plan_hash, scope_hash, policy_hash, secret_hash, allowed_effects, state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')
                """,
                (
                    grant_id,
                    reference,
                    Phase.P0.value,
                    actor.actor_id,
                    actor.actor_type,
                    actor.invocation_id,
                    identity_binding.plan_hash,
                    identity_binding.scope_hash,
                    identity_binding.policy_hash,
                    hashlib.sha256(grant_secret.encode("utf-8")).hexdigest(),
                    canonical_json([effect.value for effect in allowed_effects]),
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return PhaseGrant(
        grant_id=grant_id,
        bearer_secret=grant_secret,
        authorization_ref=reference,
        phase=Phase.P0,
        actor=actor,
        identity_binding=identity_binding,
        allowed_side_effects=allowed_effects,
    )


def _require_active_phase_grant(
    connection: sqlite3.Connection,
    phase_grant: PhaseGrant,
) -> sqlite3.Row:
    _require_control_plane_schema(connection)
    grant_row = connection.execute(
        "SELECT * FROM phase_grants WHERE grant_id = ?",
        (phase_grant.grant_id,),
    ).fetchone()
    if grant_row is None or grant_row["state"] != "ACTIVE":
        raise AuthorizationError("phase grant is not active")
    if (
        grant_row["authorization_ref"] != phase_grant.authorization_ref
        or grant_row["phase"] != phase_grant.phase.value
        or grant_row["actor_id"] != phase_grant.actor.actor_id
        or grant_row["actor_type"] != phase_grant.actor.actor_type
        or grant_row["invocation_id"] != phase_grant.actor.invocation_id
        or grant_row["plan_hash"] != phase_grant.identity_binding.plan_hash
        or grant_row["scope_hash"] != phase_grant.identity_binding.scope_hash
        or grant_row["policy_hash"] != phase_grant.identity_binding.policy_hash
        or grant_row["allowed_effects"]
        != canonical_json(
            [effect.value for effect in phase_grant.allowed_side_effects]
        )
        or not hmac.compare_digest(
            grant_row["secret_hash"],
            hashlib.sha256(
                phase_grant.bearer_secret.encode("utf-8")
            ).hexdigest(),
        )
    ):
        raise AuthorizationError("phase grant capability mismatch")
    _require_control_plane_identity(connection, phase_grant.identity_binding)
    return grant_row


def issue_task_ticket(
    phase_grant: PhaseGrant,
    entry_id: str,
    effect: SideEffect,
    resource_scope: str | Path,
    idempotency_key: str,
) -> TaskTicket:
    """Issue one task capability only when the active phase allows its effect."""
    if not isinstance(phase_grant, PhaseGrant):
        raise AuthorizationError("phase grant must be a PhaseGrant capability")
    if not isinstance(effect, SideEffect):
        raise AuthorizationError("unknown side effect is denied")
    if phase_grant.phase is not Phase.P0 or effect not in phase_grant.allowed_side_effects:
        raise AuthorizationError(f"side effect {effect.value} is not allowed by P0")
    entry = str(entry_id or "").strip()
    key = str(idempotency_key or "").strip()
    if not entry or not key:
        raise AuthorizationError("entry id and idempotency key are required")
    resolved_scope = _resolve_authorized_resource(resource_scope)
    if not _CONTROL_PLANE_DB_PATH.is_file():
        raise AuthorizationError("pre-provisioned control-plane store is required")

    request_payload = {
        "grant_id": phase_grant.grant_id,
        "entry_id": entry,
        "effect": effect.value,
        "resource_scope": str(resolved_scope),
        "idempotency_key": key,
    }
    request_hash = canonical_sha256(request_payload)
    ticket_id = hashlib.sha256(
        f"{phase_grant.grant_id}|{key}".encode("utf-8")
    ).hexdigest()
    ticket_secret = hmac.new(
        phase_grant.bearer_secret.encode("utf-8"),
        request_hash.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()

    with closing(_connect_control_plane_store()) as connection:
        connection.row_factory = sqlite3.Row
        _begin_immediate(connection)
        try:
            _require_active_phase_grant(connection, phase_grant)

            permissions = connection.execute(
                """
                SELECT resource_root FROM entry_permissions
                WHERE entry_id = ? AND phase = 'P0' AND effect = ?
                """,
                (entry, effect.value),
            ).fetchall()
            allowed = False
            for permission in permissions:
                try:
                    approved_root = _resolve_approved_resource_root(
                        permission["resource_root"]
                    )
                    resolved_scope.relative_to(approved_root)
                except ValueError:
                    continue
                allowed = True
                break
            if not allowed:
                raise AuthorizationError("entry effect or resource scope is not approved")

            existing = connection.execute(
                """
                SELECT request_hash FROM task_tickets
                WHERE grant_id = ? AND idempotency_key = ?
                """,
                (phase_grant.grant_id, key),
            ).fetchone()
            if existing is not None:
                if existing["request_hash"] != request_hash:
                    raise AuthorizationError(
                        "idempotency key was reused with different semantics"
                    )
            else:
                unresolved = connection.execute(
                    """
                    SELECT ticket_id FROM task_tickets
                    WHERE effect = ? AND resource_scope = ?
                      AND state = 'IN_DOUBT'
                    LIMIT 1
                    """,
                    (
                        effect.value,
                        str(resolved_scope),
                    ),
                ).fetchone()
                if unresolved is not None:
                    raise AuthorizationError(
                        "unresolved IN_DOUBT side effect cannot be reissued"
                    )
                connection.execute(
                    """
                    INSERT INTO task_tickets
                    (ticket_id, grant_id, entry_id, effect, resource_scope,
                     idempotency_key, request_hash, secret_hash, state)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ISSUED')
                    """,
                    (
                        ticket_id,
                        phase_grant.grant_id,
                        entry,
                        effect.value,
                        str(resolved_scope),
                        key,
                        request_hash,
                        hashlib.sha256(ticket_secret.encode("utf-8")).hexdigest(),
                    ),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return TaskTicket(
        ticket_id=ticket_id,
        bearer_secret=ticket_secret,
        grant_id=phase_grant.grant_id,
        authorization_ref=phase_grant.authorization_ref,
        entry_id=entry,
        effect=effect,
        resource_scope=str(resolved_scope),
        idempotency_key=key,
        actor=phase_grant.actor,
        identity_binding=phase_grant.identity_binding,
    )


def begin_side_effect(
    task_ticket: TaskTicket,
    expected_entry_id: str,
    expected_effect: SideEffect,
    expected_resource: str | Path,
) -> SideEffectLease:
    """Atomically claim one issued ticket immediately before its side effect."""
    if not isinstance(task_ticket, TaskTicket):
        raise AuthorizationError("task ticket must be a TaskTicket capability")
    if not isinstance(expected_effect, SideEffect):
        raise AuthorizationError("unknown side effect is denied")
    resolved_resource = str(_resolve_side_effect_resource(expected_resource))
    if (
        str(expected_entry_id).strip() != task_ticket.entry_id
        or expected_effect is not task_ticket.effect
        or resolved_resource != task_ticket.resource_scope
    ):
        raise AuthorizationError("task ticket entry, effect, or resource mismatch")
    if not _CONTROL_PLANE_DB_PATH.is_file():
        raise AuthorizationError("pre-provisioned control-plane store is required")

    lease_id = secrets.token_urlsafe(24)
    lease_secret = secrets.token_urlsafe(32)
    with closing(_connect_control_plane_store()) as connection:
        connection.row_factory = sqlite3.Row
        _begin_immediate(connection)
        try:
            _require_control_plane_schema(connection)
            row = connection.execute(
                "SELECT * FROM task_tickets WHERE ticket_id = ?",
                (task_ticket.ticket_id,),
            ).fetchone()
            if row is None:
                raise AuthorizationError("unknown task ticket")
            if row["state"] != "ISSUED":
                raise AuthorizationError("task ticket is not ISSUED")
            grant_row = connection.execute(
                "SELECT * FROM phase_grants WHERE grant_id = ?",
                (row["grant_id"],),
            ).fetchone()
            if grant_row is None or grant_row["state"] != "ACTIVE":
                raise AuthorizationError("phase grant is not active")
            if (
                row["grant_id"] != task_ticket.grant_id
                or row["entry_id"] != task_ticket.entry_id
                or row["effect"] != task_ticket.effect.value
                or row["resource_scope"] != task_ticket.resource_scope
                or row["idempotency_key"] != task_ticket.idempotency_key
                or grant_row["authorization_ref"] != task_ticket.authorization_ref
                or grant_row["actor_id"] != task_ticket.actor.actor_id
                or grant_row["actor_type"] != task_ticket.actor.actor_type
                or grant_row["invocation_id"] != task_ticket.actor.invocation_id
                or grant_row["plan_hash"] != task_ticket.identity_binding.plan_hash
                or grant_row["scope_hash"] != task_ticket.identity_binding.scope_hash
                or grant_row["policy_hash"] != task_ticket.identity_binding.policy_hash
                or not hmac.compare_digest(
                    row["secret_hash"],
                    hashlib.sha256(
                        task_ticket.bearer_secret.encode("utf-8")
                    ).hexdigest(),
                )
            ):
                raise AuthorizationError("task ticket capability mismatch")
            _require_control_plane_identity(connection, task_ticket.identity_binding)
            permissions = connection.execute(
                """
                SELECT resource_root FROM entry_permissions
                WHERE entry_id = ? AND phase = 'P0' AND effect = ?
                """,
                (row["entry_id"], row["effect"]),
            ).fetchall()
            still_allowed = False
            for permission in permissions:
                approved_root = _resolve_approved_resource_root(
                    permission["resource_root"]
                )
                try:
                    Path(resolved_resource).relative_to(approved_root)
                except ValueError:
                    continue
                still_allowed = True
                break
            if not still_allowed:
                raise AuthorizationError(
                    "entry effect or resource scope is no longer approved"
                )
            unresolved = connection.execute(
                """
                SELECT state FROM task_tickets
                WHERE ticket_id <> ? AND effect = ? AND resource_scope = ?
                  AND state IN ('IN_PROGRESS', 'IN_DOUBT')
                ORDER BY CASE state WHEN 'IN_DOUBT' THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (
                    task_ticket.ticket_id,
                    row["effect"],
                    row["resource_scope"],
                ),
            ).fetchone()
            if unresolved is not None:
                if unresolved["state"] == "IN_DOUBT":
                    raise AuthorizationError(
                        "unresolved IN_DOUBT side effect cannot be replayed"
                    )
                raise AuthorizationError(
                    "side effect is already IN_PROGRESS for this resource"
                )
            updated = connection.execute(
                """
                UPDATE task_tickets
                SET state = 'IN_PROGRESS', lease_id = ?, lease_secret_hash = ?
                WHERE ticket_id = ? AND state = 'ISSUED'
                """,
                (
                    lease_id,
                    hashlib.sha256(lease_secret.encode("utf-8")).hexdigest(),
                    task_ticket.ticket_id,
                ),
            )
            if updated.rowcount != 1:
                raise AuthorizationError("task ticket claim lost a concurrent race")
            connection.execute(
                """
                INSERT INTO side_effect_events(ticket_id, event_type, evidence_ref)
                VALUES (?, 'BEGIN', NULL)
                """,
                (task_ticket.ticket_id,),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return SideEffectLease(
        lease_id=lease_id,
        bearer_secret=lease_secret,
        ticket_id=task_ticket.ticket_id,
        grant_id=task_ticket.grant_id,
        authorization_ref=task_ticket.authorization_ref,
        entry_id=task_ticket.entry_id,
        effect=task_ticket.effect,
        resource_scope=task_ticket.resource_scope,
        actor=task_ticket.actor,
        identity_binding=task_ticket.identity_binding,
    )


def finish_side_effect(
    lease: SideEffectLease,
    outcome: str,
    evidence_ref: str,
) -> TicketSnapshot:
    """Finish one in-progress effect exactly once and append its evidence event."""
    if not isinstance(lease, SideEffectLease):
        raise AuthorizationError("side effect lease must be a SideEffectLease capability")
    terminal_state = str(outcome or "").strip().upper()
    if terminal_state not in {"SUCCEEDED", "FAILED", "IN_DOUBT"}:
        raise AuthorizationError("side effect outcome is invalid")
    evidence = str(evidence_ref or "").strip()
    if not evidence:
        raise AuthorizationError("side effect evidence reference is required")
    if not _CONTROL_PLANE_DB_PATH.is_file():
        raise AuthorizationError("pre-provisioned control-plane store is required")

    with closing(_connect_control_plane_store()) as connection:
        connection.row_factory = sqlite3.Row
        _begin_immediate(connection)
        try:
            _require_control_plane_schema(connection)
            row = connection.execute(
                "SELECT * FROM task_tickets WHERE ticket_id = ?",
                (lease.ticket_id,),
            ).fetchone()
            if row is None:
                raise AuthorizationError("unknown task ticket")
            if row["state"] != "IN_PROGRESS":
                raise AuthorizationError("task ticket is not IN_PROGRESS")
            grant_row = connection.execute(
                "SELECT * FROM phase_grants WHERE grant_id = ?",
                (row["grant_id"],),
            ).fetchone()
            if (
                row["grant_id"] != lease.grant_id
                or row["entry_id"] != lease.entry_id
                or row["effect"] != lease.effect.value
                or row["resource_scope"] != lease.resource_scope
                or row["lease_id"] != lease.lease_id
                or grant_row is None
                or grant_row["authorization_ref"] != lease.authorization_ref
                or grant_row["actor_id"] != lease.actor.actor_id
                or grant_row["actor_type"] != lease.actor.actor_type
                or grant_row["invocation_id"] != lease.actor.invocation_id
                or grant_row["plan_hash"] != lease.identity_binding.plan_hash
                or grant_row["scope_hash"] != lease.identity_binding.scope_hash
                or grant_row["policy_hash"] != lease.identity_binding.policy_hash
                or not hmac.compare_digest(
                    row["lease_secret_hash"],
                    hashlib.sha256(lease.bearer_secret.encode("utf-8")).hexdigest(),
                )
            ):
                raise AuthorizationError("side effect lease capability mismatch")
            _require_control_plane_identity(connection, lease.identity_binding)
            updated = connection.execute(
                """
                UPDATE task_tickets SET state = ?
                WHERE ticket_id = ? AND state = 'IN_PROGRESS'
                """,
                (terminal_state, lease.ticket_id),
            )
            if updated.rowcount != 1:
                raise AuthorizationError("side effect finish lost a concurrent race")
            connection.execute(
                """
                INSERT INTO side_effect_events(ticket_id, event_type, evidence_ref)
                VALUES (?, ?, ?)
                """,
                (lease.ticket_id, terminal_state, evidence),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return TicketSnapshot(
        ticket_id=lease.ticket_id,
        state=terminal_state,
        evidence_ref=evidence,
    )


def mark_side_effect_in_doubt(
    phase_grant: PhaseGrant,
    *,
    ticket_id: str,
    evidence_ref: str,
) -> TicketSnapshot:
    """Close a crashed in-progress ticket without replaying its side effect."""
    if not isinstance(phase_grant, PhaseGrant):
        raise AuthorizationError("phase grant must be a PhaseGrant capability")
    ticket = str(ticket_id or "").strip()
    evidence = str(evidence_ref or "").strip()
    if not ticket or not evidence:
        raise AuthorizationError("ticket id and crash evidence reference are required")
    if not _CONTROL_PLANE_DB_PATH.is_file():
        raise AuthorizationError("pre-provisioned control-plane store is required")

    with closing(_connect_control_plane_store()) as connection:
        connection.row_factory = sqlite3.Row
        _begin_immediate(connection)
        try:
            _require_active_phase_grant(connection, phase_grant)

            ticket_row = connection.execute(
                "SELECT grant_id, state FROM task_tickets WHERE ticket_id = ?",
                (ticket,),
            ).fetchone()
            if ticket_row is None or ticket_row["grant_id"] != phase_grant.grant_id:
                raise AuthorizationError("task ticket does not belong to the phase grant")
            if ticket_row["state"] != "IN_PROGRESS":
                raise AuthorizationError("task ticket is not IN_PROGRESS")
            updated = connection.execute(
                """
                UPDATE task_tickets SET state = 'IN_DOUBT'
                WHERE ticket_id = ? AND grant_id = ? AND state = 'IN_PROGRESS'
                """,
                (ticket, phase_grant.grant_id),
            )
            if updated.rowcount != 1:
                raise AuthorizationError("in-doubt transition lost a concurrent race")
            connection.execute(
                """
                INSERT INTO side_effect_events(ticket_id, event_type, evidence_ref)
                VALUES (?, 'IN_DOUBT', ?)
                """,
                (ticket, evidence),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return TicketSnapshot(
        ticket_id=ticket,
        state="IN_DOUBT",
        evidence_ref=evidence,
    )


@dataclass(frozen=True)
class AuthorizationGrant:
    token_id: str
    phase: Phase
    actor_type: str
    allowed_side_effects: tuple[SideEffect, ...]
    plan_hash: str
    scope_hash: str
    policy_hash: str

    def allows(self, effect: SideEffect) -> bool:
        return False


def _require_hash(value: str, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
        raise AuthorizationError(f"{field_name} must be a 64-character SHA-256 hex digest")
    return normalized


@dataclass(frozen=True)
class EntryRecord:
    entry_id: str
    path: str
    kind: str
    callable_name: str
    actor_type: str
    content_sha256: str | None = None
    disposition: str = "LEGACY_UNAUDITED"
    trust_state: str = "legacy_unaudited"
    declared_side_effects: tuple[SideEffect, ...] = ()
    declared_phase: Phase | None = None
    resource_roots: tuple[str, ...] = ()
    external_metadata: tuple[tuple[str, str], ...] = ()
    source: str = "filesystem_inventory"

    def __post_init__(self) -> None:
        if self.actor_type not in ACTOR_TYPES:
            raise ValueError(
                f"actor_type must be one of {sorted(ACTOR_TYPES)}"
            )
        if self.disposition not in _ENTRY_DISPOSITIONS:
            raise ValueError("entry disposition is not in the closed set")
        if not isinstance(self.trust_state, str) or not self.trust_state.strip():
            raise ValueError("entry trust_state must be a non-empty string")
        if self.content_sha256 is not None and re.fullmatch(
            r"[0-9a-f]{64}",
            self.content_sha256,
        ) is None:
            raise ValueError("entry content_sha256 must be a SHA-256 digest or null")
        if not all(isinstance(root, str) and root.strip() for root in self.resource_roots):
            raise ValueError("entry resource_roots must contain non-empty strings")
        if not all(
            isinstance(key, str)
            and key.strip()
            and isinstance(value, str)
            and value.strip()
            for key, value in self.external_metadata
        ):
            raise ValueError("entry external_metadata must contain text pairs")


@dataclass(frozen=True)
class ReviewedEntryPolicy:
    """Records loaded from a separately reviewed, identity-bound policy file."""

    plan_hash: str
    scope_hash: str
    policy_hash: str
    records: tuple[EntryRecord, ...]

    def __post_init__(self) -> None:
        for field_name in ("plan_hash", "scope_hash", "policy_hash"):
            value = str(getattr(self, field_name) or "").strip().lower()
            if re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError(
                    "reviewed entry policy identity binding is invalid"
                )
            object.__setattr__(self, field_name, value)
        if not isinstance(self.records, tuple) or not all(
            isinstance(record, EntryRecord) for record in self.records
        ):
            raise ValueError("reviewed entry policy records are invalid")


def _safe_relative(root: Path, path: Path) -> str | None:
    try:
        resolved_root = root.resolve()
        resolved = path.resolve()
        relative = resolved.relative_to(resolved_root)
    except (OSError, ValueError):
        return None
    return relative.as_posix()


def _directory_is_excluded(root: Path, directory: Path) -> bool:
    name = directory.name.casefold()
    if name in _EXCLUDED_DIRECTORY_NAMES or name.endswith(".egg-info"):
        return True
    try:
        relative = directory.relative_to(root).as_posix().casefold()
    except ValueError:
        return True
    return relative in _EXCLUDED_RELATIVE_DIRECTORIES


def _relative_file_is_excluded(relative: str) -> bool:
    directory_parts = relative.split("/")[:-1]
    if any(
        part.casefold() in _EXCLUDED_DIRECTORY_NAMES
        or part.casefold().endswith(".egg-info")
        for part in directory_parts
    ):
        return True
    directory = "/".join(directory_parts).casefold()
    return any(
        directory == excluded
        or directory.startswith(f"{excluded}/")
        for excluded in _EXCLUDED_RELATIVE_DIRECTORIES
    )


def _fail_inventory_walk(error: OSError) -> None:
    raise EntryNotDeclaredError("entry inventory scan failed") from error


def _iter_bounded_files(root: Path) -> Iterable[tuple[str, Path]]:
    root = root.resolve()
    seen: set[str] = set()
    for bounded in _BOUNDED_ROOTS:
        base = root if bounded == "." else root / bounded
        if (
            not base.exists()
        ):
            continue
        if _path_is_reparse_point(base) or not base.is_dir():
            raise EntryNotDeclaredError(
                f"bounded source root is unsafe: {bounded}"
            )
        if bounded == ".":
            candidates: Iterable[Path] = sorted(
                base.iterdir(),
                key=lambda path: path.name.casefold(),
            )
        else:
            bounded_files: list[Path] = []
            for current, directory_names, file_names in os.walk(
                base,
                topdown=True,
                followlinks=False,
                onerror=_fail_inventory_walk,
            ):
                current_path = Path(current)
                retained_directories: list[str] = []
                for directory_name in sorted(
                    directory_names,
                    key=str.casefold,
                ):
                    child = current_path / directory_name
                    if _directory_is_excluded(root, child):
                        continue
                    if _path_is_reparse_point(child):
                        raise EntryNotDeclaredError(
                            "bounded source directory is a reparse point: "
                            + str(child)
                        )
                    retained_directories.append(directory_name)
                directory_names[:] = retained_directories
                bounded_files.extend(
                    current_path / file_name
                    for file_name in sorted(file_names, key=str.casefold)
                )
            candidates = bounded_files
        for path in candidates:
            if bounded == "." and path.is_dir():
                continue
            if _path_is_reparse_point(path):
                raise EntryNotDeclaredError(
                    f"bounded source file is a reparse point: {path}"
                )
            if not path.is_file() or path.suffix.lower() not in _SCRIPT_SUFFIXES:
                continue
            relative = _safe_relative(root, path)
            if relative is None or relative in seen:
                continue
            # Exclude only the repository root data/ tree. tools/data/ is a
            # bounded executable source tree and must remain visible.
            folded_relative = relative.casefold()
            if folded_relative == "data" or folded_relative.startswith("data/"):
                continue
            if _relative_file_is_excluded(relative):
                continue
            seen.add(relative)
            yield relative, path


def _callable_name(path: Path, kind: str) -> str:
    if kind != "python_module":
        return "<batch>" if kind == "batch" else "<script>"
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        raise EntryNotDeclaredError(
            f"unable to read executable source: {path}"
        ) from error
    if re.search(r"if\s+__name__\s*==\s*['\"]__main__['\"]", text):
        return "main"
    return "<module-import>"


def _content_sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise EntryNotDeclaredError(
            f"unable to hash executable source: {path}"
        ) from error


def _exact_runtime_dependency_records(root: Path) -> tuple[EntryRecord, ...]:
    marker = root / _THS_BRIDGE_MARKER_PATH
    if not marker.exists():
        return ()
    if _path_is_reparse_point(marker) or not marker.is_file():
        raise EntryNotDeclaredError(
            f"runtime dependency marker is missing or unsafe: {_THS_BRIDGE_MARKER_PATH}"
        )
    records: list[EntryRecord] = []
    for relative in _THS_BRIDGE_RUNTIME_PATHS:
        path = root.joinpath(*relative.split("/"))
        if _path_is_reparse_point(path) or not path.is_file():
            raise EntryNotDeclaredError(
                f"required runtime dependency is missing or unsafe: {relative}"
            )
        if path.suffix.lower() in _SCRIPT_SUFFIXES:
            continue
        records.append(
            EntryRecord(
                entry_id=f"runtime:{relative}",
                path=relative,
                kind="runtime_dependency",
                callable_name="<runtime-dependency>",
                actor_type="scheduler",
                content_sha256=_content_sha256(path),
                disposition="PRODUCTION_DAILY",
                trust_state="production_daily",
                source="runtime_dependency_inventory",
            )
        )
    return tuple(records)


def _conservative_entry_classification(
    relative: str,
) -> tuple[str, str, str]:
    if relative in _PRODUCTION_DAILY_PATHS:
        return ("scheduler", "PRODUCTION_DAILY", "production_daily")
    if relative in _ADMIN_ONLY_PATHS:
        return ("human", "ADMIN_ONLY", "admin_only")
    if relative.startswith("tests/"):
        return ("automation", "TEST_ONLY", "test_only")
    if relative.startswith("apps/"):
        return ("automation", "DENIED_WEB", "denied_web")
    if relative.startswith(("tools/", "l2/")):
        return ("human", "ADMIN_ONLY", "admin_only")
    actor_type = (
        "legacy_runner"
        if relative.startswith(("research_automation/", "research/", "ag2_research/"))
        else "automation"
    )
    return (actor_type, "LEGACY_UNAUDITED", "legacy_unaudited")


class EntryInventory:
    """Scan only bounded source roots and return stable records."""

    @staticmethod
    def scan(
        root: str | Path,
        *,
        scheduler_records: Iterable[dict[str, str]] | None = None,
        include_required_import_seams: bool = True,
    ) -> tuple[EntryRecord, ...]:
        if not isinstance(include_required_import_seams, bool):
            raise ValueError("include_required_import_seams must be boolean")
        candidate_root = Path(root)
        if _path_is_reparse_point(candidate_root) or not candidate_root.is_dir():
            raise EntryNotDeclaredError(
                "inventory root must be an existing non-reparse directory"
            )
        root_path = candidate_root.resolve()
        records: list[EntryRecord] = []
        for relative, path in _iter_bounded_files(root_path):
            suffix = path.suffix.lower()
            kind = {
                ".py": "python_module",
                ".bat": "batch",
                ".ps1": "powershell",
                ".sh": "shell",
            }[suffix]
            actor_type, disposition, trust_state = (
                _conservative_entry_classification(relative)
            )
            records.append(
                EntryRecord(
                    entry_id=f"file:{relative}",
                    path=relative,
                    kind=kind,
                    callable_name=_callable_name(path, kind),
                    actor_type=actor_type,
                    content_sha256=_content_sha256(path),
                    disposition=disposition,
                    trust_state=trust_state,
                )
            )
        records.extend(_exact_runtime_dependency_records(root_path))
        if include_required_import_seams:
            for module_name, callable_name, relative, effects in _REQUIRED_IMPORT_SEAMS:
                seam_path = root_path / relative
                if (
                    _path_is_reparse_point(seam_path)
                    or not seam_path.is_file()
                ):
                    raise EntryNotDeclaredError(
                        f"required import seam is missing or unsafe: {relative}"
                    )
                records.append(
                    EntryRecord(
                        entry_id=f"callable:{module_name}:{callable_name}",
                        path=relative,
                        kind="python_callable",
                        callable_name=callable_name,
                        actor_type="legacy_runner",
                        content_sha256=_content_sha256(seam_path),
                        declared_side_effects=effects,
                        source="required_import_seam",
                    )
                )
        scheduler_items = tuple(scheduler_records or ())
        if not scheduler_items:
            scheduler_items = (
                {"task_path": _PRODUCTION_DAILY_SCHEDULER_PATH},
            )
        for item in scheduler_items:
            task_path = str(
                item.get("task_path")
                or item.get("path")
                or _PRODUCTION_DAILY_SCHEDULER_PATH
            ).strip().replace("\\", "/")
            scheduler_action = str(
                item.get("command")
                or item.get("action")
                or "<unknown>"
            ).strip()
            scheduler_hash = str(
                item.get("task_xml_sha256")
                or item.get("content_sha256")
                or ""
            ).strip().lower()
            if re.fullmatch(r"[0-9a-f]{64}", scheduler_hash) is None:
                scheduler_hash = None
            scheduler_metadata = tuple(
                sorted(
                    {
                        "action": scheduler_action,
                        "state": str(
                            item.get("state")
                            or item.get("status")
                            or "UNKNOWN"
                        ),
                        "principal": str(item.get("principal") or "<unknown>"),
                        "trigger": str(item.get("trigger") or "<unknown>"),
                        "acl_summary": str(
                            item.get("acl_summary") or "<unknown>"
                        ),
                    }.items()
                )
            )
            records.append(
                EntryRecord(
                    entry_id=f"external:scheduler:{task_path}",
                    path=task_path,
                    kind="external_scheduler",
                    callable_name=scheduler_action,
                    actor_type="scheduler",
                    content_sha256=scheduler_hash,
                    disposition="PRODUCTION_DAILY",
                    trust_state="production_daily",
                    external_metadata=scheduler_metadata,
                    source="external_scheduler_inventory",
                )
            )
        return tuple(sorted(records, key=lambda record: (record.kind, record.path, record.entry_id)))

    @staticmethod
    def assert_declared(
        records: Iterable[EntryRecord],
        reviewed_policy_path: str | Path,
        *,
        identity_binding: IdentityBinding | None = None,
    ) -> None:
        if not isinstance(reviewed_policy_path, (str, os.PathLike)):
            raise EntryNotDeclaredError(
                "a reviewed entry policy file is required for declaration checks"
            )
        if not isinstance(identity_binding, IdentityBinding):
            raise EntryNotDeclaredError(
                "an approved identity binding is required for declaration checks"
            )
        try:
            reviewed_policy = EntryInventory.load_policy(
                reviewed_policy_path,
                identity_binding=identity_binding,
            )
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise EntryNotDeclaredError(
                "the reviewed entry policy file is invalid"
            ) from error
        declared_records = reviewed_policy.records
        declared_by_id: dict[str, EntryRecord] = {}
        for record in declared_records:
            if record.entry_id in declared_by_id:
                raise EntryNotDeclaredError(f"duplicate declared entry id: {record.entry_id}")
            declared_by_id[record.entry_id] = record
        actual_by_id: dict[str, EntryRecord] = {}
        for record in records:
            if record.entry_id in actual_by_id:
                raise EntryNotDeclaredError(
                    f"duplicate actual entry id: {record.entry_id}"
                )
            actual_by_id[record.entry_id] = record
        missing = sorted(set(actual_by_id) - set(declared_by_id))
        if missing:
            raise EntryNotDeclaredError(
                "entry inventory contains undeclared entries: " + ", ".join(missing)
            )
        stale = sorted(set(declared_by_id) - set(actual_by_id))
        if stale:
            raise EntryNotDeclaredError(
                "entry policy contains entries absent from inventory: "
                + ", ".join(stale)
            )
        mismatched = []
        for entry_id, actual in actual_by_id.items():
            declared = declared_by_id[entry_id]
            actual_signature = (
                actual.path,
                actual.kind,
                actual.callable_name,
                actual.actor_type,
                actual.content_sha256,
                actual.disposition,
                actual.trust_state,
                actual.declared_side_effects,
                actual.declared_phase,
                actual.resource_roots,
                actual.external_metadata,
                actual.source,
            )
            declared_signature = (
                declared.path,
                declared.kind,
                declared.callable_name,
                declared.actor_type,
                declared.content_sha256,
                declared.disposition,
                declared.trust_state,
                declared.declared_side_effects,
                declared.declared_phase,
                declared.resource_roots,
                declared.external_metadata,
                declared.source,
            )
            if actual_signature != declared_signature:
                mismatched.append(entry_id)
        if mismatched:
            raise EntryNotDeclaredError(
                "entry inventory metadata differs from the declaration: "
                + ", ".join(sorted(mismatched))
            )

    @staticmethod
    def _manifest_payload(records: Iterable[EntryRecord]) -> dict[str, object]:
        entries = []
        for record in sorted(records, key=lambda item: (item.kind, item.path, item.entry_id)):
            entries.append(
                {
                    "entry_id": record.entry_id,
                    "path": record.path,
                    "kind": record.kind,
                    "callable_name": record.callable_name,
                    "actor_type": record.actor_type,
                    "content_sha256": record.content_sha256,
                    "disposition": record.disposition,
                    "trust_state": record.trust_state,
                    "declared_side_effects": [effect.value for effect in record.declared_side_effects],
                    "declared_phase": record.declared_phase.value if record.declared_phase else None,
                    "resource_roots": list(record.resource_roots),
                    "external_metadata": dict(record.external_metadata),
                    "source": record.source,
                }
            )
        return {"schema_version": 1, "entries": entries}

    @staticmethod
    def write_manifest(path: str | Path, records: Iterable[EntryRecord]) -> str:
        raise AuthorizationError("unticketed manifest writes are disabled")

    @staticmethod
    def render_manifest(
        records: Iterable[EntryRecord],
    ) -> tuple[str, str]:
        payload = EntryInventory._manifest_payload(records)
        text = canonical_json(payload) + "\n"
        return text, canonical_sha256(payload)

    @staticmethod
    def _records_from_entries(entries: object) -> tuple[EntryRecord, ...]:
        if not isinstance(entries, list):
            raise ValueError("entry records must be a list")
        records: list[EntryRecord] = []
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError("entry inventory record must be an object")
            if frozenset(entry) != _ENTRY_RECORD_FIELDS:
                raise ValueError(
                    "entry inventory record must contain exactly the required fields"
                )
            if not isinstance(entry["declared_side_effects"], list):
                raise ValueError("declared_side_effects must be a list")
            if not isinstance(entry["resource_roots"], list):
                raise ValueError("resource_roots must be a list")
            if not isinstance(entry["external_metadata"], dict):
                raise ValueError("external_metadata must be an object")
            effects = tuple(
                SideEffect(value)
                for value in entry["declared_side_effects"]
            )
            phase_value = entry["declared_phase"]
            records.append(
                EntryRecord(
                    entry_id=str(entry["entry_id"]),
                    path=str(entry["path"]),
                    kind=str(entry["kind"]),
                    callable_name=str(entry["callable_name"]),
                    actor_type=str(entry["actor_type"]),
                    content_sha256=(
                        str(entry["content_sha256"])
                        if entry.get("content_sha256") is not None
                        else None
                    ),
                    disposition=str(entry["disposition"]),
                    trust_state=str(entry["trust_state"]),
                    declared_side_effects=effects,
                    declared_phase=Phase(phase_value) if phase_value else None,
                    resource_roots=tuple(
                        str(root) for root in entry["resource_roots"]
                    ),
                    external_metadata=tuple(
                        sorted(
                            (
                                str(key),
                                str(value),
                            )
                            for key, value in entry["external_metadata"].items()
                        )
                    ),
                    source=str(entry["source"]),
                )
            )
        return tuple(sorted(records, key=lambda item: (item.kind, item.path, item.entry_id)))

    @staticmethod
    def load_manifest(path: str | Path) -> tuple[EntryRecord, ...]:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise ValueError("unsupported entry inventory manifest")
        return EntryInventory._records_from_entries(payload.get("entries"))

    @staticmethod
    def load_policy(
        path: str | Path,
        *,
        identity_binding: IdentityBinding,
    ) -> ReviewedEntryPolicy:
        if not isinstance(identity_binding, IdentityBinding):
            raise ValueError(
                "reviewed entry policy requires an approved identity binding"
            )
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version")
            != "control_plane.entry_policy.v1"
            or payload.get("review_state") != "APPROVED"
        ):
            raise ValueError("a reviewed entry policy is required")
        reviewed_policy = ReviewedEntryPolicy(
            plan_hash=str(payload.get("plan_hash") or ""),
            scope_hash=str(payload.get("scope_hash") or ""),
            policy_hash=str(payload.get("policy_hash") or ""),
            records=EntryInventory._records_from_entries(
                payload.get("entries")
            ),
        )
        if (
            reviewed_policy.plan_hash,
            reviewed_policy.scope_hash,
            reviewed_policy.policy_hash,
        ) != (
            identity_binding.plan_hash,
            identity_binding.scope_hash,
            identity_binding.policy_hash,
        ):
            raise ValueError("reviewed entry policy identity binding mismatch")
        return reviewed_policy


class PhaseAuthorizer:
    """Fail-closed compatibility shell for the retired phase-token API."""

    _DISABLED_MESSAGE = "legacy phase-token authorization is disabled"

    def __init__(
        self,
        db_path: str | Path,
        *,
        approved_plan_hash: str,
        approved_scope_hash: str,
        approved_policy_hash: str,
    ) -> None:
        self.db_path = Path(db_path)
        self.approved_plan_hash = _require_hash(
            approved_plan_hash,
            "approved_plan_hash",
        )
        self.approved_scope_hash = _require_hash(
            approved_scope_hash,
            "approved_scope_hash",
        )
        self.approved_policy_hash = _require_hash(
            approved_policy_hash,
            "approved_policy_hash",
        )

    def issue_phase_token(self, *args: object, **kwargs: object) -> str:
        raise AuthorizationError(self._DISABLED_MESSAGE)

    def consume_phase_token(
        self,
        *args: object,
        **kwargs: object,
    ) -> AuthorizationGrant:
        raise AuthorizationError(self._DISABLED_MESSAGE)

    def record_gate(self, *args: object, **kwargs: object) -> None:
        raise AuthorizationError(self._DISABLED_MESSAGE)

    def assert_side_effect(self, *args: object, **kwargs: object) -> None:
        raise AuthorizationError(self._DISABLED_MESSAGE)


@dataclass(frozen=True)
class EntryGuard:
    """Capability object passed to an entrypoint after token consumption."""

    authorizer: PhaseAuthorizer
    grant: AuthorizationGrant | None = None

    def assert_side_effect(self, effect: SideEffect) -> None:
        raise AuthorizationError("legacy phase-token authorization is disabled")


__all__ = [
    "AuthorizationError",
    "EntryInventory",
    "EntryNotDeclaredError",
    "EntryRecord",
    "begin_side_effect",
    "claim_phase",
    "finish_side_effect",
    "issue_task_ticket",
    "mark_side_effect_in_doubt",
]
