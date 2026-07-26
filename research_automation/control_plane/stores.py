"""Physically isolated stores owned by the research control plane."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
import os
from pathlib import Path
import secrets


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


class StoreError(RuntimeError):
    """Base error for trusted control-plane storage."""


class StoreConfigurationError(StoreError):
    """Raised when fixed store locations violate isolation rules."""


class StoreBootstrapError(StoreError):
    """Raised when trusted first-time provisioning cannot complete."""


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


def _path_identity(path: str | Path) -> str:
    return os.path.normcase(str(Path(path).resolve(strict=False)))


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
        connection.execute(
            f"""
            CREATE TABLE {metadata_table} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID
            """
        )
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
    if resolved_authority.exists() or resolved_operational.exists():
        raise StoreBootstrapError("control-plane stores are already provisioned")

    installation_id = secrets.token_hex(32)
    created: list[Path] = []
    try:
        _provision_store(
            resolved_authority,
            store_kind="AUTHORITY_STORE",
            metadata_table="authority_meta",
            installation_id=installation_id,
        )
        created.append(resolved_authority)
        _provision_store(
            resolved_operational,
            store_kind="OPERATIONAL_JOURNAL",
            metadata_table="operational_meta",
            installation_id=installation_id,
        )
        created.append(resolved_operational)
    except (OSError, sqlite3.DatabaseError) as error:
        for created_path in created:
            created_path.unlink(missing_ok=True)
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


def trusted_bootstrap() -> StoreBootstrapReceipt:
    """Provision the two fixed-path stores from a trusted entrypoint."""

    return _trusted_bootstrap_at_paths(
        authority_path=_AUTHORITY_STORE_PATH,
        operational_path=_OPERATIONAL_STORE_PATH,
    )


def _read_store_metadata(path: Path, metadata_table: str) -> dict[str, str]:
    database_uri = path.resolve(strict=False).as_uri()
    try:
        connection = sqlite3.connect(
            f"{database_uri}?mode=ro",
            uri=True,
            isolation_level=None,
        )
        try:
            return {
                str(key): str(value)
                for key, value in connection.execute(
                    f"SELECT key, value FROM {metadata_table}"
                )
            }
        finally:
            connection.close()
    except (OSError, ValueError, sqlite3.DatabaseError) as error:
        raise StoreBootstrapError("control-plane store pair is unavailable") from error


def read_store_pair_descriptor() -> StorePairDescriptor:
    """Read only the fixed pair identity; never return a SQL connection."""

    authority = _read_store_metadata(_AUTHORITY_STORE_PATH, "authority_meta")
    operational = _read_store_metadata(
        _OPERATIONAL_STORE_PATH,
        "operational_meta",
    )
    installation_id = authority.get("installation_id")
    if (
        not installation_id
        or operational.get("installation_id") != installation_id
        or authority.get("store_kind") != "AUTHORITY_STORE"
        or operational.get("store_kind") != "OPERATIONAL_JOURNAL"
        or authority.get("schema_version") != "1"
        or operational.get("schema_version") != "1"
    ):
        raise StoreBootstrapError("control-plane store pair identity mismatch")
    return StorePairDescriptor(
        installation_id=installation_id,
        authority_kind=authority["store_kind"],
        operational_kind=operational["store_kind"],
    )


__all__ = [
    "StoreBootstrapError",
    "StoreBootstrapReceipt",
    "StoreConfigurationError",
    "StoreError",
    "StorePairDescriptor",
    "read_store_pair_descriptor",
    "trusted_bootstrap",
]
