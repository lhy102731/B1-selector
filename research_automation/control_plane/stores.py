"""Physically isolated stores owned by the research control plane."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
import os
from pathlib import Path


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


def _path_identity(path: str | Path) -> str:
    return os.path.normcase(str(Path(path).resolve(strict=False)))


def _provision_store(path: Path, *, owner: str, metadata_table: str) -> None:
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
            (("store_owner", owner), ("schema_version", "1")),
        )
        connection.execute("PRAGMA user_version = 1")
        connection.commit()
    finally:
        connection.close()


def trusted_bootstrap(
    *,
    authority_path: str | Path,
    operational_path: str | Path,
) -> StoreBootstrapReceipt:
    """Provision the trusted stores after validating their isolation."""

    resolved_authority = Path(authority_path).resolve(strict=False)
    resolved_operational = Path(operational_path).resolve(strict=False)
    if _path_identity(resolved_authority) == _path_identity(resolved_operational):
        raise StoreConfigurationError(
            "authority and operational stores must use different SQLite files"
        )
    if resolved_authority.exists() or resolved_operational.exists():
        raise StoreBootstrapError("control-plane stores are already provisioned")

    created: list[Path] = []
    try:
        _provision_store(
            resolved_authority,
            owner="AUTHORITY_STORE",
            metadata_table="authority_meta",
        )
        created.append(resolved_authority)
        _provision_store(
            resolved_operational,
            owner="OPERATIONAL_JOURNAL",
            metadata_table="operational_meta",
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
    )


__all__ = [
    "StoreBootstrapError",
    "StoreBootstrapReceipt",
    "StoreConfigurationError",
    "StoreError",
    "trusted_bootstrap",
]
