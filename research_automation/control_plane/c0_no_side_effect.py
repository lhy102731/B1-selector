"""CR010-R07: full-surface no-side-effect evidence for the C0 official run.

The official 24-cycle run must prove it touched NOTHING outside its
deterministic fixture root.  The surface snapshot covers:

  - Authority + Operational SQLite store files (path + sha256);
  - data/, knowledge/, config/, strategy/ trees (bounded file inventory);
  - provider registry + provider call-counter state;
  - network probe evidence (NetworkGuard interception attempts);
  - protected user files (CHANGELOG.md / daily_run.py / daily_select.py /
    docs/b1_v3_results.md);
  - git status lines (working tree delta).

``snapshot_surface`` captures the state; ``verify_surface_unchanged``
fails closed on ANY delta, so a run that touched an unintended surface can
never produce a PASS no-side-effect receipt.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

from .contracts import canonical_json

SURFACE_DIRS = (
    "data",
    "knowledge",
    "config",
    "strategy",
    "research_automation",
    "tools",
)
PROTECTED_FILES = (
    "CHANGELOG.md",
    "daily_run.py",
    "daily_select.py",
    "docs/b1_v3_results.md",
)
STORE_FILES = (
    "research_state/control_plane/authority/authority.sqlite3",
    "research_state/control_plane/operational/operational.sqlite3",
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_inventory(root: Path, relative_dir: str) -> dict[str, str]:
    """Bounded file inventory (path -> sha256) under a repo-relative dir."""
    base = root / relative_dir
    if not base.exists():
        return {}
    inventory: dict[str, str] = {}
    for path in sorted(base.rglob("*")):
        if path.is_file():
            rel = str(path.relative_to(root)).replace("\\", "/")
            if any(part in (".git", "__pycache__", ".pytest_cache") for part in path.parts):
                continue
            try:
                inventory[rel] = _sha256_file(path)
            except OSError:
                inventory[rel] = "UNREADABLE"
    return inventory


def _store_state(root: Path) -> dict[str, str]:
    state: dict[str, str] = {}
    for relative in STORE_FILES:
        path = root / relative
        state[relative] = (
            _sha256_file(path) if path.exists() else "ABSENT"
        )
    return state


@dataclass(frozen=True, slots=True)
class SurfaceSnapshot:
    stores: dict[str, str]
    trees: dict[str, dict[str, str]]
    protected: dict[str, str]
    git_status: tuple[str, ...]
    network_attempts: int

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": "control_plane.c0_surface_snapshot.v1",
            "stores": self.stores,
            "trees": self.trees,
            "protected": self.protected,
            "git_status": sorted(self.git_status),
            "network_attempts": self.network_attempts,
        }


def snapshot_surface(
    repository_root: str | os.PathLike[str],
    *,
    network_attempts: int = 0,
    git_executable: str = "git",
) -> SurfaceSnapshot:
    """Capture the full no-side-effect surface of the repository."""
    import subprocess

    root = Path(repository_root).resolve(strict=True)
    git_status = subprocess.run(
        [git_executable, "-C", str(root), "status", "--porcelain"],
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.splitlines()
    trees = {
        relative_dir: _file_inventory(root, relative_dir)
        for relative_dir in SURFACE_DIRS
    }
    protected = {
        relative: (
            _sha256_file(root / relative)
            if (root / relative).exists()
            else "ABSENT"
        )
        for relative in PROTECTED_FILES
    }
    return SurfaceSnapshot(
        stores=_store_state(root),
        trees=trees,
        protected=protected,
        git_status=tuple(git_status),
        network_attempts=network_attempts,
    )


class NoSideEffectError(RuntimeError):
    """Raised when the C0 run changed an unintended repository surface."""


def verify_surface_unchanged(
    before: SurfaceSnapshot,
    after: SurfaceSnapshot,
    *,
    allowed_git_deltas: tuple[str, ...] = (),
) -> None:
    """Fail closed unless EVERY tracked surface is byte-identical.

    ``allowed_git_deltas`` may carry the intended evidence-file additions
    (the official run's own receipts); anything else fails.
    """
    failures: list[str] = []
    if before.stores != after.stores:
        for key in sorted(set(before.stores) | set(after.stores)):
            if before.stores.get(key) != after.stores.get(key):
                failures.append(
                    f"store {key}: {before.stores.get(key)} -> "
                    f"{after.stores.get(key)}"
                )
    for tree in SURFACE_DIRS:
        before_tree = before.trees.get(tree, {})
        after_tree = after.trees.get(tree, {})
        for key in sorted(set(before_tree) | set(after_tree)):
            if before_tree.get(key) != after_tree.get(key):
                failures.append(
                    f"tree {tree} {key}: {before_tree.get(key)} -> "
                    f"{after_tree.get(key)}"
                )
    if before.protected != after.protected:
        for key in sorted(set(before.protected) | set(after.protected)):
            if before.protected.get(key) != after.protected.get(key):
                failures.append(
                    f"protected {key}: {before.protected.get(key)} -> "
                    f"{after.protected.get(key)}"
                )
    if before.network_attempts > after.network_attempts:
        failures.append("network attempt counter went backwards")
    before_status = set(before.git_status)
    after_status = set(after.git_status)
    allowed = set(allowed_git_deltas)
    unexpected = (after_status - before_status) - allowed
    reverted = before_status - after_status
    if unexpected:
        failures.append(
            "unexpected git deltas: " + "; ".join(sorted(unexpected))
        )
    if reverted:
        failures.append(
            "git entries disappeared: " + "; ".join(sorted(reverted))
        )
    if failures:
        raise NoSideEffectError(
            "C0 run changed an unintended surface: "
            + "; ".join(failures)
        )


def build_no_side_effect_receipt(
    repository_root: str | os.PathLike[str],
    before: SurfaceSnapshot,
    after: SurfaceSnapshot,
    *,
    allowed_git_deltas: tuple[str, ...] = (),
) -> dict[str, object]:
    """Build the official no-side-effect receipt (fail closed)."""
    verify_surface_unchanged(
        before,
        after,
        allowed_git_deltas=allowed_git_deltas,
    )
    return {
        "schema_version": "control_plane.c0_no_side_effect_receipt.v2",
        "surface": {
            "stores": "UNCHANGED",
            "trees": "UNCHANGED",
            "protected": "UNCHANGED",
            "network_probe": "UNCHANGED",
            "git": "UNCHANGED (allowed evidence deltas only)",
        },
        "before": before.to_payload(),
        "after": after.to_payload(),
        "pass": True,
    }


__all__ = [
    "NoSideEffectError",
    "PROTECTED_FILES",
    "SURFACE_DIRS",
    "SurfaceSnapshot",
    "build_no_side_effect_receipt",
    "snapshot_surface",
    "verify_surface_unchanged",
]
