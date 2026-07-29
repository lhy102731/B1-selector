"""Shared atomic publication for immutable directory releases."""

from __future__ import annotations

import os
import re
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Protocol


_RELEASE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class ReleaseAdapter(Protocol):
    """Validate one complete candidate and return its immutable identity."""

    def validate(self, release: Path) -> str: ...


class ReleaseBusyError(RuntimeError):
    """Raised when another publisher owns the release lock."""


class ReleaseConflictError(RuntimeError):
    """Raised when CURRENT changed after the caller prepared a candidate."""


@dataclass(frozen=True, slots=True)
class PromotionReceipt:
    release_id: str
    previous_release_id: str | None
    current_path: Path


class ImmutableReleaseStore:
    """Promote validated same-volume candidates without exposing half-state."""

    def __init__(self, root: Path, *, adapter: ReleaseAdapter) -> None:
        self._root = Path(root)
        self._adapter = adapter

    def stage(self, release_id: str) -> Path:
        canonical_id = self._canonical_release_id(release_id)
        candidate_root = self._root / "candidate"
        candidate_root.mkdir(parents=True, exist_ok=True)
        candidate = candidate_root / canonical_id
        if candidate.exists():
            raise FileExistsError(f"release candidate already exists: {canonical_id}")
        return candidate

    def promote(
        self,
        candidate: Path,
        *,
        expected_current_id: str | None,
    ) -> PromotionReceipt:
        candidate = self._candidate_path(candidate)
        release_id = self._validate_release(candidate)
        current = self._root / "current"
        previous = self._root / "previous"

        with self._exclusive_lock():
            release_id = self._validate_release(candidate)
            current_id = self._current_id(current)
            if current_id != expected_current_id:
                raise ReleaseConflictError(
                    "CURRENT identity changed before promotion"
                )

            transaction = self._root / f".promotion.{uuid.uuid4().hex}.tmp"
            parked_current = transaction / "current"
            parked_previous = transaction / "previous"
            transaction.mkdir(parents=False)
            try:
                if previous.exists():
                    os.replace(previous, parked_previous)
                if current.exists():
                    os.replace(current, parked_current)
                os.replace(candidate, current)
                if self._validate_release(current) != release_id:
                    raise ReleaseConflictError(
                        "promoted CURRENT identity does not match the candidate"
                    )

                if parked_previous.exists():
                    archive = self._root / "archive"
                    archive.mkdir(exist_ok=True)
                    archive_name = (
                        "previous-"
                        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
                        + "-"
                        + uuid.uuid4().hex[:8]
                    )
                    os.replace(parked_previous, archive / archive_name)
                if parked_current.exists():
                    os.replace(parked_current, previous)
                transaction.rmdir()
            except Exception:
                if current.exists() and not candidate.exists():
                    os.replace(current, candidate)
                if parked_current.exists():
                    os.replace(parked_current, current)
                if parked_previous.exists():
                    os.replace(parked_previous, previous)
                if transaction.exists():
                    transaction.rmdir()
                raise

        return PromotionReceipt(
            release_id=release_id,
            previous_release_id=current_id,
            current_path=current,
        )

    @staticmethod
    def _canonical_release_id(release_id: str) -> str:
        if not isinstance(release_id, str) or _RELEASE_ID.fullmatch(release_id) is None:
            raise ValueError("release_id must be a canonical path-safe identifier")
        return release_id

    def _candidate_path(self, candidate: Path) -> Path:
        candidate_root = (self._root / "candidate").resolve()
        resolved = Path(candidate).resolve()
        if resolved.parent != candidate_root:
            raise ValueError("candidate must be a direct child of the candidate root")
        self._canonical_release_id(resolved.name)
        return resolved

    def _validate_release(self, release: Path) -> str:
        release_id = self._adapter.validate(release)
        return self._canonical_release_id(release_id)

    def _current_id(self, current: Path) -> str | None:
        return self._validate_release(current) if current.exists() else None

    @contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        self._root.mkdir(parents=True, exist_ok=True)
        lock = self._root / ".publish.lock"
        try:
            descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as error:
            raise ReleaseBusyError("immutable release publication is busy") from error
        os.close(descriptor)
        try:
            yield
        finally:
            lock.unlink(missing_ok=True)


__all__ = [
    "ImmutableReleaseStore",
    "PromotionReceipt",
    "ReleaseAdapter",
    "ReleaseBusyError",
    "ReleaseConflictError",
]
