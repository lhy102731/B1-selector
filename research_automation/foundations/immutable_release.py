"""Shared atomic publication for immutable directory releases."""

from __future__ import annotations

import json
import os
import re
import time
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


@dataclass(frozen=True, slots=True)
class RollbackReceipt:
    release_id: str
    previous_release_id: str
    current_path: Path


@dataclass(frozen=True, slots=True)
class RecoveryReceipt:
    action: str
    current_path: Path | None


class ImmutableReleaseStore:
    """Promote validated same-volume candidates without exposing half-state."""

    def __init__(
        self,
        root: Path,
        *,
        adapter: ReleaseAdapter,
        lock_timeout_seconds: float = 30.0,
    ) -> None:
        self._root = Path(root)
        self._adapter = adapter
        if (
            isinstance(lock_timeout_seconds, bool)
            or not isinstance(lock_timeout_seconds, (int, float))
            or lock_timeout_seconds <= 0
        ):
            raise ValueError("lock_timeout_seconds must be positive")
        self._lock_timeout_seconds = float(lock_timeout_seconds)

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
            transaction_record = transaction / "transaction.json"
            self._write_transaction(
                transaction_record,
                {
                    "schema_version": "immutable_release.transaction.v1",
                    "operation": "PROMOTE",
                    "candidate_name": candidate.name,
                    "candidate_release_id": release_id,
                    "expected_current_id": expected_current_id,
                },
            )
            moved_current_to_previous = False
            archived_previous: Path | None = None
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

                if parked_current.exists():
                    os.replace(parked_current, previous)
                    moved_current_to_previous = True
                if parked_previous.exists():
                    archive = self._root / "archive"
                    archive.mkdir(exist_ok=True)
                    archive_name = (
                        "previous-"
                        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
                        + "-"
                        + uuid.uuid4().hex[:8]
                    )
                    archived_previous = archive / archive_name
                    os.replace(parked_previous, archived_previous)
                transaction_record.unlink()
                transaction.rmdir()
            except Exception:
                if current.exists() and not candidate.exists():
                    os.replace(current, candidate)
                if parked_current.exists():
                    os.replace(parked_current, current)
                elif moved_current_to_previous and previous.exists():
                    os.replace(previous, current)
                if parked_previous.exists():
                    os.replace(parked_previous, previous)
                elif archived_previous is not None and archived_previous.exists():
                    os.replace(archived_previous, previous)
                    archive = archived_previous.parent
                    if not any(archive.iterdir()):
                        archive.rmdir()
                transaction_record.unlink(missing_ok=True)
                if transaction.exists():
                    transaction.rmdir()
                raise

        return PromotionReceipt(
            release_id=release_id,
            previous_release_id=current_id,
            current_path=current,
        )

    def rollback(self, *, expected_current_id: str) -> RollbackReceipt:
        expected_id = self._canonical_release_id(expected_current_id)
        current = self._root / "current"
        previous = self._root / "previous"
        with self._exclusive_lock():
            current_id = self._validate_release(current)
            previous_id = self._validate_release(previous)
            if current_id != expected_id:
                raise ReleaseConflictError(
                    "CURRENT identity changed before rollback"
                )

            transaction = self._root / f".rollback.{uuid.uuid4().hex}.tmp"
            parked_current = transaction / "current"
            transaction.mkdir(parents=False)
            moved_previous = False
            completed_swap = False
            try:
                os.replace(current, parked_current)
                os.replace(previous, current)
                moved_previous = True
                os.replace(parked_current, previous)
                completed_swap = True
                if (
                    self._validate_release(current) != previous_id
                    or self._validate_release(previous) != current_id
                ):
                    raise ReleaseConflictError(
                        "rollback release identities do not match the prior slots"
                    )
                transaction.rmdir()
            except Exception:
                if completed_swap:
                    parked_rollback = transaction / "rolled-back-current"
                    os.replace(current, parked_rollback)
                    os.replace(previous, current)
                    os.replace(parked_rollback, previous)
                elif moved_previous:
                    os.replace(current, previous)
                    if parked_current.exists():
                        os.replace(parked_current, current)
                elif parked_current.exists():
                    os.replace(parked_current, current)
                if transaction.exists():
                    transaction.rmdir()
                raise

        return RollbackReceipt(
            release_id=previous_id,
            previous_release_id=current_id,
            current_path=current,
        )

    def recover(self) -> RecoveryReceipt:
        self._root.mkdir(parents=True, exist_ok=True)
        with self._exclusive_lock():
            transactions = sorted(self._root.glob(".promotion.*.tmp"))
            if not transactions:
                return RecoveryReceipt(action="NO_ACTION", current_path=None)
            if len(transactions) != 1 or not transactions[0].is_dir():
                raise ReleaseConflictError(
                    "immutable release recovery found ambiguous transactions"
                )
            transaction = transactions[0]
            record_path = transaction / "transaction.json"
            try:
                record = json.loads(record_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise ReleaseConflictError(
                    "immutable release transaction record is invalid"
                ) from error
            required = {
                "schema_version",
                "operation",
                "candidate_name",
                "candidate_release_id",
                "expected_current_id",
            }
            if (
                set(record) != required
                or record["schema_version"] != "immutable_release.transaction.v1"
                or record["operation"] != "PROMOTE"
            ):
                raise ReleaseConflictError(
                    "immutable release transaction record is invalid"
                )
            candidate_name = self._canonical_release_id(record["candidate_name"])
            candidate_release_id = self._canonical_release_id(
                record["candidate_release_id"]
            )
            expected_current_id = record["expected_current_id"]
            if expected_current_id is not None:
                self._canonical_release_id(expected_current_id)
            candidate = self._root / "candidate" / candidate_name
            current = self._root / "current"
            previous = self._root / "previous"
            parked_current = transaction / "current"
            parked_previous = transaction / "previous"

            if parked_current.exists():
                if current.exists():
                    if self._validate_release(current) != candidate_release_id:
                        raise ReleaseConflictError(
                            "interrupted CURRENT does not match the candidate"
                        )
                    candidate.parent.mkdir(parents=True, exist_ok=True)
                    if candidate.exists():
                        raise ReleaseConflictError(
                            "interrupted candidate destination already exists"
                        )
                    os.replace(current, candidate)
                elif not candidate.exists():
                    raise ReleaseConflictError(
                        "interrupted promotion lost both CURRENT and candidate"
                    )
                os.replace(parked_current, current)
                if parked_previous.exists():
                    os.replace(parked_previous, previous)
                action = "ROLLED_BACK_INTERRUPTED_PROMOTION"
            elif not current.exists():
                if parked_previous.exists():
                    os.replace(parked_previous, previous)
                action = "ROLLED_BACK_INTERRUPTED_PROMOTION"
            else:
                if self._validate_release(current) != candidate_release_id:
                    raise ReleaseConflictError(
                        "completed CURRENT does not match the transaction"
                    )
                if parked_previous.exists():
                    archive = self._root / "archive"
                    archive.mkdir(exist_ok=True)
                    os.replace(
                        parked_previous,
                        archive / f"previous-recovered-{uuid.uuid4().hex[:8]}",
                    )
                action = "COMPLETED_INTERRUPTED_PROMOTION"

            record_path.unlink()
            transaction.rmdir()
            return RecoveryReceipt(action=action, current_path=current)

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

    @staticmethod
    def _write_transaction(path: Path, payload: dict[str, object]) -> None:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        with path.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())

    @contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        self._root.mkdir(parents=True, exist_ok=True)
        lock = self._root / ".publish.lock"
        descriptor = os.open(lock, os.O_CREAT | os.O_RDWR)
        if os.fstat(descriptor).st_size == 0:
            os.write(descriptor, b"0")
        deadline = time.monotonic() + self._lock_timeout_seconds
        acquired = False
        while not acquired:
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError as error:
                if time.monotonic() >= deadline:
                    os.close(descriptor)
                    raise ReleaseBusyError(
                        "immutable release publication is busy"
                    ) from error
                time.sleep(0.05)
        try:
            yield
        finally:
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
                lock.unlink(missing_ok=True)


__all__ = [
    "ImmutableReleaseStore",
    "PromotionReceipt",
    "RollbackReceipt",
    "ReleaseAdapter",
    "ReleaseBusyError",
    "ReleaseConflictError",
    "RecoveryReceipt",
]
