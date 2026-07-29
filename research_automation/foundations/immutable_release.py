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


class ReleaseLeaseExpiredError(RuntimeError):
    """Raised when a released or superseded read lease is reused."""


class ReleaseReadLease:
    """Process-scoped shared lock that keeps one CURRENT release stable."""

    __slots__ = (
        "release_id",
        "fencing_token",
        "_root",
        "_descriptor",
        "_released",
    )

    def __init__(
        self,
        *,
        release_id: str,
        fencing_token: int,
        root: Path,
        descriptor: int,
    ) -> None:
        self.release_id = release_id
        self.fencing_token = fencing_token
        self._root = root
        self._descriptor = descriptor
        self._released = False

    @property
    def active(self) -> bool:
        return not self._released

    def release(self) -> None:
        if self._released:
            return
        try:
            os.lseek(self._descriptor, 0, os.SEEK_SET)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._descriptor, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._descriptor, fcntl.LOCK_UN)
        finally:
            os.close(self._descriptor)
            self._released = True

    def __enter__(self) -> ReleaseReadLease:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


@dataclass(frozen=True, slots=True)
class PromotionReceipt:
    release_id: str
    previous_release_id: str | None
    current_path: Path
    fencing_token: int


@dataclass(frozen=True, slots=True)
class RollbackReceipt:
    release_id: str
    previous_release_id: str
    current_path: Path
    fencing_token: int


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

    def acquire_read_lease(
        self,
        *,
        expected_release_id: str,
    ) -> ReleaseReadLease:
        expected_id = self._canonical_release_id(expected_release_id)
        descriptor = self._acquire_lock_descriptor(shared=True)
        try:
            current_id = self._validate_release(self._root / "current")
            if current_id != expected_id:
                raise ReleaseConflictError(
                    "CURRENT identity changed before read lease acquisition"
                )
            return ReleaseReadLease(
                release_id=current_id,
                fencing_token=self._read_fencing_token_locked(descriptor),
                root=self._root.resolve(),
                descriptor=descriptor,
            )
        except Exception:
            self._unlock_and_close(descriptor)
            raise

    def validate_read_lease(self, lease: ReleaseReadLease) -> str:
        if not isinstance(lease, ReleaseReadLease) or not lease.active:
            raise ReleaseLeaseExpiredError("release read lease is no longer active")
        if lease._root != self._root.resolve():
            raise ReleaseLeaseExpiredError(
                "release read lease belongs to a different store"
            )
        if self._read_fencing_token_locked(lease._descriptor) != lease.fencing_token:
            raise ReleaseLeaseExpiredError("release read lease fencing token expired")
        current_id = self._validate_release(self._root / "current")
        if current_id != lease.release_id:
            raise ReleaseLeaseExpiredError("release read lease identity expired")
        return current_id

    def promote(
        self,
        candidate: Path,
        *,
        expected_current_id: str | None,
        expected_candidate_id: str | None = None,
    ) -> PromotionReceipt:
        candidate = self._candidate_path(candidate)
        release_id = self._validate_release(candidate)
        if expected_candidate_id is not None:
            expected_candidate_id = self._canonical_release_id(
                expected_candidate_id
            )
            if release_id != expected_candidate_id:
                raise ReleaseConflictError(
                    "candidate identity changed before promotion"
                )
        current = self._root / "current"
        previous = self._root / "previous"

        with self._exclusive_lock() as descriptor:
            self._require_no_pending_transactions()
            locked_release_id = self._validate_release(candidate)
            if locked_release_id != release_id or (
                expected_candidate_id is not None
                and locked_release_id != expected_candidate_id
            ):
                raise ReleaseConflictError(
                    "candidate identity changed while acquiring the lock"
                )
            current_id = self._current_id(current)
            if current_id != expected_current_id:
                raise ReleaseConflictError(
                    "CURRENT identity changed before promotion"
                )
            fencing_token = self._advance_fencing_token_locked(descriptor)

            transaction = self._root / f".promotion.{uuid.uuid4().hex}.tmp"
            parked_current = transaction / "current"
            parked_previous = transaction / "previous"
            transaction_record = self._start_transaction(
                transaction,
                {
                    "schema_version": "immutable_release.transaction.v1",
                    "operation": "PROMOTE",
                    "candidate_name": candidate.name,
                    "candidate_release_id": release_id,
                    "expected_current_id": expected_current_id,
                },
            )
            committed = False
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
                if current_id is None:
                    committed = True

                if parked_current.exists():
                    os.replace(parked_current, previous)
                    committed = True
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
                if committed:
                    raise
                if current.exists() and not candidate.exists():
                    os.replace(current, candidate)
                if parked_current.exists():
                    os.replace(parked_current, current)
                if parked_previous.exists():
                    os.replace(parked_previous, previous)
                transaction_record.unlink(missing_ok=True)
                if transaction.exists():
                    transaction.rmdir()
                raise

        return PromotionReceipt(
            release_id=release_id,
            previous_release_id=current_id,
            current_path=current,
            fencing_token=fencing_token,
        )

    def rollback(self, *, expected_current_id: str) -> RollbackReceipt:
        expected_id = self._canonical_release_id(expected_current_id)
        current = self._root / "current"
        previous = self._root / "previous"
        with self._exclusive_lock() as descriptor:
            self._require_no_pending_transactions()
            current_id = self._validate_release(current)
            previous_id = self._validate_release(previous)
            if current_id != expected_id:
                raise ReleaseConflictError(
                    "CURRENT identity changed before rollback"
                )
            fencing_token = self._advance_fencing_token_locked(descriptor)

            transaction = self._root / f".rollback.{uuid.uuid4().hex}.tmp"
            parked_current = transaction / "current"
            transaction_record = self._start_transaction(
                transaction,
                {
                    "schema_version": "immutable_release.transaction.v1",
                    "operation": "ROLLBACK",
                    "current_release_id": current_id,
                    "previous_release_id": previous_id,
                },
            )
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
                transaction_record.unlink()
                transaction.rmdir()
            except Exception:
                if completed_swap:
                    raise
                if moved_previous:
                    os.replace(current, previous)
                    if parked_current.exists():
                        os.replace(parked_current, current)
                elif parked_current.exists():
                    os.replace(parked_current, current)
                transaction_record.unlink(missing_ok=True)
                if transaction.exists():
                    transaction.rmdir()
                raise

        return RollbackReceipt(
            release_id=previous_id,
            previous_release_id=current_id,
            current_path=current,
            fencing_token=fencing_token,
        )

    def recover(self) -> RecoveryReceipt:
        self._root.mkdir(parents=True, exist_ok=True)
        with self._exclusive_lock():
            transactions = sorted(self._root.glob(".promotion.*.tmp"))
            transactions.extend(sorted(self._root.glob(".rollback.*.tmp")))
            if not transactions:
                return RecoveryReceipt(action="NO_ACTION", current_path=None)
            if len(transactions) != 1 or not transactions[0].is_dir():
                raise ReleaseConflictError(
                    "immutable release recovery found ambiguous transactions"
                )
            transaction = transactions[0]
            if not any(transaction.iterdir()):
                transaction.rmdir()
                current = self._root / "current"
                return RecoveryReceipt(
                    action="CLEANED_EMPTY_TRANSACTION",
                    current_path=current if current.exists() else None,
                )
            record_path = transaction / "transaction.json"
            try:
                record = json.loads(record_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise ReleaseConflictError(
                    "immutable release transaction record is invalid"
                ) from error
            if not isinstance(record, dict):
                raise ReleaseConflictError(
                    "immutable release transaction record is invalid"
                )
            operation = record.get("operation")
            expected_operation = (
                "ROLLBACK"
                if transaction.name.startswith(".rollback.")
                else "PROMOTE"
            )
            if operation != expected_operation:
                raise ReleaseConflictError(
                    "immutable release transaction record is invalid"
                )
            allowed_content = {"transaction.json", "current"}
            if operation == "PROMOTE":
                allowed_content.add("previous")
            if any(
                path.name not in allowed_content
                for path in transaction.iterdir()
            ):
                raise ReleaseConflictError(
                    "immutable release transaction content is invalid"
                )
            if operation == "ROLLBACK":
                return self._recover_rollback_transaction(transaction, record)
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
                expected_current_id = self._canonical_release_id(
                    expected_current_id
                )
            candidate = self._root / "candidate" / candidate_name
            current = self._root / "current"
            previous = self._root / "previous"
            parked_current = transaction / "current"
            parked_previous = transaction / "previous"

            if (
                not parked_current.exists()
                and candidate.exists()
                and self._current_id(current) == expected_current_id
            ):
                if self._validate_release(candidate) != candidate_release_id:
                    raise ReleaseConflictError(
                        "interrupted candidate does not match the transaction"
                    )
                if parked_previous.exists():
                    if previous.exists():
                        raise ReleaseConflictError(
                            "interrupted previous slot has two occupants"
                        )
                    self._validate_release(parked_previous)
                    os.replace(parked_previous, previous)
                action = "ROLLED_BACK_INTERRUPTED_PROMOTION"
            elif parked_current.exists():
                if (
                    expected_current_id is None
                    or self._validate_release(parked_current)
                    != expected_current_id
                ):
                    raise ReleaseConflictError(
                        "parked CURRENT does not match the transaction"
                    )
                if parked_previous.exists() and previous.exists():
                    raise ReleaseConflictError(
                        "interrupted previous slot has two occupants"
                    )
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
                raise ReleaseConflictError(
                    "interrupted promotion lost CURRENT"
                )
            else:
                if self._validate_release(current) != candidate_release_id:
                    raise ReleaseConflictError(
                        "completed CURRENT does not match the transaction"
                    )
                if expected_current_id is None:
                    previous_matches = not previous.exists()
                else:
                    previous_matches = (
                        previous.exists()
                        and self._validate_release(previous)
                        == expected_current_id
                    )
                if not previous_matches:
                    raise ReleaseConflictError(
                        "completed PREVIOUS does not match the transaction"
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

    def _recover_rollback_transaction(
        self,
        transaction: Path,
        record: dict[str, object],
    ) -> RecoveryReceipt:
        required = {
            "schema_version",
            "operation",
            "current_release_id",
            "previous_release_id",
        }
        if (
            set(record) != required
            or record["schema_version"] != "immutable_release.transaction.v1"
            or record["operation"] != "ROLLBACK"
        ):
            raise ReleaseConflictError(
                "immutable release transaction record is invalid"
            )
        current_release_id = self._canonical_release_id(
            record["current_release_id"]
        )
        previous_release_id = self._canonical_release_id(
            record["previous_release_id"]
        )
        current = self._root / "current"
        previous = self._root / "previous"
        parked_current = transaction / "current"
        if (
            parked_current.exists()
            and current.exists()
            and not previous.exists()
            and self._validate_release(parked_current) == current_release_id
            and self._validate_release(current) == previous_release_id
        ):
            os.replace(current, previous)
            os.replace(parked_current, current)
            action = "ROLLED_BACK_INTERRUPTED_ROLLBACK"
        elif (
            parked_current.exists()
            and not current.exists()
            and previous.exists()
            and self._validate_release(parked_current) == current_release_id
            and self._validate_release(previous) == previous_release_id
        ):
            os.replace(parked_current, current)
            action = "ROLLED_BACK_INTERRUPTED_ROLLBACK"
        elif (
            not parked_current.exists()
            and current.exists()
            and previous.exists()
            and self._validate_release(current) == previous_release_id
            and self._validate_release(previous) == current_release_id
        ):
            action = "COMPLETED_INTERRUPTED_ROLLBACK"
        elif (
            not parked_current.exists()
            and current.exists()
            and previous.exists()
            and self._validate_release(current) == current_release_id
            and self._validate_release(previous) == previous_release_id
        ):
            action = "ROLLED_BACK_INTERRUPTED_ROLLBACK"
        else:
            raise ReleaseConflictError(
                "interrupted rollback state does not match the transaction"
            )
        (transaction / "transaction.json").unlink()
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

    def _require_no_pending_transactions(self) -> None:
        pending = sorted(self._root.glob(".promotion.*.tmp"))
        pending.extend(sorted(self._root.glob(".rollback.*.tmp")))
        if pending:
            raise ReleaseConflictError(
                "immutable release recovery required before another write"
            )

    def _start_transaction(
        self,
        transaction: Path,
        payload: dict[str, object],
    ) -> Path:
        transaction.mkdir(parents=False)
        record = transaction / "transaction.json"
        try:
            self._write_transaction(record, payload)
        except Exception:
            record.unlink(missing_ok=True)
            transaction.rmdir()
            raise
        return record

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
    def _exclusive_lock(self) -> Iterator[int]:
        descriptor = self._acquire_lock_descriptor(shared=False)
        try:
            yield descriptor
        finally:
            self._unlock_and_close(descriptor)

    @staticmethod
    def _read_fencing_token_locked(descriptor: int) -> int:
        os.lseek(descriptor, 0, os.SEEK_SET)
        raw = os.read(descriptor, 64)
        if re.fullmatch(rb"0|[1-9][0-9]{0,18}", raw) is None:
            raise ReleaseConflictError("publication fencing token is invalid")
        return int(raw)

    def _advance_fencing_token_locked(self, descriptor: int) -> int:
        next_token = self._read_fencing_token_locked(descriptor) + 1
        if next_token >= 10**19:
            raise ReleaseConflictError("publication fencing token is exhausted")
        encoded = str(next_token).encode("ascii")
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.write(descriptor, encoded)
        os.ftruncate(descriptor, len(encoded))
        os.fsync(descriptor)
        return next_token

    def _acquire_lock_descriptor(self, *, shared: bool) -> int:
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

                    mode = msvcrt.LK_NBRLCK if shared else msvcrt.LK_NBLCK
                    msvcrt.locking(descriptor, mode, 1)
                else:
                    import fcntl

                    mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
                    fcntl.flock(descriptor, mode | fcntl.LOCK_NB)
                acquired = True
            except OSError as error:
                if time.monotonic() >= deadline:
                    os.close(descriptor)
                    raise ReleaseBusyError(
                        "immutable release publication is busy"
                    ) from error
                time.sleep(0.05)
        return descriptor

    @staticmethod
    def _unlock_and_close(descriptor: int) -> None:
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


__all__ = [
    "ImmutableReleaseStore",
    "PromotionReceipt",
    "RollbackReceipt",
    "ReleaseAdapter",
    "ReleaseBusyError",
    "ReleaseConflictError",
    "ReleaseLeaseExpiredError",
    "ReleaseReadLease",
    "RecoveryReceipt",
]
