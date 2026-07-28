"""Create-only publication for independently reviewed entry policies."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .artifact_semantics import validate_reviewed_entry_policy


MAX_REVIEWED_POLICY_BYTES = 4 * 1024 * 1024
_POLICY_NAMESPACE = ("research_state", "control_plane", "policies")
_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400


class PolicyPublicationError(RuntimeError):
    """Raised when a reviewed policy cannot be published without replacement."""


@dataclass(frozen=True, slots=True)
class PublishedReviewedEntryPolicy:
    reference: str
    file_sha256: str
    policy_payload_sha256: str
    inventory_payload_sha256: str
    review_receipt_sha256: str
    reviewer_id: str


def _is_reparse_point(path: Path) -> bool:
    try:
        if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except OSError as error:
        raise PolicyPublicationError(
            "unable to inspect reviewed-policy namespace"
        ) from error
    return bool(
        attributes
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", _FILE_ATTRIBUTE_REPARSE_POINT)
    )


def _policy_directory(repository_root: str | Path) -> tuple[Path, Path]:
    try:
        root = Path(repository_root).resolve(strict=True)
    except (OSError, TypeError, ValueError) as error:
        raise PolicyPublicationError("repository root is unavailable") from error
    if not root.is_dir():
        raise PolicyPublicationError("repository root is not a directory")

    current = root
    for component in _POLICY_NAMESPACE[:-1]:
        current = current / component
        if not current.is_dir() or _is_reparse_point(current):
            raise PolicyPublicationError(
                "reviewed-policy namespace parent is unavailable or unsafe"
            )
    policy_directory = current / _POLICY_NAMESPACE[-1]
    try:
        policy_directory.mkdir(exist_ok=True)
    except OSError as error:
        raise PolicyPublicationError(
            "unable to create reviewed-policy namespace"
        ) from error
    if not policy_directory.is_dir() or _is_reparse_point(policy_directory):
        raise PolicyPublicationError(
            "reviewed-policy namespace is unavailable or unsafe"
        )
    try:
        policy_directory.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as error:
        raise PolicyPublicationError(
            "reviewed-policy namespace escapes the repository"
        ) from error
    return root, policy_directory


def _require_exact_existing_policy(path: Path, expected: bytes) -> None:
    if _is_reparse_point(path) or not path.is_file():
        raise PolicyPublicationError(
            "content-addressed reviewed-policy path is not a regular file"
        )
    try:
        with path.open("rb") as stream:
            observed = stream.read(len(expected) + 1)
    except OSError as error:
        raise PolicyPublicationError(
            "content-addressed reviewed-policy file is unreadable"
        ) from error
    if observed != expected:
        raise PolicyPublicationError(
            "content-addressed reviewed-policy file has conflicting bytes"
        )


def publish_reviewed_entry_policy(
    raw: bytes,
    *,
    repository_root: str | Path,
    expected_plan_version: str,
    expected_phase: str,
    expected_attempt_id: str,
    expected_identity: Mapping[str, str],
    final_inventory: Mapping[str, object],
) -> PublishedReviewedEntryPolicy:
    """Validate and atomically publish one immutable content-addressed policy."""

    if not isinstance(raw, bytes):
        raise PolicyPublicationError("reviewed policy input must be bytes")
    if not raw or len(raw) > MAX_REVIEWED_POLICY_BYTES:
        raise PolicyPublicationError("reviewed policy input has an invalid size")
    policy = validate_reviewed_entry_policy(
        raw,
        expected_plan_version=expected_plan_version,
        expected_phase=expected_phase,
        expected_attempt_id=expected_attempt_id,
        expected_identity=expected_identity,
        final_inventory=final_inventory,
    )
    root, policy_directory = _policy_directory(repository_root)
    file_sha256 = hashlib.sha256(raw).hexdigest()
    destination = policy_directory / f"{file_sha256}.json"
    reference = destination.relative_to(root).as_posix()

    if destination.exists() or os.path.lexists(destination):
        _require_exact_existing_policy(destination, raw)
    else:
        descriptor: int | None = None
        temporary_path: Path | None = None
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{file_sha256}.",
                suffix=".tmp",
                dir=policy_directory,
            )
            temporary_path = Path(temporary_name)
            with os.fdopen(descriptor, "wb") as stream:
                descriptor = None
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary_path, destination)
            except FileExistsError:
                _require_exact_existing_policy(destination, raw)
            _require_exact_existing_policy(destination, raw)
        except PolicyPublicationError:
            raise
        except OSError as error:
            raise PolicyPublicationError(
                "unable to publish reviewed policy create-only"
            ) from error
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    return PublishedReviewedEntryPolicy(
        reference=reference,
        file_sha256=file_sha256,
        policy_payload_sha256=str(policy["policy_payload_sha256"]),
        inventory_payload_sha256=str(policy["inventory_payload_sha256"]),
        review_receipt_sha256=str(policy["review_receipt_sha256"]),
        reviewer_id=str(policy["reviewer_id"]),
    )


__all__ = [
    "MAX_REVIEWED_POLICY_BYTES",
    "PolicyPublicationError",
    "PublishedReviewedEntryPolicy",
    "publish_reviewed_entry_policy",
]
