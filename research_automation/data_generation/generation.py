"""Candidate-first publication of immutable market-data generations."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path

from research_automation.control_plane.contracts import canonical_json
from research_automation.foundations.immutable_release import (
    ImmutableReleaseStore,
    ReleaseBusyError,
    ReleaseReadLease,
)

from .contracts import (
    GENERATION_MANIFEST_V1,
    GENERATION_PUBLISH_PENDING_V1,
    GenerationManifest,
    GenerationPublishPending,
    generation_contract_registry,
)


@dataclass(frozen=True, slots=True)
class StagedGeneration:
    path: Path
    generation_id: str
    expected_current_id: str | None


class GenerationPublicationPendingError(RuntimeError):
    """Raised when a publication intent must wait for existing readers."""


class GenerationPublicationConflictError(RuntimeError):
    """Raised when a different publication intent already exists."""


class GenerationReadLease:
    """Generation-specific view over one shared immutable-release lease."""

    __slots__ = ("_release_lease",)

    def __init__(self, release_lease: ReleaseReadLease) -> None:
        self._release_lease = release_lease

    @property
    def generation_id(self) -> str:
        return self._release_lease.release_id

    @property
    def fencing_token(self) -> int:
        return self._release_lease.fencing_token

    @property
    def active(self) -> bool:
        return self._release_lease.active

    def release(self) -> None:
        self._release_lease.release()

    def __enter__(self) -> GenerationReadLease:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


class _GenerationReleaseAdapter:
    def validate(self, release: Path) -> str:
        raw = (release / "manifest.json").read_bytes()
        parsed = generation_contract_registry().parse_json(
            GENERATION_MANIFEST_V1,
            raw,
        )
        if not isinstance(parsed, GenerationManifest):
            raise TypeError("generation registry returned the wrong contract")
        return parsed.generation_id


class GenerationPublisher:
    """Stage strict manifests and publish them through the shared store."""

    def __init__(
        self,
        root: Path,
        *,
        lock_timeout_seconds: float = 30.0,
    ) -> None:
        self._root = Path(root)
        self._adapter = _GenerationReleaseAdapter()
        self._store = ImmutableReleaseStore(
            self._root,
            adapter=self._adapter,
            lock_timeout_seconds=lock_timeout_seconds,
        )

    def stage(self, manifest: GenerationManifest) -> StagedGeneration:
        if not isinstance(manifest, GenerationManifest):
            raise TypeError("manifest must be a GenerationManifest")
        current = self._root / "current"
        expected_current_id = (
            self._adapter.validate(current) if current.is_dir() else None
        )
        candidate = self._store.stage(manifest.generation_id)
        candidate.mkdir(parents=False)
        manifest_path = candidate / "manifest.json"
        temporary = candidate / f".manifest.{uuid.uuid4().hex}.tmp"
        payload = canonical_json(manifest.model_dump(mode="json")).encode("utf-8")
        try:
            with temporary.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, manifest_path)
            if self._adapter.validate(candidate) != manifest.generation_id:
                raise ValueError("staged generation identity mismatch")
        except Exception:
            temporary.unlink(missing_ok=True)
            if manifest_path.exists():
                manifest_path.unlink()
            candidate.rmdir()
            raise
        return StagedGeneration(
            path=candidate,
            generation_id=manifest.generation_id,
            expected_current_id=expected_current_id,
        )

    def publish(self, staged: StagedGeneration) -> GenerationManifest:
        if not isinstance(staged, StagedGeneration):
            raise TypeError("staged must be a StagedGeneration")
        manifest = self._read_manifest(staged.path)
        if manifest.generation_id != staged.generation_id:
            raise ValueError("staged generation identity changed before publish")
        pending = GenerationPublishPending(
            schema_version=GENERATION_PUBLISH_PENDING_V1,
            status="PUBLISH_PENDING",
            candidate_generation_id=staged.generation_id,
            expected_current_generation_id=staged.expected_current_id,
        )
        self._record_publish_pending(pending)
        try:
            self._store.promote(
                staged.path,
                expected_current_id=staged.expected_current_id,
                expected_candidate_id=staged.generation_id,
            )
        except ReleaseBusyError as error:
            raise GenerationPublicationPendingError(
                "generation publication is pending active read leases"
            ) from error
        self._clear_publish_pending(pending)
        return manifest

    def acquire_read_lease(
        self,
        *,
        expected_generation_id: str,
    ) -> GenerationReadLease:
        if self.pending_publication() is not None:
            raise GenerationPublicationPendingError(
                "a pending generation publication blocks a new read lease"
            )
        release_lease = self._store.acquire_read_lease(
            expected_release_id=expected_generation_id,
        )
        if self.pending_publication() is not None:
            release_lease.release()
            raise GenerationPublicationPendingError(
                "a pending generation publication blocks a new read lease"
            )
        return GenerationReadLease(release_lease)

    def read(self, lease: GenerationReadLease) -> GenerationManifest:
        if not isinstance(lease, GenerationReadLease):
            raise TypeError("lease must be a GenerationReadLease")
        generation_id = self._store.validate_read_lease(lease._release_lease)
        manifest = self._read_manifest(self._root / "current")
        if manifest.generation_id != generation_id:
            raise RuntimeError("leased generation identity changed")
        return manifest

    def pending_publication(self) -> GenerationPublishPending | None:
        pending_path = self._root / ".publish_pending.json"
        if not pending_path.exists():
            return None
        parsed = generation_contract_registry().parse_json(
            GENERATION_PUBLISH_PENDING_V1,
            pending_path.read_bytes(),
        )
        if not isinstance(parsed, GenerationPublishPending):
            raise TypeError("generation registry returned the wrong contract")
        return parsed

    def recover_pending_publication(self) -> GenerationManifest | None:
        pending = self.pending_publication()
        if pending is None:
            return None
        self._store.recover()
        current = self.read_current()
        if current.generation_id != pending.candidate_generation_id:
            raise GenerationPublicationConflictError(
                "pending publication does not match CURRENT"
            )
        self._clear_publish_pending(pending)
        return current

    def read_current(self) -> GenerationManifest:
        return self._read_manifest(self._root / "current")

    @staticmethod
    def _pending_payload(pending: GenerationPublishPending) -> bytes:
        return canonical_json(pending.model_dump(mode="json")).encode("utf-8")

    def _record_publish_pending(
        self,
        pending: GenerationPublishPending,
    ) -> None:
        existing = self.pending_publication()
        if existing is not None:
            if existing != pending:
                raise GenerationPublicationConflictError(
                    "a different generation publication is already pending"
                )
            return
        self._root.mkdir(parents=True, exist_ok=True)
        pending_path = self._root / ".publish_pending.json"
        temporary = self._root / f".publish_pending.{uuid.uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as handle:
                handle.write(self._pending_payload(pending))
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, pending_path)
            except FileExistsError:
                existing = self.pending_publication()
                if existing != pending:
                    raise GenerationPublicationConflictError(
                        "a different generation publication is already pending"
                    )
        finally:
            temporary.unlink(missing_ok=True)

    def _clear_publish_pending(
        self,
        expected: GenerationPublishPending,
    ) -> None:
        if self.pending_publication() != expected:
            raise GenerationPublicationConflictError(
                "pending generation publication changed before completion"
            )
        (self._root / ".publish_pending.json").unlink()

    @staticmethod
    def _read_manifest(path: Path) -> GenerationManifest:
        parsed = generation_contract_registry().parse_json(
            GENERATION_MANIFEST_V1,
            (path / "manifest.json").read_bytes(),
        )
        if not isinstance(parsed, GenerationManifest):
            raise TypeError("generation registry returned the wrong contract")
        return parsed


__all__ = [
    "GenerationPublicationConflictError",
    "GenerationPublicationPendingError",
    "GenerationPublisher",
    "GenerationReadLease",
    "StagedGeneration",
]
