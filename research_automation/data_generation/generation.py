"""Candidate-first publication of immutable market-data generations."""

from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path

from research_automation.control_plane.contracts import canonical_json
from research_automation.foundations.immutable_release import (
    ImmutableReleaseStore,
    ReleaseBusyError,
    ReleaseConflictError,
    ReleaseReadLease,
)
from research_automation.foundations.artifact_identity import (
    ArtifactIdentity,
    ArtifactIdentityMismatchError,
    ArtifactLocationError,
    ArtifactLocator,
    identify_file,
    verify_file_identity,
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


class GenerationMutatedError(RuntimeError):
    """Raised when a pinned generation or touched artifact changes."""


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


class GenerationPin:
    """Lease one generation and content-bind only explicitly touched files."""

    __slots__ = (
        "_publisher",
        "_lease",
        "_manifest",
        "_data_root",
        "_identities",
        "_identity_lock",
    )

    def __init__(
        self,
        publisher: GenerationPublisher,
        lease: GenerationReadLease,
        manifest: GenerationManifest,
        data_root: Path,
    ) -> None:
        self._publisher = publisher
        self._lease = lease
        self._manifest = manifest
        self._data_root = Path(data_root).resolve(strict=True)
        self._identities: dict[str, ArtifactIdentity] = {}
        self._identity_lock = threading.Lock()

    @property
    def generation_id(self) -> str:
        return self._manifest.generation_id

    @property
    def data_snapshot_id(self) -> str:
        return self.generation_id

    @property
    def manifest(self) -> GenerationManifest:
        return self._manifest

    @property
    def active(self) -> bool:
        return self._lease.active

    def artifact_path(self, relative_path: str) -> Path:
        """Validate a relative artifact path before any filesystem access."""
        locator = ArtifactLocator(
            schema_version="research.artifact_locator.v1",
            storage_root=self._data_root.as_posix(),
            path=relative_path,
            size_bytes=0,
            mtime_ns=0,
        )
        return self._data_root / locator.path

    def verify_artifact(
        self,
        relative_path: str,
        *,
        content_schema: str,
        kind: str,
        logical_role: str,
        producer: str = "research.data_generation.GenerationPin",
    ) -> ArtifactIdentity:
        try:
            artifact_path = self.artifact_path(relative_path)
            current = self._publisher.read(self._lease)
            if current.generation_id != self.generation_id:
                raise GenerationMutatedError("GENERATION_MUTATED")
            observed = artifact_path.stat()
            locator = ArtifactLocator(
                schema_version="research.artifact_locator.v1",
                storage_root=self._data_root.as_posix(),
                path=relative_path,
                size_bytes=observed.st_size,
                mtime_ns=observed.st_mtime_ns,
            )
            with self._identity_lock:
                identity = self._identities.get(locator.path)
                if identity is None:
                    identity = identify_file(
                        locator,
                        content_schema=content_schema,
                        producer=producer,
                        generation=self.generation_id,
                        kind=kind,
                        logical_role=logical_role,
                    )
                    self._identities[locator.path] = identity
                    return identity
                expected_semantics = (
                    content_schema,
                    producer,
                    kind,
                    logical_role,
                )
                observed_semantics = (
                    identity.content_schema,
                    identity.producer,
                    identity.kind,
                    identity.logical_role,
                )
                if observed_semantics != expected_semantics:
                    raise GenerationMutatedError("GENERATION_MUTATED")
                verify_file_identity(locator, identity)
                return identity
        except GenerationMutatedError:
            raise
        except (
            ArtifactIdentityMismatchError,
            ArtifactLocationError,
            OSError,
            ValueError,
        ) as error:
            raise GenerationMutatedError("GENERATION_MUTATED") from error

    def verify_touched_artifact_ids(
        self,
        artifact_ids: tuple[str, ...],
        *,
        exclude_artifact_id: str | None = None,
    ) -> tuple[str, ...]:
        """Reverify content identities previously established by this pin."""
        if not isinstance(artifact_ids, tuple) or any(
            type(artifact_id) is not str for artifact_id in artifact_ids
        ):
            raise TypeError("artifact_ids must be a tuple of strings")
        with self._identity_lock:
            identities_by_id = {
                identity.artifact_id: (path, identity)
                for path, identity in self._identities.items()
            }
            touched: list[tuple[str, ArtifactIdentity]] = []
            for artifact_id in artifact_ids:
                if artifact_id == exclude_artifact_id:
                    raise ValueError("cache artifact cannot be its own source")
                try:
                    touched.append(identities_by_id[artifact_id])
                except KeyError as error:
                    raise ValueError(
                        "source artifact was not verified by this generation pin"
                    ) from error
        for path, identity in touched:
            current = self.verify_artifact(
                path,
                content_schema=identity.content_schema,
                producer=identity.producer,
                kind=identity.kind,
                logical_role=identity.logical_role,
            )
            if current.artifact_id != identity.artifact_id:
                raise GenerationMutatedError("GENERATION_MUTATED")
        return tuple(sorted(artifact_ids))

    def release(self) -> None:
        self._lease.release()

    def __enter__(self) -> GenerationPin:
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
        pending = GenerationPublishPending(
            schema_version=GENERATION_PUBLISH_PENDING_V1,
            status="PUBLISH_PENDING",
            candidate_generation_id=staged.generation_id,
            expected_current_generation_id=staged.expected_current_id,
        )
        try:
            manifest = self._read_manifest(staged.path)
        except FileNotFoundError:
            completed = self._complete_existing_publication(pending)
            if completed is not None:
                return completed
            raise
        if manifest.generation_id != staged.generation_id:
            raise ValueError("staged generation identity changed before publish")
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
        except (FileNotFoundError, ReleaseConflictError) as error:
            completed = self._complete_existing_publication(pending)
            if completed is not None:
                return completed
            raise GenerationPublicationConflictError(
                "generation publication conflicted; pending intent was retained "
                "for explicit reconciliation"
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

    def pin_current(
        self,
        *,
        expected_generation_id: str,
        data_root: Path,
    ) -> GenerationPin:
        lease = self.acquire_read_lease(
            expected_generation_id=expected_generation_id,
        )
        try:
            manifest = self.read(lease)
            return GenerationPin(self, lease, manifest, data_root)
        except Exception:
            lease.release()
            raise

    def read(self, lease: GenerationReadLease) -> GenerationManifest:
        if not isinstance(lease, GenerationReadLease):
            raise TypeError("lease must be a GenerationReadLease")
        generation_id = self._store.validate_read_lease(lease._release_lease)
        manifest = self._read_manifest(self._root / "current")
        if manifest.generation_id != generation_id:
            raise RuntimeError("leased generation identity changed")
        return manifest

    def pending_publication(self) -> GenerationPublishPending | None:
        paths = sorted(self._root.glob(".publish_pending.*.json"))
        if not paths:
            return None
        if len(paths) != 1:
            raise GenerationPublicationConflictError(
                "multiple generation publications are pending"
            )
        pending_path = paths[0]
        parsed = generation_contract_registry().parse_json(
            GENERATION_PUBLISH_PENDING_V1,
            pending_path.read_bytes(),
        )
        if not isinstance(parsed, GenerationPublishPending):
            raise TypeError("generation registry returned the wrong contract")
        if pending_path != self._pending_path(parsed):
            raise GenerationPublicationConflictError(
                "pending publication filename does not match its identity"
            )
        return parsed

    def recover_pending_publication(self) -> GenerationManifest | None:
        pending = self.pending_publication()
        if pending is None:
            return None
        self._store.recover()
        current_path = self._root / "current"
        current = self._read_manifest(current_path) if current_path.is_dir() else None
        if (
            current is not None
            and current.generation_id == pending.candidate_generation_id
        ):
            self._clear_publish_pending(pending)
            return current
        current_id = None if current is None else current.generation_id
        if current_id != pending.expected_current_generation_id:
            raise GenerationPublicationConflictError(
                "pending publication expected a different CURRENT"
            )
        candidate = self._root / "candidate" / pending.candidate_generation_id
        manifest = self._read_manifest(candidate)
        if manifest.generation_id != pending.candidate_generation_id:
            raise GenerationPublicationConflictError(
                "pending publication candidate identity changed"
            )
        try:
            self._store.promote(
                candidate,
                expected_current_id=pending.expected_current_generation_id,
                expected_candidate_id=pending.candidate_generation_id,
            )
        except ReleaseBusyError as error:
            raise GenerationPublicationPendingError(
                "generation publication is still waiting for the publication lock"
            ) from error
        self._clear_publish_pending(pending)
        return manifest

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
        pending_path = self._pending_path(pending)
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
        existing = self.pending_publication()
        if existing is not None and existing != expected:
            raise GenerationPublicationConflictError(
                "pending generation publication changed before completion"
            )
        self._pending_path(expected).unlink(missing_ok=True)

    def _complete_existing_publication(
        self,
        pending: GenerationPublishPending,
    ) -> GenerationManifest | None:
        self._store.recover()
        current_path = self._root / "current"
        if not current_path.is_dir():
            return None
        current = self._read_manifest(current_path)
        if current.generation_id != pending.candidate_generation_id:
            return None
        self._clear_publish_pending(pending)
        return current

    def _pending_path(self, pending: GenerationPublishPending) -> Path:
        expected_id = pending.expected_current_generation_id or "NONE"
        return self._root / (
            f".publish_pending.{pending.candidate_generation_id}."
            f"{expected_id}.json"
        )

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
    "GenerationMutatedError",
    "GenerationPin",
    "GenerationPublicationConflictError",
    "GenerationPublicationPendingError",
    "GenerationPublisher",
    "GenerationReadLease",
    "StagedGeneration",
]
