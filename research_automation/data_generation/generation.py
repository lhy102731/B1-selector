"""Candidate-first publication of immutable market-data generations."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path

from research_automation.control_plane.contracts import canonical_json
from research_automation.foundations.immutable_release import ImmutableReleaseStore

from .contracts import (
    GENERATION_MANIFEST_V1,
    GenerationManifest,
    generation_contract_registry,
)


@dataclass(frozen=True, slots=True)
class StagedGeneration:
    path: Path
    generation_id: str
    expected_current_id: str | None


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

    def __init__(self, root: Path) -> None:
        self._root = Path(root)
        self._adapter = _GenerationReleaseAdapter()
        self._store = ImmutableReleaseStore(self._root, adapter=self._adapter)

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
        if self._adapter.validate(staged.path) != staged.generation_id:
            raise ValueError("staged generation identity changed before publish")
        self._store.promote(
            staged.path,
            expected_current_id=staged.expected_current_id,
        )
        return self.read_current()

    def read_current(self) -> GenerationManifest:
        current = self._root / "current"
        parsed = generation_contract_registry().parse_json(
            GENERATION_MANIFEST_V1,
            (current / "manifest.json").read_bytes(),
        )
        if not isinstance(parsed, GenerationManifest):
            raise TypeError("generation registry returned the wrong contract")
        return parsed


__all__ = [
    "GenerationPublisher",
    "StagedGeneration",
]
