"""Opt-in, candidate-first staging for daily market-data deltas.

The legacy daily updater still owns the production CSV paths.  This module is
deliberately a separate seam: callers provide already-produced candidate bytes
and typed provider evidence, and the adapter writes only an immutable delta
under a caller-owned staging root.  Publication is delegated to
``GenerationPublisher``; this module does not introduce another WAL or lock.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Callable, Iterable, Mapping

from research_automation.control_plane.contracts import canonical_json

from .contracts import (
    GENERATION_MANIFEST_V1,
    GenerationManifest,
    generation_contract_registry,
)
from .generation import (
    GenerationPublicationPendingError,
    GenerationPublisher,
    StagedGeneration,
)
from .market_data import (
    BarAvailability,
    BarObservation,
    BarUse,
    FetchFailure,
    MarketSessionKey,
    NoBarConfirmation,
    classify_bar_availability,
)


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PRESENT_STATUSES = frozenset({"inserted", "updated", "present"})
_NO_BAR_STATUSES = frozenset({"no_today_bar", "unknown_no_bar"})
_CONFIRMED_NO_BAR_STATUSES = frozenset({"no_bar_confirmed"})
_FETCH_FAILURE_STATUSES = frozenset({"fetch_failed"})
_ALLOWED_CANDIDATE_NAMES = frozenset(
    {"manifest.json", "daily_delta_manifest.json", "candidate_binding.json", "delta"}
)


class DailyStagingValidationError(ValueError):
    """Raised when a candidate cannot be proven safe to publish."""


class DailyPublishStatus(str, Enum):
    """Outcome of staging or attempting one generation publication."""

    STAGED = "STAGED"
    PUBLISHED = "PUBLISHED"
    PUBLISH_PENDING = "PUBLISH_PENDING"


@dataclass(frozen=True, slots=True)
class DailyBarUpdate:
    """One legacy-updater result converted into typed staging input.

    ``status='no_today_bar'`` intentionally remains ambiguous unless a typed
    ``NoBarConfirmation`` is supplied.  This prevents an empty provider result
    from silently becoming a suspension label.
    """

    key: MarketSessionKey
    relative_path: str
    status: str
    csv_bytes: bytes | None = None
    bar_payload: Mapping[str, object] | None = None
    no_bar_confirmation: NoBarConfirmation | None = None
    fetch_failure: FetchFailure | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, MarketSessionKey):
            raise DailyStagingValidationError("update key must be a MarketSessionKey")
        if not isinstance(self.relative_path, str) or not self.relative_path.strip():
            raise DailyStagingValidationError("update relative_path is required")
        if not isinstance(self.status, str) or self.status != self.status.strip():
            raise DailyStagingValidationError("update status must be canonical text")
        if self.csv_bytes is not None and not isinstance(self.csv_bytes, bytes):
            raise DailyStagingValidationError("csv_bytes must be bytes")
        if self.bar_payload is not None and not isinstance(self.bar_payload, Mapping):
            raise DailyStagingValidationError("bar_payload must be a mapping")
        if self.no_bar_confirmation is not None and not isinstance(
            self.no_bar_confirmation,
            NoBarConfirmation,
        ):
            raise DailyStagingValidationError("no_bar_confirmation is not typed")
        if self.fetch_failure is not None and not isinstance(
            self.fetch_failure,
            FetchFailure,
        ):
            raise DailyStagingValidationError("fetch_failure is not typed")


@dataclass(frozen=True, slots=True)
class StagedDailyUpdate:
    """Validated candidate delta and its generation publication capability."""

    status: DailyPublishStatus
    generation_id: str
    parent_generation_id: str | None
    candidate_path: Path
    delta_manifest_sha256: str
    observations: tuple[BarObservation, ...]
    staged_generation: StagedGeneration


@dataclass(frozen=True, slots=True)
class DailyPublishResult:
    """Public result for a publication attempt or pending recovery."""

    status: DailyPublishStatus
    generation_id: str
    parent_generation_id: str | None
    candidate_path: Path
    delta_manifest_sha256: str


class DailyUpdaterStagingAdapter:
    """Stage a content-bound daily delta under a caller-owned root.

    The adapter accepts bytes rather than a production path.  Consequently a
    test or an opt-in integration can wrap the legacy updater on a temporary
    copy, while the default updater remains completely untouched.
    """

    __slots__ = ("_staging_root", "_publisher", "_generation_root")

    def __init__(
        self,
        staging_root: Path,
        *,
        lock_timeout_seconds: float = 30.0,
    ) -> None:
        if not isinstance(staging_root, Path):
            staging_root = Path(staging_root)
        root = staging_root.expanduser().resolve(strict=False)
        if root == Path(root.anchor):
            raise DailyStagingValidationError("staging root must not be a filesystem root")
        repository_root = Path(__file__).resolve().parents[2]
        for forbidden_name in ("data", "data_ths", "models", "outputs"):
            forbidden_root = (repository_root / forbidden_name).resolve(strict=False)
            try:
                root.relative_to(forbidden_root)
            except ValueError:
                continue
            raise DailyStagingValidationError(
                "staging root cannot be inside a production data/artifact root"
            )
        root.mkdir(parents=True, exist_ok=True)
        self._staging_root = root
        self._generation_root = root / "generations"
        self._publisher = GenerationPublisher(
            self._generation_root,
            lock_timeout_seconds=lock_timeout_seconds,
        )

    @property
    def publisher(self) -> GenerationPublisher:
        """Expose the existing publisher for explicit lease coordination."""

        return self._publisher

    @property
    def staging_root(self) -> Path:
        return self._staging_root

    def stage(
        self,
        manifest: GenerationManifest,
        updates: Iterable[DailyBarUpdate],
    ) -> StagedDailyUpdate:
        """Validate updates and write one immutable candidate delta.

        Unknown provider absence and fetch failures are intentionally blocking:
        a daily cycle may publish confirmed suspensions, but it must not publish
        a partially observed universe.
        """

        if not isinstance(manifest, GenerationManifest):
            raise DailyStagingValidationError("manifest must be a GenerationManifest")
        if self._publisher.pending_publication() is not None:
            raise GenerationPublicationPendingError(
                "an earlier daily publication is pending reconciliation"
            )
        update_values = tuple(updates)
        if not update_values:
            raise DailyStagingValidationError("at least one update is required")
        cutoff = date.fromisoformat(manifest.csv_cutoff)
        observations: list[BarObservation] = []
        normalized: list[tuple[DailyBarUpdate, BarObservation, str | None, int]] = []
        seen_keys: set[MarketSessionKey] = set()
        seen_paths: set[str] = set()
        errors: list[str] = []

        for update in update_values:
            if not isinstance(update, DailyBarUpdate):
                errors.append("update is not a DailyBarUpdate")
                continue
            if update.key in seen_keys:
                errors.append(f"duplicate instrument/session: {update.key.instrument_id}")
                continue
            seen_keys.add(update.key)
            relative_path = self._safe_relative_path(
                update.relative_path,
                key=update.key,
            )
            if relative_path in seen_paths:
                errors.append(f"duplicate staged path: {relative_path}")
                continue
            seen_paths.add(relative_path)
            if update.key.session_date != cutoff:
                errors.append(
                    f"session date {update.key.session_date.isoformat()} does not match cutoff {cutoff.isoformat()}"
                )
                continue
            try:
                observation, content_hash, byte_length = self._classify_update(update)
            except (DailyStagingValidationError, ValueError) as error:
                errors.append(str(error))
                continue
            observations.append(observation)
            normalized.append((update, observation, content_hash, byte_length))
            if observation.availability in {
                BarAvailability.UNKNOWN_NO_BAR,
                BarAvailability.FETCH_FAILED,
            }:
                errors.append(
                    f"{observation.availability.value} blocks publication for {update.key.instrument_id}"
                )

        if errors:
            raise DailyStagingValidationError("; ".join(errors))

        parent_id = self._current_generation_id()
        delta_payload = self._delta_payload(
            parent_generation_id=parent_id,
            cutoff=cutoff,
            normalized=normalized,
        )
        delta_bytes = canonical_json(delta_payload).encode("utf-8")
        delta_hash = hashlib.sha256(delta_bytes).hexdigest()
        delta_reference = f"daily-delta:{delta_hash}"
        if any(reference.startswith("daily-delta:") for reference in manifest.cache_manifest_references):
            raise DailyStagingValidationError(
                "manifest already contains a daily delta reference"
            )
        candidate_manifest = manifest.model_copy(
            update={
                "cache_manifest_references": tuple(
                    (*manifest.cache_manifest_references, delta_reference)
                )
            }
        )
        try:
            staged_generation = self._publisher.stage(candidate_manifest)
            if staged_generation.expected_current_id != parent_id:
                raise DailyStagingValidationError(
                    "CURRENT changed while staging the daily delta"
                )
            candidate = staged_generation.path
            self._write_candidate_payload(
                candidate,
                delta_bytes=delta_bytes,
                binding={
                    "schema_version": "research.data_generation.daily_candidate_binding.v1",
                    "parent_generation_id": parent_id,
                    "candidate_generation_id": staged_generation.generation_id,
                    "delta_manifest_sha256": delta_hash,
                },
                normalized=normalized,
            )
            self._validate_candidate(
                candidate,
                expected_generation_id=staged_generation.generation_id,
                expected_parent_id=parent_id,
                expected_delta_hash=delta_hash,
            )
        except Exception:
            if "staged_generation" in locals():
                self._remove_owned_candidate(staged_generation.path)
            raise

        return StagedDailyUpdate(
            status=DailyPublishStatus.STAGED,
            generation_id=staged_generation.generation_id,
            parent_generation_id=parent_id,
            candidate_path=staged_generation.path,
            delta_manifest_sha256=delta_hash,
            observations=tuple(observations),
            staged_generation=staged_generation,
        )

    def publish(self, staged: StagedDailyUpdate) -> DailyPublishResult:
        """Publish one validated candidate, preserving pending state on readers."""

        if not isinstance(staged, StagedDailyUpdate):
            raise DailyStagingValidationError("staged value is invalid")
        pending = self._publisher.pending_publication()
        if pending is not None and pending.candidate_generation_id != staged.generation_id:
            raise GenerationPublicationPendingError(
                "a different daily publication is pending reconciliation"
            )
        self._validate_candidate(
            staged.candidate_path,
            expected_generation_id=staged.generation_id,
            expected_parent_id=staged.parent_generation_id,
            expected_delta_hash=staged.delta_manifest_sha256,
        )
        try:
            self._publisher.publish(
                staged.staged_generation,
                candidate_validator=self._make_candidate_validator(
                    expected_generation_id=staged.generation_id,
                    expected_parent_id=staged.parent_generation_id,
                    expected_delta_hash=staged.delta_manifest_sha256,
                ),
            )
        except GenerationPublicationPendingError:
            return DailyPublishResult(
                status=DailyPublishStatus.PUBLISH_PENDING,
                generation_id=staged.generation_id,
                parent_generation_id=staged.parent_generation_id,
                candidate_path=staged.candidate_path,
                delta_manifest_sha256=staged.delta_manifest_sha256,
            )
        current = self._generation_root / "current"
        self._validate_candidate(
            current,
            expected_generation_id=staged.generation_id,
            expected_parent_id=staged.parent_generation_id,
            expected_delta_hash=staged.delta_manifest_sha256,
        )
        return DailyPublishResult(
            status=DailyPublishStatus.PUBLISHED,
            generation_id=staged.generation_id,
            parent_generation_id=staged.parent_generation_id,
            candidate_path=current,
            delta_manifest_sha256=staged.delta_manifest_sha256,
        )

    def resume_pending(self) -> DailyPublishResult | None:
        """Validate and resume the existing publisher pending intent after a crash."""

        pending = self._publisher.pending_publication()
        if pending is None:
            return None
        candidate = self._generation_root / "candidate" / pending.candidate_generation_id
        current = self._generation_root / "current"
        validation_path = current if current.is_dir() and not candidate.exists() else candidate
        binding = self._read_binding(validation_path)
        delta_hash = binding["delta_manifest_sha256"]
        parent_id = binding["parent_generation_id"]
        if parent_id != pending.expected_current_generation_id:
            raise DailyStagingValidationError(
                "pending publication parent identity does not match its intent"
            )
        self._validate_candidate(
            validation_path,
            expected_generation_id=pending.candidate_generation_id,
            expected_parent_id=parent_id,
            expected_delta_hash=delta_hash,
        )
        try:
            self._publisher.recover_pending_publication(
                candidate_validator=self._make_candidate_validator(
                    expected_generation_id=pending.candidate_generation_id,
                    expected_parent_id=parent_id,
                    expected_delta_hash=delta_hash,
                ),
            )
        except GenerationPublicationPendingError:
            return DailyPublishResult(
                status=DailyPublishStatus.PUBLISH_PENDING,
                generation_id=pending.candidate_generation_id,
                parent_generation_id=parent_id,
                candidate_path=validation_path,
                delta_manifest_sha256=delta_hash,
            )
        current = self._generation_root / "current"
        self._validate_candidate(
            current,
            expected_generation_id=pending.candidate_generation_id,
            expected_parent_id=parent_id,
            expected_delta_hash=delta_hash,
        )
        return DailyPublishResult(
            status=DailyPublishStatus.PUBLISHED,
            generation_id=pending.candidate_generation_id,
            parent_generation_id=parent_id,
            candidate_path=current,
            delta_manifest_sha256=delta_hash,
        )

    def _make_candidate_validator(
        self,
        *,
        expected_generation_id: str,
        expected_parent_id: str | None,
        expected_delta_hash: str,
    ) -> Callable[[Path, GenerationManifest], None]:
        """Build a validator invoked again by GenerationPublisher under its lock."""

        def validate(path: Path, manifest: GenerationManifest) -> None:
            # ImmutableReleaseStore validates CURRENT as well as the candidate
            # while swapping.  Only the generation being promoted carries this
            # adapter's delta binding; older CURRENT/PREVIOUS releases are left
            # to the publisher's own manifest validator.
            if manifest.generation_id != expected_generation_id:
                return
            self._validate_candidate(
                path,
                expected_generation_id=expected_generation_id,
                expected_parent_id=expected_parent_id,
                expected_delta_hash=expected_delta_hash,
            )

        return validate

    @staticmethod
    def _classify_update(
        update: DailyBarUpdate,
    ) -> tuple[BarObservation, str | None, int]:
        status = update.status
        if status in _PRESENT_STATUSES:
            if update.csv_bytes is None or not update.csv_bytes:
                raise DailyStagingValidationError(
                    f"{status} requires non-empty csv_bytes"
                )
            try:
                DailyUpdaterStagingAdapter._validate_csv_payload(
                    update.csv_bytes,
                    key=update.key,
                )
            except UnicodeDecodeError as error:
                raise DailyStagingValidationError("staged CSV is not valid GBK") from error
            content_hash = hashlib.sha256(update.csv_bytes).hexdigest()
            payload = dict(update.bar_payload or {})
            supplied_hash = payload.get("csv_sha256")
            if supplied_hash is not None and supplied_hash != content_hash:
                raise DailyStagingValidationError("bar payload CSV identity does not match bytes")
            payload.update(
                {
                    "csv_sha256": content_hash,
                    "instrument_id": update.key.instrument_id,
                    "session_date": update.key.session_date.isoformat(),
                }
            )
            observation = classify_bar_availability(
                key=update.key,
                bar_payload=payload,
            )
            observation.require_usable_for(BarUse.FEATURE)
            return observation, content_hash, len(update.csv_bytes)

        if status in _NO_BAR_STATUSES:
            if update.csv_bytes is not None or update.bar_payload is not None:
                raise DailyStagingValidationError(
                    "non-present status cannot carry CSV or bar payload"
                )
            observation = classify_bar_availability(
                key=update.key,
                no_bar_confirmation=update.no_bar_confirmation,
            )
            return observation, None, 0

        if status in _CONFIRMED_NO_BAR_STATUSES:
            if update.csv_bytes is not None or update.bar_payload is not None:
                raise DailyStagingValidationError(
                    "non-present status cannot carry CSV or bar payload"
                )
            if update.no_bar_confirmation is None:
                raise DailyStagingValidationError(
                    "confirmed no-bar status requires NoBarConfirmation"
                )
            observation = classify_bar_availability(
                key=update.key,
                no_bar_confirmation=update.no_bar_confirmation,
            )
            return observation, None, 0

        if status in _FETCH_FAILURE_STATUSES:
            if update.csv_bytes is not None or update.bar_payload is not None:
                raise DailyStagingValidationError(
                    "non-present status cannot carry CSV or bar payload"
                )
            if update.fetch_failure is None:
                raise DailyStagingValidationError(
                    "fetch_failed status requires FetchFailure"
                )
            observation = classify_bar_availability(
                key=update.key,
                fetch_failure=update.fetch_failure,
            )
            return observation, None, 0

        raise DailyStagingValidationError(f"unsupported updater status: {status}")

    @staticmethod
    def _safe_relative_path(relative_path: str, *, key: MarketSessionKey) -> str:
        normalized = relative_path.replace("\\", "/")
        parts = normalized.split("/")
        if (
            not normalized
            or normalized.startswith("/")
            or ":" in normalized
            or any(part in {"", ".", ".."} for part in parts)
            or any(character in '<>"|?*' for character in normalized)
            or any(ord(character) < 32 for character in normalized)
            or any(part.endswith((" ", ".")) for part in parts)
            or any(
                part.split(".", 1)[0].casefold()
                in {"con", "prn", "aux", "nul", "clock$", *[f"com{i}" for i in range(1, 10)], *[f"lpt{i}" for i in range(1, 10)]}
                for part in parts
            )
        ):
            raise DailyStagingValidationError("staged path is not a safe relative path")
        if Path(normalized).suffix.casefold() != ".csv":
            raise DailyStagingValidationError("staged path must be a CSV")
        if Path(normalized).stem != key.instrument_id:
            raise DailyStagingValidationError("staged path does not match instrument")
        return normalized

    @staticmethod
    def _validate_csv_payload(content: bytes, *, key: MarketSessionKey) -> None:
        """Check the minimum source-CSV contract without touching production data."""

        text = content.decode("gbk")
        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames is None:
            raise DailyStagingValidationError("staged CSV has no header")
        normalized_fields = {
            str(field).strip().casefold()
            for field in reader.fieldnames
            if field is not None
        }
        required_fields = {"date", "open", "high", "low", "close", "volume"}
        if not required_fields.issubset(normalized_fields):
            raise DailyStagingValidationError("staged CSV lacks required OHLCV fields")
        target = key.session_date.isoformat()
        target_compact = target.replace("-", "")
        found = False
        for row in reader:
            row_date = str(row.get("date") or row.get("Date") or "").strip()
            if row_date not in {target, target_compact}:
                continue
            found = True
            try:
                close = float(str(row.get("close") or row.get("Close")))
                volume = float(str(row.get("volume") or row.get("Volume")))
            except (TypeError, ValueError) as error:
                raise DailyStagingValidationError(
                    "target CSV bar has invalid OHLCV values"
                ) from error
            if not (close > 0 and volume >= 0):
                raise DailyStagingValidationError("target CSV bar has invalid OHLCV values")
        if not found:
            raise DailyStagingValidationError("staged CSV has no target-session bar")

    @staticmethod
    def _delta_payload(
        *,
        parent_generation_id: str | None,
        cutoff: date,
        normalized: list[tuple[DailyBarUpdate, BarObservation, str | None, int]],
    ) -> dict[str, object]:
        entries: list[dict[str, object]] = []
        for update, observation, content_hash, byte_length in normalized:
            evidence_source: str | None = None
            evidence_ref: str | None = None
            if observation.no_bar_confirmation is not None:
                evidence_source = observation.no_bar_confirmation.source_id
                evidence_ref = observation.no_bar_confirmation.evidence_ref
            elif observation.fetch_failure is not None:
                evidence_source = observation.fetch_failure.source_id
                evidence_ref = observation.fetch_failure.error_ref
            entries.append(
                {
                    "relative_path": update.relative_path.replace("\\", "/"),
                    "instrument_id": update.key.instrument_id,
                    "session_date": update.key.session_date.isoformat(),
                    "availability": observation.availability.value,
                    "content_sha256": content_hash,
                    "byte_length": byte_length,
                    "evidence_source": evidence_source,
                    "evidence_ref": evidence_ref,
                }
            )
        entries.sort(key=lambda entry: str(entry["relative_path"]))
        return {
            "schema_version": "research.data_generation.daily_delta_manifest.v1",
            "parent_generation_id": parent_generation_id,
            "csv_cutoff": cutoff.isoformat(),
            "entries": entries,
        }

    def _current_generation_id(self) -> str | None:
        current = self._generation_root / "current"
        if current.is_symlink() or not current.is_dir():
            return None
        try:
            return self._publisher.read_current().generation_id
        except (OSError, ValueError) as error:
            raise DailyStagingValidationError("CURRENT generation is invalid") from error

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
                temporary = Path(handle.name)
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _write_candidate_payload(
        self,
        candidate: Path,
        *,
        delta_bytes: bytes,
        binding: Mapping[str, object],
        normalized: list[tuple[DailyBarUpdate, BarObservation, str | None, int]],
    ) -> None:
        (candidate / "delta").mkdir(parents=True, exist_ok=True)
        self._atomic_write(candidate / "daily_delta_manifest.json", delta_bytes)
        self._atomic_write(
            candidate / "candidate_binding.json",
            canonical_json(dict(binding)).encode("utf-8"),
        )
        for update, observation, content_hash, _ in normalized:
            if observation.availability is not BarAvailability.PRESENT:
                continue
            if update.csv_bytes is None or content_hash is None:
                raise DailyStagingValidationError("present update lost CSV bytes")
            relative = self._safe_relative_path(update.relative_path, key=update.key)
            self._atomic_write(candidate / "delta" / relative, update.csv_bytes)

    def _read_binding(self, candidate: Path) -> dict[str, object]:
        try:
            binding_bytes = (candidate / "candidate_binding.json").read_bytes()
            value = json.loads(binding_bytes)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            raise DailyStagingValidationError("candidate binding is unreadable") from error
        if not isinstance(value, dict):
            raise DailyStagingValidationError("candidate binding is not an object")
        try:
            canonical_binding_bytes = canonical_json(value).encode("utf-8")
        except (TypeError, ValueError) as error:
            raise DailyStagingValidationError("candidate binding is not canonical") from error
        if canonical_binding_bytes != binding_bytes:
            raise DailyStagingValidationError("candidate binding is not canonical")
        required = {
            "schema_version",
            "parent_generation_id",
            "candidate_generation_id",
            "delta_manifest_sha256",
        }
        if set(value) != required or value["schema_version"] != "research.data_generation.daily_candidate_binding.v1":
            raise DailyStagingValidationError("candidate binding contract is invalid")
        candidate_id = value["candidate_generation_id"]
        delta_hash = value["delta_manifest_sha256"]
        parent_id = value["parent_generation_id"]
        if not isinstance(candidate_id, str) or _SHA256_RE.fullmatch(candidate_id) is None:
            raise DailyStagingValidationError("candidate generation identity is invalid")
        if not isinstance(delta_hash, str) or _SHA256_RE.fullmatch(delta_hash) is None:
            raise DailyStagingValidationError("delta identity is invalid")
        if parent_id is not None and (
            not isinstance(parent_id, str) or _SHA256_RE.fullmatch(parent_id) is None
        ):
            raise DailyStagingValidationError("parent generation identity is invalid")
        return value

    def _validate_candidate(
        self,
        candidate: Path,
        *,
        expected_generation_id: str,
        expected_parent_id: str | None,
        expected_delta_hash: str,
    ) -> None:
        original_candidate = candidate
        if original_candidate.is_symlink():
            raise DailyStagingValidationError("candidate cannot be a symlink")
        generation_root = self._generation_root.resolve(strict=True)
        candidate_root = (generation_root / "candidate").resolve(strict=True)
        current_root = (generation_root / "current").resolve(strict=False)
        try:
            candidate = candidate.resolve(strict=True)
            try:
                candidate.relative_to(candidate_root)
            except ValueError:
                candidate.relative_to(current_root)
        except (OSError, ValueError) as error:
            raise DailyStagingValidationError("candidate escaped its staging root") from error
        if not candidate.is_dir():
            raise DailyStagingValidationError("candidate is not a directory")
        if any(
            path.name not in _ALLOWED_CANDIDATE_NAMES or path.is_symlink()
            for path in candidate.iterdir()
        ):
            raise DailyStagingValidationError("candidate contains unexpected files")
        for required_name in (
            "manifest.json",
            "daily_delta_manifest.json",
            "candidate_binding.json",
        ):
            required_path = candidate / required_name
            if required_path.is_symlink() or not required_path.is_file():
                raise DailyStagingValidationError("candidate contains an invalid control file")
        try:
            manifest_bytes = (candidate / "manifest.json").read_bytes()
            manifest = generation_contract_registry().parse_json(
                GENERATION_MANIFEST_V1,
                manifest_bytes,
            )
        except (OSError, ValueError) as error:
            raise DailyStagingValidationError("candidate generation manifest is invalid") from error
        if not isinstance(manifest, GenerationManifest) or manifest.generation_id != expected_generation_id:
            raise DailyStagingValidationError("candidate generation identity mismatch")
        if canonical_json(manifest.model_dump(mode="json")).encode("utf-8") != manifest_bytes:
            raise DailyStagingValidationError("candidate generation manifest is not canonical")
        if manifest.cache_manifest_references.count(f"daily-delta:{expected_delta_hash}") != 1 or sum(
            reference.startswith("daily-delta:")
            for reference in manifest.cache_manifest_references
        ) != 1:
            raise DailyStagingValidationError("generation manifest is not bound to the delta")
        binding = self._read_binding(candidate)
        if (
            binding["candidate_generation_id"] != expected_generation_id
            or binding["parent_generation_id"] != expected_parent_id
            or binding["delta_manifest_sha256"] != expected_delta_hash
        ):
            raise DailyStagingValidationError("candidate parent or delta binding mismatch")
        try:
            delta_bytes = (candidate / "daily_delta_manifest.json").read_bytes()
            delta = json.loads(delta_bytes)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            raise DailyStagingValidationError("delta manifest is unreadable") from error
        if hashlib.sha256(delta_bytes).hexdigest() != expected_delta_hash:
            raise DailyStagingValidationError("delta manifest identity mismatch")
        try:
            canonical_delta_bytes = canonical_json(delta).encode("utf-8")
        except (TypeError, ValueError) as error:
            raise DailyStagingValidationError("delta manifest is not canonical") from error
        if canonical_delta_bytes != delta_bytes:
            raise DailyStagingValidationError("delta manifest is not canonical")
        if not isinstance(delta, dict) or set(delta) != {
            "schema_version",
            "parent_generation_id",
            "csv_cutoff",
            "entries",
        }:
            raise DailyStagingValidationError("delta manifest contract is invalid")
        if delta["schema_version"] != "research.data_generation.daily_delta_manifest.v1":
            raise DailyStagingValidationError("delta manifest schema is invalid")
        if delta["parent_generation_id"] != expected_parent_id:
            raise DailyStagingValidationError("delta parent identity mismatch")
        try:
            delta_cutoff = date.fromisoformat(str(delta["csv_cutoff"])).isoformat()
        except (KeyError, TypeError, ValueError) as error:
            raise DailyStagingValidationError("delta cutoff is invalid") from error
        if delta_cutoff != str(manifest.csv_cutoff):
            raise DailyStagingValidationError("delta cutoff does not match generation manifest")
        entries = delta["entries"]
        if not isinstance(entries, list):
            raise DailyStagingValidationError("delta entries are invalid")
        if not entries:
            raise DailyStagingValidationError("delta entries cannot be empty")
        if any(not isinstance(entry, dict) for entry in entries):
            raise DailyStagingValidationError("delta entry is invalid")
        if entries != sorted(entries, key=lambda entry: str(entry.get("relative_path", ""))):
            raise DailyStagingValidationError("delta entries are not canonically ordered")
        seen: set[str] = set()
        seen_keys: set[MarketSessionKey] = set()
        delta_root = candidate / "delta"
        if not delta_root.is_dir() or delta_root.is_symlink():
            raise DailyStagingValidationError("delta root is invalid")
        expected_files: set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                raise DailyStagingValidationError("delta entry is invalid")
            required = {
                "relative_path",
                "instrument_id",
                "session_date",
                "availability",
                "content_sha256",
                "byte_length",
                "evidence_source",
                "evidence_ref",
            }
            if set(entry) != required:
                raise DailyStagingValidationError("delta entry fields are invalid")
            relative = str(entry["relative_path"])
            try:
                key = MarketSessionKey(
                    str(entry["instrument_id"]),
                    date.fromisoformat(str(entry["session_date"])),
                )
            except (TypeError, ValueError) as error:
                raise DailyStagingValidationError("delta entry session key is invalid") from error
            if key in seen_keys:
                raise DailyStagingValidationError("delta contains duplicate instrument sessions")
            seen_keys.add(key)
            normalized = self._safe_relative_path(relative, key=key)
            folded = normalized.casefold()
            if folded in {path.casefold() for path in seen}:
                raise DailyStagingValidationError("delta contains duplicate paths")
            seen.add(normalized)
            try:
                availability = BarAvailability(str(entry["availability"]))
            except ValueError as error:
                raise DailyStagingValidationError("delta availability is invalid") from error
            target = delta_root / normalized
            try:
                target.resolve(strict=False).relative_to(delta_root.resolve(strict=True))
            except (OSError, ValueError) as error:
                raise DailyStagingValidationError("delta path escaped its root") from error
            if availability is BarAvailability.PRESENT:
                if entry["evidence_source"] is not None or entry["evidence_ref"] is not None:
                    raise DailyStagingValidationError(
                        "present delta carries missing-data evidence"
                    )
                expected_files.add(normalized.casefold())
                if target.is_symlink() or not target.is_file() or not isinstance(entry["content_sha256"], str) or _SHA256_RE.fullmatch(entry["content_sha256"]) is None:
                    raise DailyStagingValidationError("present delta file is missing")
                if type(entry["byte_length"]) is not int or entry["byte_length"] < 0:
                    raise DailyStagingValidationError("present delta length is invalid")
                content = target.read_bytes()
                self._validate_csv_payload(content, key=key)
                if hashlib.sha256(content).hexdigest() != entry["content_sha256"]:
                    raise DailyStagingValidationError("staged CSV bytes changed")
                if len(content) != int(entry["byte_length"]):
                    raise DailyStagingValidationError("staged CSV length changed")
            else:
                if target.exists():
                    raise DailyStagingValidationError("non-present delta carries CSV bytes")
                if entry["content_sha256"] is not None or type(entry["byte_length"]) is not int or entry["byte_length"] != 0:
                    raise DailyStagingValidationError("non-present delta has content identity")
                if availability is BarAvailability.NO_BAR_CONFIRMED and (
                    not isinstance(entry["evidence_source"], str)
                    or not entry["evidence_source"].strip()
                    or entry["evidence_source"] != entry["evidence_source"].strip()
                    or not isinstance(entry["evidence_ref"], str)
                    or not entry["evidence_ref"].strip()
                    or entry["evidence_ref"] != entry["evidence_ref"].strip()
                ):
                    raise DailyStagingValidationError("confirmed no-bar evidence is missing")
                if availability is not BarAvailability.NO_BAR_CONFIRMED and (
                    entry["evidence_source"] is not None
                    or entry["evidence_ref"] is not None
                ):
                    raise DailyStagingValidationError("unconfirmed delta carries evidence")
            if availability in {
                BarAvailability.UNKNOWN_NO_BAR,
                BarAvailability.FETCH_FAILED,
            }:
                raise DailyStagingValidationError(
                    "unresolved missing-data state cannot be published"
                )
            if key.session_date.isoformat() != delta_cutoff:
                raise DailyStagingValidationError("delta entry session does not match cutoff")
        actual_files = {
            path.relative_to(delta_root).as_posix().casefold()
            for path in delta_root.rglob("*")
            if path.is_file()
        }
        if actual_files != expected_files:
            raise DailyStagingValidationError("delta contains an unbound file")
        if any(path.is_symlink() for path in delta_root.rglob("*")):
            raise DailyStagingValidationError("delta contains a symlink")

    def _remove_owned_candidate(self, candidate: Path) -> None:
        try:
            resolved = candidate.resolve(strict=False)
            candidate_root = (self._generation_root / "candidate").resolve(strict=False)
            resolved.relative_to(candidate_root)
        except (OSError, ValueError):
            return
        if resolved.is_dir():
            for child in sorted(resolved.rglob("*"), reverse=True):
                if child.is_file() or child.is_symlink():
                    child.unlink(missing_ok=True)
                elif child.is_dir():
                    child.rmdir()
            resolved.rmdir()


__all__ = [
    "DailyBarUpdate",
    "DailyPublishResult",
    "DailyPublishStatus",
    "DailyStagingValidationError",
    "DailyUpdaterStagingAdapter",
    "StagedDailyUpdate",
]
