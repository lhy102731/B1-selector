"""Strict typed contracts for post-P0 research foundations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType

from pydantic import BaseModel, ConfigDict, ValidationError

from research_automation.control_plane.artifact_semantics import (
    ArtifactSemanticError,
    parse_strict_json,
)
from research_automation.control_plane.contracts import canonical_json


DRAFT_2020_12 = "https://json-schema.org/draft/2020-12/schema"


class ContractValidationError(ValueError):
    """Raised when contract bytes do not satisfy the expected version."""


class UnknownContractVersionError(ValueError):
    """Raised when a caller requests an unregistered contract version."""


class ContractMigrationError(ValueError):
    """Raised when no exact registered contract migration is available."""


class StrictContractModel(BaseModel):
    """Immutable, coercion-free base for native research contracts."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class ContractRegistry:
    """Resolve an explicitly expected schema version to one typed contract."""

    def __init__(
        self,
        *,
        version: str,
        contracts: Mapping[str, type[StrictContractModel]],
        migrations: Mapping[
            tuple[str, str],
            Callable[[Mapping[str, object]], Mapping[str, object]],
        ]
        | None = None,
        max_json_bytes: int = 64 * 1024,
    ) -> None:
        if not isinstance(version, str) or not version or version != version.strip():
            raise ValueError("registry version must be a non-empty canonical string")
        definitions = dict(contracts)
        if not definitions:
            raise ValueError("contract registry must contain at least one contract")
        for schema_version, model in definitions.items():
            if (
                not isinstance(schema_version, str)
                or not schema_version
                or schema_version != schema_version.strip()
                or not isinstance(model, type)
                or not issubclass(model, StrictContractModel)
            ):
                raise ValueError("contract definitions are invalid")
            if (
                model.model_config.get("strict") is not True
                or model.model_config.get("frozen") is not True
                or model.model_config.get("extra") != "forbid"
            ):
                raise ValueError(
                    "contract model policy must remain strict, frozen, and "
                    "extra-forbid"
                )
            if any(
                not field.is_required()
                for field in model.model_fields.values()
            ):
                raise ValueError(
                    "contract model fields must all be explicitly required"
                )
            version_schema = (
                model.model_json_schema(mode="validation")
                .get("properties", {})
                .get("schema_version", {})
            )
            if version_schema.get("const") != schema_version:
                raise ValueError(
                    "registry key must match the model schema_version literal"
                )
        self._version = version
        self._contracts = MappingProxyType(definitions)
        migration_definitions = dict(migrations or {})
        for edge, migration in migration_definitions.items():
            if (
                not isinstance(edge, tuple)
                or len(edge) != 2
                or edge[0] == edge[1]
                or edge[0] not in definitions
                or edge[1] not in definitions
                or not callable(migration)
            ):
                raise ValueError("contract migration definitions are invalid")
        self._migrations = MappingProxyType(migration_definitions)
        if type(max_json_bytes) is not int or max_json_bytes < 1:
            raise ValueError("max_json_bytes must be a positive exact integer")
        self._max_json_bytes = max_json_bytes

    @property
    def version(self) -> str:
        return self._version

    @property
    def versions(self) -> tuple[str, ...]:
        return tuple(sorted(self._contracts))

    def parse_json(
        self,
        expected_schema_version: str,
        raw: bytes,
    ) -> StrictContractModel:
        try:
            model = self._contracts[expected_schema_version]
        except KeyError as error:
            raise UnknownContractVersionError(
                f"unknown contract schema version: {expected_schema_version!r}"
            ) from error
        if not isinstance(raw, bytes):
            raise ContractValidationError("contract input must be bytes")
        if len(raw) > self._max_json_bytes:
            raise ContractValidationError("contract input exceeds its byte limit")
        invalid_json = False
        try:
            payload = parse_strict_json(raw, artifact_name=expected_schema_version)
        except ArtifactSemanticError:
            invalid_json = True
        if invalid_json:
            raise ContractValidationError(
                "contract input is not strict UTF-8 JSON"
            )
        if payload.get("schema_version") != expected_schema_version:
            raise ContractValidationError(
                "payload schema_version does not match the expected contract"
            )
        validation_summary: str | None = None
        try:
            parsed = model.model_validate_json(raw, strict=True)
        except ValidationError as error:
            error_types = sorted(
                {
                    str(item.get("type", "validation_error"))
                    for item in error.errors(
                        include_url=False,
                        include_context=False,
                        include_input=False,
                    )
                }
            )
            validation_summary = ",".join(error_types) or "validation_error"
        if validation_summary is not None:
            raise ContractValidationError(
                "contract payload failed strict validation: "
                f"{validation_summary}"
            )
        return parsed

    def json_schema(self, schema_version: str) -> dict[str, object]:
        try:
            model = self._contracts[schema_version]
        except KeyError as error:
            raise UnknownContractVersionError(
                f"unknown contract schema version: {schema_version!r}"
            ) from error
        generated = model.model_json_schema(mode="validation")
        return {"$schema": DRAFT_2020_12, **generated}

    def json_schema_bytes(self, schema_version: str) -> bytes:
        return canonical_json(self.json_schema(schema_version)).encode("utf-8")

    def parse_mapping(
        self,
        expected_schema_version: str,
        payload: Mapping[str, object],
    ) -> StrictContractModel:
        if not isinstance(payload, Mapping):
            raise ContractValidationError("contract payload must be a mapping")
        serialization_failed = False
        try:
            raw = canonical_json(payload).encode("utf-8")
        except (TypeError, ValueError):
            serialization_failed = True
        if serialization_failed:
            raise ContractValidationError(
                "contract payload cannot be serialized as canonical JSON"
            )
        return self.parse_json(expected_schema_version, raw)

    def migrate_exact(
        self,
        source_schema_version: str,
        target_schema_version: str,
        payload: Mapping[str, object],
    ) -> StrictContractModel:
        edge = (source_schema_version, target_schema_version)
        try:
            migration = self._migrations[edge]
        except KeyError as error:
            raise ContractMigrationError(
                "no exact registered contract migration is available: "
                f"{source_schema_version!r} -> {target_schema_version!r}"
            ) from error
        source = self.parse_mapping(source_schema_version, payload)
        source_payload = MappingProxyType(source.model_dump(mode="python"))
        migration_failed = False
        try:
            migrated = migration(source_payload)
        except Exception:
            migration_failed = True
        if migration_failed:
            raise ContractMigrationError(
                "registered contract migration failed"
            )
        if not isinstance(migrated, Mapping):
            raise ContractMigrationError("registered contract migration returned no mapping")
        return self.parse_mapping(target_schema_version, migrated)


__all__ = [
    "ContractRegistry",
    "ContractMigrationError",
    "ContractValidationError",
    "DRAFT_2020_12",
    "StrictContractModel",
    "UnknownContractVersionError",
]
