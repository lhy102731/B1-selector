"""Strict, deterministic protocol contracts for P1 dry compilation.

This module deliberately stops at identity and conformance.  It does not read
artifacts, start a runner, access a dataset, call a provider, or authorize a
side effect.  Later phases may consume the frozen ``ExecutionSpec`` through a
separately scoped adapter.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
import hashlib
import math
import re
from typing import Annotated, Literal

from pydantic import (
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from research_automation.control_plane.contracts import (
    Actor,
    SideEffect,
    canonical_json,
    canonical_sha256,
)

from .artifact_identity import ArtifactIdentity
from .contract_registry import (
    ContractRegistry,
    StrictContractModel,
)


PROTOCOL_DEFINITION_V1 = "research.protocol_definition.v1"
PROTOCOL_APPROVAL_V1 = "research.protocol_approval.v1"
PROTOCOL_APPROVAL_STATEMENT_V1 = "research.protocol_approval_statement.v1"
PROTOCOL_AMENDMENT_V1 = "research.protocol_amendment.v1"
PROTOCOL_AMENDMENT_STATEMENT_V1 = "research.protocol_amendment_statement.v1"
EXECUTION_SPEC_V1 = "research.execution_spec.v1"

IDENTICAL = "IDENTICAL"
IMMATERIAL_ALLOWLISTED = "IMMATERIAL_ALLOWLISTED"
APPROVED_AMENDMENT = "APPROVED_AMENDMENT"
MATERIAL_UNAPPROVED = "MATERIAL_UNAPPROVED"
CONFORMANCE_VALUES = frozenset(
    {IDENTICAL, IMMATERIAL_ALLOWLISTED, APPROVED_AMENDMENT, MATERIAL_UNAPPROVED}
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
NonNegativeInt = Annotated[int, Field(ge=0)]
PositiveInt = Annotated[int, Field(gt=0)]

_SAFE_SIDE_EFFECTS = frozenset(
    {
        SideEffect.READ,
        SideEffect.WRITE_STAGING,
        SideEffect.RUN_RESEARCH,
        SideEffect.START_SUBPROCESS,
    }
)
_RUNNER_CODE_PROFILES = frozenset(
    {
        ("runner-source", "source", "research.source_snapshot.v1"),
        ("runner-source", "directory", "research.directory_manifest.v1"),
    }
)
_DATASET_PROFILES = frozenset(
    {
        ("dataset-bars", "dataset", "research.market_data.v1"),
    }
)
_REQUIRED_FORBIDDEN_FEATURES = frozenset(
    {
        "return_pct",
        "exit_date",
        "exit_price",
        "hold_days",
        "entry_date_high",
        "entry_date_low",
        "entry_date_close",
        "t1_close",
    }
)
_ENTRY_OPEN_FEATURE_REFERENCES = {
    "overnight_gap_pct": ("signal_day_close",),
    "entry_open_to_yellow_pct": ("signal_day_yellow",),
    "entry_open_to_ma5_pct": ("signal_day_ma5",),
}
_FUTURE_DATA_TOKENS = (
    "return_pct",
    "exit_date",
    "exit_price",
    "hold_days",
    "entry_date_",
    "t1_",
    "t+1",
    "future",
)
_IMMATERIAL_PATHS = frozenset(
    {
        "/metadata/display_name",
        "/metadata/notes",
    }
)
_RESERVED_COMPILER_IDENTITIES = frozenset(
    {
        "research.protocol_compiler",
        "protocol-compiler",
        "compiler-generated",
    }
)
_SECRET_MARKERS = (
    "${",
    "api_key=",
    "apikey=",
    "password=",
    "secret=",
    "authorization:",
    "x-api-key",
    "bearer ",
)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?:api[_-]?key|private[_-]?key|client[_-]?secret|token|password|secret|authorization)"
    r"\s*[:=]",
    re.IGNORECASE,
)


def _is_compiler_identity(value: str) -> bool:
    normalized = value.casefold().replace("-", "_")
    return "compiler" in normalized or "protocol_compiler" in normalized


class ProtocolContractError(ValueError):
    """Base error for invalid protocol contracts."""


class ProtocolApprovalError(ProtocolContractError):
    """Raised when an external approval statement is absent or inconsistent."""


class MaterialProtocolChangeError(ProtocolContractError):
    """Raised when an ExecutionSpec contains unapproved material drift."""


def _canonical_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty canonical string")
    return value


def _canonical_identifier(value: str, field_name: str) -> str:
    _canonical_text(value, field_name)
    if re.fullmatch(r"[a-z][a-z0-9_]*", value) is None:
        raise ValueError(f"{field_name} must use lowercase underscore identifiers")
    return value


def _canonical_date(value: str, field_name: str) -> str:
    _canonical_text(value, field_name)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must be an ISO date") from error
    if parsed.isoformat() != value:
        raise ValueError(f"{field_name} must use canonical YYYY-MM-DD form")
    return value


def _unique_strings(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    if not values:
        raise ValueError(f"{field_name} must not be empty")
    if any(not isinstance(value, str) or not value or value != value.strip() for value in values):
        raise ValueError(f"{field_name} must contain canonical strings")
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must not contain duplicates")
    return values


def _sorted_unique_strings(
    values: tuple[str, ...],
    field_name: str,
) -> tuple[str, ...]:
    _unique_strings(values, field_name)
    if values != tuple(sorted(values)):
        raise ValueError(f"{field_name} must be sorted")
    return values


def _assert_no_credential_material(value: object) -> None:
    """Reject obvious credential carriers without logging their contents."""
    if isinstance(value, Mapping):
        for key, child in value.items():
            if isinstance(key, str) and _SECRET_ASSIGNMENT_RE.search(key):
                raise ValueError("protocol contains credential material")
            _assert_no_credential_material(child)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for child in value:
            _assert_no_credential_material(child)
        return
    if not isinstance(value, str):
        return
    lowered = value.casefold()
    if (
        lowered.startswith("sk-")
        or _SECRET_ASSIGNMENT_RE.search(value) is not None
        or any(marker in lowered for marker in _SECRET_MARKERS)
    ):
        raise ValueError("protocol contains credential material")


def _safe_validation_summary(error: ValidationError) -> str:
    parts: list[str] = []
    for item in error.errors(
        include_url=False,
        include_context=False,
        include_input=False,
    ):
        location = ".".join(str(part) for part in item.get("loc", ())) or "contract"
        message = str(item.get("msg", "validation error"))
        parts.append(f"{location}: {message}")
    return "; ".join(parts) or "contract validation failed"


def _date_range(dataset: "DatasetBinding") -> tuple[date, date]:
    return date.fromisoformat(dataset.window_start), date.fromisoformat(dataset.window_end)


def _contains_future_data_token(value: str) -> bool:
    lowered = value.casefold()
    return any(token in lowered for token in _FUTURE_DATA_TOKENS)


class ProtocolMetadata(StrictContractModel):
    schema_version: Literal["research.protocol_metadata.v1"]
    display_name: str
    notes: str

    _validate_display_name = field_validator("display_name")(
        lambda value: _canonical_text(value, "display_name")
    )

    @field_validator("notes")
    @classmethod
    def _validate_notes(cls, value: str) -> str:
        if not isinstance(value, str) or value != value.strip():
            raise ValueError("notes must be a trimmed string")
        lowered = value.casefold()
        if (
            _SECRET_ASSIGNMENT_RE.search(value) is not None
            or "bearer " in lowered
        ):
            raise ValueError("notes must not contain credential material")
        return value


class LabelDefinition(StrictContractModel):
    schema_version: Literal["research.label_definition.v1"]
    label_id: str
    entry_rule_id: str
    exit_rule_id: str
    horizon_trading_days: PositiveInt

    @field_validator("label_id", "entry_rule_id", "exit_rule_id")
    @classmethod
    def _validate_ids(cls, value: str, info: object) -> str:
        return _canonical_text(value, str(getattr(info, "field_name", "label field")))


class FeatureField(StrictContractModel):
    schema_version: Literal["research.feature_field.v1"]
    name: str
    availability: Literal["SIGNAL_DAY_CLOSE", "ENTRY_DATE_OPEN"]
    reference_fields: tuple[str, ...]

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _canonical_identifier(value, "feature name")

    @field_validator("reference_fields")
    @classmethod
    def _validate_references(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        _sorted_unique_strings(value, "feature reference_fields")
        return tuple(
            _canonical_identifier(item, "feature reference") for item in value
        )


class FeatureBoundary(StrictContractModel):
    schema_version: Literal["research.feature_boundary.v1"]
    boundary_id: str
    feature_fields: tuple[FeatureField, ...]
    forbidden_feature_names: tuple[str, ...]

    @field_validator("boundary_id")
    @classmethod
    def _validate_boundary_id(cls, value: str) -> str:
        return _canonical_text(value, "boundary_id")

    @field_validator("forbidden_feature_names")
    @classmethod
    def _validate_forbidden_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        _sorted_unique_strings(value, "forbidden_feature_names")
        for item in value:
            _canonical_identifier(item, "forbidden feature name")
        missing = _REQUIRED_FORBIDDEN_FEATURES - set(value)
        if missing:
            raise ValueError(
                "forbidden_feature_names must include the known future-data fields"
            )
        return value

    @model_validator(mode="after")
    def _validate_boundary(self) -> "FeatureBoundary":
        if not self.feature_fields:
            raise ValueError("feature_fields must not be empty")
        names = tuple(field.name for field in self.feature_fields)
        if names != tuple(sorted(names)):
            raise ValueError("feature_fields must be sorted by name")
        if len(names) != len(set(names)):
            raise ValueError("feature_fields must not contain duplicates")
        forbidden = set(self.forbidden_feature_names)
        for field in self.feature_fields:
            lowered = field.name.casefold()
            if field.name in forbidden or _contains_future_data_token(lowered):
                raise ValueError("feature crosses the future-data boundary")
            if (
                lowered in {"entry_open", "entry_date_open"}
                or lowered.startswith("entry_open_")
            ) and field.name not in _ENTRY_OPEN_FEATURE_REFERENCES:
                raise ValueError("raw entry-open feature is outside the explicit allowlist")
            expected = _ENTRY_OPEN_FEATURE_REFERENCES.get(field.name)
            if expected is not None and (
                field.availability != "ENTRY_DATE_OPEN"
                or field.reference_fields != expected
            ):
                raise ValueError("entry-open feature must use signal-day references")
            if field.availability == "ENTRY_DATE_OPEN" and expected is None:
                raise ValueError("entry-open feature is outside the explicit allowlist")
            for reference in field.reference_fields:
                reference_key = reference.casefold()
                if (
                    reference in forbidden
                    or _contains_future_data_token(reference_key)
                    or "entry_open" in reference_key
                ):
                    raise ValueError("feature uses a future-data reference")
                if (
                    field.availability == "SIGNAL_DAY_CLOSE"
                    and not reference_key.startswith("signal_day_")
                ):
                    raise ValueError("signal-day feature uses a non-signal-day reference")
        return self


class DatasetBinding(StrictContractModel):
    schema_version: Literal["research.dataset_binding.v1"]
    dataset_id: str
    role: Literal["TRAIN", "VALIDATION", "FOLD_TEST"]
    artifact_id: Sha256
    window_start: str
    window_end: str

    @field_validator("dataset_id")
    @classmethod
    def _validate_dataset_id(cls, value: str) -> str:
        return _canonical_text(value, "dataset_id")

    _validate_start = field_validator("window_start")(
        lambda value: _canonical_date(value, "window_start")
    )
    _validate_end = field_validator("window_end")(
        lambda value: _canonical_date(value, "window_end")
    )

    @model_validator(mode="after")
    def _validate_range(self) -> "DatasetBinding":
        if self.window_start > self.window_end:
            raise ValueError("dataset window_start must not be after window_end")
        return self


class FoldSpec(StrictContractModel):
    schema_version: Literal["research.fold_spec.v1"]
    fold_id: str
    train_dataset_id: str
    validation_dataset_id: str
    test_dataset_id: str
    purge_trading_days: NonNegativeInt
    embargo_trading_days: NonNegativeInt

    @field_validator(
        "fold_id",
        "train_dataset_id",
        "validation_dataset_id",
        "test_dataset_id",
    )
    @classmethod
    def _validate_ids(cls, value: str, info: object) -> str:
        return _canonical_text(value, str(getattr(info, "field_name", "fold field")))


class RunnerSpec(StrictContractModel):
    schema_version: Literal["research.runner_spec.v1"]
    runner_id: str
    entrypoint: str
    code_artifact_ids: tuple[Sha256, ...]
    argument_schema_sha256: Sha256
    compute_backend: Literal["CPU", "CUDA", "ROCM", "MPS"]
    backend_version: str

    @field_validator("runner_id", "entrypoint", "backend_version")
    @classmethod
    def _validate_text(cls, value: str, info: object) -> str:
        field_name = str(getattr(info, "field_name", "runner field"))
        _canonical_text(value, field_name)
        if field_name == "entrypoint" and re.fullmatch(
            r"[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*:[a-zA-Z_][a-zA-Z0-9_]*",
            value,
        ) is None:
            raise ValueError("entrypoint must be a safe module:function reference")
        return value

    @field_validator("code_artifact_ids")
    @classmethod
    def _validate_code_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique_strings(value, "code_artifact_ids")


class FoldSelection(StrictContractModel):
    schema_version: Literal["research.fold_selection.v1"]
    fold_id: str
    training_dataset_id: str
    threshold_source: Literal["PREREGISTERED", "VALIDATION_SELECTED"]
    threshold_dataset_ids: tuple[str, ...]
    threshold_value: float

    @field_validator("fold_id", "training_dataset_id")
    @classmethod
    def _validate_ids(cls, value: str, info: object) -> str:
        return _canonical_text(value, str(getattr(info, "field_name", "selection field")))

    @field_validator("threshold_dataset_ids")
    @classmethod
    def _validate_threshold_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value:
            return _sorted_unique_strings(value, "threshold_dataset_ids")
        return value

    @field_validator("threshold_value")
    @classmethod
    def _validate_threshold(cls, value: float) -> float:
        if not isinstance(value, float) or not math.isfinite(value):
            raise ValueError("threshold_value must be a finite float")
        return value

    @model_validator(mode="after")
    def _validate_threshold_source(self) -> "FoldSelection":
        if self.threshold_source == "PREREGISTERED" and self.threshold_dataset_ids:
            raise ValueError("PREREGISTERED threshold must not use selection datasets")
        if self.threshold_source == "VALIDATION_SELECTED" and not self.threshold_dataset_ids:
            raise ValueError("VALIDATION_SELECTED threshold requires a validation dataset")
        return self


class ModelThresholdSpec(StrictContractModel):
    schema_version: Literal["research.model_threshold_spec.v1"]
    model_mode: Literal["NONE", "TRAIN_NEW", "FROZEN"]
    model_family: str
    model_artifact_id: Sha256 | None
    hyperparameter_artifact_id: Sha256
    selection_by_fold: tuple[FoldSelection, ...]
    promotion_gate_id: str

    @field_validator("model_family", "promotion_gate_id")
    @classmethod
    def _validate_text(cls, value: str, info: object) -> str:
        return _canonical_text(value, str(getattr(info, "field_name", "model field")))

    @field_validator("selection_by_fold")
    @classmethod
    def _validate_selection_by_fold(
        cls,
        value: tuple[FoldSelection, ...],
    ) -> tuple[FoldSelection, ...]:
        if not value:
            raise ValueError("selection_by_fold must not be empty")
        fold_ids = tuple(item.fold_id for item in value)
        if fold_ids != tuple(sorted(fold_ids)):
            raise ValueError("selection_by_fold must be sorted by fold_id")
        if len(fold_ids) != len(set(fold_ids)):
            raise ValueError("selection_by_fold must not contain duplicate folds")
        return value

    @model_validator(mode="after")
    def _validate_model_mode(self) -> "ModelThresholdSpec":
        if self.model_mode == "FROZEN" and self.model_artifact_id is None:
            raise ValueError("FROZEN model mode requires a model artifact")
        if self.model_mode != "FROZEN" and self.model_artifact_id is not None:
            raise ValueError("only FROZEN model mode may bind a model artifact")
        if self.model_mode == "NONE" and self.model_family != "NONE":
            raise ValueError("NONE model mode requires model_family='NONE'")
        return self


class RosterMember(StrictContractModel):
    schema_version: Literal["research.roster_member.v1"]
    role: str
    provider_profile_id: str
    model_id: str
    public_identity_sha256: Sha256
    redacted: Literal[True]

    @field_validator("role", "provider_profile_id", "model_id")
    @classmethod
    def _validate_text(cls, value: str, info: object) -> str:
        return _canonical_text(value, str(getattr(info, "field_name", "roster field")))


class OutputContract(StrictContractModel):
    schema_version: Literal["research.output_contract.v1"]
    logical_role: str
    output_schema_id: str
    destination_class: Literal["STAGING_ONLY", "CONTROL_PLANE_METADATA"]

    @field_validator("logical_role", "output_schema_id")
    @classmethod
    def _validate_text(cls, value: str, info: object) -> str:
        return _canonical_text(value, str(getattr(info, "field_name", "output field")))


class ProtocolDefinition(StrictContractModel):
    schema_version: Literal["research.protocol_definition.v1"]
    protocol_id: str
    metadata: ProtocolMetadata
    generation_id: str
    generation_manifest_artifact_id: Sha256
    universe_id: str
    calendar_id: str
    adjustment_scheme_id: str
    validation_design: Literal["ROLLING_FORWARD"]
    fold_window_policy_id: str
    label: LabelDefinition
    datasets: tuple[DatasetBinding, ...]
    folds: tuple[FoldSpec, ...]
    feature_boundary: FeatureBoundary
    code_artifacts: tuple[ArtifactIdentity, ...]
    input_artifacts: tuple[ArtifactIdentity, ...]
    runner: RunnerSpec
    model: ModelThresholdSpec
    roster: tuple[RosterMember, ...]
    output_contracts: tuple[OutputContract, ...]
    allowed_side_effects: tuple[SideEffect, ...]

    @field_validator(
        "protocol_id",
        "generation_id",
        "universe_id",
        "calendar_id",
        "adjustment_scheme_id",
        "fold_window_policy_id",
    )
    @classmethod
    def _validate_text(cls, value: str, info: object) -> str:
        return _canonical_text(value, str(getattr(info, "field_name", "protocol field")))

    @field_validator("code_artifacts", "input_artifacts")
    @classmethod
    def _validate_artifact_order(
        cls,
        value: tuple[ArtifactIdentity, ...],
        info: object,
    ) -> tuple[ArtifactIdentity, ...]:
        if not value:
            raise ValueError(f"{getattr(info, 'field_name', 'artifacts')} must not be empty")
        ids = tuple(item.artifact_id for item in value)
        if ids != tuple(sorted(ids)):
            raise ValueError(f"{getattr(info, 'field_name', 'artifacts')} must be sorted by artifact_id")
        if len(ids) != len(set(ids)):
            raise ValueError(f"{getattr(info, 'field_name', 'artifacts')} must not contain duplicates")
        return value

    @field_validator("folds")
    @classmethod
    def _validate_folds_present(cls, value: tuple[FoldSpec, ...]) -> tuple[FoldSpec, ...]:
        if not value:
            raise ValueError("folds must not be empty")
        ids = tuple(item.fold_id for item in value)
        if ids != tuple(sorted(ids)):
            raise ValueError("folds must be sorted by fold_id")
        if len(ids) != len(set(ids)):
            raise ValueError("folds must not contain duplicates")
        return value

    @field_validator("datasets")
    @classmethod
    def _validate_datasets_present(
        cls,
        value: tuple[DatasetBinding, ...],
    ) -> tuple[DatasetBinding, ...]:
        if not value:
            raise ValueError("datasets must not be empty")
        ids = tuple(item.dataset_id for item in value)
        if ids != tuple(sorted(ids)):
            raise ValueError("datasets must be sorted by dataset_id")
        if len(ids) != len(set(ids)):
            raise ValueError("datasets must not contain duplicates")
        return value

    @field_validator("roster")
    @classmethod
    def _validate_roster_present(
        cls,
        value: tuple[RosterMember, ...],
    ) -> tuple[RosterMember, ...]:
        if not value:
            raise ValueError("roster must not be empty")
        roles = tuple(item.role for item in value)
        if roles != tuple(sorted(roles)):
            raise ValueError("roster must be sorted by role")
        if len(roles) != len(set(roles)):
            raise ValueError("roster roles must not be duplicated")
        return value

    @field_validator("output_contracts")
    @classmethod
    def _validate_outputs(
        cls,
        value: tuple[OutputContract, ...],
    ) -> tuple[OutputContract, ...]:
        if not value:
            raise ValueError("output_contracts must not be empty")
        roles = tuple(item.logical_role for item in value)
        if roles != tuple(sorted(roles)):
            raise ValueError("output_contracts must be sorted by logical_role")
        if len(roles) != len(set(roles)):
            raise ValueError("output logical roles must not be duplicated")
        return value

    @field_validator("allowed_side_effects")
    @classmethod
    def _validate_effects(cls, value: tuple[SideEffect, ...]) -> tuple[SideEffect, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("allowed_side_effects must be unique and non-empty")
        if value != tuple(sorted(value, key=lambda item: item.value)):
            raise ValueError("allowed_side_effects must be sorted")
        if not set(value).issubset(_SAFE_SIDE_EFFECTS):
            raise ValueError("protocol contains an unsafe side effect")
        return value

    @model_validator(mode="after")
    def _validate_bindings(self) -> "ProtocolDefinition":
        _assert_no_credential_material(self.model_dump(mode="json"))
        input_by_id = {item.artifact_id: item for item in self.input_artifacts}
        code_by_id = {item.artifact_id: item for item in self.code_artifacts}
        generation_manifest = input_by_id.get(self.generation_manifest_artifact_id)
        if generation_manifest is None:
            raise ValueError("generation manifest must be a full input artifact identity")
        if generation_manifest.logical_role != "generation-manifest":
            raise ValueError("generation manifest artifact has the wrong logical role")
        if (
            generation_manifest.content_schema != "research.generation_manifest.v1"
            or generation_manifest.kind != "manifest"
        ):
            raise ValueError("generation manifest artifact has the wrong content binding")
        if generation_manifest.generation != self.generation_id:
            raise ValueError("generation manifest does not bind generation_id")

        if set(self.runner.code_artifact_ids) != set(code_by_id):
            raise ValueError("runner code identity must equal the declared code artifacts")
        for code in code_by_id.values():
            profile = (code.logical_role, code.kind, code.content_schema)
            if profile not in _RUNNER_CODE_PROFILES:
                raise ValueError("code artifact has the wrong semantic binding")
        datasets = {item.dataset_id: item for item in self.datasets}
        if len(datasets) != len(self.datasets):
            raise ValueError("dataset ids must be unique")
        for dataset in self.datasets:
            artifact = input_by_id.get(dataset.artifact_id)
            if artifact is None:
                raise ValueError("dataset references an undeclared artifact")
            if artifact.generation != self.generation_id:
                raise ValueError("dataset crosses generation boundary")
            profile = (artifact.logical_role, artifact.kind, artifact.content_schema)
            if profile not in _DATASET_PROFILES:
                raise ValueError("dataset artifact has the wrong semantic binding")

        previous_ranges: tuple[date, date, date, date, date, date] | None = None
        for fold in self.folds:
            try:
                train = datasets[fold.train_dataset_id]
                validation = datasets[fold.validation_dataset_id]
                test = datasets[fold.test_dataset_id]
            except KeyError as error:
                raise ValueError("fold references an unknown dataset") from error
            if (train.role, validation.role, test.role) != (
                "TRAIN",
                "VALIDATION",
                "FOLD_TEST",
            ):
                raise ValueError("each fold must bind TRAIN, VALIDATION, and FOLD_TEST exactly once")
            train_start, train_end = _date_range(train)
            validation_start, validation_end = _date_range(validation)
            test_start, test_end = _date_range(test)
            if not (train_end < validation_start and validation_end < test_start):
                raise ValueError("fold windows must be chronological and disjoint")
            current_ranges = (
                train_start,
                train_end,
                validation_start,
                validation_end,
                test_start,
                test_end,
            )
            if previous_ranges is not None:
                if any(
                    current <= previous
                    for current, previous in zip(current_ranges, previous_ranges)
                ):
                    raise ValueError(
                        "rolling-forward fold windows must all move forward"
                    )
                if test_start <= previous_ranges[-1]:
                    raise ValueError("unseen test windows must not overlap across folds")
            previous_ranges = current_ranges
            if fold.purge_trading_days < 0 or fold.embargo_trading_days < 0:
                raise ValueError("purge and embargo must be non-negative")

        fold_by_id = {item.fold_id: item for item in self.folds}
        selection_by_fold = {item.fold_id: item for item in self.model.selection_by_fold}
        if set(selection_by_fold) != set(fold_by_id):
            raise ValueError("threshold/model selection must bind every fold exactly once")
        for fold_id, selection in selection_by_fold.items():
            fold = fold_by_id[fold_id]
            if selection.training_dataset_id != fold.train_dataset_id:
                raise ValueError("model selection must use the fold's TRAIN dataset")
            if selection.threshold_source == "VALIDATION_SELECTED":
                if any(
                    item not in datasets or item != fold.validation_dataset_id
                    for item in selection.threshold_dataset_ids
                ):
                    raise ValueError(
                        "threshold selection cannot use another fold or fold-test data"
                    )
        hyperparameter = input_by_id.get(self.model.hyperparameter_artifact_id)
        if hyperparameter is None or hyperparameter.logical_role != "model-hyperparameters":
            raise ValueError("hyperparameter artifact identity is missing or has the wrong role")
        if (
            hyperparameter.content_schema != "research.hyperparameters.v1"
            or hyperparameter.kind != "model-config"
        ):
            raise ValueError("hyperparameter artifact has the wrong content binding")
        if self.model.model_artifact_id is not None and self.model.model_artifact_id not in input_by_id:
            raise ValueError("frozen model artifact must be declared as an input")
        if self.model.model_artifact_id is not None:
            model_artifact = input_by_id[self.model.model_artifact_id]
            if model_artifact.logical_role != "model-artifact":
                raise ValueError("frozen model artifact has the wrong logical role")
            if (
                model_artifact.content_schema != "research.model_artifact.v1"
                or model_artifact.kind != "model"
            ):
                raise ValueError("frozen model artifact has the wrong content binding")
        return self


class ProtocolApprovalStatement(StrictContractModel):
    schema_version: Literal["research.protocol_approval_statement.v1"]
    approved_protocol_sha256: Sha256
    decision: Literal["APPROVED"]
    approver: Actor

    @model_validator(mode="after")
    def _validate_provenance(self) -> "ProtocolApprovalStatement":
        _assert_no_credential_material(self.model_dump(mode="json"))
        if (
            self.approver.actor_id in _RESERVED_COMPILER_IDENTITIES
            or _is_compiler_identity(self.approver.actor_id)
        ):
            raise ValueError("protocol compiler cannot be the approver")
        return self


class ProtocolApproval(StrictContractModel):
    """A byte-bound external approval statement, not an authority grant."""

    schema_version: Literal["research.protocol_approval.v1"]
    statement: ProtocolApprovalStatement
    approval_evidence: ArtifactIdentity
    evidence_trust: Literal["UNVERIFIED_EXTERNAL_STATEMENT"]

    @model_validator(mode="after")
    def _validate_provenance(self) -> "ProtocolApproval":
        _assert_no_credential_material(self.model_dump(mode="json"))
        if self.approval_evidence.logical_role != "protocol-approval":
            raise ValueError("approval evidence must be a protocol-approval artifact")
        if (
            self.approval_evidence.content_schema != PROTOCOL_APPROVAL_STATEMENT_V1
            or self.approval_evidence.kind != "review"
        ):
            raise ValueError("approval evidence has the wrong content binding")
        statement_bytes = canonical_json(
            self.statement.model_dump(mode="json", warnings=False)
        ).encode("utf-8")
        if (
            self.approval_evidence.content_sha256
            != hashlib.sha256(statement_bytes).hexdigest()
            or self.approval_evidence.byte_length != len(statement_bytes)
        ):
            raise ValueError("approval evidence must bind the canonical statement bytes")
        if self.approval_evidence.producer != self.statement.approver.actor_id:
            raise ValueError("approval evidence producer must match the statement approver")
        if (
            self.approval_evidence.producer in _RESERVED_COMPILER_IDENTITIES
            or _is_compiler_identity(self.approval_evidence.producer)
        ):
            raise ValueError("compiler-generated evidence cannot approve a protocol")
        return self

    @property
    def approved_protocol_sha256(self) -> str:
        return self.statement.approved_protocol_sha256

    @property
    def approver(self) -> Actor:
        return self.statement.approver


class ProtocolAmendmentStatement(StrictContractModel):
    schema_version: Literal["research.protocol_amendment_statement.v1"]
    base_protocol_sha256: Sha256
    executed_protocol_sha256: Sha256
    changed_paths: tuple[str, ...]
    decision: Literal["APPROVED"]
    approver: Actor

    @field_validator("changed_paths")
    @classmethod
    def _validate_paths(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique_strings(value, "changed_paths")

    @model_validator(mode="after")
    def _validate_provenance(self) -> "ProtocolAmendmentStatement":
        _assert_no_credential_material(self.model_dump(mode="json"))
        if (
            self.approver.actor_id in _RESERVED_COMPILER_IDENTITIES
            or _is_compiler_identity(self.approver.actor_id)
        ):
            raise ValueError("protocol compiler cannot approve an amendment")
        return self


class ProtocolAmendment(StrictContractModel):
    """A byte-bound external amendment statement, not an authority grant."""

    schema_version: Literal["research.protocol_amendment.v1"]
    statement: ProtocolAmendmentStatement
    amendment_evidence: ArtifactIdentity
    evidence_trust: Literal["UNVERIFIED_EXTERNAL_STATEMENT"]

    @model_validator(mode="after")
    def _validate_provenance(self) -> "ProtocolAmendment":
        _assert_no_credential_material(self.model_dump(mode="json"))
        if self.amendment_evidence.logical_role != "protocol-amendment":
            raise ValueError("amendment evidence must be a protocol-amendment artifact")
        if (
            self.amendment_evidence.content_schema != PROTOCOL_AMENDMENT_STATEMENT_V1
            or self.amendment_evidence.kind != "review"
        ):
            raise ValueError("amendment evidence has the wrong content binding")
        statement_bytes = canonical_json(
            self.statement.model_dump(mode="json", warnings=False)
        ).encode("utf-8")
        if (
            self.amendment_evidence.content_sha256
            != hashlib.sha256(statement_bytes).hexdigest()
            or self.amendment_evidence.byte_length != len(statement_bytes)
        ):
            raise ValueError("amendment evidence must bind the canonical statement bytes")
        if self.amendment_evidence.producer != self.statement.approver.actor_id:
            raise ValueError("amendment evidence producer must match the statement approver")
        if (
            self.amendment_evidence.producer in _RESERVED_COMPILER_IDENTITIES
            or _is_compiler_identity(self.amendment_evidence.producer)
        ):
            raise ValueError("compiler-generated evidence cannot approve an amendment")
        return self

    @property
    def base_protocol_sha256(self) -> str:
        return self.statement.base_protocol_sha256

    @property
    def executed_protocol_sha256(self) -> str:
        return self.statement.executed_protocol_sha256

    @property
    def changed_paths(self) -> tuple[str, ...]:
        return self.statement.changed_paths

    @property
    def approver(self) -> Actor:
        return self.statement.approver


class ProtocolDifference(StrictContractModel):
    schema_version: Literal["research.protocol_difference.v1"]
    path: str
    classification: Literal["IMMATERIAL", "MATERIAL"]
    approved_value_sha256: Sha256 | None
    executed_value_sha256: Sha256 | None

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        if not value.startswith("/") or value == "/":
            raise ValueError("protocol difference path must be a JSON pointer")
        return value


class ExecutionSpec(StrictContractModel):
    schema_version: Literal["research.execution_spec.v1"]
    approved_protocol: ProtocolDefinition | None
    approved_protocol_sha256: Sha256 | None
    executed_protocol_sha256: Sha256
    approval: ProtocolApproval | None
    amendment: ProtocolAmendment | None
    approval_evidence_artifact_id: Sha256 | None
    amendment_evidence_artifact_id: Sha256 | None
    protocol: ProtocolDefinition
    differences: tuple[ProtocolDifference, ...]
    conformance: Literal[
        "IDENTICAL",
        "IMMATERIAL_ALLOWLISTED",
        "APPROVED_AMENDMENT",
        "MATERIAL_UNAPPROVED",
    ]

    @model_validator(mode="after")
    def _validate_identity_and_conformance(self) -> "ExecutionSpec":
        executed_protocol = _validated_protocol(self.protocol)
        if executed_protocol != self.protocol:
            raise ValueError("execution spec protocol is not strictly canonical")
        if self.executed_protocol_sha256 != protocol_sha256(self.protocol):
            raise ValueError("executed protocol hash does not match the frozen protocol")
        paths = tuple(item.path for item in self.differences)
        if paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise ValueError("protocol differences must be sorted and unique")
        material = [item for item in self.differences if item.classification == "MATERIAL"]
        immaterial = [item for item in self.differences if item.classification == "IMMATERIAL"]
        if self.approved_protocol is None:
            if any(
                value is not None
                for value in (
                    self.approved_protocol_sha256,
                    self.approval,
                    self.amendment,
                    self.approval_evidence_artifact_id,
                    self.amendment_evidence_artifact_id,
                )
            ) or self.differences:
                raise ValueError("unapproved protocol cannot carry approval or diff evidence")
            if self.conformance != MATERIAL_UNAPPROVED:
                raise ValueError("a protocol without an approval baseline is unapproved")
            return self

        approved_protocol = _validated_protocol(self.approved_protocol)
        if approved_protocol != self.approved_protocol:
            raise ValueError("approved protocol is not strictly canonical")
        approved_hash = protocol_sha256(approved_protocol)
        if self.approved_protocol_sha256 != approved_hash:
            raise ValueError("approved protocol hash does not match the frozen protocol")
        if self.approval is None:
            raise ValueError("approved protocol requires an approval binding")
        approval = _validated_approval(self.approval)
        if approval.approved_protocol_sha256 != approved_hash:
            raise ValueError("approval binding does not match the approved protocol")
        if self.approval_evidence_artifact_id != approval.approval_evidence.artifact_id:
            raise ValueError("approval evidence identity does not match the approval binding")

        expected_differences = diff_protocols(approved_protocol, self.protocol)
        if self.differences != expected_differences:
            raise ValueError("execution spec differences do not match the protocol diff")
        if not expected_differences:
            expected_conformance = IDENTICAL
        elif not material:
            expected_conformance = IMMATERIAL_ALLOWLISTED
        elif self.amendment is not None:
            amendment = _validated_amendment(self.amendment)
            expected_material_paths = tuple(
                item.path for item in expected_differences if item.classification == "MATERIAL"
            )
            if (
                amendment.base_protocol_sha256 != approved_hash
                or amendment.executed_protocol_sha256 != self.executed_protocol_sha256
                or amendment.changed_paths != expected_material_paths
            ):
                raise ValueError("amendment binding does not match the protocol diff")
            if self.amendment_evidence_artifact_id != amendment.amendment_evidence.artifact_id:
                raise ValueError("amendment evidence identity does not match the amendment binding")
            expected_conformance = APPROVED_AMENDMENT
        else:
            expected_conformance = MATERIAL_UNAPPROVED
        if self.conformance != expected_conformance:
            raise ValueError("execution spec conformance does not match its evidence")
        if self.conformance != APPROVED_AMENDMENT and self.amendment is not None:
            raise ValueError("only an approved material amendment may be attached")
        if self.conformance != APPROVED_AMENDMENT and self.amendment_evidence_artifact_id is not None:
            raise ValueError("amendment evidence cannot exist without an approved amendment")
        if self.conformance == IDENTICAL and self.approved_protocol_sha256 != self.executed_protocol_sha256:
            raise ValueError("IDENTICAL protocol hashes must match")
        if self.conformance != IDENTICAL and self.approved_protocol_sha256 == self.executed_protocol_sha256:
            raise ValueError("changed protocol hashes must differ")
        return self

    @property
    def execution_spec_id(self) -> str:
        validated = _validated_execution_spec(self)
        return canonical_sha256(
            {
                "domain": EXECUTION_SPEC_V1,
                "payload": validated.model_dump(mode="json"),
            }
        )

    @property
    def protocol_conformant(self) -> bool:
        """Return semantic conformance only; never execution authorization."""
        return _validated_execution_spec(self).conformance != MATERIAL_UNAPPROVED


def protocol_sha256(protocol: ProtocolDefinition) -> str:
    """Hash protocol semantics without a self-referential field."""
    protocol = _validated_protocol(protocol)
    return canonical_sha256(
        {
            "domain": PROTOCOL_DEFINITION_V1,
            "payload": protocol.model_dump(mode="json"),
        }
    )


def _validated_protocol(protocol: ProtocolDefinition) -> ProtocolDefinition:
    """Revalidate typed inputs so model_copy/model_construct cannot bypass gates."""
    if not isinstance(protocol, ProtocolDefinition):
        raise TypeError("protocol must be a ProtocolDefinition")
    try:
        return ProtocolDefinition.model_validate(
            protocol.model_dump(mode="python", warnings=False),
            strict=True,
        )
    except ValidationError as error:
        raise ProtocolContractError(
            f"protocol failed strict revalidation: {_safe_validation_summary(error)}"
        ) from None


def _validated_approval(approval: ProtocolApproval) -> ProtocolApproval:
    if not isinstance(approval, ProtocolApproval):
        raise TypeError("approval must be a ProtocolApproval")
    try:
        return ProtocolApproval.model_validate_json(
            canonical_json(
                approval.model_dump(mode="json", warnings=False)
            ).encode("utf-8"),
            strict=True,
        )
    except ValidationError as error:
        raise ProtocolApprovalError(
            f"approval failed strict revalidation: {_safe_validation_summary(error)}"
        ) from None


def _validated_amendment(amendment: ProtocolAmendment) -> ProtocolAmendment:
    if not isinstance(amendment, ProtocolAmendment):
        raise TypeError("amendment must be a ProtocolAmendment")
    try:
        return ProtocolAmendment.model_validate_json(
            canonical_json(
                amendment.model_dump(mode="json", warnings=False)
            ).encode("utf-8"),
            strict=True,
        )
    except ValidationError as error:
        raise ProtocolApprovalError(
            f"amendment failed strict revalidation: {_safe_validation_summary(error)}"
        ) from None


def _validated_execution_spec(spec: ExecutionSpec) -> ExecutionSpec:
    if not isinstance(spec, ExecutionSpec):
        raise TypeError("spec must be an ExecutionSpec")
    try:
        return ExecutionSpec.model_validate_json(
            canonical_json(spec.model_dump(mode="json", warnings=False)).encode("utf-8"),
            strict=True,
        )
    except ValidationError as error:
        raise ProtocolContractError(
            f"execution spec failed strict revalidation: {_safe_validation_summary(error)}"
        ) from None


def _value_sha256(value: object, *, present: bool) -> str:
    return canonical_sha256(
        {
            "domain": "research.protocol_difference_value.v1",
            "present": present,
            "value": value,
        }
    )


def _escape_pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _flatten(value: object, path: str = "") -> dict[str, object]:
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key in sorted(value):
            child = f"{path}/{_escape_pointer(str(key))}"
            result.update(_flatten(value[key], child))
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        result = {}
        for index, item in enumerate(value):
            result.update(_flatten(item, f"{path}/{index}"))
        return result
    return {path or "/": value}


def diff_protocols(
    approved: ProtocolDefinition,
    executed: ProtocolDefinition,
) -> tuple[ProtocolDifference, ...]:
    approved = _validated_protocol(approved)
    executed = _validated_protocol(executed)
    approved_flat = _flatten(approved.model_dump(mode="json"))
    executed_flat = _flatten(executed.model_dump(mode="json"))
    differences: list[ProtocolDifference] = []
    for path in sorted(set(approved_flat) | set(executed_flat)):
        approved_present = path in approved_flat
        executed_present = path in executed_flat
        if approved_present and executed_present and approved_flat[path] == executed_flat[path]:
            continue
        classification = "IMMATERIAL" if path in _IMMATERIAL_PATHS else "MATERIAL"
        differences.append(
            ProtocolDifference(
                schema_version="research.protocol_difference.v1",
                path=path,
                classification=classification,
                approved_value_sha256=(
                    _value_sha256(approved_flat[path], present=True)
                    if approved_present
                    else None
                ),
                executed_value_sha256=(
                    _value_sha256(executed_flat[path], present=True)
                    if executed_present
                    else None
                ),
            )
        )
    return tuple(differences)


def classify_conformance(
    executed: ProtocolDefinition,
    *,
    approved: ProtocolDefinition | None,
    approval: ProtocolApproval | None,
    amendment: ProtocolAmendment | None,
) -> str:
    return compile_execution_spec(
        executed,
        approved_protocol=approved,
        approval=approval,
        amendment=amendment,
    ).conformance


def compile_execution_spec(
    executed: ProtocolDefinition,
    *,
    approved_protocol: ProtocolDefinition | None,
    approval: ProtocolApproval | None,
    amendment: ProtocolAmendment | None,
) -> ExecutionSpec:
    """Compile a frozen, dry-only ExecutionSpec and classify conformance.

    Approval statements are byte-bound external claims.  Their authority,
    revocation status, and ability to authorize execution are intentionally
    left to later trusted control-plane phases.
    """
    executed = _validated_protocol(executed)
    if approved_protocol is not None:
        approved_protocol = _validated_protocol(approved_protocol)
    if approved_protocol is None:
        if approval is not None or amendment is not None:
            raise ProtocolApprovalError(
                "approval or amendment cannot exist without an approved protocol"
            )
        return ExecutionSpec(
            schema_version=EXECUTION_SPEC_V1,
            approved_protocol=None,
            approved_protocol_sha256=None,
            executed_protocol_sha256=protocol_sha256(executed),
            approval=None,
            amendment=None,
            approval_evidence_artifact_id=None,
            amendment_evidence_artifact_id=None,
            protocol=executed,
            differences=(),
            conformance=MATERIAL_UNAPPROVED,
        )
    if not isinstance(approval, ProtocolApproval):
        raise ProtocolApprovalError(
            "an approved protocol requires an external byte-bound ProtocolApproval"
        )
    approval = _validated_approval(approval)
    if amendment is not None:
        amendment = _validated_amendment(amendment)
    approved_hash = protocol_sha256(approved_protocol)
    if approval.approved_protocol_sha256 != approved_hash:
        raise ProtocolApprovalError("approval does not bind the approved protocol hash")
    executed_hash = protocol_sha256(executed)
    differences = diff_protocols(approved_protocol, executed)
    material_paths = tuple(
        item.path for item in differences if item.classification == "MATERIAL"
    )
    if not differences:
        conformance = IDENTICAL
        if amendment is not None:
            raise ProtocolApprovalError("an identical protocol cannot carry an amendment")
    elif not material_paths:
        conformance = IMMATERIAL_ALLOWLISTED
        if amendment is not None:
            raise ProtocolApprovalError("metadata-only changes cannot carry an amendment")
    elif amendment is None:
        conformance = MATERIAL_UNAPPROVED
    else:
        if (
            amendment.base_protocol_sha256 != approved_hash
            or amendment.executed_protocol_sha256 != executed_hash
            or amendment.changed_paths != material_paths
        ):
            raise ProtocolApprovalError(
                "amendment does not bind the exact material protocol diff"
            )
        conformance = APPROVED_AMENDMENT
    return ExecutionSpec(
        schema_version=EXECUTION_SPEC_V1,
        approved_protocol=approved_protocol,
        approved_protocol_sha256=approved_hash,
        executed_protocol_sha256=executed_hash,
        approval=approval,
        amendment=amendment,
        approval_evidence_artifact_id=approval.approval_evidence.artifact_id,
        amendment_evidence_artifact_id=(
            None if amendment is None else amendment.amendment_evidence.artifact_id
        ),
        protocol=executed,
        differences=differences,
        conformance=conformance,
    )


def require_protocol_conformant(spec: ExecutionSpec) -> None:
    """Reject unapproved material drift without granting any execution right.

    A successful return does not create a ticket, lease, data-access grant,
    side-effect authorization, Campaign permission, or Holdout permission.
    """
    if not isinstance(spec, ExecutionSpec):
        raise TypeError("spec must be an ExecutionSpec")
    validated = _validated_execution_spec(spec)
    if validated.conformance == MATERIAL_UNAPPROVED:
        raise MaterialProtocolChangeError(
            "material protocol change is unapproved and does not conform"
        )


def protocol_registry() -> ContractRegistry:
    """Return the single versioned registry for protocol contracts."""
    return ContractRegistry(
        version="research.protocol_registry.v1",
        contracts={
            PROTOCOL_DEFINITION_V1: ProtocolDefinition,
            PROTOCOL_APPROVAL_STATEMENT_V1: ProtocolApprovalStatement,
            PROTOCOL_APPROVAL_V1: ProtocolApproval,
            PROTOCOL_AMENDMENT_STATEMENT_V1: ProtocolAmendmentStatement,
            PROTOCOL_AMENDMENT_V1: ProtocolAmendment,
            EXECUTION_SPEC_V1: ExecutionSpec,
        },
    )


__all__ = [
    "APPROVED_AMENDMENT",
    "CONFORMANCE_VALUES",
    "EXECUTION_SPEC_V1",
    "IDENTICAL",
    "IMMATERIAL_ALLOWLISTED",
    "MATERIAL_UNAPPROVED",
    "PROTOCOL_AMENDMENT_V1",
    "PROTOCOL_AMENDMENT_STATEMENT_V1",
    "PROTOCOL_APPROVAL_V1",
    "PROTOCOL_APPROVAL_STATEMENT_V1",
    "PROTOCOL_DEFINITION_V1",
    "DatasetBinding",
    "ExecutionSpec",
    "FeatureBoundary",
    "FeatureField",
    "FoldSelection",
    "FoldSpec",
    "LabelDefinition",
    "MaterialProtocolChangeError",
    "ModelThresholdSpec",
    "OutputContract",
    "ProtocolAmendment",
    "ProtocolAmendmentStatement",
    "ProtocolApproval",
    "ProtocolApprovalStatement",
    "ProtocolContractError",
    "ProtocolDefinition",
    "ProtocolDifference",
    "ProtocolMetadata",
    "ProtocolApprovalError",
    "RosterMember",
    "RunnerSpec",
    "classify_conformance",
    "compile_execution_spec",
    "diff_protocols",
    "protocol_registry",
    "protocol_sha256",
    "require_protocol_conformant",
]
