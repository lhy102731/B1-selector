"""Explicit read-only adapters for frozen P0 and historical KBase contracts."""

from __future__ import annotations

from functools import lru_cache
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
from typing import Annotated, Literal

from pydantic import Field, StringConstraints, ValidationError

from research_automation.control_plane.artifact_semantics import (
    ArtifactSemanticError,
    parse_strict_json,
)
from research_automation.control_plane.task_reports import (
    TaskReportValidationError,
    parse_task_report_v2_bytes,
)

from .contract_registry import StrictContractModel


LegacySha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
LegacyNonNegativeInt = Annotated[int, Field(ge=0)]
_MAX_LEGACY_CONTRACT_BYTES = 128 * 1024
_KBASE_SCHEMA_MODULE = (
    Path(__file__).resolve().parents[2] / "ag2_research" / "kbase" / "schemas.py"
)


class LegacyContractAdapterError(ValueError):
    """Raised when a legacy document cannot form a supported typed view."""


class LegacyKBaseArtifactReference(StrictContractModel):
    """Preserved historical identifier that cannot authorize trusted execution."""

    schema_version: Literal["research.legacy_kbase_artifact_reference.v1"]
    profile: Literal["kbase.content_fingerprint.v1"]
    legacy_artifact_id: LegacySha256
    authorization_eligible: Literal[False]


class LegacyKBasePath(StrictContractModel):
    role: str
    path: str


class LegacyKBaseCatalogEntryView(StrictContractModel):
    """Deeply immutable view of the supported historical KBase catalog v1."""

    schema_version: Literal["research.legacy_kbase_catalog_entry_view.v1"]
    catalog_schema_version: Literal[1]
    source_id: str
    object_type: Literal[
        "source_packet",
        "family",
        "source_note",
        "book",
        "video",
        "event_card",
        "map",
    ]
    title: str
    aliases: tuple[str, ...]
    people: tuple[str, ...]
    family_id: str | None
    voice_role: str
    source_type: str
    date_start: str | None
    date_end: str | None
    topics: tuple[str, ...]
    summary: str
    reliability: Literal["low", "medium", "high", "unverified"]
    review_status: Literal["source_only", "review_required", "reviewed", "blocked"]
    available_layers: tuple[
        Literal["summary", "statements", "evidence", "raw", "visual"], ...
    ]
    warnings: tuple[str, ...]
    parent_ids: tuple[str, ...]
    paths: tuple[LegacyKBasePath, ...]
    content_fingerprint: LegacySha256
    source_schema_version: int | str | None


class LegacyP0IdentityBinding(StrictContractModel):
    plan_hash: LegacySha256
    scope_hash: LegacySha256
    instruction_policy_hash: LegacySha256


class LegacyP0TaskRequirements(StrictContractModel):
    required_test_receipt_ids: tuple[str, ...]
    required_review_receipt_ids: tuple[str, ...]
    required_evidence_ids: tuple[str, ...]


class LegacyP0TestReceipt(StrictContractModel):
    receipt_id: str
    command: str
    exit_code: int
    result: Literal["PASS", "FAIL"]


class LegacyP0ReviewReceipt(StrictContractModel):
    receipt_id: str
    reviewer_id: str
    exit_code: int
    result: Literal["PASS", "FAIL"]
    findings_sha256: LegacySha256


class LegacyP0EvidenceReference(StrictContractModel):
    evidence_id: str
    evidence_ref: str
    evidence_sha256: LegacySha256
    status: Literal["VERIFIED", "INVALID", "IN_DOUBT"]


class LegacyP0ReviewFinding(StrictContractModel):
    finding_id: str
    review_receipt_id: str
    severity: Literal["BLOCKING", "NON_BLOCKING"]
    status: Literal["OPEN", "RESOLVED"]
    summary: str
    resolution: str | None


class LegacyP0ChangedFile(StrictContractModel):
    path: str
    change_type: Literal["ADD", "MODIFY", "DELETE"]
    baseline_sha256: LegacySha256 | None
    current_sha256: LegacySha256 | None


class LegacyP0InvocationUsage(StrictContractModel):
    status: Literal["REPORTED", "ESTIMATED", "UNKNOWN"]
    input_tokens: LegacyNonNegativeInt | None
    output_tokens: LegacyNonNegativeInt | None
    total_tokens: LegacyNonNegativeInt | None


class LegacyP0ExternalInvocation(StrictContractModel):
    invocation_id: str
    invocation_ref: str
    invocation_sha256: LegacySha256
    usage: LegacyP0InvocationUsage


class LegacyP0SideEffectSummary(StrictContractModel):
    observed: tuple[str, ...]
    unauthorized: tuple[str, ...]


class LegacyP0TaskReportV2View(StrictContractModel):
    """Typed read-only mirror layered behind the sealed TaskReport validator."""

    schema_version: Literal["control_plane.task_report.v2"]
    plan_version: str
    phase: Literal["P0", "P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8"]
    task_id: str
    attempt_id: str
    authorization_ref: str
    ticket_id: str
    identity_binding: LegacyP0IdentityBinding
    objective: str
    dependencies: tuple[str, ...]
    idempotency_key: str
    task_spec_ref: str
    task_spec_sha256: LegacySha256
    requirements: LegacyP0TaskRequirements
    allowed_files: tuple[str, ...]
    forbidden_files: tuple[str, ...]
    baseline_ref: str
    baseline_sha256: LegacySha256
    input_evidence_refs: tuple[LegacyP0EvidenceReference, ...]
    test_receipts: tuple[LegacyP0TestReceipt, ...]
    review_receipts: tuple[LegacyP0ReviewReceipt, ...]
    review_findings: tuple[LegacyP0ReviewFinding, ...]
    changed_files: tuple[LegacyP0ChangedFile, ...]
    unexpected_changes: tuple[str, ...]
    external_invocations: tuple[LegacyP0ExternalInvocation, ...]
    side_effect_summary: LegacyP0SideEffectSummary
    ticket_state: Literal["SUCCEEDED", "FAILED", "IN_DOUBT"]
    outcome: Literal["PASS", "FAIL", "BLOCKED", "IN_DOUBT"]
    reason_codes: tuple[str, ...]
    started_at: str
    completed_at: str
    report_payload_sha256: LegacySha256


@lru_cache(maxsize=1)
def _load_legacy_kbase_schema_module() -> ModuleType:
    """Load the fixed legacy validator without executing ag2_research.__init__."""
    spec = spec_from_file_location(
        "_p1_legacy_kbase_schemas",
        _KBASE_SCHEMA_MODULE,
    )
    if spec is None or spec.loader is None:
        raise LegacyContractAdapterError("legacy KBase validator is unavailable")
    try:
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
    except (ImportError, OSError) as error:
        raise LegacyContractAdapterError(
            "legacy KBase validator is unavailable"
        ) from error
    return module


def _validate_legacy_kbase_catalog_payload(payload: dict[str, object]) -> None:
    module = _load_legacy_kbase_schema_module()
    try:
        module.validate_catalog_entry(payload)
    except module.ContractValidationError as error:
        raise LegacyContractAdapterError(str(error)) from error


def preserve_legacy_kbase_artifact_reference(
    content_fingerprint: str,
) -> LegacyKBaseArtifactReference:
    """Wrap an existing KBase hash without rewriting or re-hashing it."""
    return LegacyKBaseArtifactReference(
        schema_version="research.legacy_kbase_artifact_reference.v1",
        profile="kbase.content_fingerprint.v1",
        legacy_artifact_id=content_fingerprint,
        authorization_eligible=False,
    )


def read_legacy_kbase_catalog_entry(raw: bytes) -> LegacyKBaseCatalogEntryView:
    """Validate with the legacy authority, then expose only supported schema v1."""
    if not isinstance(raw, bytes):
        raise LegacyContractAdapterError("legacy KBase catalog input must be bytes")
    if len(raw) > _MAX_LEGACY_CONTRACT_BYTES:
        raise LegacyContractAdapterError("legacy KBase catalog input exceeds its byte limit")
    try:
        payload = parse_strict_json(raw, artifact_name="legacy_kbase_catalog_entry")
        _validate_legacy_kbase_catalog_payload(payload)
    except ArtifactSemanticError as error:
        raise LegacyContractAdapterError(str(error)) from error
    if payload.get("catalog_schema_version") != 1:
        raise LegacyContractAdapterError("unsupported legacy KBase catalog schema version")
    paths = payload["paths"]
    if not isinstance(paths, dict):
        raise LegacyContractAdapterError("legacy KBase catalog paths are invalid")
    return LegacyKBaseCatalogEntryView(
        schema_version="research.legacy_kbase_catalog_entry_view.v1",
        catalog_schema_version=1,
        source_id=payload["source_id"],
        object_type=payload["object_type"],
        title=payload["title"],
        aliases=tuple(payload["aliases"]),
        people=tuple(payload["people"]),
        family_id=payload["family_id"],
        voice_role=payload["voice_role"],
        source_type=payload["source_type"],
        date_start=payload["date_start"],
        date_end=payload["date_end"],
        topics=tuple(payload["topics"]),
        summary=payload["summary"],
        reliability=payload["reliability"],
        review_status=payload["review_status"],
        available_layers=tuple(payload["available_layers"]),
        warnings=tuple(payload["warnings"]),
        parent_ids=tuple(payload["parent_ids"]),
        paths=tuple(
            LegacyKBasePath(role=role, path=path)
            for role, path in sorted(paths.items())
        ),
        content_fingerprint=payload["content_fingerprint"],
        source_schema_version=payload["source_schema_version"],
    )


def read_legacy_p0_task_report_v2(raw: bytes) -> LegacyP0TaskReportV2View:
    """Run the sealed P0 validator before constructing a typed read-only mirror."""
    try:
        parse_task_report_v2_bytes(raw)
    except TaskReportValidationError as error:
        raise LegacyContractAdapterError(str(error)) from error
    try:
        return LegacyP0TaskReportV2View.model_validate_json(raw, strict=True)
    except ValidationError as error:
        raise LegacyContractAdapterError(
            "sealed TaskReport passed but its typed mirror is incompatible"
        ) from error


__all__ = [
    "LegacyContractAdapterError",
    "LegacyKBaseArtifactReference",
    "LegacyKBaseCatalogEntryView",
    "LegacyKBasePath",
    "LegacyP0TaskReportV2View",
    "preserve_legacy_kbase_artifact_reference",
    "read_legacy_kbase_catalog_entry",
    "read_legacy_p0_task_report_v2",
]
