"""Create-only historical audit corrections for the P3 boundary."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
from datetime import datetime, timezone
import os
import tempfile

from .contracts import canonical_json
from .contracts import Phase, SideEffect
from .sqlite_uow import _SqliteUnitOfWork
from . import stores


class AuditAddendumError(RuntimeError):
    pass


class AuditAddendumConflict(AuditAddendumError):
    pass


_MAX_REFS = 64
_MAX_REF = 512
_REF_RE = re.compile(r"[A-Za-z0-9_.:/#-]{1,512}\Z")
_AUDIT_TASK_ID = "P3R1-T3-HISTORICAL-AUDIT-ADDENDUM"


def _checked_refs(values: tuple[str, ...], name: str) -> list[str]:
    if not isinstance(values, tuple) or len(values) > _MAX_REFS:
        raise ValueError(f"{name} has too many references")
    checked = []
    for value in values:
        if not isinstance(value, str) or len(value) > _MAX_REF or ".." in value or not _REF_RE.fullmatch(value):
            raise ValueError(f"{name} contains an unsafe bounded reference")
        checked.append(value)
    return checked


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_historical_audit_addendum(
    *, repository_root: str | Path, source_refs: tuple[str, ...], output_ref: str,
    supersedes: tuple[str, ...], downstream_parent_refs: tuple[str, ...], recorded_at: str,
    authority_lease: object,
) -> Path:
    if (
        not isinstance(authority_lease, stores.TaskExecutionLease)
        or authority_lease.phase is not Phase.P3
        or authority_lease.task_id != _AUDIT_TASK_ID
        or SideEffect.WRITE_CONTROL_PLANE not in authority_lease.allowed_side_effects
    ):
        raise PermissionError("an active P3 audit-addendum task lease is required")

    def authorize(connection):
        row = stores._AuthorityStore._require_task_lease(connection, authority_lease)
        task_spec = json.loads(str(row["task_spec_payload_json"]))
        allowed_files = tuple(task_spec.get("allowed_files", ()))
        if not any(
            output_ref == allowed or (
                isinstance(allowed, str)
                and allowed.endswith("/")
                and output_ref.startswith(allowed)
            )
            for allowed in allowed_files
        ):
            raise PermissionError("audit addendum output is outside the frozen task spec")

    _SqliteUnitOfWork(stores._authority_spec())._read(authorize)
    root = Path(repository_root).resolve()
    if not isinstance(source_refs, tuple) or not source_refs:
        raise ValueError("source_refs must be non-empty")
    if not isinstance(recorded_at, str):
        raise ValueError("recorded_at must be a string")
    try:
        parsed_recorded_at = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("recorded_at must be an ISO timestamp") from error
    if parsed_recorded_at.tzinfo is None:
        raise ValueError("recorded_at must include a timezone offset")
    recorded_at = parsed_recorded_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if not isinstance(output_ref, str) or not output_ref.startswith(
        "research_state/control_plane/audit_addenda/"
    ):
        raise ValueError("addendum must be under the control-plane audit namespace")
    if ".." in output_ref or not _REF_RE.fullmatch(output_ref):
        raise ValueError("unsafe addendum path")
    destination = (root / output_ref).resolve()
    if root not in destination.parents:
        raise ValueError("unsafe addendum path")
    source_refs = tuple(_checked_refs(source_refs, "source_refs"))
    supersedes = tuple(_checked_refs(supersedes, "supersedes"))
    downstream_parent_refs = tuple(_checked_refs(downstream_parent_refs, "downstream_parent_refs"))
    source_records = []
    for ref in source_refs:
        source = (root / ref).resolve()
        if root not in source.parents or not source.is_file():
            raise AuditAddendumError("historical source is missing or unsafe")
        source_records.append({"ref": ref, "sha256": _sha(source), "bytes": source.stat().st_size})
    payload = {
        "schema_version": "control_plane.historical_audit_addendum.v1",
        "phase": "P3", "recorded_at": recorded_at,
        "comparison_pass_count": 0, "comparison_total": 9,
        "data_cutoff": "2026-07-08",
        "corrected_access_state": "TEST_LABELS_AND_TEST_DERIVED_RANKIC_MATERIALIZED_NOT_USED_FOR_PREFLIGHT_GATE",
        "protocol_reconstruction": "PARTIAL",
        "source_artifacts": source_records,
        "supersedes": list(supersedes),
        "invalidated_fields": [
            "test_outcomes_opened", "unseen_test_claim", "preflight_gate_passed",
            "rankic_materialized_for_preflight", "promotion_gate_passed",
        ],
        "downstream_parent_quarantine": list(downstream_parent_refs),
        "promotion_status": "RESEARCH_ONLY_NOT_PROMOTABLE",
        "original_artifacts_untouched": True,
    }
    raw = canonical_json(payload).encode("utf-8")
    if len(raw) > 256 * 1024:
        raise ValueError("audit addendum exceeds bounded size")
    payload["payload_sha256"] = hashlib.sha256(raw).hexdigest()
    final_raw = canonical_json(payload).encode("utf-8")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if destination.read_bytes() != final_raw:
            raise AuditAddendumConflict("existing audit addendum has conflicting bytes")
        return destination
    temporary_path: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=".audit-addendum-", dir=destination.parent)
        temporary_path = Path(temporary_name)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(final_raw)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary_path, destination)
        except FileExistsError:
            if destination.read_bytes() != final_raw:
                raise AuditAddendumConflict("existing audit addendum has conflicting bytes")
        return destination
    except AuditAddendumConflict:
        raise
    except OSError as error:
        raise AuditAddendumError("unable to publish audit addendum atomically") from error
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


__all__ = ["AuditAddendumConflict", "AuditAddendumError", "build_historical_audit_addendum"]
