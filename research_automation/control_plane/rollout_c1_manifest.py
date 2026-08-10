"""C1 manifest-to-report mapping lint (C0-POOL-003)."""

from __future__ import annotations

import re
from typing import Mapping

FINAL_STATE_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
USAGE_RECORD_KEYS = ("model", "status", "input_tokens", "output_tokens", "total_tokens")


def lint_acceptance_mapping(manifest: Mapping, report: Mapping) -> list[str]:
    """Return a list of problems mapping the C1 scope manifest acceptance matrix
    to the report acceptance section. Empty list means aligned."""
    if not isinstance(manifest, Mapping):
        return ["manifest must be a mapping"]
    if not isinstance(report, Mapping):
        return ["report must be a mapping"]
    matrix = manifest.get("acceptance_matrix")
    acceptance = report.get("acceptance")
    if not isinstance(matrix, Mapping):
        return ["manifest must define acceptance_matrix as a mapping"]
    if not isinstance(acceptance, Mapping):
        return ["report must define acceptance as a mapping"]
    manifest_keys = set(matrix)
    report_keys = set(acceptance)
    problems: list[str] = []
    for key in sorted(manifest_keys | report_keys):
        if key in manifest_keys and key not in report_keys:
            problems.append(f"missing acceptance key: {key}")
        elif key in report_keys and key not in manifest_keys:
            problems.append(f"unexpected acceptance key: {key}")
    return problems


def validate_report_shape(report: Mapping) -> list[str]:
    """Return a list of schema/shape problems for a C1 dry-run report."""
    problems: list[str] = []
    if not isinstance(report, Mapping):
        return ["report must be a mapping"]
    # Imported lazily so this module stays importable while rollout_c1_report is
    # still partially initialized (the report slice may import this module).
    from research_automation.control_plane.rollout_c1_report import C1_REPORT_SCHEMA
    if report.get("schema_version") != C1_REPORT_SCHEMA:
        problems.append(f"missing or invalid schema_version (expected {C1_REPORT_SCHEMA})")
    attempt_id = report.get("attempt_id")
    if not isinstance(attempt_id, str) or not attempt_id.strip():
        problems.append("attempt_id must be a non-empty string")
    models = report.get("models")
    if not isinstance(models, list):
        problems.append("models must be a list of non-empty strings")
    else:
        for index, model in enumerate(models):
            if not isinstance(model, str) or not model.strip():
                problems.append(f"models[{index}] must be a non-empty string")
    if not isinstance(report.get("pass"), bool):
        problems.append("pass must be a boolean")
    digest = report.get("final_state_digest")
    if not isinstance(digest, str) or FINAL_STATE_DIGEST_RE.fullmatch(digest) is None:
        problems.append("final_state_digest must be a 64-character lowercase hex digest")
    usage_records = report.get("usage_records")
    if not isinstance(usage_records, list):
        problems.append("usage_records must be a list of dicts")
    else:
        for index, record in enumerate(usage_records):
            if not isinstance(record, Mapping):
                problems.append(f"usage_records[{index}] must be a dict")
                continue
            for key in USAGE_RECORD_KEYS:
                if key not in record:
                    problems.append(f"usage_records[{index}] missing key '{key}'")
    if not isinstance(report.get("acceptance"), Mapping):
        problems.append("acceptance must be a dict")
    return problems
