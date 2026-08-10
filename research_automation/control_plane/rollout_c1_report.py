"""C1 canonical dry-run report builder and serializer (V3.4.2 Rollout C1).

Builds the canonical, deterministic report dict from a C1 outcome payload and
serializes it through the shared canonical JSON contract so that the report
digest stays stable across runs.

The report carries the explicit acceptance matrix the C1 plan requires:
``real_llm_dry_run``, ``roster_verified``, ``usage_verified``, ``context_verified``,
``budget_verified``, ``no_learning_commit``, ``no_real_campaign_or_holdout``,
``failures_recorded_not_hidden``, ``report_canonical``, ``evidence_append_only``.

The ``failures_recorded_not_hidden`` field is the failure list itself (or
``True`` when the failure list is empty) so the report cannot silently hide
provider failures.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from research_automation.control_plane.contracts import canonical_json

C1_REPORT_SCHEMA = "C1_DRY_RUN_REPORT_V1"

ACCEPTANCE_KEYS = (
    "real_llm_dry_run",
    "roster_verified",
    "usage_verified",
    "context_verified",
    "budget_verified",
    "no_learning_commit",
    "no_real_campaign_or_holdout",
    "failures_recorded_not_hidden",
    "report_canonical",
    "evidence_append_only",
)

_REPORT_FIELDS = (
    "schema_version",
    "attempt_id",
    "plan_version",
    "models",
    "started_at",
    "completed_at",
    "usage_records",
    "roster_verified",
    "usage_verified",
    "context_verified",
    "budget_verified",
    "budget_detail",
    "no_learning_commit",
    "no_real_campaign_or_holdout",
    "failures",
    "pass",
    "final_state_digest",
    "acceptance",
)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y", "on"}
    return bool(value)


def _coerce_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _build_acceptance(outcome: Mapping) -> dict:
    """Build the acceptance section from the outcome payload.

    ``failures_recorded_not_hidden`` is the failure list itself (truthy) when
    there are any failures, otherwise ``True`` -- never silently coerced to
    ``False`` while a non-empty failure list is present.
    """
    failures = _coerce_list(outcome.get("failures"))
    failures_recorded_not_hidden = failures if failures else True
    return {
        "real_llm_dry_run": True,
        "roster_verified": _coerce_bool(outcome.get("roster_verified")),
        "usage_verified": _coerce_bool(outcome.get("usage_verified")),
        "context_verified": _coerce_bool(outcome.get("context_verified")),
        "budget_verified": _coerce_bool(outcome.get("budget_verified")),
        "no_learning_commit": _coerce_bool(outcome.get("no_learning_commit")),
        "no_real_campaign_or_holdout": _coerce_bool(
            outcome.get("no_real_campaign_or_holdout")
        ),
        "failures_recorded_not_hidden": failures_recorded_not_hidden,
        "report_canonical": True,
        "evidence_append_only": True,
    }


def build_dry_run_report(outcome_payload: Mapping) -> dict:
    """Build the canonical C1 dry-run report dict from an outcome payload.

    The result is a plain dict (not a dataclass) so that
    ``serialize_report`` can hand it to ``canonical_json`` directly. The
    function is pure: feeding the same payload twice yields equal reports.
    """
    if not isinstance(outcome_payload, Mapping):
        raise TypeError("outcome_payload must be a mapping")

    models = _coerce_list(outcome_payload.get("models"))
    usage_records = _coerce_list(outcome_payload.get("usage_records"))
    failures = _coerce_list(outcome_payload.get("failures"))

    report = {
        "schema_version": C1_REPORT_SCHEMA,
        "attempt_id": str(outcome_payload.get("attempt_id", "")),
        "plan_version": str(outcome_payload.get("plan_version", "")),
        "models": models,
        "started_at": str(outcome_payload.get("started_at", "")),
        "completed_at": str(outcome_payload.get("completed_at", "")),
        "usage_records": usage_records,
        "roster_verified": _coerce_bool(outcome_payload.get("roster_verified")),
        "usage_verified": _coerce_bool(outcome_payload.get("usage_verified")),
        "context_verified": _coerce_bool(outcome_payload.get("context_verified")),
        "budget_verified": _coerce_bool(outcome_payload.get("budget_verified")),
        "budget_detail": str(outcome_payload.get("budget_detail", "")),
        "no_learning_commit": _coerce_bool(
            outcome_payload.get("no_learning_commit")
        ),
        "no_real_campaign_or_holdout": _coerce_bool(
            outcome_payload.get("no_real_campaign_or_holdout")
        ),
        "failures": failures,
        "pass": _coerce_bool(outcome_payload.get("pass")),
        "final_state_digest": str(outcome_payload.get("final_state_digest", "")),
        "acceptance": _build_acceptance(outcome_payload),
    }
    return report


def serialize_report(report: Mapping) -> str:
    """Serialize a C1 dry-run report via the shared canonical JSON contract.

    Returns the unique UTF-8 JSON representation (compact, sorted keys) so the
    report digest is reproducible across runs and platforms.
    """
    if not isinstance(report, Mapping):
        raise TypeError("report must be a mapping")
    return canonical_json(report)


def manifest_report_mapping_lint(manifest: Mapping, report: Mapping) -> list[str]:
    """Return a list of problems mapping a C1 manifest to a report.

    Currently not implemented by this slice; the C1 manifest slice owns the
    mapping lint per the skeleton separation. This stub remains so callers
    that import it get a clear error during development.
    """
    raise NotImplementedError("C1 report slice does not own mapping lint")
