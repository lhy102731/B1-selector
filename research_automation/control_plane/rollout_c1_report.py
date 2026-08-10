"""C1 report schema, serialization and manifest-to-report mapping lint."""

from __future__ import annotations

from typing import Mapping

C1_REPORT_SCHEMA = "C1_DRY_RUN_REPORT_V1"


def build_dry_run_report(outcome_payload: Mapping) -> dict:
    raise NotImplementedError("C1 report slice pending implementation")


def serialize_report(report: Mapping) -> str:
    raise NotImplementedError("C1 report slice pending implementation")


def manifest_report_mapping_lint(manifest: Mapping, report: Mapping) -> list[str]:
    raise NotImplementedError("C1 report slice pending implementation")
