"""C1 manifest-to-report mapping lint (C0-POOL-003)."""

from __future__ import annotations

from typing import Mapping


def lint_acceptance_mapping(manifest: Mapping, report: Mapping) -> list[str]:
    """Return a list of problems mapping the C1 scope manifest acceptance matrix
    to the report acceptance section. Empty list means aligned."""
    raise NotImplementedError("C1 manifest slice pending implementation")


def validate_report_shape(report: Mapping) -> list[str]:
    """Return a list of schema/shape problems for a C1 dry-run report."""
    raise NotImplementedError("C1 manifest slice pending implementation")
