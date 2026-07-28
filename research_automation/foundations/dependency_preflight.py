"""Read-only dependency verification against an exact hashed requirements lock."""

from __future__ import annotations

import hashlib
import platform
import re
import sys
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path


_DISTRIBUTION_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_EXACT_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]*$")
_HASH_OPTION = re.compile(
    r"^--hash=sha256:(?P<digest>[0-9a-f]{64})(?:[ \t]*\\)?$"
)


@dataclass(frozen=True)
class DependencyCheck:
    distribution: str
    expected_version: str
    installed_version: str | None
    status: str


@dataclass(frozen=True)
class DependencyPreflightReport:
    schema_version: str
    status: str
    lock_sha256: str | None
    python_implementation: str
    python_version: str
    checked_distributions: tuple[DependencyCheck, ...]
    issue_codes: tuple[str, ...]


class DependencyPreflightError(RuntimeError):
    def __init__(self, report: DependencyPreflightReport) -> None:
        super().__init__("DEPENDENCY_PREFLIGHT_FAILED")
        self.report = report


def _normalize_distribution_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _parse_lock(lock_bytes: bytes) -> dict[str, str]:
    try:
        text = lock_bytes.decode("utf-8")
    except UnicodeError as error:
        raise ValueError("lock is not strict UTF-8") from error
    if text.startswith("\ufeff"):
        raise ValueError("lock must not contain a byte-order mark")

    pins: dict[str, str] = {}
    current_name: str | None = None
    current_version: str | None = None
    current_hashes: set[str] = set()

    def finish_record() -> None:
        nonlocal current_name, current_version, current_hashes
        if current_name is None:
            return
        if not current_hashes or current_version is None:
            raise ValueError("every locked distribution requires a sha256 hash")
        if current_name in pins:
            raise ValueError("duplicate locked distribution")
        pins[current_name] = current_version
        current_name = None
        current_version = None
        current_hashes = set()

    for physical_line in text.splitlines():
        stripped = physical_line.strip()
        if not stripped or stripped.startswith("#"):
            if current_name is not None:
                raise ValueError("continued requirement was interrupted")
            continue
        if stripped.startswith("--hash="):
            if current_name is None:
                raise ValueError("hash option has no requirement")
            match = _HASH_OPTION.fullmatch(stripped)
            if match is None:
                raise ValueError("lock contains an invalid hash option")
            digest = match.group("digest")
            if digest in current_hashes:
                raise ValueError("lock contains a duplicate hash option")
            current_hashes.add(digest)
            if not stripped.endswith("\\"):
                finish_record()
            continue

        if current_name is not None:
            raise ValueError("continued requirement did not receive another hash")
        if not stripped.endswith("\\"):
            raise ValueError("locked requirements must continue to hash options")
        requirement = stripped[:-1].rstrip()
        if "==" not in requirement or any(
            token in requirement for token in (";", "@", " --")
        ):
            raise ValueError("lock requires exact distribution pins")
        raw_name, version = requirement.split("==", 1)
        if (
            not _DISTRIBUTION_NAME.fullmatch(raw_name)
            or not _EXACT_VERSION.fullmatch(version)
        ):
            raise ValueError("lock contains an invalid exact pin")
        current_name = _normalize_distribution_name(raw_name)
        current_version = version

    if current_name is not None:
        raise ValueError("lock ends with a dangling continuation")
    if not pins:
        raise ValueError("lock contains no distributions")
    return pins


def _report(
    *,
    status: str,
    lock_sha256: str | None,
    checked: tuple[DependencyCheck, ...] = (),
    issue_codes: tuple[str, ...] = (),
) -> DependencyPreflightReport:
    return DependencyPreflightReport(
        schema_version="control_plane.dependency_preflight.v1",
        status=status,
        lock_sha256=lock_sha256,
        python_implementation=platform.python_implementation(),
        python_version=".".join(str(part) for part in sys.version_info[:3]),
        checked_distributions=checked,
        issue_codes=issue_codes,
    )


def inspect_dependency_environment(
    lock_path: str | Path,
) -> DependencyPreflightReport:
    """Compare only lock-declared distributions without importing or installing them."""
    try:
        lock_bytes = Path(lock_path).read_bytes()
    except OSError:
        return _report(
            status="FAIL",
            lock_sha256=None,
            issue_codes=("LOCK_INVALID",),
        )
    lock_sha256 = hashlib.sha256(lock_bytes).hexdigest()
    try:
        pins = _parse_lock(lock_bytes)
    except ValueError:
        return _report(
            status="FAIL",
            lock_sha256=lock_sha256,
            issue_codes=("LOCK_INVALID",),
        )

    checks: list[DependencyCheck] = []
    issue_codes: set[str] = set()
    if (
        platform.python_implementation() != "CPython"
        or sys.version_info[:2] != (3, 13)
    ):
        issue_codes.add("UNSUPPORTED_RUNTIME")
    for distribution, expected_version in sorted(pins.items()):
        try:
            installed_version = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            installed_version = None
        if installed_version is None:
            check_status = "MISSING"
            issue_codes.add("MISSING_DISTRIBUTION")
        elif installed_version != expected_version:
            check_status = "MISMATCH"
            issue_codes.add("VERSION_MISMATCH")
        else:
            check_status = "MATCH"
        checks.append(
            DependencyCheck(
                distribution=distribution,
                expected_version=expected_version,
                installed_version=installed_version,
                status=check_status,
            )
        )
    return _report(
        status="FAIL" if issue_codes else "PASS",
        lock_sha256=lock_sha256,
        checked=tuple(checks),
        issue_codes=tuple(sorted(issue_codes)),
    )


def require_dependency_environment(
    lock_path: str | Path,
) -> DependencyPreflightReport:
    report = inspect_dependency_environment(lock_path)
    if report.status != "PASS":
        raise DependencyPreflightError(report)
    return report


__all__ = [
    "DependencyCheck",
    "DependencyPreflightError",
    "DependencyPreflightReport",
    "inspect_dependency_environment",
    "require_dependency_environment",
]
