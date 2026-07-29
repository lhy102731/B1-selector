"""Command-line entry points for the trusted research control plane."""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Sequence, TextIO

from .artifact_semantics import (
    ArtifactBindingError,
    ArtifactSemanticError,
    validate_code_freeze_manifest,
    validate_final_inventory,
    validate_implementation_baseline,
    validate_reviewed_entry_policy,
    validate_scheduler_inventory,
)
from .contracts import Phase, canonical_json
from .gates import (
    GateAuthorityMismatchError,
    GateBuildError,
    GateEvidenceError,
    PhaseGateBuilder,
    GateValidationError,
    PhaseGateCloser,
    PhaseGateVerifier,
    _project_task_report_evidence,
    parse_gate_report_v1_bytes,
)
from .inventory import UnstableInventoryError, verify_current_git_inventory
from .sqlite_uow import SqliteUnitOfWorkError
from .stores import (
    AuthorityReader,
    AuthorityRootError,
    PendingOutboxError,
    PhaseGateClosureError,
    StoreError,
    TaskReportAuthorityError,
)
from .task_reports import (
    TaskReportValidationError,
    parse_task_report_v2_bytes,
)


_MAX_GATE_REPORT_BYTES = 256 * 1024
_MAX_GATE_ARTIFACT_BYTES = 4 * 1024 * 1024
_MAX_AUTHORITY_CAPABILITY_CHARS = 4096
_FORBIDDEN_GATE_OUTPUT_ROOTS = frozenset(
    {
        "apps",
        "config",
        "docs",
        "research_automation",
        "strategy",
        "tests",
        "tools",
        "utils",
    }
)
_FORBIDDEN_GATE_OUTPUT_SUFFIXES = frozenset(
    {".cfg", ".ini", ".py", ".pyc", ".toml", ".yaml", ".yml"}
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="control-plane")
    commands = parser.add_subparsers(dest="command", required=True)
    gate = commands.add_parser("gate")
    gate_commands = gate.add_subparsers(dest="gate_command", required=True)
    preflight = gate_commands.add_parser("preflight")
    preflight.add_argument(
        "--phase",
        required=True,
        choices=[phase.value for phase in Phase],
    )
    preflight.add_argument("--attempt-id", required=True)
    build = gate_commands.add_parser("build")
    build.add_argument(
        "--phase",
        required=True,
        choices=[phase.value for phase in Phase],
    )
    build.add_argument("--attempt-id", required=True)
    build.add_argument("--baseline", required=True)
    build.add_argument("--freeze-manifest", required=True)
    build.add_argument("--inventory", required=True)
    build.add_argument("--entry-policy", required=True)
    build.add_argument("--scheduler-inventory", required=True)
    build.add_argument("--task-report-id", required=True, action="append")
    build.add_argument("--output", required=True)
    verify = gate_commands.add_parser("verify")
    verify.add_argument(
        "--phase",
        required=True,
        choices=[phase.value for phase in Phase],
    )
    verify.add_argument("--attempt-id", required=True)
    verify.add_argument("--report", required=True)
    verify.add_argument("--read-only", action="store_true", required=True)
    close = gate_commands.add_parser("close")
    close.add_argument(
        "--phase",
        required=True,
        choices=[phase.value for phase in Phase],
    )
    close.add_argument("--attempt-id", required=True)
    close.add_argument("--report", required=True)
    close.add_argument(
        "--capability-stdin",
        action="store_true",
        required=True,
    )
    return parser


def _read_repository_file(
    path_text: str,
    repository_root: Path,
    *,
    max_bytes: int = _MAX_GATE_REPORT_BYTES,
) -> bytes:
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer")
    root = repository_root.resolve(strict=True)
    resolved = _resolve_repository_path(path_text, root, strict=True)
    try:
        if not resolved.is_file():
            raise GateEvidenceError("gate report path is not a file")
        with resolved.open("rb") as stream:
            raw = stream.read(max_bytes + 1)
    except GateEvidenceError:
        raise
    except (OSError, ValueError) as error:
        raise GateEvidenceError(
            "gate report path is unavailable or outside the repository"
        ) from error
    if len(raw) > max_bytes:
        raise GateEvidenceError("repository evidence exceeds its byte limit")
    return raw


def _resolve_repository_path(
    path_text: str,
    repository_root: Path,
    *,
    strict: bool,
) -> Path:
    if not isinstance(path_text, str) or not path_text.strip():
        raise GateEvidenceError("repository path is empty")
    normalized = path_text.replace("\\", "/")
    drive, tail = os.path.splitdrive(normalized)
    if (
        ":" in tail
        or normalized.startswith("//?/")
        or normalized.startswith("//./")
        or any(
            character in '<>"|?*' or ord(character) < 32
            for character in normalized
        )
    ):
        raise GateEvidenceError("repository path contains ambiguous characters")
    if drive and not Path(path_text).is_absolute():
        raise GateEvidenceError("repository path has an invalid drive prefix")
    root = repository_root.resolve(strict=True)
    requested = Path(path_text)
    candidate = requested if requested.is_absolute() else root / requested
    try:
        resolved = candidate.resolve(strict=strict)
        resolved.relative_to(root)
    except (OSError, ValueError) as error:
        raise GateEvidenceError(
            "repository path is unavailable or outside the repository"
        ) from error
    return resolved


def _repository_reference(path_text: str, repository_root: Path) -> str:
    root = repository_root.resolve(strict=True)
    resolved = _resolve_repository_path(path_text, root, strict=True)
    try:
        relative = resolved.relative_to(root)
    except ValueError as error:
        raise GateEvidenceError(
            "repository reference is unavailable or outside the repository"
        ) from error
    reference = relative.as_posix()
    if not reference or reference == ".":
        raise GateEvidenceError("repository reference is invalid")
    return reference


def _write_repository_bytes(
    path_text: str,
    repository_root: Path,
    raw: bytes,
    *,
    protected_paths: Iterable[Path] = (),
) -> Path:
    root = repository_root.resolve(strict=True)
    resolved = _resolve_repository_path(path_text, root, strict=False)
    relative = resolved.relative_to(root)
    if relative.parts and relative.parts[0].casefold() in {
        item.casefold() for item in _FORBIDDEN_GATE_OUTPUT_ROOTS
    }:
        raise GateEvidenceError(
            "gate output must be published outside source and configuration roots"
        )
    if resolved.suffix.casefold() in _FORBIDDEN_GATE_OUTPUT_SUFFIXES:
        raise GateEvidenceError("gate output has a forbidden source/config suffix")
    protected = {
        path.resolve(strict=False)
        for path in protected_paths
    }
    if resolved in protected:
        raise GateEvidenceError("gate output collides with an input evidence file")
    if resolved.exists():
        raise GateEvidenceError("gate output already exists and is immutable")
    if not resolved.parent.is_dir():
        raise GateEvidenceError("output directory does not exist")
    temporary_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=resolved.parent,
            prefix=f".{resolved.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary_path = stream.name
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary_path, resolved)
        except FileExistsError as error:
            raise GateEvidenceError(
                "gate output already exists and is immutable"
            ) from error
    except GateEvidenceError:
        raise
    except OSError as error:
        raise GateEvidenceError("unable to publish gate candidate") from error
    finally:
        if temporary_path is not None:
            try:
                Path(temporary_path).unlink()
            except OSError:
                pass
    return resolved


def _build_gate_candidate(
    args: argparse.Namespace,
    *,
    repository_root: Path,
    authority_reader: AuthorityReader,
    stdout: TextIO,
) -> int:
    phase = Phase(str(args.phase))
    if authority_reader.phase_gate_closure(phase, args.attempt_id) is not None:
        raise GateAuthorityMismatchError(
            "phase attempt already has an immutable gate closure"
        )
    protected_paths: list[Path] = []
    parsed_task_reports: list[dict[str, object]] = []
    task_report_records: list[dict[str, object]] = []
    task_references: set[str] = set()
    task_ticket_ids: set[str] = set()
    for task_path_text in args.task_report_id:
        task_reference = _repository_reference(
            task_path_text,
            repository_root,
        )
        if task_reference in task_references:
            raise GateEvidenceError("TaskReport references must be distinct")
        task_references.add(task_reference)
        protected_paths.append(
            _resolve_repository_path(
                task_path_text,
                repository_root,
                strict=True,
            )
        )
        task_bytes = _read_repository_file(task_path_text, repository_root)
        try:
            task_report = parse_task_report_v2_bytes(task_bytes)
        except TaskReportValidationError as error:
            raise GateEvidenceError("TaskReport input is invalid") from error
        if (
            task_report["phase"] != args.phase
            or task_report["attempt_id"] != args.attempt_id
        ):
            raise GateAuthorityMismatchError(
                "TaskReport input does not match the requested gate"
            )
        try:
            authority_reader.verify_task_report_binding(task_report)
        except TaskReportAuthorityError as error:
            raise GateAuthorityMismatchError(
                "TaskReport input does not match trusted authority"
            ) from error
        ticket_id = str(task_report["ticket_id"])
        if ticket_id in task_ticket_ids:
            raise GateEvidenceError("TaskReport ticket ids must be distinct")
        task_ticket_ids.add(ticket_id)
        parsed_task_reports.append(task_report)
        task_report_records.append(
            {
                "report_ref": task_reference,
                "report_sha256": hashlib.sha256(task_bytes).hexdigest(),
                "ticket_id": ticket_id,
                "outcome": task_report["outcome"],
            }
        )
    task_report_records.sort(key=lambda item: str(item["ticket_id"]))
    parsed_task_reports.sort(key=lambda item: str(item["ticket_id"]))
    primary_task_report = parsed_task_reports[0]
    primary_identity = primary_task_report["identity_binding"]
    if not isinstance(primary_identity, dict):
        raise GateEvidenceError("TaskReport identity binding is invalid")
    if any(
        task_report["plan_version"] != primary_task_report["plan_version"]
        or task_report["identity_binding"] != primary_identity
        for task_report in parsed_task_reports
    ):
        raise GateAuthorityMismatchError(
            "TaskReports do not share one gate identity"
        )

    artifacts: dict[str, dict[str, str]] = {}
    artifact_payloads: dict[str, bytes] = {}
    for option_name, field_name in (
        ("baseline", "implementation_baseline"),
        ("freeze_manifest", "code_freeze_manifest"),
        ("inventory", "final_inventory"),
        ("entry_policy", "reviewed_entry_policy"),
        ("scheduler_inventory", "scheduler_inventory"),
    ):
        path_text = str(getattr(args, option_name))
        protected_paths.append(
            _resolve_repository_path(
                path_text,
                repository_root,
                strict=True,
            )
        )
        artifact_bytes = _read_repository_file(
            path_text,
            repository_root,
            max_bytes=_MAX_GATE_ARTIFACT_BYTES,
        )
        artifacts[field_name] = {
            "ref": _repository_reference(path_text, repository_root),
            "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
        }
        artifact_payloads[field_name] = artifact_bytes
    artifact_refs = {artifact["ref"] for artifact in artifacts.values()}
    if len(artifact_refs) != len(artifacts) or (
        task_references & artifact_refs
    ):
        raise GateEvidenceError("gate evidence references must be distinct")
    policy_artifact = artifacts["reviewed_entry_policy"]
    if policy_artifact["ref"] != (
        "research_state/control_plane/policies/"
        f"{policy_artifact['sha256']}.json"
    ):
        raise GateEvidenceError(
            "reviewed entry policy is outside the immutable policy namespace"
        )
    identity_binding = primary_identity
    expected_identity = {
        "plan_hash": str(identity_binding["plan_hash"]),
        "scope_hash": str(identity_binding["scope_hash"]),
        "instruction_policy_hash": str(
            identity_binding["instruction_policy_hash"]
        ),
    }
    try:
        validate_implementation_baseline(
            artifact_payloads["implementation_baseline"],
            expected_plan_version=str(primary_task_report["plan_version"]),
            expected_phase=str(args.phase),
            expected_attempt_id=str(args.attempt_id),
            repository_root=repository_root,
        )
        freeze_manifest = validate_code_freeze_manifest(
            artifact_payloads["code_freeze_manifest"],
            expected_plan_version=str(primary_task_report["plan_version"]),
            expected_phase=str(args.phase),
            expected_attempt_id=str(args.attempt_id),
            expected_identity=expected_identity,
            repository_root=repository_root,
        )
        if (
            freeze_manifest["schema_version"]
            != "control_plane.code_freeze_manifest.v2"
        ):
            raise ArtifactSemanticError(
                "new phase gates require Git source identity evidence"
            )
        final_inventory = validate_final_inventory(
            artifact_payloads["final_inventory"],
            expected_plan_version=str(primary_task_report["plan_version"]),
            expected_phase=str(args.phase),
            expected_attempt_id=str(args.attempt_id),
            expected_identity=expected_identity,
            freeze_manifest=freeze_manifest,
        )
        try:
            verify_current_git_inventory(
                repository_root,
                freeze_manifest=freeze_manifest,
                final_inventory=final_inventory,
            )
        except UnstableInventoryError as error:
            raise ArtifactSemanticError(
                f"current executable surface cannot be verified: {error}"
            ) from error
        validate_reviewed_entry_policy(
            artifact_payloads["reviewed_entry_policy"],
            expected_plan_version=str(primary_task_report["plan_version"]),
            expected_phase=str(args.phase),
            expected_attempt_id=str(args.attempt_id),
            expected_identity=expected_identity,
            final_inventory=final_inventory,
        )
        _, scheduler_status = validate_scheduler_inventory(
            artifact_payloads["scheduler_inventory"],
            expected_phase=str(args.phase),
            final_inventory=final_inventory,
        )
    except ArtifactBindingError as error:
        raise GateAuthorityMismatchError(str(error)) from error
    except ArtifactSemanticError as error:
        raise GateEvidenceError(str(error)) from error
    if any(
        task_report["baseline_ref"]
        != artifacts["implementation_baseline"]["ref"]
        or task_report["baseline_sha256"]
        != artifacts["implementation_baseline"]["sha256"]
        for task_report in parsed_task_reports
    ):
        raise GateAuthorityMismatchError(
            "TaskReport baseline does not match the gate baseline"
        )
    snapshot = authority_reader.phase_gate_snapshot(
        phase,
        args.attempt_id,
    )
    projected = _project_task_report_evidence(parsed_task_reports)
    draft: dict[str, object] = {
        "plan_version": str(primary_task_report["plan_version"]),
        "phase": args.phase,
        "attempt_id": args.attempt_id,
        "identity_binding": dict(identity_binding),
        "task_reports": task_report_records,
        "implementation_baseline": artifacts["implementation_baseline"],
        "code_freeze_manifest": artifacts["code_freeze_manifest"],
        "final_inventory": artifacts["final_inventory"],
        "reviewed_entry_policy": artifacts["reviewed_entry_policy"],
        "scheduler_inventory": {
            **artifacts["scheduler_inventory"],
            "status": scheduler_status,
        },
        "test_receipts": projected["test_receipts"],
        "authority_snapshot": snapshot.to_report_dict(),
        "side_effect_summary": projected["side_effect_summary"],
        "file_delta_summary": projected["file_delta_summary"],
        "unresolved_risks": projected["unresolved_risks"],
    }
    candidate = PhaseGateBuilder().build(draft)
    output_path = _write_repository_bytes(
        args.output,
        repository_root,
        canonical_json(candidate).encode("utf-8"),
        protected_paths=protected_paths,
    )
    stdout.write(
        canonical_json(
            {
                "attempt_id": candidate["attempt_id"],
                "output": output_path.relative_to(
                    repository_root.resolve(strict=True)
                ).as_posix(),
                "phase": candidate["phase"],
                "status": "BUILT",
                "verdict": candidate["verdict"],
            }
        )
        + "\n"
    )
    return 0 if candidate["verdict"] == "PASS" else 2


def _emit_error(stderr: TextIO, error: Exception) -> None:
    stderr.write(
        canonical_json(
            {
                "error": type(error).__name__,
                "message": str(error),
            }
        )
        + "\n"
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    stdin: TextIO | None = None,
    authority_reader: AuthorityReader | None = None,
    repository_root: str | Path | None = None,
) -> int:
    output = stdout or sys.stdout
    errors = stderr or sys.stderr
    args = _build_parser().parse_args(argv)
    root = Path(
        repository_root
        if repository_root is not None
        else Path(__file__).resolve().parents[2]
    )
    try:
        reader = authority_reader or AuthorityReader()
        if args.gate_command == "preflight":
            phase = Phase(str(args.phase))
            snapshot = reader.phase_gate_snapshot(phase, args.attempt_id)
            closure = reader.phase_gate_closure(phase, args.attempt_id)
            blocked = bool(
                snapshot.active_entry_policy_sha256 is None
                or len(snapshot.active_grant_ids) != 1
                or snapshot.open_ticket_ids
                or snapshot.failed_ticket_ids
                or snapshot.in_doubt_ticket_ids
                or snapshot.pending_outbox_count
            )
            status = "CLOSED" if closure is not None else (
                "BLOCKED" if blocked else "READY"
            )
            output.write(
                canonical_json(
                    {
                        "attempt_id": args.attempt_id,
                        "authority_snapshot": snapshot.to_report_dict(),
                        "closure_id": (
                            None if closure is None else closure.closure_id
                        ),
                        "phase": phase.value,
                        "status": status,
                    }
                )
                + "\n"
            )
            return 4 if closure is not None else (2 if blocked else 0)
        if args.gate_command == "build":
            return _build_gate_candidate(
                args,
                repository_root=root,
                authority_reader=reader,
                stdout=output,
            )
        raw = _read_repository_file(args.report, root)
        report = parse_gate_report_v1_bytes(raw)
        if (
            report["phase"] != args.phase
            or report["attempt_id"] != args.attempt_id
        ):
            raise GateAuthorityMismatchError(
                "CLI phase or attempt does not match the GateReport"
            )
        if args.gate_command == "verify":
            PhaseGateVerifier(
                authority_reader=reader,
                repository_root=root,
            ).verify(report)
        else:
            capability_input = (stdin or sys.stdin).read(
                _MAX_AUTHORITY_CAPABILITY_CHARS + 1
            )
            if len(capability_input) > _MAX_AUTHORITY_CAPABILITY_CHARS:
                raise GateAuthorityMismatchError(
                    "authority capability stdin exceeds its size limit"
                )
            capability = capability_input.rstrip("\r\n")
            if not capability:
                raise GateAuthorityMismatchError(
                    "authority capability stdin is empty"
                )
            closure = PhaseGateCloser(
                root_secret=capability,
                authority_reader=reader,
                repository_root=root,
            ).close_bytes(raw)
            output.write(
                canonical_json(
                    {
                        "attempt_id": closure.attempt_id,
                        "closure_id": closure.closure_id,
                        "gate_report_sha256": closure.gate_report_sha256,
                        "phase": closure.phase.value,
                        "status": "CLOSED",
                        "verdict": closure.verdict,
                    }
                )
                + "\n"
            )
            return 0 if closure.verdict == "PASS" else 2
    except GateAuthorityMismatchError as error:
        _emit_error(errors, error)
        return 4
    except AuthorityRootError as error:
        _emit_error(errors, error)
        return 4
    except PhaseGateClosureError as error:
        _emit_error(errors, error)
        return 4
    except PendingOutboxError as error:
        _emit_error(errors, error)
        return 5
    except (
        GateBuildError,
        GateValidationError,
        GateEvidenceError,
        OSError,
    ) as error:
        _emit_error(errors, error)
        return 3
    except (StoreError, SqliteUnitOfWorkError) as error:
        _emit_error(errors, error)
        return 5

    verdict = str(report["verdict"])
    output.write(
        canonical_json(
            {
                "attempt_id": report["attempt_id"],
                "phase": report["phase"],
                "status": "VERIFIED",
                "verdict": verdict,
            }
        )
        + "\n"
    )
    return 0 if verdict == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
