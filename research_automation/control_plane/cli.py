"""Command-line entry points for the trusted research control plane."""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
from pathlib import Path
from typing import Sequence, TextIO

from .contracts import Phase, canonical_json
from .gates import (
    GateAuthorityMismatchError,
    GateBuildError,
    GateEvidenceError,
    PhaseGateBuilder,
    GateValidationError,
    PhaseGateCloser,
    PhaseGateVerifier,
    parse_gate_report_v1_bytes,
)
from .sqlite_uow import SqliteUnitOfWorkError
from .stores import (
    AuthorityReader,
    AuthorityRootError,
    PendingOutboxError,
    PhaseGateClosureError,
    StoreError,
)
from .task_reports import (
    TaskReportValidationError,
    parse_task_report_v2_bytes,
)


_MAX_GATE_REPORT_BYTES = 256 * 1024
_MAX_GATE_ARTIFACT_BYTES = 4 * 1024 * 1024
_MAX_AUTHORITY_CAPABILITY_CHARS = 4096


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
    build.add_argument("--freeze-manifest", required=True)
    build.add_argument("--inventory", required=True)
    build.add_argument("--entry-policy", required=True)
    build.add_argument("--scheduler-inventory", required=True)
    build.add_argument("--task-report-id", required=True)
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
    requested = Path(path_text)
    candidate = requested if requested.is_absolute() else root / requested
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
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


def _repository_reference(path_text: str, repository_root: Path) -> str:
    root = repository_root.resolve(strict=True)
    requested = Path(path_text)
    candidate = requested if requested.is_absolute() else root / requested
    try:
        resolved = candidate.resolve(strict=True)
        relative = resolved.relative_to(root)
    except (OSError, ValueError) as error:
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
) -> Path:
    root = repository_root.resolve(strict=True)
    requested = Path(path_text)
    candidate = requested if requested.is_absolute() else root / requested
    try:
        resolved = candidate.resolve(strict=False)
        resolved.relative_to(root)
    except (OSError, ValueError) as error:
        raise GateEvidenceError(
            "output path is outside the repository"
        ) from error
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
        os.replace(temporary_path, resolved)
    except OSError as error:
        if temporary_path is not None:
            try:
                Path(temporary_path).unlink()
            except OSError:
                pass
        raise GateEvidenceError("unable to publish gate candidate") from error
    return resolved


def _build_gate_candidate(
    args: argparse.Namespace,
    *,
    repository_root: Path,
    authority_reader: AuthorityReader,
    stdout: TextIO,
) -> int:
    task_reference = _repository_reference(
        args.task_report_id,
        repository_root,
    )
    task_bytes = _read_repository_file(args.task_report_id, repository_root)
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

    artifacts: dict[str, dict[str, str]] = {}
    for option_name, field_name in (
        ("freeze_manifest", "code_freeze_manifest"),
        ("inventory", "final_inventory"),
        ("entry_policy", "reviewed_entry_policy"),
        ("scheduler_inventory", "scheduler_inventory"),
    ):
        path_text = str(getattr(args, option_name))
        artifact_bytes = _read_repository_file(
            path_text,
            repository_root,
            max_bytes=_MAX_GATE_ARTIFACT_BYTES,
        )
        artifacts[field_name] = {
            "ref": _repository_reference(path_text, repository_root),
            "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
        }
    snapshot = authority_reader.phase_gate_snapshot(
        Phase(str(args.phase)),
        args.attempt_id,
    )
    identity_binding = task_report["identity_binding"]
    if not isinstance(identity_binding, dict):
        raise GateEvidenceError("TaskReport identity binding is invalid")
    draft: dict[str, object] = {
        "plan_version": str(task_report["plan_version"]),
        "phase": args.phase,
        "attempt_id": args.attempt_id,
        "identity_binding": dict(identity_binding),
        "task_reports": [
            {
                "report_ref": task_reference,
                "report_sha256": hashlib.sha256(task_bytes).hexdigest(),
                "ticket_id": task_report["ticket_id"],
                "outcome": task_report["outcome"],
            }
        ],
        "implementation_baseline": dict(
            artifacts["code_freeze_manifest"]
        ),
        "code_freeze_manifest": artifacts["code_freeze_manifest"],
        "final_inventory": artifacts["final_inventory"],
        "reviewed_entry_policy": artifacts["reviewed_entry_policy"],
        "scheduler_inventory": {
            **artifacts["scheduler_inventory"],
            "status": "UNKNOWN",
        },
        "test_receipts": [],
        "authority_snapshot": snapshot.to_report_dict(),
        "side_effect_summary": {"observed": [], "unauthorized": []},
        "file_delta_summary": {
            "changed_files": [],
            "unexpected_changes": [],
        },
        "unresolved_risks": [],
    }
    candidate = PhaseGateBuilder().build(draft)
    output_path = _write_repository_bytes(
        args.output,
        repository_root,
        canonical_json(candidate).encode("utf-8"),
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
    return 0


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
                len(snapshot.active_grant_ids) != 1
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
