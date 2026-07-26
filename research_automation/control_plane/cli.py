"""Command-line entry points for the trusted research control plane."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence, TextIO

from .contracts import Phase, canonical_json
from .gates import (
    GateAuthorityMismatchError,
    GateEvidenceError,
    GateValidationError,
    PhaseGateVerifier,
    parse_gate_report_v1_bytes,
)
from .stores import AuthorityReader, StoreError


_MAX_GATE_REPORT_BYTES = 256 * 1024


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="control-plane")
    commands = parser.add_subparsers(dest="command", required=True)
    gate = commands.add_parser("gate")
    gate_commands = gate.add_subparsers(dest="gate_command", required=True)
    verify = gate_commands.add_parser("verify")
    verify.add_argument(
        "--phase",
        required=True,
        choices=[phase.value for phase in Phase],
    )
    verify.add_argument("--attempt-id", required=True)
    verify.add_argument("--report", required=True)
    verify.add_argument("--read-only", action="store_true", required=True)
    return parser


def _read_repository_file(
    path_text: str,
    repository_root: Path,
) -> bytes:
    root = repository_root.resolve(strict=True)
    requested = Path(path_text)
    candidate = requested if requested.is_absolute() else root / requested
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
        if not resolved.is_file():
            raise GateEvidenceError("gate report path is not a file")
        with resolved.open("rb") as stream:
            raw = stream.read(_MAX_GATE_REPORT_BYTES + 1)
    except GateEvidenceError:
        raise
    except (OSError, ValueError) as error:
        raise GateEvidenceError(
            "gate report path is unavailable or outside the repository"
        ) from error
    if len(raw) > _MAX_GATE_REPORT_BYTES:
        raise GateEvidenceError("gate report exceeds its byte limit")
    return raw


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
        raw = _read_repository_file(args.report, root)
        report = parse_gate_report_v1_bytes(raw)
        if (
            report["phase"] != args.phase
            or report["attempt_id"] != args.attempt_id
        ):
            raise GateAuthorityMismatchError(
                "CLI phase or attempt does not match the GateReport"
            )
        PhaseGateVerifier(
            authority_reader=authority_reader,
            repository_root=root,
        ).verify(report)
    except GateAuthorityMismatchError as error:
        _emit_error(errors, error)
        return 4
    except (GateValidationError, GateEvidenceError, OSError) as error:
        _emit_error(errors, error)
        return 3
    except StoreError as error:
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
