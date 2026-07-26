"""Fail-before-effect guards for P0R2 execution sinks.

The public sink wrappers keep authority validation immediately adjacent to the
first externally visible operation.  This module intentionally does not grant
capabilities or mutate the AuthorityStore; it only consumes a live lease.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .contracts import SideEffect
from .stores import AuthorityReader, TaskExecutionLease


class ExecutionAuthorizationError(RuntimeError):
    """Raised when a sink cannot prove its immutable execution intent."""


_SHA256_RE = r"[0-9a-f]{64}\Z"


@dataclass(frozen=True, slots=True)
class RunnerIdentity:
    module: str
    callable_name: str
    source_ref: str
    source_sha256: str

    def __post_init__(self) -> None:
        for field_name in ("module", "callable_name", "source_ref"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value or value != value.strip():
                raise ValueError(f"runner {field_name} must be canonical")
        if (
            not isinstance(self.source_sha256, str)
            or re.fullmatch(_SHA256_RE, self.source_sha256) is None
        ):
            raise ValueError("runner source_sha256 must be a lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class ExecutionInvocation:
    intent_ref: str
    entry_id: str
    effect: SideEffect
    operation: str
    argv: tuple[str, ...]
    cwd: str | None
    runner: RunnerIdentity
    resource_paths: tuple[str, ...]

    def __post_init__(self) -> None:
        for field_name in ("intent_ref", "entry_id", "operation"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value or value != value.strip():
                raise ValueError(f"invocation {field_name} must be canonical")
        if not isinstance(self.effect, SideEffect):
            raise ValueError("invocation effect must be a SideEffect")
        if (
            not isinstance(self.argv, tuple)
            or not self.argv
            or any(
                not isinstance(item, str) or not item or item != item.strip()
                for item in self.argv
            )
        ):
            raise ValueError("invocation argv must contain canonical strings")
        if self.cwd is not None and (
            not isinstance(self.cwd, str) or not self.cwd or self.cwd != self.cwd.strip()
        ):
            raise ValueError("invocation cwd must be canonical or None")
        if (
            not isinstance(self.resource_paths, tuple)
            or any(
                not isinstance(item, str) or not item or item != item.strip()
                for item in self.resource_paths
            )
        ):
            raise ValueError("invocation resource_paths must be canonical")


class AuthorizedSubprocess:
    """Subprocess sink whose runner is unreachable without a live lease."""

    def __init__(
        self,
        *,
        authority_reader: AuthorityReader,
        repository_root: str | Path,
        runner: Callable[..., object] | None = None,
    ) -> None:
        self._authority_reader = authority_reader
        self._repository_root = Path(repository_root)
        self._runner = runner or subprocess.run

    def run(
        self,
        lease: TaskExecutionLease | None,
        invocation: ExecutionInvocation,
    ) -> object:
        if not isinstance(lease, TaskExecutionLease):
            raise ExecutionAuthorizationError("a live task lease is required")
        # The full intent and resource verification is added by the next
        # vertical slice.  Keep this first boundary fail-closed meanwhile.
        raise ExecutionAuthorizationError("execution intent has not been verified")


__all__ = [
    "AuthorizedSubprocess",
    "ExecutionAuthorizationError",
    "ExecutionInvocation",
    "RunnerIdentity",
]
