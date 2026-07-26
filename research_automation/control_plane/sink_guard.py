"""Fail-before-effect guards for P0R2 execution sinks.

The public sink wrappers keep authority validation immediately adjacent to the
first externally visible operation.  This module intentionally does not grant
capabilities or mutate the AuthorityStore; it only consumes a live lease.
"""

from __future__ import annotations

import json
import hashlib
import os
import re
import stat
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .artifact_semantics import ArtifactSemanticError, parse_strict_json
from .contracts import SideEffect, canonical_json
from .stores import (
    AuthorityReader,
    TaskExecutionLease,
    TaskTicketError,
)


class ExecutionAuthorizationError(RuntimeError):
    """Raised when a sink cannot prove its immutable execution intent."""


_SHA256_RE = r"[0-9a-f]{64}\Z"
_INTENT_FIELDS = frozenset(
    {
        "schema_version",
        "intent_id",
        "plan_version",
        "phase",
        "attempt_id",
        "task_id",
        "identity_binding",
        "entry_id",
        "operation",
        "effect",
        "argv",
        "cwd",
        "runner",
        "resource_roots",
        "resource_paths",
        "intent_payload_sha256",
    }
)
_RUNNER_FIELDS = frozenset(
    {"module", "callable_name", "source_ref", "source_sha256"}
)
_IDENTITY_FIELDS = frozenset(
    {"plan_hash", "scope_hash", "instruction_policy_hash"}
)
_ALLOWED_OPERATIONS = frozenset(
    {
        "SUBPROCESS",
        "PATCH_APPLY",
        "REPAIR",
        "FULL_CYCLE",
        "AUTONOMOUS",
        "EXPERIMENT",
        "DISCOVERY",
        "REGISTRY_WRITE",
        "SNAPSHOT_WRITE",
        "HANDOFF_WRITE",
        "KBASE_WRITE",
        "EVOLUTION",
        "BACKLOG",
        "AUTORUN",
        "WRITEBACK",
    }
)
_INTENT_NAMESPACE = "research_state/control_plane/p0r2/intents/"
_PLAN_VERSION = "V3.4.2-P0R2"
_MAX_INTENT_BYTES = 256 * 1024
_MAX_RUNNER_SOURCE_BYTES = 4 * 1024 * 1024


def _canonical_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ExecutionAuthorizationError(
            f"{field_name} must be a canonical non-empty string"
        )
    return value


def _sha256(value: object, field_name: str) -> str:
    value = _canonical_string(value, field_name)
    if re.fullmatch(_SHA256_RE, value) is None:
        raise ExecutionAuthorizationError(
            f"{field_name} must be a lowercase SHA-256"
        )
    return value


def _string_array(
    value: object,
    field_name: str,
    *,
    unique: bool = True,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ExecutionAuthorizationError(f"{field_name} must be an array")
    result = tuple(_canonical_string(item, field_name) for item in value)
    if unique and len(result) != len(set(result)):
        raise ExecutionAuthorizationError(f"{field_name} must not contain duplicates")
    return result


def _reject_unsafe_absolute_lexical_path(raw: str, field_name: str) -> None:
    normalized = raw.replace("\\", "/")
    drive, tail = os.path.splitdrive(normalized)
    if (
        normalized.startswith("//?/")
        or normalized.startswith("//./")
        or normalized.startswith("//")
        or ":" in tail
        or any(character in '<>"|?*' for character in normalized)
        or any(ord(character) < 32 or ord(character) == 127 for character in normalized)
    ):
        raise ExecutionAuthorizationError(
            f"{field_name} contains an unsafe Windows path form"
        )
    if drive and not Path(raw).is_absolute():
        raise ExecutionAuthorizationError(
            f"{field_name} contains a drive-relative path"
        )
    if any(part in {".", ".."} for part in normalized.split("/")):
        raise ExecutionAuthorizationError(
            f"{field_name} contains traversal components"
        )


def _is_reparse_point(path: Path) -> bool:
    try:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        if callable(is_junction) and is_junction():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
        return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    except OSError as error:
        raise ExecutionAuthorizationError(
            f"unable to inspect reparse state for {path}"
        ) from error


def _resolve_absolute_path(
    raw: str,
    *,
    field_name: str,
    require_directory: bool = False,
) -> Path:
    _reject_unsafe_absolute_lexical_path(raw, field_name)
    candidate = Path(raw)
    if not candidate.is_absolute():
        raise ExecutionAuthorizationError(
            f"{field_name} must be absolute"
        )
    current = candidate
    while True:
        if current.exists() and _is_reparse_point(current):
            raise ExecutionAuthorizationError(
                f"{field_name} contains a reparse point"
            )
        if current.parent == current:
            break
        current = current.parent
    try:
        resolved = candidate.resolve(strict=require_directory)
    except (OSError, ValueError) as error:
        raise ExecutionAuthorizationError(
            f"{field_name} cannot be resolved"
        ) from error
    if require_directory and not resolved.is_dir():
        raise ExecutionAuthorizationError(f"{field_name} is not a directory")
    return resolved


@dataclass(frozen=True, slots=True)
class ExecutionPermit:
    lease_id: str
    ticket_id: str
    intent_sha256: str
    effect: SideEffect
    operation: str
    argv: tuple[str, ...]
    cwd: Path | None
    resource_paths: tuple[Path, ...]


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
        if not isinstance(self.runner, RunnerIdentity):
            raise ValueError("invocation runner must be a RunnerIdentity")
        if (
            not isinstance(self.resource_paths, tuple)
            or any(
                not isinstance(item, str) or not item or item != item.strip()
                for item in self.resource_paths
            )
        ):
            raise ValueError("invocation resource_paths must be canonical")
        if len(self.resource_paths) != len(set(self.resource_paths)):
            raise ValueError("invocation resource_paths must be unique")


class ExecutionSinkGuard:
    """Validate one immutable intent immediately before a sink's first effect."""

    def __init__(
        self,
        *,
        authority_reader: AuthorityReader,
        repository_root: str | Path,
    ) -> None:
        if not isinstance(authority_reader, AuthorityReader):
            raise TypeError("authority_reader must be an AuthorityReader")
        try:
            root = Path(repository_root).resolve(strict=True)
        except (OSError, ValueError) as error:
            raise ExecutionAuthorizationError(
                "repository root is unavailable"
            ) from error
        if not root.is_dir() or _is_reparse_point(root):
            raise ExecutionAuthorizationError("repository root is unsafe")
        self._authority_reader = authority_reader
        self._repository_root = root

    def _read_intent(self, reference: str) -> tuple[bytes, Path]:
        reference = _canonical_string(reference, "intent_ref").replace("\\", "/")
        if not reference.startswith(_INTENT_NAMESPACE):
            raise ExecutionAuthorizationError(
                "execution intent must be in the control-plane intent namespace"
            )
        if (
            reference.startswith("/")
            or any(part in {"", ".", ".."} for part in reference.split("/"))
            or ":" in reference
        ):
            raise ExecutionAuthorizationError("execution intent reference is unsafe")
        candidate = self._repository_root.joinpath(*reference.split("/"))
        current = candidate
        while True:
            if current.exists() and _is_reparse_point(current):
                raise ExecutionAuthorizationError(
                    "execution intent path contains a reparse point"
                )
            if current == self._repository_root or current.parent == current:
                break
            current = current.parent
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(self._repository_root)
            if not resolved.is_file():
                raise ExecutionAuthorizationError("execution intent is not a file")
            with resolved.open("rb") as stream:
                raw = stream.read(_MAX_INTENT_BYTES + 1)
        except ExecutionAuthorizationError:
            raise
        except (OSError, ValueError) as error:
            raise ExecutionAuthorizationError("execution intent is unavailable") from error
        if len(raw) > _MAX_INTENT_BYTES:
            raise ExecutionAuthorizationError("execution intent exceeds its size limit")
        return raw, resolved

    def _parse_intent(
        self,
        raw: bytes,
        *,
        binding: object,
        invocation: ExecutionInvocation,
    ) -> ExecutionPermit:
        try:
            payload = parse_strict_json(raw, artifact_name="execution_intent")
        except ArtifactSemanticError as error:
            raise ExecutionAuthorizationError(str(error)) from error
        if set(payload) != _INTENT_FIELDS:
            raise ExecutionAuthorizationError("execution intent field contract is invalid")
        if payload["schema_version"] != "control_plane.execution_intent.v1":
            raise ExecutionAuthorizationError("execution intent schema is unsupported")
        for field_name in ("intent_id", "plan_version", "phase", "attempt_id", "task_id"):
            _canonical_string(payload[field_name], f"intent.{field_name}")
        if payload["plan_version"] != _PLAN_VERSION:
            raise ExecutionAuthorizationError("execution intent plan version is invalid")
        if payload["phase"] != binding.phase.value or payload["attempt_id"] != binding.attempt_id:
            raise ExecutionAuthorizationError("execution intent phase/attempt mismatch")
        if payload["task_id"] != binding.task_id:
            raise ExecutionAuthorizationError("execution intent task mismatch")
        identity = payload["identity_binding"]
        if not isinstance(identity, Mapping) or set(identity) != _IDENTITY_FIELDS:
            raise ExecutionAuthorizationError("execution intent identity is invalid")
        for field_name in _IDENTITY_FIELDS:
            if _sha256(identity[field_name], f"intent.identity.{field_name}") != getattr(
                binding.identity,
                field_name,
            ):
                raise ExecutionAuthorizationError("execution intent identity mismatch")
        entry_id = _canonical_string(payload["entry_id"], "intent.entry_id")
        operation = _canonical_string(payload["operation"], "intent.operation")
        if operation not in _ALLOWED_OPERATIONS:
            raise ExecutionAuthorizationError("execution intent operation is invalid")
        effect_text = _canonical_string(payload["effect"], "intent.effect")
        try:
            effect = SideEffect(effect_text)
        except ValueError as error:
            raise ExecutionAuthorizationError("execution intent effect is invalid") from error
        if effect not in binding.allowed_side_effects:
            raise ExecutionAuthorizationError("lease does not authorize this effect")
        argv = _string_array(payload["argv"], "intent.argv", unique=False)
        cwd_raw = payload["cwd"]
        if cwd_raw is not None:
            cwd_raw = _canonical_string(cwd_raw, "intent.cwd")
        runner = payload["runner"]
        if not isinstance(runner, Mapping) or set(runner) != _RUNNER_FIELDS:
            raise ExecutionAuthorizationError("execution intent runner is invalid")
        runner_values = {
            field_name: _canonical_string(runner[field_name], f"intent.runner.{field_name}")
            for field_name in ("module", "callable_name", "source_ref")
        }
        runner_values["source_sha256"] = _sha256(
            runner["source_sha256"],
            "intent.runner.source_sha256",
        )
        roots_raw = _string_array(payload["resource_roots"], "intent.resource_roots")
        paths_raw = _string_array(payload["resource_paths"], "intent.resource_paths")
        roots = tuple(
            _resolve_absolute_path(value, field_name="intent.resource_root", require_directory=True)
            for value in roots_raw
        )
        resource_paths = tuple(
            _resolve_absolute_path(value, field_name="intent.resource_path")
            for value in paths_raw
        )
        if not roots:
            raise ExecutionAuthorizationError("execution intent requires a resource root")
        for resource in resource_paths:
            if not any(_is_contained(resource, root) for root in roots):
                raise ExecutionAuthorizationError("resource path escapes its approved root")
        cwd = (
            None
            if cwd_raw is None
            else _resolve_absolute_path(cwd_raw, field_name="intent.cwd", require_directory=True)
        )
        if cwd is not None and not any(_is_contained(cwd, root) for root in roots):
            raise ExecutionAuthorizationError("working directory escapes its approved root")
        if (
            entry_id != invocation.entry_id
            or operation != invocation.operation
            or effect is not invocation.effect
            or argv != invocation.argv
            or runner_values["module"] != invocation.runner.module
            or runner_values["callable_name"] != invocation.runner.callable_name
            or runner_values["source_ref"] != invocation.runner.source_ref
            or runner_values["source_sha256"] != invocation.runner.source_sha256
        ):
            raise ExecutionAuthorizationError("invocation differs from immutable intent")
        invocation_cwd = (
            None
            if invocation.cwd is None
            else _resolve_absolute_path(invocation.cwd, field_name="invocation.cwd", require_directory=True)
        )
        if invocation_cwd != cwd or tuple(
            _resolve_absolute_path(value, field_name="invocation.resource_path")
            for value in invocation.resource_paths
        ) != resource_paths:
            raise ExecutionAuthorizationError("invocation resources differ from immutable intent")
        intent_without_hash = dict(payload)
        intent_without_hash.pop("intent_payload_sha256", None)
        expected_intent_hash = hashlib.sha256(
            canonical_json(intent_without_hash).encode("utf-8")
        ).hexdigest()
        intent_sha256 = _sha256(payload["intent_payload_sha256"], "intent_payload_sha256")
        if intent_sha256 != expected_intent_hash:
            raise ExecutionAuthorizationError("execution intent self-hash mismatch")
        return ExecutionPermit(
            lease_id=binding.lease_id,
            ticket_id=binding.ticket_id,
            intent_sha256=intent_sha256,
            effect=effect,
            operation=operation,
            argv=argv,
            cwd=cwd,
            resource_paths=resource_paths,
        )

    def authorize(
        self,
        lease: TaskExecutionLease,
        invocation: ExecutionInvocation,
    ) -> ExecutionPermit:
        if not isinstance(lease, TaskExecutionLease):
            raise ExecutionAuthorizationError("a live task lease is required")
        if not isinstance(invocation, ExecutionInvocation):
            raise ExecutionAuthorizationError("execution invocation is invalid")
        try:
            binding = self._authority_reader.execution_lease_binding(lease)
            try:
                task_spec = json.loads(binding.task_spec_payload_json)
            except (TypeError, ValueError, json.JSONDecodeError) as error:
                raise ExecutionAuthorizationError("task spec payload is invalid") from error
            if not isinstance(task_spec, Mapping):
                raise ExecutionAuthorizationError("task spec payload is invalid")
            evidence_refs = task_spec.get("input_evidence_refs")
            if not isinstance(evidence_refs, list):
                raise ExecutionAuthorizationError("task spec intent evidence is missing")
            matching = [
                item
                for item in evidence_refs
                if isinstance(item, Mapping)
                and item.get("evidence_id") == "execution-intent"
            ]
            if len(matching) != 1:
                raise ExecutionAuthorizationError("task spec must bind one execution intent")
            requirements = task_spec.get("requirements")
            if (
                not isinstance(requirements, Mapping)
                or "execution-intent"
                not in requirements.get("required_evidence_ids", [])
            ):
                raise ExecutionAuthorizationError(
                    "task spec does not require its execution intent"
                )
            evidence = matching[0]
            if (
                evidence.get("evidence_ref") != invocation.intent_ref
                or evidence.get("status") != "VERIFIED"
            ):
                raise ExecutionAuthorizationError("execution intent evidence binding is invalid")
            raw, _ = self._read_intent(invocation.intent_ref)
            if hashlib.sha256(raw).hexdigest() != evidence.get("evidence_sha256"):
                raise ExecutionAuthorizationError("execution intent evidence hash mismatch")
            permit = self._parse_intent(raw, binding=binding, invocation=invocation)
            runner_path = invocation.runner.source_ref.replace("\\", "/")
            if (
                runner_path.startswith("/")
                or any(part in {"", ".", ".."} for part in runner_path.split("/"))
                or ":" in runner_path
            ):
                raise ExecutionAuthorizationError("runner source reference is unsafe")
            source = self._repository_root.joinpath(*runner_path.split("/"))
            current = source
            while True:
                if current.exists() and _is_reparse_point(current):
                    raise ExecutionAuthorizationError(
                        "runner source path contains a reparse point"
                    )
                if current == self._repository_root or current.parent == current:
                    break
                current = current.parent
            resolved_source = source.resolve(strict=True)
            resolved_source.relative_to(self._repository_root)
            with resolved_source.open("rb") as stream:
                source_bytes = stream.read(_MAX_RUNNER_SOURCE_BYTES + 1)
            if len(source_bytes) > _MAX_RUNNER_SOURCE_BYTES:
                raise ExecutionAuthorizationError(
                    "runner source exceeds its size limit"
                )
            source_sha256 = hashlib.sha256(source_bytes).hexdigest()
            if source_sha256 != invocation.runner.source_sha256:
                raise ExecutionAuthorizationError("runner source identity changed")
            return permit
        except ExecutionAuthorizationError:
            raise
        except (TaskTicketError, OSError, ValueError) as error:
            raise ExecutionAuthorizationError("execution authority is unavailable") from error


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
        self._guard = ExecutionSinkGuard(
            authority_reader=authority_reader,
            repository_root=repository_root,
        )
        self._runner = runner or subprocess.run

    def run(
        self,
        lease: TaskExecutionLease | None,
        invocation: ExecutionInvocation,
    ) -> object:
        if not isinstance(lease, TaskExecutionLease):
            raise ExecutionAuthorizationError("a live task lease is required")
        permit = self._guard.authorize(lease, invocation)
        if (
            permit.operation != "SUBPROCESS"
            or permit.effect is not SideEffect.START_SUBPROCESS
            or permit.cwd is None
        ):
            raise ExecutionAuthorizationError(
                "subprocess sink requires a bound subprocess operation and cwd"
            )
        kwargs: dict[str, object] = {}
        if permit.cwd is not None:
            kwargs["cwd"] = str(permit.cwd)
        return self._runner(list(permit.argv), **kwargs)


class AuthorizedPatchApplier:
    """Patch sink whose audit write and Git runner are fenced by one lease."""

    def __init__(
        self,
        *,
        authority_reader: AuthorityReader,
        repository_root: str | Path,
        runner: Callable[..., object] | None = None,
    ) -> None:
        self._guard = ExecutionSinkGuard(
            authority_reader=authority_reader,
            repository_root=repository_root,
        )
        try:
            self._repository_root = Path(repository_root).resolve(strict=True)
        except (OSError, ValueError) as error:
            raise ExecutionAuthorizationError(
                "repository root is unavailable"
            ) from error
        self._runner = runner or subprocess.run

    def apply(
        self,
        lease: TaskExecutionLease | None,
        invocation: ExecutionInvocation,
        diff_text: str,
        *,
        audit_path: str | Path,
    ) -> object:
        if not isinstance(lease, TaskExecutionLease):
            raise ExecutionAuthorizationError("a live task lease is required")
        if (
            invocation.runner.module != __name__
            or invocation.runner.callable_name != "AuthorizedPatchApplier.apply"
        ):
            raise ExecutionAuthorizationError(
                "patch invocation entry identity is invalid"
            )
        permit = self._guard.authorize(lease, invocation)
        if (
            permit.operation != "PATCH_APPLY"
            or permit.effect is not SideEffect.GIT_MUTATION
            or permit.cwd is None
        ):
            raise ExecutionAuthorizationError(
                "patch sink requires a bound patch operation and workspace"
            )
        if not isinstance(diff_text, str) or not diff_text:
            raise ExecutionAuthorizationError("patch text must be a non-empty string")
        try:
            diff_bytes = diff_text.encode("utf-8", errors="strict")
        except UnicodeError as error:
            raise ExecutionAuthorizationError("patch text must be valid UTF-8") from error
        if len(diff_bytes) > 4 * 1024 * 1024:
            raise ExecutionAuthorizationError("patch text exceeds its size limit")

        audit = _resolve_absolute_path(str(audit_path), field_name="audit_path")
        if audit.exists():
            raise ExecutionAuthorizationError("audit path already exists")
        audit_parent = _resolve_absolute_path(
            str(audit.parent),
            field_name="audit_parent",
            require_directory=True,
        )
        if audit_parent != audit.parent:
            raise ExecutionAuthorizationError("audit path parent is not canonical")
        if audit == permit.cwd or not _is_contained(audit, permit.cwd):
            raise ExecutionAuthorizationError("audit path is outside the isolated workspace")

        targets = _parse_patch_target_paths(diff_text)
        target_paths: list[Path] = []
        target_states: dict[Path, tuple[bool, str | None]] = {}
        for relative in targets:
            target = _resolve_absolute_path(
                str(permit.cwd / relative),
                field_name="patch_target",
            )
            if not _is_contained(target, permit.cwd):
                raise ExecutionAuthorizationError("patch target escapes the workspace")
            if target.exists():
                if not target.is_file():
                    raise ExecutionAuthorizationError("patch target must be a regular file")
                target_states[target] = (
                    True,
                    hashlib.sha256(target.read_bytes()).hexdigest(),
                )
            else:
                if target.is_symlink():
                    raise ExecutionAuthorizationError("new patch target may not be a symlink")
                parent = _resolve_absolute_path(
                    str(target.parent),
                    field_name="patch_target_parent",
                    require_directory=True,
                )
                if parent != target.parent:
                    raise ExecutionAuthorizationError(
                        "new patch target parent is not canonical"
                    )
                target_states[target] = (False, None)
            target_paths.append(target)
        if not target_paths:
            raise ExecutionAuthorizationError("patch contains no target files")
        if len(set(target_paths)) != len(target_paths):
            raise ExecutionAuthorizationError("patch target list contains duplicates")
        expected_resources = tuple(target_paths) + (audit,)
        if permit.resource_paths != expected_resources:
            raise ExecutionAuthorizationError(
                "patch targets and audit path are not exactly bound by the intent"
            )
        expected_argv = ("git", "apply", str(audit))
        if permit.argv != expected_argv:
            raise ExecutionAuthorizationError(
                "patch Git argv is not the immutable authorized command"
            )
        if permit.cwd == self._repository_root:
            raise ExecutionAuthorizationError("patch workspace must be isolated from repository root")

        # Every target, the audit parent, the exact argv and the workspace have
        # been validated before the first write or subprocess invocation.
        try:
            with audit.open("xb") as stream:
                stream.write(diff_bytes)
                stream.flush()
                os.fsync(stream.fileno())
        except (OSError, ValueError) as error:
            raise ExecutionAuthorizationError("unable to persist patch audit") from error
        audit_sha256 = hashlib.sha256(diff_bytes).hexdigest()

        def _revalidate_state() -> None:
            if not audit.is_file() or _is_reparse_point(audit):
                raise ExecutionAuthorizationError("patch audit path changed before Git effect")
            if hashlib.sha256(audit.read_bytes()).hexdigest() != audit_sha256:
                raise ExecutionAuthorizationError("patch audit bytes changed before Git effect")
            for target, (existed, before_hash) in target_states.items():
                now_exists = target.exists()
                if now_exists != existed:
                    raise ExecutionAuthorizationError("patch target existence changed before Git effect")
                if not now_exists:
                    if target.is_symlink():
                        raise ExecutionAuthorizationError("new patch target became a symlink")
                    continue
                if not target.is_file() or _is_reparse_point(target):
                    raise ExecutionAuthorizationError("patch target changed to an unsafe file")
                if hashlib.sha256(target.read_bytes()).hexdigest() != before_hash:
                    raise ExecutionAuthorizationError("patch target bytes changed before Git effect")

        _revalidate_state()

        common_kwargs: dict[str, object] = {
            "cwd": str(permit.cwd),
            "capture_output": True,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
            "check": False,
        }
        check_argv = ["git", "apply", "--check", str(audit)]
        check_result = self._runner(check_argv, **common_kwargs)
        if getattr(check_result, "returncode", None) != 0:
            raise ExecutionAuthorizationError("git apply --check rejected the patch")
        _revalidate_state()
        apply_result = self._runner(list(permit.argv), **common_kwargs)
        if getattr(apply_result, "returncode", None) != 0:
            raise ExecutionAuthorizationError("git apply rejected the authorized patch")
        return apply_result


class AuthorizedPathMutation:
    """Revalidate one exact set of paths immediately before a file mutation."""

    def __init__(
        self,
        *,
        authority_reader: AuthorityReader,
        repository_root: str | Path,
    ) -> None:
        self._guard = ExecutionSinkGuard(
            authority_reader=authority_reader,
            repository_root=repository_root,
        )

    def authorize(
        self,
        lease: TaskExecutionLease | None,
        invocation: ExecutionInvocation,
        *,
        operation: str,
        effect: SideEffect,
        module: str,
        callable_name: str,
        paths: Sequence[str | Path],
    ) -> ExecutionPermit:
        if not isinstance(lease, TaskExecutionLease):
            raise ExecutionAuthorizationError("a live task lease is required")
        if not isinstance(invocation, ExecutionInvocation):
            raise ExecutionAuthorizationError("execution invocation is invalid")
        permit = self._guard.authorize(lease, invocation)
        if permit.operation != operation or permit.effect is not effect:
            raise ExecutionAuthorizationError("path mutation operation/effect is invalid")
        if (
            invocation.runner.module != module
            or invocation.runner.callable_name != callable_name
        ):
            raise ExecutionAuthorizationError("path mutation entry identity is invalid")
        resolved_paths = tuple(
            _resolve_absolute_path(str(path), field_name="mutation_path")
            for path in paths
        )
        if permit.resource_paths != resolved_paths:
            raise ExecutionAuthorizationError(
                "path mutation resources differ from immutable intent"
            )
        return permit


def _parse_patch_target_paths(diff_text: str) -> tuple[str, ...]:
    """Return canonical relative paths from a deliberately narrow unified diff."""
    lines = diff_text.splitlines()
    targets: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line.startswith("--- "):
            index += 1
            continue
        if index + 1 >= len(lines) or not lines[index + 1].startswith("+++ "):
            raise ExecutionAuthorizationError("patch has an incomplete file header")
        old_name = _parse_patch_header_path(line[4:], prefix="a/")
        new_name = _parse_patch_header_path(lines[index + 1][4:], prefix="b/")
        if old_name is None and new_name is None:
            raise ExecutionAuthorizationError("patch file header cannot be /dev/null twice")
        if old_name is not None and new_name is not None and old_name != new_name:
            raise ExecutionAuthorizationError("patch renames are not authorized")
        target_name = new_name if new_name is not None else old_name
        if target_name is None:
            raise ExecutionAuthorizationError("patch target path is missing")
        targets.append(target_name)
        index += 2
    if not targets:
        raise ExecutionAuthorizationError("patch has no unified file headers")
    if len(set(targets)) != len(targets):
        raise ExecutionAuthorizationError("patch contains duplicate file headers")
    return tuple(targets)


def _parse_patch_header_path(raw: str, *, prefix: str) -> str | None:
    if "\t" in raw:
        raise ExecutionAuthorizationError("patch headers may not contain timestamps")
    value = raw.strip()
    if value == "/dev/null":
        return None
    if not value.startswith(prefix):
        raise ExecutionAuthorizationError("patch header path prefix is invalid")
    value = value[len(prefix) :]
    if (
        not value
        or "\\" in value
        or value.startswith("/")
        or ":" in value
        or any(character in '<>\"|?*' for character in value)
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ExecutionAuthorizationError("patch header path is unsafe")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ExecutionAuthorizationError("patch header path contains traversal")
    return "/".join(parts)


def _is_contained(candidate: Path, root: Path) -> bool:
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    return True


__all__ = [
    "AuthorizedPathMutation",
    "AuthorizedPatchApplier",
    "AuthorizedSubprocess",
    "ExecutionAuthorizationError",
    "ExecutionInvocation",
    "RunnerIdentity",
]
