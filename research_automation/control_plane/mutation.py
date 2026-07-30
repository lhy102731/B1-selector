"""Disposable, allowlisted source mutation transactions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import shlex
import subprocess
import tempfile
import threading
import time
from types import MappingProxyType

from research_automation.control_plane.contracts import (
    Actor,
    Phase,
    SideEffect,
    canonical_json,
)
from research_automation.control_plane.stores import (
    AuthorityReader,
    TaskExecutionLease,
    TaskTicketError,
)


class MutationRejected(RuntimeError):
    """Raised before an unsafe or out-of-scope patch can touch a workspace."""


class MutationStateInDoubt(MutationRejected):
    """Raised when a started container cannot be proven absent."""


@dataclass(frozen=True, slots=True)
class MutationTestReceipt:
    argv: tuple[str, ...]
    returncode: int
    stdout_sha256: str
    stderr_sha256: str
    workspace_sha256: tuple[tuple[str, str], ...]
    sandbox_profile_sha256: str
    sandbox_controls: tuple[str, ...]
    container_runtime_sha256: str
    container_image: str


@dataclass(frozen=True, slots=True)
class MutationResult:
    actor: Actor
    changed_files: tuple[str, ...]
    files: Mapping[str, bytes]
    before_sha256: Mapping[str, str]
    after_sha256: Mapping[str, str]
    selected_test_receipts: tuple[MutationTestReceipt, ...]


@dataclass(frozen=True, slots=True)
class _BoundedProcessResult:
    returncode: int
    stdout_sha256: str
    stderr_sha256: str
    stdout_sample: bytes


def _stream_sha256(
    stream,
    limit: int,
    overflow: threading.Event,
    capture_bytes: int,
) -> tuple[str, bytes]:
    digest = hashlib.sha256()
    sample = bytearray()
    total = 0
    while True:
        chunk = stream.read(64 * 1024)
        if not chunk:
            return digest.hexdigest(), bytes(sample)
        total += len(chunk)
        if total > limit:
            overflow.set()
            continue
        digest.update(chunk)
        if len(sample) < capture_bytes:
            sample.extend(chunk[: capture_bytes - len(sample)])


def _run_bounded_process(
    argv: tuple[str, ...],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout_seconds: float,
    output_limit_bytes: int,
    capture_stdout_bytes: int = 0,
    operation_label: str = "selected test",
) -> _BoundedProcessResult:
    try:
        process = subprocess.Popen(
            argv,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as error:
        raise MutationRejected(f"{operation_label} unavailable") from error
    assert process.stdout is not None and process.stderr is not None
    overflow = threading.Event()
    stream_results: dict[str, tuple[str, bytes]] = {}
    stream_errors: list[BaseException] = []

    def consume(name: str, stream) -> None:
        try:
            stream_results[name] = _stream_sha256(
                stream,
                output_limit_bytes,
                overflow,
                capture_stdout_bytes if name == "stdout" else 0,
            )
        except (OSError, ValueError) as error:
            stream_errors.append(error)

    readers = (
        threading.Thread(target=consume, args=("stdout", process.stdout), daemon=True),
        threading.Thread(target=consume, args=("stderr", process.stderr), daemon=True),
    )
    for reader in readers:
        reader.start()
    deadline = time.monotonic() + timeout_seconds
    failure: str | None = None
    while process.poll() is None:
        if overflow.is_set():
            failure = f"{operation_label} output exceeded limit"
            break
        if time.monotonic() >= deadline:
            failure = f"{operation_label} timed out"
            break
        overflow.wait(0.02)
    if failure is not None:
        process.kill()
    try:
        returncode = process.wait(timeout=5)
    except subprocess.TimeoutExpired as error:
        process.kill()
        raise MutationRejected(f"{operation_label} process did not stop") from error
    for reader in readers:
        reader.join(timeout=5)
    if any(reader.is_alive() for reader in readers):
        raise MutationRejected(f"{operation_label} output stream did not close")
    if stream_errors:
        raise MutationRejected(f"{operation_label} output stream failed") from stream_errors[0]
    if failure is None and overflow.is_set():
        failure = f"{operation_label} output exceeded limit"
    if failure is not None:
        raise MutationRejected(failure)
    return _BoundedProcessResult(
        returncode=returncode,
        stdout_sha256=stream_results["stdout"][0],
        stderr_sha256=stream_results["stderr"][0],
        stdout_sample=stream_results["stdout"][1],
    )


def _canonical_relative_path(value: str) -> str:
    path = value[2:] if value.startswith(("a/", "b/")) else value
    path = path.replace("\\", "/")
    if (
        not path
        or path.startswith("/")
        or ":" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
    ):
        raise MutationRejected("patch path is unsafe")
    return path


def _windows_path_key(value: str) -> str:
    return "/".join(
        part.rstrip(" .").casefold()
        for part in value.replace("\\", "/").rstrip("/").split("/")
    )


def _patch_header_path(value: str) -> str | None:
    try:
        fields = shlex.split(value)
    except ValueError as error:
        raise MutationRejected("patch header is malformed") from error
    if len(fields) != 1:
        raise MutationRejected("patch header is malformed")
    if fields[0] == "/dev/null":
        return None
    return _canonical_relative_path(fields[0])


def _has_reparse_component(root: Path, relative: str) -> bool:
    current = root
    for part in relative.split("/"):
        current = current / part
        if current.is_symlink() or (
            hasattr(current, "is_junction") and current.is_junction()
        ):
            return True
    return False


def _has_nested_repository(root: Path, relative: str) -> bool:
    current = root
    for part in relative.split("/")[:-1]:
        current = current / part
        marker = current / ".git"
        if marker.exists() or marker.is_symlink() or (
            hasattr(marker, "is_junction") and marker.is_junction()
        ):
            return True
    return False


class MutationTransaction:
    """Validate and eventually execute one patch in a disposable workspace."""

    def __init__(
        self,
        *,
        repository_root: str | Path,
        allowed_files: tuple[str, ...],
        selected_tests: tuple[tuple[str, ...], ...],
        authority_lease: TaskExecutionLease,
        sandbox_policy_bytes: bytes,
        support_files: tuple[str, ...] = (),
    ) -> None:
        self._root = Path(repository_root).resolve()
        if not self._root.is_dir():
            raise MutationRejected("repository root is unavailable")
        if not isinstance(authority_lease, TaskExecutionLease):
            raise MutationRejected("Authority-bound mutation lease is required")
        try:
            binding = AuthorityReader().execution_lease_binding(authority_lease)
        except (TaskTicketError, OSError, ValueError) as error:
            raise MutationRejected("mutation authority is unavailable") from error
        required_effects = {
            SideEffect.READ,
            SideEffect.WRITE_STAGING,
            SideEffect.GIT_MUTATION,
            SideEffect.START_SUBPROCESS,
        }
        if binding.phase is not Phase.P4:
            raise MutationRejected("mutation requires P4 authority")
        if not required_effects.issubset(binding.allowed_side_effects):
            raise MutationRejected("mutation lease lacks required authority")
        if not isinstance(sandbox_policy_bytes, bytes) or not sandbox_policy_bytes:
            raise MutationRejected("sandbox policy is unavailable")
        if len(sandbox_policy_bytes) > 64 * 1024:
            raise MutationRejected("sandbox policy is too large")
        try:
            sandbox_policy = json.loads(sandbox_policy_bytes.decode("utf-8", "strict"))
            task_spec = json.loads(binding.task_spec_payload_json)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise MutationRejected("sandbox policy binding is invalid") from error
        policy_fields = {
            "schema_version",
            "policy_id",
            "container_runtime_path",
            "container_runtime_sha256",
            "container_image",
            "daemon_uri",
            "repository_root",
            "allowed_files",
            "support_files",
            "selected_tests",
            "git_runtime_path",
            "git_runtime_sha256",
        }
        if (
            not isinstance(sandbox_policy, dict)
            or set(sandbox_policy) != policy_fields
            or sandbox_policy.get("schema_version")
            != "control_plane.mutation_sandbox_policy.v2"
            or canonical_json(sandbox_policy).encode("utf-8") != sandbox_policy_bytes
        ):
            raise MutationRejected("sandbox policy binding is invalid")
        policy_sha256 = hashlib.sha256(sandbox_policy_bytes).hexdigest()
        evidence_refs = task_spec.get("input_evidence_refs", [])
        matching_evidence = [
            evidence
            for evidence in evidence_refs
            if isinstance(evidence, dict)
            and evidence.get("evidence_id") == "mutation-sandbox-policy"
        ]
        if len(matching_evidence) != 1 or (
            matching_evidence[0].get("status") != "VERIFIED"
            or matching_evidence[0].get("evidence_sha256") != policy_sha256
        ):
            raise MutationRejected("sandbox policy is not Authority-bound")
        try:
            policy_allowed_files = frozenset(
                _canonical_relative_path(path)
                for path in sandbox_policy["allowed_files"]
            )
            policy_support_files = frozenset(
                _canonical_relative_path(path)
                for path in sandbox_policy["support_files"]
            )
            policy_selected_tests = tuple(
                tuple(command) for command in sandbox_policy["selected_tests"]
            )
        except (TypeError, ValueError) as error:
            raise MutationRejected("sandbox mutation scope is invalid") from error
        task_allowed_files = task_spec.get("allowed_files")
        forbidden_files = task_spec.get("forbidden_files")
        if (
            sandbox_policy.get("repository_root") != self._root.as_posix()
            or not isinstance(task_allowed_files, list)
            or frozenset(task_allowed_files) != policy_allowed_files
            or not isinstance(forbidden_files, list)
        ):
            raise MutationRejected("sandbox mutation scope is not Authority-bound")
        for relative in policy_allowed_files | policy_support_files:
            normalized = _windows_path_key(relative)
            if any(
                normalized == _windows_path_key(rule)
                or normalized.startswith(_windows_path_key(rule) + "/")
                for rule in forbidden_files
                if isinstance(rule, str) and ":" not in rule
            ):
                raise MutationRejected("sandbox mutation scope intersects forbidden files")
        runtime_path = Path(str(sandbox_policy["container_runtime_path"]))
        try:
            if _has_reparse_component(runtime_path.parent, runtime_path.name):
                raise MutationRejected("container runtime is unsafe")
            runtime_path = runtime_path.resolve(strict=True)
            runtime_path.relative_to(self._root)
        except ValueError:
            pass
        except OSError as error:
            raise MutationRejected("container runtime is unavailable") from error
        else:
            raise MutationRejected("container runtime must be outside repository")
        if not runtime_path.is_file():
            raise MutationRejected("container runtime is unsafe")
        if runtime_path.name.lower() not in {"docker.exe", "podman.exe"}:
            raise MutationRejected("container runtime identity is invalid")
        container_runtime_sha256 = str(sandbox_policy["container_runtime_sha256"])
        if not re.fullmatch(r"[0-9a-f]{64}", container_runtime_sha256):
            raise MutationRejected("container runtime hash is invalid")
        if hashlib.sha256(runtime_path.read_bytes()).hexdigest() != container_runtime_sha256:
            raise MutationRejected("container runtime hash mismatch")
        container_image = str(sandbox_policy["container_image"])
        if not re.fullmatch(
            r"[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}",
            container_image,
        ):
            raise MutationRejected("container image digest is invalid")
        daemon_uri = sandbox_policy.get("daemon_uri")
        if daemon_uri not in {
            "npipe:////./pipe/dockerDesktopLinuxEngine",
            "npipe:////./pipe/docker_engine",
        }:
            raise MutationRejected("container daemon binding is invalid")
        git_runtime_path = Path(str(sandbox_policy["git_runtime_path"]))
        try:
            if _has_reparse_component(git_runtime_path.parent, git_runtime_path.name):
                raise MutationRejected("Git runtime is unsafe")
            git_runtime_path = git_runtime_path.resolve(strict=True)
            git_runtime_path.relative_to(self._root)
        except ValueError:
            pass
        except OSError as error:
            raise MutationRejected("Git runtime is unavailable") from error
        else:
            raise MutationRejected("Git runtime must be outside repository")
        git_runtime_sha256 = str(sandbox_policy["git_runtime_sha256"])
        if (
            git_runtime_path.name.lower() != "git.exe"
            or re.fullmatch(r"[0-9a-f]{64}", git_runtime_sha256) is None
            or hashlib.sha256(git_runtime_path.read_bytes()).hexdigest()
            != git_runtime_sha256
        ):
            raise MutationRejected("Git runtime identity is invalid")
        if not isinstance(selected_tests, tuple) or not selected_tests:
            raise MutationRejected("selected tests are required")
        for command in selected_tests:
            if (
                not isinstance(command, tuple)
                or not command
                or any(
                    not isinstance(argument, str)
                    or not argument
                    or "\x00" in argument
                    for argument in command
                )
            ):
                raise MutationRejected("selected test command is invalid")
            if any(separator in command[0] for separator in ("/", "\\", ":")):
                raise MutationRejected("selected test container command is invalid")
        self._actor = binding.actor
        self._authority_lease = authority_lease
        self._sandbox_policy_sha256 = policy_sha256
        self._container_runtime = runtime_path
        self._container_runtime_sha256 = container_runtime_sha256
        self._container_image = container_image
        self._daemon_uri = str(daemon_uri)
        self._git_runtime = git_runtime_path
        self._git_runtime_sha256 = git_runtime_sha256
        self._selected_tests = selected_tests
        self._allowed_files = frozenset(
            _canonical_relative_path(path) for path in allowed_files
        )
        self._support_files = frozenset(
            _canonical_relative_path(path) for path in support_files
        )
        if self._allowed_files & self._support_files:
            raise MutationRejected("support files must be read-only")
        if (
            self._allowed_files != policy_allowed_files
            or self._support_files != policy_support_files
            or self._selected_tests != policy_selected_tests
        ):
            raise MutationRejected("mutation request differs from Authority-bound scope")
        self._staged_files = self._allowed_files | self._support_files

    def _revalidate_authority(self) -> None:
        try:
            binding = AuthorityReader().execution_lease_binding(self._authority_lease)
            task_spec = json.loads(binding.task_spec_payload_json)
        except (TaskTicketError, OSError, ValueError, json.JSONDecodeError) as error:
            raise MutationRejected("mutation authority changed") from error
        evidence_refs = task_spec.get("input_evidence_refs", [])
        if (
            binding.actor != self._actor
            or binding.phase is not Phase.P4
            or not {
                SideEffect.READ,
                SideEffect.WRITE_STAGING,
                SideEffect.GIT_MUTATION,
                SideEffect.START_SUBPROCESS,
            }.issubset(binding.allowed_side_effects)
            or sum(
                1
                for evidence in evidence_refs
                if isinstance(evidence, dict)
                and evidence.get("evidence_id") == "mutation-sandbox-policy"
                and evidence.get("status") == "VERIFIED"
                and evidence.get("evidence_sha256") == self._sandbox_policy_sha256
            )
            != 1
        ):
            raise MutationRejected("mutation authority changed")

    def _cleanup_container(
        self,
        container_identity: str,
        *,
        filter_expression: str,
        identity_known: bool,
        runtime_path: Path,
        cwd: Path,
        env: dict[str, str],
    ) -> None:
        deadline = time.monotonic() + 5
        absent_since: float | None = None
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            command_timeout = max(0.1, min(1.0, remaining))
            try:
                if (
                    hashlib.sha256(runtime_path.read_bytes()).hexdigest()
                    != self._container_runtime_sha256
                ):
                    raise MutationRejected("staged container runtime changed")
                removal = _run_bounded_process(
                    (str(runtime_path), "rm", "--force", container_identity),
                    cwd=cwd,
                    env=env,
                    timeout_seconds=command_timeout,
                    output_limit_bytes=64 * 1024,
                )
                listing = _run_bounded_process(
                    (
                        str(runtime_path),
                        "container",
                        "ls",
                        "--all",
                        "--quiet",
                        "--filter",
                        filter_expression,
                    ),
                    cwd=cwd,
                    env=env,
                    timeout_seconds=command_timeout,
                    output_limit_bytes=64 * 1024,
                    capture_stdout_bytes=256,
                )
                if removal.returncode not in {0, 1} or listing.returncode != 0:
                    raise MutationRejected("container daemon cleanup command failed")
            except (MutationRejected, OSError):
                absent_since = None
                time.sleep(0.1)
                continue
            if listing.stdout_sample.strip():
                absent_since = None
            else:
                now = time.monotonic()
                if absent_since is None:
                    absent_since = now
                stable_seconds = now - absent_since
                if identity_known and stable_seconds >= 0.1:
                    return
            time.sleep(0.1)
        raise MutationStateInDoubt(
            "selected test container state is IN_DOUBT after cleanup deadline"
        )

    def apply(self, patch: bytes) -> MutationResult:
        if not isinstance(patch, bytes) or not patch:
            raise MutationRejected("patch must be non-empty bytes")
        if len(patch) > 4 * 1024 * 1024:
            raise MutationRejected("patch exceeds size limit")
        try:
            text = patch.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise MutationRejected("patch must be strict UTF-8") from error
        lines = text.splitlines()
        mode_prefixes = (
            "new file mode ",
            "new mode ",
            "old mode ",
            "deleted file mode ",
        )
        if any(
            line.startswith(mode_prefixes)
            and line.rsplit(" ", 1)[-1] in {"120000", "160000"}
            for line in lines
        ):
            raise MutationRejected("unsafe patch object mode")
        paths: set[str] = set()
        in_hunk = False
        for line in lines:
            if line.startswith("diff --git "):
                in_hunk = False
                try:
                    fields = shlex.split(line)
                except ValueError as error:
                    raise MutationRejected("patch header is malformed") from error
                if len(fields) != 4:
                    raise MutationRejected("patch header is malformed")
                paths.update(
                    (
                        _canonical_relative_path(fields[2]),
                        _canonical_relative_path(fields[3]),
                    )
                )
                continue
            if line.startswith("@@ "):
                in_hunk = True
                continue
            if line == "GIT binary patch" or line.startswith(
                mode_prefixes + ("rename from ", "rename to ", "copy from ", "copy to ")
            ):
                raise MutationRejected("unsupported patch operation")
            if in_hunk:
                if line.startswith(("--- ", "+++ ")):
                    raise MutationRejected("ambiguous patch header")
                continue
            for prefix in ("--- ", "+++ ", "rename from ", "rename to ", "copy from ", "copy to "):
                if line.startswith(prefix):
                    header_path = _patch_header_path(line[len(prefix) :])
                    if header_path is None:
                        raise MutationRejected("unsupported patch operation")
                    paths.add(header_path)
                    break
        if not paths:
            raise MutationRejected("patch has no bounded file headers")
        if not paths.issubset(self._allowed_files):
            raise MutationRejected("patch path is outside the allowlist")
        before: dict[str, str] = {}
        with tempfile.TemporaryDirectory(prefix="control-plane-mutation-") as tmp:
            transaction_root = Path(tmp)
            workspace = transaction_root / "workspace"
            control = transaction_root / "control"
            workspace.mkdir()
            control.mkdir()
            docker_config = control / "docker-config"
            docker_config.mkdir()
            (docker_config / "config.json").write_bytes(b"{}")
            runtime_copy = control / self._container_runtime.name
            try:
                shutil.copy2(self._container_runtime, runtime_copy)
                runtime_copy_sha256 = hashlib.sha256(runtime_copy.read_bytes()).hexdigest()
            except OSError as error:
                raise MutationRejected("container runtime staging failed") from error
            if runtime_copy_sha256 != self._container_runtime_sha256:
                raise MutationRejected("container runtime staging failed")
            patch_file = control / "mutation.diff"
            patch_file.write_bytes(patch)
            patch_sha256 = hashlib.sha256(patch).hexdigest()
            for relative in sorted(self._staged_files):
                source = self._root.joinpath(*relative.split("/"))
                try:
                    if _has_reparse_component(self._root, relative):
                        raise MutationRejected("allowlisted source is unsafe")
                    if _has_nested_repository(self._root, relative):
                        raise MutationRejected("allowlisted source crosses nested repository")
                    resolved = source.resolve(strict=True)
                    resolved.relative_to(self._root)
                except (OSError, ValueError) as error:
                    raise MutationRejected("allowlisted source is unavailable") from error
                if not resolved.is_file():
                    raise MutationRejected("allowlisted source is unsafe")
                raw = resolved.read_bytes()
                before[relative] = hashlib.sha256(raw).hexdigest()
                target = workspace.joinpath(*relative.split("/"))
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(resolved, target)
            for command in (
                (
                    str(self._git_runtime),
                    "-c",
                    "core.autocrlf=false",
                    "apply",
                    "--check",
                    str(patch_file),
                ),
                (
                    str(self._git_runtime),
                    "-c",
                    "core.autocrlf=false",
                    "apply",
                    str(patch_file),
                ),
            ):
                self._revalidate_authority()
                try:
                    current_git_sha256 = hashlib.sha256(
                        self._git_runtime.read_bytes()
                    ).hexdigest()
                except OSError as error:
                    raise MutationRejected("Git runtime changed") from error
                if current_git_sha256 != self._git_runtime_sha256:
                    raise MutationRejected("Git runtime changed")
                if hashlib.sha256(patch_file.read_bytes()).hexdigest() != patch_sha256:
                    raise MutationRejected("patch bytes changed before Git apply")
                system_root = os.environ.get("SystemRoot")
                comspec = os.environ.get("ComSpec")
                if not system_root or not comspec:
                    raise MutationRejected("Git runtime environment is unavailable")
                completed = _run_bounded_process(
                    command,
                    cwd=workspace,
                    env={
                        "SystemRoot": system_root,
                        "ComSpec": comspec,
                        "TEMP": str(control),
                        "TMP": str(control),
                        "HOME": str(control),
                        "GIT_CONFIG_NOSYSTEM": "1",
                        "GIT_CONFIG_GLOBAL": os.devnull,
                        "GIT_TERMINAL_PROMPT": "0",
                    },
                    timeout_seconds=30,
                    output_limit_bytes=1024 * 1024,
                    operation_label="Git",
                )
                if completed.returncode != 0:
                    raise MutationRejected("git apply rejected the patch")
            files = {
                relative: workspace.joinpath(*relative.split("/")).read_bytes()
                for relative in sorted(paths)
            }
            for relative, raw in files.items():
                if relative.endswith(".py"):
                    try:
                        compile(raw, relative, "exec")
                    except (SyntaxError, ValueError) as error:
                        raise MutationRejected("patched Python file does not compile") from error
            workspace_sha256 = {
                relative: hashlib.sha256(
                    workspace.joinpath(*relative.split("/")).read_bytes()
                ).hexdigest()
                for relative in sorted(self._staged_files)
            }
            test_receipts: list[MutationTestReceipt] = []
            for command in self._selected_tests:
                self._revalidate_authority()
                try:
                    runtime_sha256 = hashlib.sha256(
                        self._container_runtime.read_bytes()
                    ).hexdigest()
                except OSError as error:
                    raise MutationRejected("container runtime changed") from error
                if runtime_sha256 != self._container_runtime_sha256:
                    raise MutationRejected("container runtime changed")
                container_name = f"cp-mutation-{secrets.token_hex(12)}"
                cidfile = control / f"{container_name}.cid"
                sandbox_controls = (
                    "--pull=never",
                    "--network=none",
                    "--read-only",
                    "--cap-drop=ALL",
                    "--security-opt=no-new-privileges:true",
                    "--pids-limit=64",
                    "--memory=512m",
                    "--cpus=1",
                    "--user=65534:65534",
                    "workspace-mount=readonly",
                    "tmpfs=/tmp:noexec,nosuid",
                )
                create_arguments = (
                    "create",
                    "--rm",
                    f"--name={container_name}",
                    f"--cidfile={cidfile}",
                    *sandbox_controls[:-2],
                    "--mount",
                    f"type=bind,source={workspace},target=/workspace,readonly",
                    "--tmpfs",
                    "/tmp:rw,noexec,nosuid,size=67108864",
                    "--workdir=/workspace",
                    "--env=HOME=/tmp",
                    "--env=PYTHONDONTWRITEBYTECODE=1",
                    "--env=PYTHONPYCACHEPREFIX=/tmp/pycache",
                    self._container_image,
                    *command,
                )
                system_root = os.environ.get("SystemRoot")
                comspec = os.environ.get("ComSpec")
                if not system_root or not comspec:
                    raise MutationRejected("container runtime environment is unavailable")
                container_env = {
                    "SystemRoot": system_root,
                    "ComSpec": comspec,
                    "TEMP": str(control),
                    "TMP": str(control),
                    "DOCKER_HOST": self._daemon_uri,
                    "DOCKER_CONFIG": str(docker_config),
                }
                cleanup_identity = container_name
                cleanup_filter = f"name=^/{container_name}$"
                identity_known = False
                try:
                    if (
                        hashlib.sha256(runtime_copy.read_bytes()).hexdigest()
                        != self._container_runtime_sha256
                    ):
                        raise MutationRejected("staged container runtime changed")
                    created = _run_bounded_process(
                        (str(runtime_copy), *create_arguments),
                        cwd=workspace,
                        env=container_env,
                        timeout_seconds=30,
                        output_limit_bytes=64 * 1024,
                    )
                    if created.returncode != 0:
                        raise MutationRejected("selected test container create failed")
                    try:
                        container_id = cidfile.read_text(encoding="ascii").strip()
                    except OSError as error:
                        raise MutationRejected(
                            "selected test container identity is unavailable"
                        ) from error
                    if re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
                        raise MutationRejected("selected test container identity is invalid")
                    cleanup_identity = container_id
                    cleanup_filter = f"id={container_id}"
                    identity_known = True
                    if (
                        hashlib.sha256(runtime_copy.read_bytes()).hexdigest()
                        != self._container_runtime_sha256
                    ):
                        raise MutationRejected("staged container runtime changed")
                    completed = _run_bounded_process(
                        (str(runtime_copy), "start", "--attach", container_id),
                        cwd=workspace,
                        env=container_env,
                        timeout_seconds=120,
                        output_limit_bytes=8 * 1024 * 1024,
                    )
                finally:
                    self._cleanup_container(
                        cleanup_identity,
                        filter_expression=cleanup_filter,
                        identity_known=identity_known,
                        runtime_path=runtime_copy,
                        cwd=workspace,
                        env=container_env,
                    )
                if completed.returncode != 0:
                    raise MutationRejected("selected test failed")
                try:
                    current_workspace_sha256 = {
                        relative: hashlib.sha256(
                            workspace.joinpath(*relative.split("/")).read_bytes()
                        ).hexdigest()
                        for relative in sorted(self._staged_files)
                    }
                except OSError as error:
                    raise MutationRejected("selected test changed workspace") from error
                if current_workspace_sha256 != workspace_sha256:
                    raise MutationRejected("selected test changed workspace")
                test_receipts.append(
                    MutationTestReceipt(
                        argv=command,
                        returncode=completed.returncode,
                        stdout_sha256=completed.stdout_sha256,
                        stderr_sha256=completed.stderr_sha256,
                        workspace_sha256=tuple(sorted(workspace_sha256.items())),
                        sandbox_profile_sha256=hashlib.sha256(
                            "\x00".join(create_arguments).encode("utf-8")
                        ).hexdigest(),
                        sandbox_controls=sandbox_controls,
                        container_runtime_sha256=self._container_runtime_sha256,
                        container_image=self._container_image,
                    )
                )
            after = {
                relative: hashlib.sha256(raw).hexdigest()
                for relative, raw in files.items()
            }
            for relative in sorted(self._staged_files):
                source = self._root.joinpath(*relative.split("/"))
                try:
                    if _has_reparse_component(self._root, relative):
                        raise MutationRejected("source tree changed during mutation")
                    if _has_nested_repository(self._root, relative):
                        raise MutationRejected("source tree changed during mutation")
                    resolved = source.resolve(strict=True)
                    resolved.relative_to(self._root)
                    current = resolved.read_bytes()
                except (OSError, ValueError) as error:
                    raise MutationRejected(
                        "source tree changed during mutation"
                    ) from error
                if hashlib.sha256(current).hexdigest() != before[relative]:
                    raise MutationRejected("source tree changed during mutation")
            changed_files = tuple(sorted(paths))
            return MutationResult(
                self._actor,
                changed_files,
                MappingProxyType(dict(files)),
                MappingProxyType(
                    {relative: before[relative] for relative in changed_files}
                ),
                MappingProxyType(dict(after)),
                tuple(test_receipts),
            )


__all__ = [
    "MutationRejected",
    "MutationResult",
    "MutationStateInDoubt",
    "MutationTestReceipt",
    "MutationTransaction",
]
