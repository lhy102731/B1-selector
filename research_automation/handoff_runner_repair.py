"""Auto-repair missing discovery handoff runners.

This module is intentionally narrow. It lets the Brick AG2-KBase autorun ask a
code-writing model for a research-only runner patch when an APPROVED handoff has
no registered Phase 6 executor. The generated patch is constrained to research
runner files and the discovery bridge, then compile-checked before the autorun
retries the handoff.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ag2_research.discovery_handoff import (
    extract_discovery_transcript,
    extract_stage_outputs,
)

from .discovery_execution_bridge import (
    extract_factor_output,
    load_handoff_document,
)
from .control_plane.contracts import SideEffect
from .control_plane.sink_guard import (
    AuthorizedPatchApplier,
    AuthorizedSubprocess,
    ExecutionAuthorizationError,
    ExecutionInvocation,
    ExecutionSinkGuard,
)
from .control_plane.stores import AuthorityReader, TaskExecutionLease


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ALLOWED_FILES = {
    "research/brick_generated_daily_factor_sqnav_phase6.py",
    "research_automation/discovery_execution_bridge.py",
    "tests/test_brick_generated_daily_factor_sqnav_phase6.py",
}
DEFAULT_CONTEXT_FILES = [
    "research_automation/discovery_execution_bridge.py",
    "research/brick_peer_relative_sqnav_phase6.py",
    "research/brick_pool_quality_topk_phase6.py",
]


@dataclass
class RepairResult:
    ok: bool
    status: str
    handoff_path: str
    output_dir: str
    factor_names: list[str] = field(default_factory=list)
    allowed_files: list[str] = field(default_factory=list)
    changed_files: list[str] = field(default_factory=list)
    prompt_path: str | None = None
    diff_path: str | None = None
    review_path: str | None = None
    error: str | None = None
    logs: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_mechanism_slug(name: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "mechanism"
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]
    return f"{base[:48]}_{digest}"


def _repair_spec(document: dict[str, Any]) -> dict[str, Any]:
    factor_output = extract_factor_output(document)
    factors = factor_output.get("factor_batch")
    if isinstance(factors, list) and any(isinstance(item, dict) for item in factors):
        factor_batch = [item for item in factors if isinstance(item, dict)]
        names = _factor_names(factor_batch)
        if not names:
            raise ValueError("factor_batch has no named factors")
        runner_path = "research/brick_generated_daily_factor_sqnav_phase6.py"
        test_path = "tests/test_brick_generated_daily_factor_sqnav_phase6.py"
        return {
            "kind": "factor_batch",
            "factor_names": names,
            "research_spec": {"factor_batch": factor_batch},
            "runner_path": runner_path,
            "test_path": test_path,
            "allowed_files": {
                runner_path,
                test_path,
                "research_automation/discovery_execution_bridge.py",
            },
        }

    mechanism = factor_output.get("research_mechanism")
    if not isinstance(mechanism, dict) or not mechanism:
        raise ValueError("handoff has neither factor_batch nor research_mechanism")
    name = str(mechanism.get("name") or "").strip()
    if not name:
        raise ValueError("research_mechanism has no name")
    slug = _safe_mechanism_slug(name)
    runner_path = f"research/brick_auto_{slug}_phase6.py"
    test_path = f"tests/test_brick_auto_{slug}_phase6.py"
    return {
        "kind": "research_mechanism",
        "factor_names": [name],
        "research_spec": {"research_mechanism": mechanism},
        "runner_path": runner_path,
        "test_path": test_path,
        "allowed_files": {
            runner_path,
            test_path,
            "research_automation/discovery_execution_bridge.py",
        },
    }


def _factor_names(factors: list[dict[str, Any]]) -> list[str]:
    return [str(item.get("name") or "").strip() for item in factors if item.get("name")]


def _extract_diff(text: str) -> str:
    fenced = re.search(r"```(?:diff|patch)?\s*\n(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip() + "\n"
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.startswith(("diff --git ", "--- ")):
            return "\n".join(lines[index:]).strip() + "\n"
    return ""


def _parse_diff_files(diff_text: str) -> set[str]:
    files: set[str] = set()
    for raw in diff_text.splitlines():
        line = raw.strip()
        if line.startswith("diff --git "):
            parts = line.split()
            if len(parts) >= 4:
                files.add(_clean_diff_path(parts[2]))
                files.add(_clean_diff_path(parts[3]))
        elif line.startswith(("--- ", "+++ ")):
            value = line.split(None, 1)[1].strip()
            if value == "/dev/null":
                continue
            files.add(_clean_diff_path(value))
    return {item for item in files if item}


def _clean_diff_path(value: str) -> str:
    value = value.strip().strip('"')
    if value.startswith(("a/", "b/")):
        value = value[2:]
    return value.replace("\\", "/")


def _validate_diff(
    diff_text: str,
    allowed_files: set[str],
    *,
    required_files: set[str] | None = None,
) -> tuple[bool, list[str], list[str]]:
    errors: list[str] = []
    files = sorted(_parse_diff_files(diff_text))
    if not diff_text.strip():
        errors.append("empty diff")
    if not files:
        errors.append("diff does not identify changed files")
    disallowed = [item for item in files if item not in allowed_files]
    if disallowed:
        errors.append(f"disallowed files: {disallowed}; allowed={sorted(allowed_files)}")
    missing_required = sorted((required_files or set()) - set(files))
    if missing_required:
        errors.append(f"required files missing from diff: {missing_required}")
    hunk_lines = sum(
        1
        for line in diff_text.splitlines()
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))
    )
    if hunk_lines > 1800:
        errors.append(f"diff too large: {hunk_lines} changed lines > 1800")
    dangerous = re.findall(
        r"^\+.*\b(?:import|from)\s+(?:os|subprocess|shutil|socket|requests|urllib|http)\b",
        diff_text,
        flags=re.MULTILINE,
    )
    if dangerous:
        errors.append("dangerous import added: " + "; ".join(dangerous[:5]))
    return not errors, files, errors


def _read_context_file(rel_path: str, max_chars: int) -> str:
    path = PROJECT_ROOT / rel_path
    if not path.exists():
        return f"# {rel_path}\n<missing>\n"
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) > max_chars:
        text = text[:max_chars] + "\n# ... truncated for repair prompt ...\n"
    return f"# {rel_path}\n```python\n{text}\n```\n"


def build_repair_prompt(
    *,
    handoff_path: Path,
    research_spec: dict[str, Any],
    failure_log: str,
    allowed_files: set[str],
    runner_path: str,
    test_path: str,
) -> str:
    document = load_handoff_document(handoff_path)
    transcript = extract_discovery_transcript(document)
    outputs = extract_stage_outputs(transcript)
    compact_handoff = {
        "strategy_id": document.get("strategy_id"),
        "topic": document.get("topic"),
        "status": document.get("status"),
        "factor_engineer": outputs.get("factor_engineer"),
        "falsification_officer": outputs.get("falsification_officer"),
    }
    context = "\n\n".join(_read_context_file(path, 30000) for path in DEFAULT_CONTEXT_FILES)
    return f"""You are the AG2 code-writing repair executor for a Brick strategy research system.

Task: the APPROVED KBase discovery handoff below failed because no registered
Phase 6 runner exists. Produce a minimal unified diff that adds or updates a
research-only runner and updates the discovery execution bridge so the handoff
can execute on retry.

Hard constraints:
- Output ONLY a unified diff. No prose before or after the diff.
- Modify only these files: {sorted(allowed_files)}
- Create or update the dedicated runner at: {runner_path}
- Create focused unit tests at: {test_path}
- Update the execution bridge with an exact, fail-closed route for this handoff.
- Do not modify backtest_brick_v2.py or any production strategy file.
- Do not write to D:\\KBase.
- The runner must be research-only and must write outputs under the supplied
  --output-dir.
- Use strict forward validation. Test years must be unseen by each fold model.
- Split by entry_date. Purge train rows whose exit_date overlaps the test start.
- Do not use market timing.
- Brick next-open features are allowed only with daily_select.py semantics:
  entry_date open versus signal-day known close/yellow/MA5.
- Do not use entry_date high/low/close, intraday data, return_pct, exit_date,
  exit_price, or hold_days as model inputs. Labels may be used only for
  training/evaluation after the split.
- Keep the implementation small. Prefer reusing existing helpers from
  brick_peer_relative_sqnav_phase6.py and brick_pool_quality_topk_phase6.py.
- The diff must include the runner, its focused test, and the bridge route.
- The runner must preregister its folds, gates, inputs, and stop conditions
  before reading outcome data.
- A failed falsification or validation gate must be archived as a completed
  NOT_PROMOTED result; it must not silently open a reserved test fold.

Expected interface:
- Runner must accept --handoff-path and --output-dir.
- Bridge retry must make `python run_research.py execute-handoff --strategy brick
  --handoff-path <handoff> --output-dir <dir>` choose the new/updated runner.

Failure log tail:
```text
{failure_log[-5000:]}
```

Research specification:
```yaml
{yaml.safe_dump(research_spec, allow_unicode=True, sort_keys=False)}
```

Compact handoff:
```yaml
{yaml.safe_dump(compact_handoff, allow_unicode=True, sort_keys=False)}
```

Existing code context:
{context}
"""


def _parse_code_review_verdict(text: str) -> str | None:
    candidates = re.findall(
        r"```(?:yaml|yml|json)?\s*\n(.*?)```",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    candidates.append(text)
    for candidate in candidates:
        try:
            payload = yaml.safe_load(candidate)
        except yaml.YAMLError:
            continue
        if not isinstance(payload, dict):
            continue
        review = payload.get("code_review")
        if not isinstance(review, dict):
            continue
        verdict = str(review.get("verdict") or "").strip().upper()
        if verdict in {"APPROVE", "REQUEST_CHANGES", "REJECT"}:
            return verdict
    return None


def _run_code_reviewer(diff_text: str, prompt: str, output_dir: Path) -> tuple[bool, str]:
    try:
        from ag2_research.agents import create_agents
        from ag2_research.config import ResearchConfig

        cfg = ResearchConfig()
        agents = create_agents(cfg, ["code_reviewer"], research_context="")
        reviewer = next(iter(agents.values()))
        review_prompt = (
            "Review this auto-generated research runner patch. "
            "Return APPROVE, REQUEST_CHANGES, or REJECT under the Code_Reviewer schema. "
            "Reject if it touches production code, changes validation scope, or uses future data.\n\n"
            "Repair task summary:\n"
            f"{prompt[:3000]}\n\n"
            "Diff:\n"
            f"{diff_text[:12000]}"
        )
        reply = reviewer.generate_reply(messages=[{"role": "user", "content": review_prompt}])
        text = reply if isinstance(reply, str) else str(reply)
        (output_dir / "code_review.txt").write_text(text, encoding="utf-8")
        verdict = _parse_code_review_verdict(text)
        return verdict == "APPROVE", text
    except Exception as exc:  # noqa: BLE001 - intended review failure must block.
        text = f"CODE_REVIEWER_FAILED: {type(exc).__name__}: {exc}"
        (output_dir / "code_review.txt").write_text(text, encoding="utf-8")
        return False, text


def _apply_diff(
    diff_text: str,
    *,
    workspace: Path,
    lease: TaskExecutionLease,
    invocation: ExecutionInvocation,
    authority_reader: AuthorityReader,
    repository_root: Path = PROJECT_ROOT,
) -> object:
    """Apply a repair diff only through the isolated authorized patch sink."""
    sink = AuthorizedPatchApplier(
        authority_reader=authority_reader,
        repository_root=repository_root,
        runner=lambda command, **kwargs: subprocess.run(
            command,
            timeout=60,
            **kwargs,
        ),
    )
    return sink.apply(
        lease,
        invocation,
        diff_text,
        audit_path=workspace / "repair.diff",
    )


def _stage_repair_workspace(
    files: list[str],
    *,
    output_dir: Path,
    project_root: Path = PROJECT_ROOT,
) -> Path:
    """Copy only repair targets into a disposable workspace."""
    workspace = output_dir / "workspace"
    workspace.mkdir(parents=True, exist_ok=False)
    for relative in files:
        source = project_root / relative
        target = workspace / relative
        if source.exists():
            if source.is_symlink() or not source.is_file():
                raise ExecutionAuthorizationError(
                    f"repair source is not a regular file: {relative}"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
    return workspace


def _compile_changed(
    files: list[str],
    *,
    workspace: Path = PROJECT_ROOT,
    lease: TaskExecutionLease | None = None,
    invocation: ExecutionInvocation | None = None,
    authority_reader: AuthorityReader | None = None,
    repository_root: Path = PROJECT_ROOT,
) -> tuple[bool, str]:
    py_files = [str(workspace / item) for item in files if item.endswith(".py")]
    if not py_files:
        return True, "no python files changed"
    if not isinstance(lease, TaskExecutionLease) or not isinstance(
        invocation, ExecutionInvocation
    ):
        return False, "compile execution authority is missing"
    command = [sys.executable, "-m", "py_compile", *py_files]
    if (
        tuple(command) != invocation.argv
        or invocation.cwd is None
        or Path(invocation.cwd).resolve() != workspace.resolve()
        or invocation.runner.module != "research_automation.handoff_runner_repair"
        or invocation.runner.callable_name != "repair_handoff_runner"
    ):
        return False, "compile command differs from execution intent"

    def _runner(argv, **kwargs):
        kwargs.setdefault("capture_output", True)
        kwargs.setdefault("text", True)
        kwargs.setdefault("encoding", "utf-8")
        kwargs.setdefault("errors", "replace")
        return subprocess.run(argv, timeout=120, **kwargs)

    try:
        proc = AuthorizedSubprocess(
            authority_reader=authority_reader or AuthorityReader(),
            repository_root=repository_root,
            runner=_runner,
        ).run(lease, invocation)
    except Exception as exc:  # noqa: BLE001 - gate failures must trigger rollback.
        return False, f"{type(exc).__name__}: {exc}"
    output = (proc.stdout or "") + (proc.stderr or "")
    return proc.returncode == 0, output.strip() or "py_compile clean"


def _run_changed_tests(
    files: list[str],
    *,
    workspace: Path = PROJECT_ROOT,
    lease: TaskExecutionLease | None = None,
    invocation: ExecutionInvocation | None = None,
    authority_reader: AuthorityReader | None = None,
    repository_root: Path = PROJECT_ROOT,
) -> tuple[bool, str]:
    modules = [
        item.removesuffix(".py").replace("/", ".")
        for item in files
        if item.startswith("tests/") and item.endswith(".py")
    ]
    if not modules:
        return True, "no changed test modules"
    if not isinstance(lease, TaskExecutionLease) or not isinstance(
        invocation, ExecutionInvocation
    ):
        return False, "test execution authority is missing"
    command = [sys.executable, "-m", "unittest", *modules]
    if (
        tuple(command) != invocation.argv
        or invocation.cwd is None
        or Path(invocation.cwd).resolve() != workspace.resolve()
        or invocation.runner.module != "research_automation.handoff_runner_repair"
        or invocation.runner.callable_name != "repair_handoff_runner"
    ):
        return False, "test command differs from execution intent"

    def _runner(argv, **kwargs):
        kwargs.setdefault("capture_output", True)
        kwargs.setdefault("text", True)
        kwargs.setdefault("encoding", "utf-8")
        kwargs.setdefault("errors", "replace")
        return subprocess.run(argv, timeout=600, **kwargs)

    try:
        proc = AuthorizedSubprocess(
            authority_reader=authority_reader or AuthorityReader(),
            repository_root=repository_root,
            runner=_runner,
        ).run(lease, invocation)
    except Exception as exc:  # noqa: BLE001 - gate failures must trigger rollback.
        return False, f"{type(exc).__name__}: {exc}"
    output = ((proc.stdout or "") + (proc.stderr or "")).strip()
    log = f"modules={modules} returncode={proc.returncode}\n{output}"
    return proc.returncode == 0, log


def _snapshot_changed_files(
    files: list[str],
    *,
    workspace: Path = PROJECT_ROOT,
) -> dict[str, bytes | None]:
    snapshot: dict[str, bytes | None] = {}
    for relative in files:
        path = workspace / relative
        snapshot[relative] = path.read_bytes() if path.is_file() else None
    return snapshot


def _snapshot_restored(
    snapshot: dict[str, bytes | None],
    *,
    workspace: Path = PROJECT_ROOT,
) -> tuple[bool, list[str]]:
    mismatches: list[str] = []
    for relative, expected in snapshot.items():
        path = workspace / relative
        if expected is None:
            if path.exists():
                mismatches.append(f"expected removed after rollback: {relative}")
        elif not path.is_file():
            mismatches.append(f"expected restored file is missing: {relative}")
        elif path.read_bytes() != expected:
            mismatches.append(f"restored content mismatch: {relative}")
    return not mismatches, mismatches


def _rollback_exact_diff(
    diff_text: str,
    *,
    output_dir: Path,
    snapshot: dict[str, bytes | None],
    workspace: Path,
) -> tuple[bool, str]:
    del diff_text, snapshot
    logs: list[str] = ["rollback=discard_isolated_workspace"]
    try:
        shutil.rmtree(workspace)
        logs.append("workspace_discarded=true")
        restored = not workspace.exists()
        logs.append(f"workspace_absent={restored}")
        output = "\n\n".join(logs)
        (output_dir / "rollback.log").write_text(output, encoding="utf-8")
        return restored, output
    except Exception as exc:  # noqa: BLE001 - rollback failures must be archived.
        logs.append(f"{type(exc).__name__}: {exc}")
        output = "\n\n".join(logs)
        (output_dir / "rollback.log").write_text(output, encoding="utf-8")
        return False, output


def repair_handoff_runner(
    *,
    handoff_path: str | Path,
    output_dir: str | Path,
    failure_log_path: str | Path | None = None,
    claude_binary: str = "claude",
    timeout: int = 900,
    dry_run: bool = False,
    skip_code_review: bool = False,
    allowed_files: set[str] | None = None,
    lease=None,
    invocation=None,
    execution_lease=None,
    execution_invocation=None,
    llm_lease=None,
    llm_invocation=None,
    execution_llm_lease=None,
    execution_llm_invocation=None,
    patch_lease=None,
    patch_invocation=None,
    execution_patch_lease=None,
    execution_patch_invocation=None,
    validation_lease=None,
    compile_invocation=None,
    test_invocation=None,
    execution_validation_lease=None,
    execution_compile_invocation=None,
    execution_test_invocation=None,
    review_lease=None,
    review_invocation=None,
    execution_review_lease=None,
    execution_review_invocation=None,
    authority_reader=None,
    repository_root: str | Path | None = None,
) -> RepairResult:
    handoff = Path(handoff_path).resolve()
    out = Path(output_dir).resolve()
    repair_root = Path(repository_root or PROJECT_ROOT).resolve()
    lease = lease if lease is not None else execution_lease
    invocation = invocation if invocation is not None else execution_invocation
    llm_lease = llm_lease if llm_lease is not None else execution_llm_lease
    llm_invocation = (
        llm_invocation
        if llm_invocation is not None
        else execution_llm_invocation
    )
    patch_lease = patch_lease if patch_lease is not None else execution_patch_lease
    patch_invocation = (
        patch_invocation
        if patch_invocation is not None
        else execution_patch_invocation
    )
    validation_lease = (
        validation_lease
        if validation_lease is not None
        else execution_validation_lease
    )
    if validation_lease is None:
        validation_lease = patch_lease
    compile_invocation = (
        compile_invocation
        if compile_invocation is not None
        else execution_compile_invocation
    )
    test_invocation = (
        test_invocation
        if test_invocation is not None
        else execution_test_invocation
    )
    review_lease = review_lease if review_lease is not None else execution_review_lease
    review_invocation = (
        review_invocation
        if review_invocation is not None
        else execution_review_invocation
    )
    if not dry_run and (
        not isinstance(lease, TaskExecutionLease)
        or not isinstance(invocation, ExecutionInvocation)
    ):
        return RepairResult(
            ok=False,
            status="unauthorized",
            handoff_path=str(handoff),
            output_dir=str(out),
            error="execution lease and invocation are required before runner repair",
            logs=["control-plane sink guard: missing execution authority"],
        )
    if not dry_run:
        if not isinstance(llm_lease, TaskExecutionLease) or not isinstance(
            llm_invocation, ExecutionInvocation
        ):
            return RepairResult(
                ok=False,
                status="unauthorized",
                handoff_path=str(handoff),
                output_dir=str(out),
                error="LLM execution lease and invocation are required before runner repair",
                logs=["control-plane sink guard: missing LLM authority"],
            )
        try:
            reader = authority_reader if isinstance(authority_reader, AuthorityReader) else AuthorityReader()
            guard = ExecutionSinkGuard(
                authority_reader=reader,
                repository_root=repair_root,
            )
            permit = guard.authorize(lease, invocation)
            if (
                permit.operation != "REPAIR"
                or permit.effect is not SideEffect.RUN_RESEARCH
            ):
                raise ExecutionAuthorizationError(
                    "runner repair requires a RUN_RESEARCH REPAIR intent"
                )
            if (
                invocation.runner.module != "research_automation.handoff_runner_repair"
                or invocation.runner.callable_name != "repair_handoff_runner"
            ):
                raise ExecutionAuthorizationError(
                    "runner repair entry identity is invalid"
                )
            required_resources = {handoff, out}
            if not required_resources.issubset(set(permit.resource_paths)):
                raise ExecutionAuthorizationError(
                    "runner repair handoff/output resources differ from execution intent"
                )
            if out == repair_root:
                raise ExecutionAuthorizationError(
                    "runner repair output must be isolated from repository root"
                )
        except (ExecutionAuthorizationError, OSError, ValueError) as error:
            return RepairResult(
                ok=False,
                status="unauthorized",
                handoff_path=str(handoff),
                output_dir=str(out),
                error=str(error),
                logs=["control-plane sink guard: execution intent rejected"],
            )
    out.mkdir(parents=True, exist_ok=True)
    try:
        document = load_handoff_document(handoff)
        spec = _repair_spec(document)
        names = list(spec["factor_names"])
        allowed = set(allowed_files) if allowed_files is not None else set(spec["allowed_files"])
    except Exception as exc:  # noqa: BLE001
        fallback_allowed = (
            set(allowed_files) if allowed_files is not None else set(DEFAULT_ALLOWED_FILES)
        )
        result = RepairResult(
            ok=False,
            status="invalid_handoff",
            handoff_path=str(handoff),
            output_dir=str(out),
            allowed_files=sorted(fallback_allowed),
            error=f"{type(exc).__name__}: {exc}",
        )
        (out / "repair_result.json").write_text(
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

    failure_log = ""
    if failure_log_path:
        path = Path(failure_log_path)
        if path.exists():
            failure_log = path.read_text(encoding="utf-8", errors="replace")
    prompt = build_repair_prompt(
        handoff_path=handoff,
        research_spec=spec["research_spec"],
        failure_log=failure_log,
        allowed_files=allowed,
        runner_path=spec["runner_path"],
        test_path=spec["test_path"],
    )
    prompt_path = out / "repair_prompt.md"
    prompt_path.write_text(prompt, encoding="utf-8")

    if dry_run:
        result = RepairResult(
            ok=True,
            status="dry_run",
            handoff_path=str(handoff),
            output_dir=str(out),
            factor_names=names,
            allowed_files=sorted(allowed),
            prompt_path=str(prompt_path),
            logs=["dry run: prompt written; no model called"],
        )
        (out / "repair_result.json").write_text(
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

    if (
        llm_invocation.argv != (claude_binary, "-p", prompt)
        or llm_invocation.cwd is None
        or Path(llm_invocation.cwd).resolve() != repair_root
        or llm_invocation.runner.module
        != "research_automation.handoff_runner_repair"
        or llm_invocation.runner.callable_name != "repair_handoff_runner"
    ):
        result = RepairResult(
            ok=False,
            status="unauthorized",
            handoff_path=str(handoff),
            output_dir=str(out),
            factor_names=names,
            allowed_files=sorted(allowed),
            prompt_path=str(prompt_path),
            error="LLM command or entry identity differs from immutable intent",
        )
        (out / "repair_result.json").write_text(
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

    started = time.time()
    def _authorized_llm_runner(command, **kwargs):
        kwargs.setdefault("capture_output", True)
        kwargs.setdefault("text", True)
        kwargs.setdefault("encoding", "utf-8")
        kwargs.setdefault("errors", "replace")
        return subprocess.run(command, timeout=timeout, **kwargs)

    llm_sink = AuthorizedSubprocess(
        authority_reader=reader,
        repository_root=repair_root,
        runner=_authorized_llm_runner,
    )
    try:
        proc = llm_sink.run(llm_lease, llm_invocation)
    except Exception as exc:  # noqa: BLE001
        result = RepairResult(
            ok=False,
            status="model_call_failed",
            handoff_path=str(handoff),
            output_dir=str(out),
            factor_names=names,
            allowed_files=sorted(allowed),
            prompt_path=str(prompt_path),
            error=f"{type(exc).__name__}: {exc}",
        )
        (out / "repair_result.json").write_text(
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

    (out / "claude_stdout.txt").write_text(proc.stdout or "", encoding="utf-8")
    (out / "claude_stderr.txt").write_text(proc.stderr or "", encoding="utf-8")
    if proc.returncode != 0:
        result = RepairResult(
            ok=False,
            status="model_returned_error",
            handoff_path=str(handoff),
            output_dir=str(out),
            factor_names=names,
            allowed_files=sorted(allowed),
            prompt_path=str(prompt_path),
            error=f"claude exit {proc.returncode}",
            logs=[(proc.stderr or "")[-1000:]],
        )
        (out / "repair_result.json").write_text(
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

    diff = _extract_diff(proc.stdout or "")
    diff_path = out / "candidate.diff"
    required_files = {
        spec["runner_path"],
        spec["test_path"],
        "research_automation/discovery_execution_bridge.py",
    }
    valid, files, errors = _validate_diff(
        diff,
        allowed,
        required_files=required_files,
    )
    if not valid:
        result = RepairResult(
            ok=False,
            status="diff_rejected",
            handoff_path=str(handoff),
            output_dir=str(out),
            factor_names=names,
            allowed_files=sorted(allowed),
            changed_files=files,
            prompt_path=str(prompt_path),
            diff_path=str(diff_path),
            error="; ".join(errors),
        )
        (out / "repair_result.json").write_text(
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

    if not skip_code_review:
        if not isinstance(review_lease, TaskExecutionLease) or not isinstance(
            review_invocation, ExecutionInvocation
        ):
            result = RepairResult(
                ok=False,
                status="unauthorized",
                handoff_path=str(handoff),
                output_dir=str(out),
                factor_names=names,
                allowed_files=sorted(allowed),
                changed_files=files,
                prompt_path=str(prompt_path),
                error="review execution lease and invocation are required before LLM review",
                logs=["control-plane sink guard: missing review authority"],
            )
            (out / "repair_result.json").write_text(
                json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return result
        try:
            review_permit = ExecutionSinkGuard(
                authority_reader=reader,
                repository_root=repair_root,
            ).authorize(review_lease, review_invocation)
            if (
                review_permit.operation != "REPAIR"
                or review_permit.effect is not SideEffect.NETWORK_EGRESS
                or review_invocation.runner.module
                != "research_automation.handoff_runner_repair"
                or review_invocation.runner.callable_name
                != "repair_handoff_runner"
            ):
                raise ExecutionAuthorizationError(
                    "review requires a NETWORK_EGRESS REPAIR intent"
                )
        except (ExecutionAuthorizationError, OSError, ValueError) as error:
            result = RepairResult(
                ok=False,
                status="unauthorized",
                handoff_path=str(handoff),
                output_dir=str(out),
                factor_names=names,
                allowed_files=sorted(allowed),
                changed_files=files,
                prompt_path=str(prompt_path),
                error=str(error),
                logs=["control-plane sink guard: review intent rejected"],
            )
            (out / "repair_result.json").write_text(
                json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return result
        review_ok, review_text = _run_code_reviewer(diff, prompt, out)
        if not review_ok:
            result = RepairResult(
                ok=False,
                status="code_reviewer_rejected",
                handoff_path=str(handoff),
                output_dir=str(out),
                factor_names=names,
                allowed_files=sorted(allowed),
                changed_files=files,
                prompt_path=str(prompt_path),
                diff_path=str(diff_path),
                review_path=str(out / "code_review.txt"),
                error=review_text[:1000],
            )
            (out / "repair_result.json").write_text(
                json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        return result

    if not isinstance(patch_lease, TaskExecutionLease) or not isinstance(
        patch_invocation, ExecutionInvocation
    ):
        result = RepairResult(
            ok=False,
            status="unauthorized",
            handoff_path=str(handoff),
            output_dir=str(out),
            factor_names=names,
            allowed_files=sorted(allowed),
            changed_files=files,
            prompt_path=str(prompt_path),
            error="patch execution lease and invocation are required before mutation",
            logs=["control-plane sink guard: missing patch authority"],
        )
        (out / "repair_result.json").write_text(
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

    try:
        workspace = _stage_repair_workspace(
            files,
            output_dir=out,
            project_root=repair_root,
        )
        diff_path = workspace / "repair.diff"
    except (ExecutionAuthorizationError, OSError, ValueError) as error:
        result = RepairResult(
            ok=False,
            status="workspace_rejected",
            handoff_path=str(handoff),
            output_dir=str(out),
            factor_names=names,
            allowed_files=sorted(allowed),
            changed_files=files,
            prompt_path=str(prompt_path),
            error=str(error),
        )
        (out / "repair_result.json").write_text(
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

    pre_apply_snapshot = _snapshot_changed_files(files, workspace=workspace)
    try:
        apply_proc = _apply_diff(
            diff,
            workspace=workspace,
            lease=patch_lease,
            invocation=patch_invocation,
            authority_reader=reader,
            repository_root=repair_root,
        )
    except (ExecutionAuthorizationError, OSError, ValueError) as error:
        apply_proc = None
        apply_error = str(error)
    else:
        apply_error = ""
    if apply_proc is None or getattr(apply_proc, "returncode", 0) != 0:
        result = RepairResult(
            ok=False,
            status="apply_failed",
            handoff_path=str(handoff),
            output_dir=str(out),
            factor_names=names,
            allowed_files=sorted(allowed),
            changed_files=files,
            prompt_path=str(prompt_path),
            diff_path=str(diff_path),
            review_path=str(out / "code_review.txt") if (out / "code_review.txt").exists() else None,
            error=(apply_error or ((getattr(apply_proc, "stdout", "") or "") +
                                   (getattr(apply_proc, "stderr", "") or "")))[-2000:],
        )
        (out / "repair_result.json").write_text(
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

    compile_ok, compile_output = _compile_changed(
        files,
        workspace=workspace,
        lease=validation_lease,
        invocation=compile_invocation,
        authority_reader=reader,
        repository_root=repair_root,
    )
    test_ok, test_output = (
        _run_changed_tests(
            files,
            workspace=workspace,
            lease=validation_lease,
            invocation=test_invocation,
            authority_reader=reader,
            repository_root=repair_root,
        )
        if compile_ok
        else (False, "tests skipped: compile failed")
    )
    (out / "compile.log").write_text(compile_output, encoding="utf-8")
    (out / "tests.log").write_text(test_output, encoding="utf-8")
    gates_ok = compile_ok and test_ok
    rollback_ok: bool | None = None
    rollback_output = ""
    if not gates_ok:
        rollback_ok, rollback_output = _rollback_exact_diff(
            diff,
            output_dir=out,
            snapshot=pre_apply_snapshot,
            workspace=workspace,
        )
    gate_failure = "compile_failed" if not compile_ok else "tests_failed"
    if gates_ok:
        final_status = "repaired"
    elif rollback_ok:
        final_status = f"{gate_failure}_rolled_back"
    else:
        final_status = f"{gate_failure}_rollback_failed"
    result = RepairResult(
        ok=gates_ok,
        status=final_status,
        handoff_path=str(handoff),
        output_dir=str(out),
        factor_names=names,
        allowed_files=sorted(allowed),
        changed_files=files,
        prompt_path=str(prompt_path),
        diff_path=str(diff_path),
        review_path=str(out / "code_review.txt") if (out / "code_review.txt").exists() else None,
        error=None if gates_ok else (compile_output if not compile_ok else test_output),
        logs=[
            f"elapsed_seconds={round(time.time() - started, 3)}",
            compile_output,
            test_output,
            rollback_output,
        ],
    )
    (out / "repair_result.json").write_text(
        json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-repair an APPROVED Brick discovery handoff runner")
    parser.add_argument("--handoff-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--failure-log")
    parser.add_argument("--claude-binary", default="claude")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-code-review", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = repair_handoff_runner(
        handoff_path=args.handoff_path,
        output_dir=args.output_dir,
        failure_log_path=args.failure_log,
        claude_binary=args.claude_binary,
        timeout=args.timeout,
        dry_run=args.dry_run,
        skip_code_review=args.skip_code_review,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
