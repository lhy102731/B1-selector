"""patch_executor.py — Claude Patch Executor with validation gates.

Phase 4A-Revised: Claude Code ONLY produces patch.diff. The system validates,
applies, and gates every patch before it reaches the backtest.

Flow: Task.md -> ClaudeCodeExecutor -> patch.diff -> PatchValidator
      -> ApplyPatch -> CompileGate -> BacktestGate -> Report

Production code is NEVER written — patches are always applied to the workspace copy.
"""
from __future__ import annotations

import ast
import difflib
import hashlib
import json
import operator
import re
import subprocess
import sys
from pathlib import Path

from .experiment import Experiment
from .experiment_runner import CodeChangeExecutor, CodeChangeResult
from .control_plane.contracts import SideEffect
from .control_plane.sink_guard import (
    AuthorizedPatchApplier,
    AuthorizedSubprocess,
    ExecutionAuthorizationError,
    ExecutionInvocation,
    ExecutionSinkGuard,
)
from .control_plane.stores import AuthorityReader, TaskExecutionLease

# ============================================================
# Constants
# ============================================================
ALLOWED_FILES = {"strategy/brick_chart_strategy.py"}
MAX_DIFF_LINES = 100
FORBIDDEN_IMPORTS = {"os", "subprocess", "shutil", "socket", "requests", "urllib", "http"}
CLAUDE_BINARY = "claude"


# ============================================================
# PatchParser — lightweight unified-diff parser
# ============================================================
def _parse_diff_files(diff_text: str) -> set[str]:
    """Extract modified file paths from a unified diff."""
    files = set()
    for line in diff_text.splitlines():
        if line.startswith("--- a/") or line.startswith("--- /"):
            f = line.split("--- ", 1)[1]
            f = f[2:] if f.startswith("a/") else f[1:] if f.startswith("/") else f
            files.add(f)
        elif line.startswith("+++ b/") or line.startswith("+++ /"):
            f = line.split("+++ ", 1)[1]
            f = f[2:] if f.startswith("b/") else f[1:] if f.startswith("/") else f
            files.add(f)
    return files


def _count_diff_hunks(diff_text: str) -> int:
    """Count total added + removed lines in a unified diff."""
    added = sum(1 for l in diff_text.splitlines() if l.startswith("+") and not l.startswith("+++"))
    removed = sum(1 for l in diff_text.splitlines() if l.startswith("-") and not l.startswith("---"))
    return added + removed


# ============================================================
# PatchValidator
# ============================================================
class PatchValidator:
    """Validates a patch.diff before it touches any files."""

    @staticmethod
    def validate(diff_text: str) -> dict:
        """Return {ok, reason, files, added_lines, removed_lines, errors}."""
        errors = []

        if not diff_text or not diff_text.strip():
            return {"ok": False, "reason": "empty diff", "files": set(), "errors": ["patch.diff is empty"]}

        files = _parse_diff_files(diff_text)
        hunk_lines = _count_diff_hunks(diff_text)

        # 1. file whitelist
        not_allowed = files - ALLOWED_FILES
        if not_allowed:
            errors.append(f"forbidden files: {not_allowed} (allowed: {ALLOWED_FILES})")

        # 2. diff size
        if hunk_lines > MAX_DIFF_LINES:
            errors.append(f"diff too large: {hunk_lines} lines (max {MAX_DIFF_LINES})")

        # 3. dangerous imports
        for imp in FORBIDDEN_IMPORTS:
            pattern = rf"^\+.*\bimport\s+{imp}\b|^\+.*\bfrom\s+{imp}\b"
            if re.search(pattern, diff_text, re.MULTILINE):
                errors.append(f"dangerous import added: {imp}")

        ok = len(errors) == 0
        return {
            "ok": ok,
            "reason": "valid" if ok else "; ".join(errors),
            "files": list(files),
            "hunk_lines": hunk_lines,
            "errors": errors,
        }


# ============================================================
# ApplyPatch — applies unified diff to workspace copy
# ============================================================
def _apply_patch_to_workspace(diff_text: str, workspace: Path) -> dict:
    """Compatibility shim; unbound patch application is deliberately disabled."""
    return {
        "ok": False,
        "files": [],
        "results": {},
        "error": "authorized patch sink required; legacy helper is disabled",
    }


def _apply_hunks(original_lines: list[str], diff_text: str, target_file: str) -> list[str]:
    """Apply hunks for target_file from diff_text to original_lines. Returns patched lines."""
    hunks = _parse_diff_hunks(diff_text, target_file)
    if not hunks:
        return original_lines

    result = list(original_lines)
    # Apply hunks from last to first to preserve line offsets
    for hunk in reversed(hunks):
        result = _apply_one_hunk(result, hunk)
    return result


def _parse_diff_hunks(diff_text: str, target_file: str) -> list[dict]:
    """Parse unified-diff hunks for target_file. Returns list of {old_start, old_count, new_start, new_count, lines}."""
    hunks = []
    in_target = False
    current = None

    for line in diff_text.splitlines():
        if line.startswith("--- ") and target_file in line:
            in_target = True
            continue
        if line.startswith("--- ") and target_file not in line:
            in_target = False
            continue
        if not in_target:
            continue
        if line.startswith("+++ "):
            continue
        if line.startswith("@@") and in_target:
            if current:
                hunks.append(current)
            m = re.match(r"@@\s*-(\d+)(?:,(\d+))?\s*\+(\d+)(?:,(\d+))?\s*@@", line)
            if m:
                current = {"old_start": int(m.group(1)), "old_count": int(m.group(2) or 1),
                           "new_start": int(m.group(3)), "new_count": int(m.group(4) or 1),
                           "lines": []}
            continue
        if current is not None:
            current["lines"].append(line)

    if current:
        hunks.append(current)
    return hunks


def _apply_one_hunk(lines: list[str], hunk: dict) -> list[str]:
    """Apply a hunk at the declared position with content alignment.

    If the declared @@ position doesn't align (common for LLM-generated diffs),
    the caller (_apply_hunks_retry) will try offsets ±1, ±2, ±3 on failure.
    """
    old_start = hunk["old_start"] - 1
    old_count = hunk["old_count"]
    old_lines = [l[1:] for l in hunk["lines"] if l.startswith("-") or l.startswith(" ")]
    new_lines = [l[1:] + "\n" for l in hunk["lines"] if l.startswith("+") or l.startswith(" ")]

    pos = min(old_start, max(0, len(lines) - 1))
    before = lines[:pos]
    after = lines[min(pos + old_count, len(lines)):]
    return before + new_lines + after


# ============================================================
# Compile & Backtest Gates
# ============================================================
def compile_gate(workspace: Path, *, runner=None) -> dict:
    """Run compileall on workspace strategy/ and utils/ — cwd=workspace so
    ``from utils.technical`` inside strategy/unified_b1_strategy.py resolves."""
    targets = []
    for sub in ["strategy", "utils"]:
        target = workspace / sub
        if not target.exists():
            continue
        targets.append(str(target))
    if not targets:
        return {"ok": True, "output": "compileall clean"}
    if runner is None:
        return {
            "ok": False,
            "output": "authorized compile subprocess runner required",
        }
    proc = runner(
        [sys.executable, "-m", "compileall", "-q", *targets],
        cwd=str(workspace),
    )
    output = (getattr(proc, "stderr", "") or "") + (getattr(proc, "stdout", "") or "")
    return {
        "ok": getattr(proc, "returncode", 1) == 0,
        "output": output.strip() or "compileall clean",
    }


def backtest_gate(backtest_result: dict) -> dict:
    """Check backtest result for fatal issues. Returns {ok, reason}."""
    if not backtest_result.get("success"):
        return {"ok": False, "reason": "backtest crashed"}
    stdout = backtest_result.get("stdout", "")
    m = re.search(r"Total:\s*(\d+)", stdout)
    trades = int(m.group(1)) if m else 0
    if trades == 0:
        return {"ok": False, "reason": "0 trades — reject"}
    return {"ok": True, "reason": f"trades={trades}"}


_NUMERIC_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}


def _safe_eval_numeric(node: ast.AST) -> float | int:
    if isinstance(node, ast.Constant) and isinstance(node.value, bool):
        raise ValueError("bool is not a supported numeric value")
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        value = _safe_eval_numeric(node.operand)
        return value if isinstance(node.op, ast.UAdd) else -value
    if isinstance(node, ast.BinOp) and type(node.op) in _NUMERIC_BIN_OPS:
        left = _safe_eval_numeric(node.left)
        right = _safe_eval_numeric(node.right)
        return _NUMERIC_BIN_OPS[type(node.op)](left, right)
    raise ValueError(f"unsupported numeric expression: {ast.dump(node, include_attributes=False)}")


def _verify_modify_constant(target_path: Path, code_change: dict) -> tuple[bool, str]:
    symbol = code_change.get("symbol")
    expected = code_change.get("value")
    if (
        not isinstance(symbol, str)
        or isinstance(expected, bool)
        or not isinstance(expected, (int, float))
    ):
        return False, f"unsupported modify_constant proposal: symbol={symbol!r} value={expected!r}"

    try:
        tree = ast.parse(target_path.read_text(encoding="utf-8"))
    except SyntaxError as e:
        return False, f"patched file does not parse: {e}"

    matches = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "default_params" for t in node.targets):
            continue
        if not isinstance(node.value, ast.Dict):
            continue
        for key, value_node in zip(node.value.keys, node.value.values):
            if isinstance(key, ast.Constant) and key.value == symbol:
                matches.append(value_node)

    if not matches:
        return False, f"default_params entry not found for symbol {symbol!r}"
    if len(matches) > 1:
        return False, f"multiple default_params assignments found for symbol {symbol}"

    try:
        actual = _safe_eval_numeric(matches[0])
    except (ValueError, ZeroDivisionError) as e:
        return False, f"{symbol} value is not safely numeric: {e}"
    if abs(float(actual) - float(expected)) <= 1e-9:
        return True, f"default_params verified: {symbol}={expected}"
    return False, f"{symbol} expected {expected!r}, found {actual!r}"


# ============================================================
# ClaudeCodeExecutor — real Claude Code CLI
# ============================================================
class ClaudePatchExecutor(CodeChangeExecutor):
    """Calls ``claude -p`` to generate a patch, validates it, and applies to workspace.

    Claude ONLY produces patch.diff. It never writes files directly.
    The system (PatchValidator + ApplyPatch + CompileGate) is the gatekeeper.
    """

    def __init__(self, binary: str = CLAUDE_BINARY, timeout: int = 300,
                 model: str | None = None, allowed_files: set | None = None):
        self.binary = binary
        self.timeout = timeout
        self.model = model
        self.allowed_files = allowed_files or ALLOWED_FILES
        self.validator = PatchValidator()
        self.last_prompt: str | None = None
        self.last_stdout: str | None = None

    def apply(
        self,
        task_path: Path,
        workspace: Path,
        experiment=None,
        *,
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
        compile_lease=None,
        compile_invocation=None,
        execution_compile_lease=None,
        execution_compile_invocation=None,
        review_lease=None,
        review_invocation=None,
        execution_review_lease=None,
        execution_review_invocation=None,
        authority_reader=None,
        repository_root: str | Path | None = None,
    ) -> CodeChangeResult:
        # P0R2 fail-before-effect boundary: a legacy caller must not even
        # reach task/KB/LLM reads without an immutable execution lease and
        # invocation.  The aliases keep adapters explicit while avoiding a
        # positional API change for old callers.
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
        compile_lease = (
            compile_lease
            if compile_lease is not None
            else execution_compile_lease
        )
        compile_invocation = (
            compile_invocation
            if compile_invocation is not None
            else execution_compile_invocation
        )
        review_lease = review_lease if review_lease is not None else execution_review_lease
        review_invocation = (
            review_invocation
            if review_invocation is not None
            else execution_review_invocation
        )
        if lease is None or invocation is None:
            return CodeChangeResult(
                ok=False,
                error="execution lease and invocation are required before patch execution",
                logs=["control-plane sink guard: missing execution authority"],
            )
        if not isinstance(lease, TaskExecutionLease) or not isinstance(
            invocation, ExecutionInvocation
        ):
            return CodeChangeResult(
                ok=False,
                error="execution lease and invocation are malformed",
                logs=["control-plane sink guard: malformed execution authority"],
            )
        try:
            reader = authority_reader if isinstance(authority_reader, AuthorityReader) else AuthorityReader()
            guard = ExecutionSinkGuard(
                authority_reader=reader,
                repository_root=repository_root or Path(__file__).resolve().parent.parent,
            )
            permit = guard.authorize(lease, invocation)
            if (
                permit.operation != "PATCH_APPLY"
                or permit.effect is not SideEffect.GIT_MUTATION
            ):
                raise ExecutionAuthorizationError(
                    "patch execution requires a GIT_MUTATION PATCH_APPLY intent"
                )
            if (
                invocation.runner.module != "research_automation.patch_executor"
                or invocation.runner.callable_name != "ClaudePatchExecutor.apply"
            ):
                raise ExecutionAuthorizationError(
                    "patch executor entry identity is invalid"
                )
        except (ExecutionAuthorizationError, OSError, ValueError) as error:
            return CodeChangeResult(
                ok=False,
                error=f"execution authorization rejected: {error}",
                logs=["control-plane sink guard: patch intent rejected"],
            )
        if not isinstance(llm_lease, TaskExecutionLease) or not isinstance(
            llm_invocation, ExecutionInvocation
        ):
            return CodeChangeResult(
                ok=False,
                error="LLM execution lease and invocation are required before patch generation",
                logs=["control-plane sink guard: missing LLM authority"],
            )
        # 1. read task
        task_md = task_path.read_text(encoding="utf-8") if task_path.exists() else "# no task"

        # 1b. KB hard-constraint gate — defense in depth (Phase 15 closure)
        if experiment is not None:
            try:
                from .kb_gate import gate_proposal_kb
                _strategy = (getattr(experiment, "strategy", None) or "b1").lower()
                _subject = "b1_v3" if _strategy == "b1" else _strategy
                _cc = self._extract_code_change(experiment)
                _proposal = getattr(experiment, "proposal", None)
                _hyp = (getattr(_proposal, "hypothesis", "") if _proposal is not None
                        else (experiment.get("proposal", {}).get("hypothesis", "")
                              if isinstance(experiment, dict) else "")) or ""
                _scope = (getattr(_proposal, "scope", None) if _proposal is not None
                          else (experiment.get("proposal", {}).get("scope", {})
                                if isinstance(experiment, dict) else {})) or {}
                _kb_verdict = gate_proposal_kb(_subject, {
                    "subject": _subject,
                    "hypothesis": _hyp,
                    "scope": {"code_change": _cc, "params": (_scope or {}).get("params", {})},
                })
                if _kb_verdict["verdict"] == "reject":
                    return CodeChangeResult(
                        ok=False,
                        error=f"KB rejected ({_kb_verdict.get('kb_version')}): "
                              f"{_kb_verdict['violations']} -- "
                              f"{_kb_verdict['reasons'][0] if _kb_verdict['reasons'] else ''}",
                        logs=[f"kb_gate: {_kb_verdict}"],
                    )
            except ImportError:
                pass  # KB optional; do not block pipeline

        # 2. read target file from workspace (so Claude sees current code)
        target_content = ""
        if experiment:
            cc = self._extract_code_change(experiment)
            rel_file = (cc or {}).get("file", "strategy/brick_chart_strategy.py")
        else:
            rel_file = "strategy/brick_chart_strategy.py"
        target_path = workspace / rel_file
        if target_path.exists():
            target_content = target_path.read_text(encoding="utf-8")

        # 3. build prompt — Claude is instructed to output ONLY a unified diff
        prompt = self._build_patch_prompt(task_md, target_content, rel_file)
        self.last_prompt = prompt

        expected_llm_argv = (self.binary, "-p", prompt)
        if (
            llm_invocation.argv != expected_llm_argv
            or llm_invocation.cwd is None
            or Path(llm_invocation.cwd).resolve() != Path(workspace).resolve()
            or llm_invocation.runner.module != "research_automation.patch_executor"
            or llm_invocation.runner.callable_name != "ClaudePatchExecutor.apply"
        ):
            return CodeChangeResult(
                ok=False,
                error="LLM command or entry identity differs from immutable intent",
                logs=["control-plane sink guard: LLM invocation mismatch"],
            )

        # 4. call Claude CLI through the shared subprocess sink
        def _authorized_llm_runner(command, **kwargs):
            kwargs.setdefault("capture_output", True)
            kwargs.setdefault("text", True)
            kwargs.setdefault("encoding", "utf-8")
            kwargs.setdefault("errors", "replace")
            return subprocess.run(command, timeout=self.timeout, **kwargs)

        llm_sink = AuthorizedSubprocess(
            authority_reader=reader,
            repository_root=repository_root or Path(__file__).resolve().parent.parent,
            runner=_authorized_llm_runner,
        )
        try:
            proc = llm_sink.run(llm_lease, llm_invocation)
        except subprocess.TimeoutExpired:
            return CodeChangeResult(ok=False, error="Claude CLI timeout",
                                    logs=["timeout after {self.timeout}s"])
        except FileNotFoundError:
            return CodeChangeResult(ok=False, error="claude CLI not found",
                                    logs=["binary: {self.binary}"])
        except ExecutionAuthorizationError as error:
            return CodeChangeResult(
                ok=False,
                error=f"LLM execution authorization rejected: {error}",
                logs=["control-plane sink guard: LLM intent rejected"],
            )

        stdout = proc.stdout or ""
        self.last_stdout = stdout

        if proc.returncode != 0:
            return CodeChangeResult(ok=False, error=f"Claude CLI exit {proc.returncode}",
                                    logs=[proc.stderr[:500] if proc.stderr else ""])

        # 5. parse FIND:/REPLACE WITH: from Claude's output
        find_text, replace_text = None, None
        lines = stdout.splitlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("FIND:"):
                val = line.split(":", 1)[1].strip() if ":" in line and len(line.split(":", 1)[1].strip()) > 0 else ""
                if not val and i + 1 < len(lines):
                    val = lines[i + 1].strip()
                if val:
                    find_text = val
            if stripped.startswith("REPLACE WITH:"):
                val = line.split(":", 1)[1].strip() if ":" in line and len(line.split(":", 1)[1].strip()) > 0 else ""
                if not val and i + 1 < len(lines):
                    val = lines[i + 1].strip()
                if val:
                    replace_text = val

        # handle "no change needed" gracefully
        no_change_hints = ("no change", "no modification", "already", "not needed", "already has")
        if not find_text and not replace_text:
            if any(h in stdout.lower() for h in no_change_hints):
                return CodeChangeResult(ok=True, changed_files=[], deleted_files=[],
                                        schema_changes=False, git_commit=None,
                                        logs=["Claude: no change needed"])
            return CodeChangeResult(ok=False, error="could not parse FIND:/REPLACE WITH: from Claude output",
                                    logs=[stdout[:500]])

        # 6. apply text replacement
        target_text = target_content
        if find_text in target_text:
            target_text = target_text.replace(find_text, replace_text, 1)
        else:
            return CodeChangeResult(ok=False, error=f"FIND line not found in {rel_file}",
                                    logs=[f"FIND: {find_text[:100]}", stdout[:500]])

        diff_text = "".join(
            difflib.unified_diff(
                target_content.splitlines(keepends=True),
                target_text.splitlines(keepends=True),
                fromfile=f"a/{rel_file}",
                tofile=f"b/{rel_file}",
                lineterm="\n",
            )
        )
        if not diff_text:
            return CodeChangeResult(
                ok=True,
                changed_files=[],
                deleted_files=[],
                schema_changes=False,
                git_commit=None,
                logs=["Claude: replacement produced no content delta"],
            )
        patch_sink = AuthorizedPatchApplier(
            authority_reader=reader,
            repository_root=repository_root or Path(__file__).resolve().parent.parent,
            runner=lambda command, **kwargs: subprocess.run(
                command,
                timeout=self.timeout,
                **kwargs,
            ),
        )
        try:
            patch_sink.apply(
                patch_lease,
                patch_invocation,
                diff_text,
                audit_path=workspace / "replace.diff",
            )
        except (ExecutionAuthorizationError, OSError, ValueError) as error:
            return CodeChangeResult(
                ok=False,
                error=f"patch application authorization rejected: {error}",
                logs=["control-plane sink guard: patch mutation rejected"],
            )
        changes_log = [f"{rel_file}: authorized patch applied"]

        cc = self._extract_code_change(experiment)
        if cc and cc.get("change_type") == "modify_constant":
            verified, detail = _verify_modify_constant(target_path, cc)
            if not verified:
                return CodeChangeResult(
                    ok=False,
                    error=f"semantic verification failed: {detail}",
                    logs=[
                        f"proposal: symbol={cc.get('symbol')} value={cc.get('value')}",
                        f"FIND: {find_text[:100]}",
                        f"REPLACE WITH: {replace_text[:100]}",
                    ],
                )
            changes_log.append(detail)

        # 7. compile gate — verify the patched file compiles
        compile_targets = [
            str(workspace / sub)
            for sub in ("strategy", "utils")
            if (workspace / sub).exists()
        ]
        if compile_targets and (
            not isinstance(compile_lease, TaskExecutionLease)
            or not isinstance(compile_invocation, ExecutionInvocation)
        ):
            return CodeChangeResult(
                ok=False,
                error="compile execution lease and invocation are required",
                logs=["discard the isolated workspace to roll back"],
            )

        def _compile_runner(command, *, cwd):
            expected = [sys.executable, "-m", "compileall", "-q", *compile_targets]
            if (
                tuple(command) != tuple(expected)
                or Path(cwd).resolve() != Path(workspace).resolve()
                or compile_invocation.runner.module
                != "research_automation.patch_executor"
                or compile_invocation.runner.callable_name
                != "ClaudePatchExecutor.apply"
            ):
                raise ExecutionAuthorizationError(
                    "compile command differs from immutable execution intent"
                )
            return AuthorizedSubprocess(
                authority_reader=reader,
                repository_root=repository_root or Path(__file__).resolve().parent.parent,
                runner=lambda argv, **kwargs: subprocess.run(
                    argv,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=30,
                    **kwargs,
                ),
            ).run(compile_lease, compile_invocation)

        _cg = compile_gate(
            workspace,
            runner=_compile_runner if compile_targets else None,
        )
        if not _cg["ok"]:
            # Rollback is performed by discarding the isolated workspace.
            return CodeChangeResult(ok=False, error=f"compile gate failed: {_cg['output'][:200]}",
                                    logs=[_cg["output"][:500],
                                          "discard the isolated workspace to roll back"])

        # 7b. Code_Reviewer gate (v4.0) — adversarial review before commit.
        # KB hard_constraints gate upstream covers already-frozen params.
        # This layer catches drift + side effects + scope violations.
        # Skipped if disable_code_reviewer=True is set on the experiment.
        disable_cr = False
        if experiment is not None:
            try:
                cc_for_cr = self._extract_code_change(experiment)
                if isinstance(experiment, dict):
                    disable_cr = bool((experiment.get("scope") or {}).get(
                        "disable_code_reviewer", False))
                elif hasattr(experiment, "proposal"):
                    disable_cr = bool((getattr(experiment.proposal, "scope", None) or {}).get(
                        "disable_code_reviewer", False))
            except Exception:
                cc_for_cr = None

        if not disable_cr:
            if not isinstance(review_lease, TaskExecutionLease) or not isinstance(
                review_invocation, ExecutionInvocation
            ):
                return CodeChangeResult(
                    ok=False,
                    error="review execution lease and invocation are required",
                    logs=["discard the isolated workspace to roll back"],
                )
            try:
                review_permit = ExecutionSinkGuard(
                    authority_reader=reader,
                    repository_root=repository_root or Path(__file__).resolve().parent.parent,
                ).authorize(review_lease, review_invocation)
                if (
                    review_permit.operation != "PATCH_APPLY"
                    or review_permit.effect is not SideEffect.NETWORK_EGRESS
                    or review_invocation.runner.module
                    != "research_automation.patch_executor"
                    or review_invocation.runner.callable_name
                    != "ClaudePatchExecutor.apply"
                ):
                    raise ExecutionAuthorizationError(
                        "code review requires a NETWORK_EGRESS PATCH_APPLY intent"
                    )
            except (ExecutionAuthorizationError, OSError, ValueError) as error:
                return CodeChangeResult(
                    ok=False,
                    error=f"review execution authorization rejected: {error}",
                    logs=["discard the isolated workspace to roll back"],
                )
            try:
                from ag2_research.config import ResearchConfig
                from ag2_research.agents import create_agents
                from research_automation.control_plane.memory import (
                    CommittedLearningLedgerReader,
                    LearningContextRouter,
                )
                _cfg = ResearchConfig()
                _kb_ctx = ""
                try:
                    from ag2_research.knowledge_base import build_context as _bc
                    _kb_ctx = _bc("b1_v3", mode="brief")
                except Exception:
                    pass
                context_root = (
                    Path(repository_root).resolve()
                    if repository_root is not None
                    else Path(__file__).resolve().parent.parent
                )
                committed = CommittedLearningLedgerReader(
                    context_root
                ).read_projection_input()
                context_messages = LearningContextRouter().build_messages(
                    committed["claims"],
                    role="falsification_officer",
                    untrusted_sources=(
                        [{"source_ref": "code-review-kbase", "content": _kb_ctx}]
                        if _kb_ctx
                        else None
                    ),
                    preexcluded_claims=committed["excluded_claims"],
                )
                if context_messages["status"] != "OK":
                    raise RuntimeError("code review learning context budget exceeded")
                reviewer_agents = create_agents(
                    _cfg,
                    ["code_reviewer"],
                    research_context={
                        "code_reviewer": context_messages["system_message"]["content"]
                    },
                )
                reviewer_agent = next(iter(reviewer_agents.values()))
                hypothesis = ""
                try:
                    _prop = getattr(experiment, "proposal", None)
                    if _prop is not None:
                        hypothesis = getattr(_prop, "hypothesis", "") or ""
                except Exception:
                    pass
                review_prompt = (
                    "Review the following patch under AG2 v4.0 Code_Reviewer schema.\n"
                    "Decision MUST be one of: APPROVE / REQUEST_CHANGES / REJECT.\n"
                    "Return structured YAML:\n"
                    "code_review:\n"
                    "  implements_design: pass | partial | fail\n"
                    "  drift_detected: <none | description>\n"
                    "  side_effects: [...]\n"
                    "  architectural_violation: <none | description>\n"
                    "  test_coverage_change: increased | unchanged | decreased\n"
                    "  verdict: APPROVE | REQUEST_CHANGES | REJECT\n"
                    "  rationale: <one paragraph>\n\n"
                    f"Design intent (proposal hypothesis):\n{hypothesis}\n\n"
                    f"Patch applied to file: {rel_file}\n\n"
                    f"FIND:\n{find_text[:1500]}\n\nREPLACE WITH:\n{replace_text[:1500]}\n"
                )
                review_out = reviewer_agent.generate_reply(
                    messages=[
                        *context_messages["untrusted_messages"],
                        {"role": "user", "content": review_prompt},
                    ]
                )
                review_text = review_out if isinstance(review_out, str) else str(review_out)
                # Parse verdict keyword; allow APPROVE without strict YAML.
                verdict_upper = review_text.upper()
                if "REJECT" in verdict_upper:
                    return CodeChangeResult(
                        ok=False,
                        error=f"Code_Reviewer REJECT: {review_text[:400]}",
                        logs=["kb_ctx: structured UNTRUSTED_DATA", review_text[:1500],
                              "discard the isolated workspace to roll back"],
                    )
                if "REQUEST_CHANGES" in verdict_upper:
                    # Soft gate: REQUEST_CHANGES means proceed but record the note.
                    # Hardening to REJECT can be enabled by setting
                    # code_reviewer_request_changes_blocks=True in config later.
                    changes_log.append(f"CODE_REVIEWER_REQUEST_CHANGES: {review_text[:300]}")
                else:
                    changes_log.append(f"CODE_REVIEWER_APPROVE: {review_text[:300]}")
            except Exception as _cr_err:
                return CodeChangeResult(
                    ok=False,
                    error=f"code reviewer unavailable: {_cr_err}",
                    logs=["discard the isolated workspace to roll back"],
                )

        changed_files = [rel_file] if find_text in target_content else []
        changed_hash = hashlib.sha256(target_path.read_bytes()).hexdigest()
        return CodeChangeResult(
            ok=True, changed_files=changed_files, deleted_files=[],
            schema_changes=False, git_commit=None,
            logs=[f"find/replace applied to {rel_file}"] + changes_log,
        )

    @staticmethod
    def _extract_code_change(experiment) -> dict | None:
        if experiment is None:
            return None
        if isinstance(experiment, dict):
            scope = experiment.get("scope") or {}
            if isinstance(scope, dict) and "code_change" in scope:
                return scope["code_change"]
            return experiment.get("code_change")
        prop = getattr(experiment, "proposal", None)
        scope = getattr(prop, "scope", None)
        if isinstance(scope, dict):
            return scope.get("code_change")
        return None

    def _build_patch_prompt(self, task_md: str, target_content: str, rel_file: str) -> str:
        return f"""You are a code editing agent. Output ONLY a find/replace instruction.

CURRENT FILE ({rel_file}):
```python
{target_content}
```

TASK:
{task_md}

INSTRUCTIONS:
- Output ONLY the exact line to find and the exact replacement.
- Format:
FIND:
    exact line from the file
REPLACE WITH:
    new line

- Do NOT output any other text or explanation.
- Only change the specific constant/value requested by the task.
- Do NOT change anything else in the file.
- Reuse the exact indentation from the file.

EXAMPLE:
FIND:
            'height_ratio': 2.0 / 3.0,
REPLACE WITH:
            'height_ratio': 1.0,

OUTPUT ONLY FIND:/REPLACE WITH: NOW:"""

    @staticmethod
    def _extract_diff(text: str) -> str:
        """Extract unified diff from Claude's output. Handles both raw diff and markdown-fenced diff."""
        # Try to find diff in ```diff fences
        m = re.search(r"```(?:diff)?\s*\n(.*?)```", text, re.DOTALL)
        if m:
            return m.group(1).strip()
        # Try to find lines starting with --- and @@
        lines = text.splitlines()
        start = None
        for i, line in enumerate(lines):
            if line.startswith("--- ") and i + 1 < len(lines):
                start = i
                break
        if start is not None:
            return "\n".join(lines[start:]).strip()
        return ""
