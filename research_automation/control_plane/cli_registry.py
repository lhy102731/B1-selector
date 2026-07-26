"""Control-plane preflight for the legacy ``run_research.py`` command registry."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .contracts import SideEffect
from .sink_guard import ExecutionAuthorizationError, ExecutionInvocation, ExecutionSinkGuard
from .stores import AuthorityReader, TaskExecutionLease


@dataclass(frozen=True)
class CliCommandSpec:
    command: str
    callable_name: str
    effect: SideEffect
    authority_required: bool = True
    dry_run_without_authority: bool = False


@dataclass(frozen=True)
class CliAuthorizationContext:
    """Verified only by ``authorize_cli_command``; never accepted from argv/env."""

    lease: TaskExecutionLease
    invocation: ExecutionInvocation
    authority_reader: AuthorityReader
    repository_root: Path


COMMAND_SPECS: dict[str, CliCommandSpec] = {
    "list": CliCommandSpec("list", "cmd_list", SideEffect.READ, authority_required=False),
    "brainstorm": CliCommandSpec("brainstorm", "cmd_brainstorm", SideEffect.NETWORK_EGRESS),
    "discover": CliCommandSpec("discover", "cmd_discover", SideEffect.NETWORK_EGRESS),
    "resume-discover": CliCommandSpec(
        "resume-discover", "cmd_resume_discover", SideEffect.NETWORK_EGRESS
    ),
    "execute-handoff": CliCommandSpec(
        "execute-handoff",
        "cmd_execute_handoff",
        SideEffect.START_SUBPROCESS,
        dry_run_without_authority=True,
    ),
    "full-cycle": CliCommandSpec("full-cycle", "cmd_full_cycle", SideEffect.RUN_RESEARCH),
    "repair-handoff-runner": CliCommandSpec(
        "repair-handoff-runner", "cmd_repair_handoff_runner", SideEffect.RUN_RESEARCH
    ),
    "review": CliCommandSpec("review", "cmd_review", SideEffect.NETWORK_EGRESS),
    "chat": CliCommandSpec("chat", "cmd_chat", SideEffect.NETWORK_EGRESS),
    "roundtable": CliCommandSpec("roundtable", "cmd_roundtable", SideEffect.NETWORK_EGRESS),
    "interactive": CliCommandSpec("interactive", "cmd_interactive", SideEffect.NETWORK_EGRESS),
}


def authorize_cli_command(
    context: CliAuthorizationContext | None,
    *,
    command: str,
    argv: tuple[str, ...],
    dry_run: bool = False,
) -> CliAuthorizationContext | None:
    """Verify one exact CLI invocation before importing/constructing a runner."""
    spec = COMMAND_SPECS.get(command)
    if spec is None:
        raise ExecutionAuthorizationError("CLI command is not in the control-plane registry")
    if not spec.authority_required or (dry_run and spec.dry_run_without_authority):
        return None
    if not isinstance(context, CliAuthorizationContext):
        raise ExecutionAuthorizationError(
            f"CLI command '{command}' requires a programmatic control-plane adapter"
        )
    permit = ExecutionSinkGuard(
        authority_reader=context.authority_reader,
        repository_root=context.repository_root,
    ).authorize(context.lease, context.invocation)
    if permit.operation != "CLI" or permit.effect is not spec.effect:
        raise ExecutionAuthorizationError(
            f"CLI command '{command}' has the wrong operation/effect authority"
        )
    if (
        context.invocation.runner.module != "run_research"
        or context.invocation.runner.callable_name != spec.callable_name
    ):
        raise ExecutionAuthorizationError("CLI command entry identity is invalid")
    if permit.argv != argv or context.invocation.argv != argv:
        raise ExecutionAuthorizationError("CLI argv differs from the immutable intent")
    return context


__all__ = [
    "COMMAND_SPECS",
    "CliAuthorizationContext",
    "CliCommandSpec",
    "authorize_cli_command",
]
