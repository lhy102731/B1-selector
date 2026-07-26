"""Fail-closed authorization helpers for the legacy Flask entry points.

The existing web applications are intentionally classified ``DENIED_WEB`` in
the reviewed entry policy.  They may still be imported for read-only tests, but
thread creation, file mutation, deletion, and configuration writes require an
explicit control-plane adapter.  No HTTP route can manufacture that adapter.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .contracts import SideEffect
from .sink_guard import (
    AuthorizedPathMutation,
    ExecutionAuthorizationError,
    ExecutionInvocation,
    ExecutionSinkGuard,
)
from .stores import AuthorityReader, TaskExecutionLease


class WebAuthorizationError(ExecutionAuthorizationError):
    """Raised when a legacy web entry has no explicit control-plane permit."""


@dataclass(frozen=True)
class WebAuthorizationContext:
    """Non-serializable adapter supplied by a trusted local caller."""

    lease: TaskExecutionLease
    invocation: ExecutionInvocation
    authority_reader: AuthorityReader
    repository_root: Path


def require_loopback_host(host: str) -> str:
    """Return a canonical loopback host; reject wildcard/external binds."""
    normalized = str(host or "").strip().lower()
    if normalized in {"localhost", "127.0.0.1", "::1"}:
        return normalized
    raise WebAuthorizationError("web server must bind to a loopback host")


def authorize_thread(context: WebAuthorizationContext | None) -> None:
    """Authorize one LLM/network thread creation, or fail closed."""
    if not isinstance(context, WebAuthorizationContext):
        raise WebAuthorizationError("web LLM thread creation is disabled without a control-plane adapter")
    permit = ExecutionSinkGuard(
        authority_reader=context.authority_reader,
        repository_root=context.repository_root,
    ).authorize(context.lease, context.invocation)
    if permit.operation != "WEB_THREAD" or permit.effect is not SideEffect.NETWORK_EGRESS:
        raise WebAuthorizationError("web thread requires a NETWORK_EGRESS WEB_THREAD intent")
    if (
        context.invocation.runner.module != "apps.web_roundtable"
        or context.invocation.runner.callable_name not in {"api_start", "_stream_roundtable"}
    ):
        raise WebAuthorizationError("web thread entry identity is invalid")


def authorize_mutation(
    context: WebAuthorizationContext | None,
    *,
    operation: str,
    callable_name: str,
    paths: Sequence[str | Path],
) -> None:
    """Authorize one exact web file mutation/deletion."""
    if not isinstance(context, WebAuthorizationContext):
        raise WebAuthorizationError("web mutation is disabled without a control-plane adapter")
    effect = {
        "WEB_WRITE": SideEffect.WRITE_STAGING,
        "WEB_CONFIG_WRITE": SideEffect.WRITE_PRODUCTION_CONFIG,
        "WEB_DELETE": SideEffect.DELETE_PATH,
    }.get(operation)
    if effect is None:
        raise WebAuthorizationError("unsupported web mutation operation")
    AuthorizedPathMutation(
        authority_reader=context.authority_reader,
        repository_root=context.repository_root,
    ).authorize(
        context.lease,
        context.invocation,
        operation=operation,
        effect=effect,
        module="apps.web_server" if operation == "WEB_CONFIG_WRITE" else "apps.web_roundtable",
        callable_name=callable_name,
        paths=paths,
    )


__all__ = [
    "WebAuthorizationContext",
    "WebAuthorizationError",
    "authorize_mutation",
    "authorize_thread",
    "require_loopback_host",
]
