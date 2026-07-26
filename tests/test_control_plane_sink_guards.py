from __future__ import annotations

import json
import hashlib
import sqlite3
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import (
    Actor,
    Phase,
    SideEffect,
    canonical_json,
)
from research_automation.control_plane.stores import (
    AuthorityIdentity,
    AuthorityReader,
    TaskTicketError,
)
from research_automation.control_plane.sink_guard import (
    AuthorizedPatchApplier,
    AuthorizedSubprocess,
    ExecutionAuthorizationError,
    ExecutionInvocation,
    RunnerIdentity,
)


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"


class ExecutionLeaseBindingTests(unittest.TestCase):
    @contextmanager
    def _live_lease(
        self,
        *,
        with_intent: bool = False,
        intent_kind: str = "subprocess",
    ) -> Iterator[tuple[object, ...]]:
        if intent_kind not in {"subprocess", "patch"}:
            raise ValueError("unsupported test intent kind")
        now = datetime(2026, 7, 26, 10, 0, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-sink-guard")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )
        task_id = (
            "P0R2-T4-PATCH" if intent_kind == "patch" else "P0R2-T4-SUBPROCESS"
        )
        intent_name = "patch" if intent_kind == "patch" else "intent"
        authorized_effect = (
            SideEffect.GIT_MUTATION
            if intent_kind == "patch"
            else SideEffect.START_SUBPROCESS
        )
        task_spec = {
            "task_id": task_id,
            "objective": f"Authorize one bounded {intent_kind} tracer.",
            "dependencies": [],
            "idempotency_key": f"p0r2-t4-{intent_kind}-001",
            "task_spec_ref": f"research_state/control_plane/p0r2/tasks/{intent_kind}.json",
            "task_spec_sha256": "d" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": ["execution-intent"],
            },
            "allowed_files": ["research_automation/control_plane/sink_guard.py"],
            "forbidden_files": ["data/"],
            "baseline_ref": "research_state/control_plane/p0r2/baseline.json",
            "baseline_sha256": "e" * 64,
            "input_evidence_refs": [
                {
                    "evidence_id": "execution-intent",
                    "evidence_ref": f"research_state/control_plane/p0r2/intents/{intent_name}.json",
                    "evidence_sha256": "f" * 64,
                    "status": "VERIFIED",
                }
            ],
        }

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            intent_info: dict[str, object] | None = None
            if with_intent:
                resource_root = root / "allowed"
                resource_root.mkdir(parents=True)
                runner_path = root / "research_automation" / "control_plane" / "sink_guard.py"
                runner_path.parent.mkdir(parents=True, exist_ok=True)
                runner_bytes = b"# authorized sink runner fixture\n"
                runner_path.write_bytes(runner_bytes)
                runner_sha256 = hashlib.sha256(runner_bytes).hexdigest()
                if intent_kind == "patch":
                    workspace = resource_root / "isolated-workspace"
                    target_path = workspace / "strategy" / "candidate.py"
                    target_path.parent.mkdir(parents=True)
                    target_path.write_text("before\n", encoding="utf-8")
                    audit_path = workspace / "audit" / "candidate.diff"
                    audit_path.parent.mkdir()
                    entry_id = "callable:test:patch"
                    operation = "PATCH_APPLY"
                    argv = ["git", "apply", str(audit_path)]
                    cwd = str(workspace)
                    callable_name = "AuthorizedPatchApplier.apply"
                    resource_paths = [str(target_path), str(audit_path)]
                else:
                    workspace = resource_root
                    target_path = None
                    audit_path = None
                    entry_id = "callable:test:subprocess"
                    operation = "SUBPROCESS"
                    argv = ["python", "-V"]
                    cwd = str(resource_root)
                    callable_name = "AuthorizedSubprocess.run"
                    resource_paths = [str(resource_root)]
                intent_payload: dict[str, object] = {
                    "schema_version": "control_plane.execution_intent.v1",
                    "intent_id": f"intent-{intent_kind}-001",
                    "plan_version": "V3.4.2-P0R2",
                    "phase": "P0",
                    "attempt_id": "p0r2-attempt-001",
                    "task_id": task_spec["task_id"],
                    "identity_binding": {
                        "plan_hash": identity.plan_hash,
                        "scope_hash": identity.scope_hash,
                        "instruction_policy_hash": identity.instruction_policy_hash,
                    },
                    "entry_id": entry_id,
                    "operation": operation,
                    "effect": authorized_effect.value,
                    "argv": argv,
                    "cwd": cwd,
                    "runner": {
                        "module": "research_automation.control_plane.sink_guard",
                        "callable_name": callable_name,
                        "source_ref": "research_automation/control_plane/sink_guard.py",
                        "source_sha256": runner_sha256,
                    },
                    "resource_roots": [str(resource_root)],
                    "resource_paths": resource_paths,
                }
                intent_payload["intent_payload_sha256"] = hashlib.sha256(
                    canonical_json(intent_payload).encode("utf-8")
                ).hexdigest()
                intent_ref = f"research_state/control_plane/p0r2/intents/{intent_name}.json"
                intent_path = root / intent_ref
                intent_path.parent.mkdir(parents=True, exist_ok=True)
                intent_bytes = canonical_json(intent_payload).encode("utf-8")
                intent_path.write_bytes(intent_bytes)
                task_spec["input_evidence_refs"][0]["evidence_sha256"] = (
                    hashlib.sha256(intent_bytes).hexdigest()
                )
                intent_info = {
                    "intent_ref": intent_ref,
                    "resource_root": str(resource_root),
                    "runner_source_ref": "research_automation/control_plane/sink_guard.py",
                    "runner_source_sha256": runner_sha256,
                    "workspace": str(workspace),
                    "target_path": None if target_path is None else str(target_path),
                    "audit_path": None if audit_path is None else str(audit_path),
                }
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
                _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=datetime(
                        2026,
                        7,
                        26,
                        11,
                        0,
                        tzinfo=timezone.utc,
                    ),
                    allowed_side_effects=(authorized_effect,),
                )
                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                ticket = authority._issue_task_ticket(
                    grant,
                    task_spec,
                    allowed_side_effects=(authorized_effect,),
                )
                lease = authority._begin_task(ticket)
                if intent_info is None:
                    yield root, task_spec, ticket, lease
                else:
                    yield root, task_spec, ticket, lease, intent_info

    def test_reader_verifies_live_lease_and_returns_frozen_task_spec(self) -> None:
        with self._live_lease() as (_, task_spec, ticket, lease):
            binding = AuthorityReader().execution_lease_binding(lease)

            self.assertEqual(binding.ticket_id, ticket.ticket_id)
            self.assertEqual(binding.lease_id, lease.lease_id)
            self.assertEqual(
                binding.allowed_side_effects,
                (SideEffect.START_SUBPROCESS,),
            )
            self.assertEqual(
                json.loads(binding.task_spec_payload_json),
                task_spec,
            )

    def test_reader_rejects_a_corrupted_frozen_task_spec(self) -> None:
        with self._live_lease() as (root, _, ticket, lease):
            connection = sqlite3.connect(root / "authority.sqlite3")
            try:
                connection.execute(
                    """
                    UPDATE task_tickets_v2
                    SET task_spec_payload_json = ?
                    WHERE ticket_id = ?
                    """,
                    ("{}", ticket.ticket_id),
                )
                connection.commit()
            finally:
                connection.close()

            with self.assertRaises(TaskTicketError):
                AuthorityReader().execution_lease_binding(lease)


class ExecutionSinkTests(unittest.TestCase):
    def test_unauthorized_subprocess_fails_before_runner_is_called(self) -> None:
        calls: list[tuple[object, ...]] = []

        def runner(*args: object, **kwargs: object) -> object:
            calls.append(args)
            return object()

        invocation = ExecutionInvocation(
            intent_ref="research_state/control_plane/p0r2/intents/intent.json",
            entry_id="callable:test:subprocess",
            effect=SideEffect.START_SUBPROCESS,
            operation="SUBPROCESS",
            argv=("python", "-V"),
            cwd=None,
            runner=RunnerIdentity(
                module="research_automation.control_plane.sink_guard",
                callable_name="AuthorizedSubprocess.run",
                source_ref="research_automation/control_plane/sink_guard.py",
                source_sha256="a" * 64,
            ),
            resource_paths=(),
        )
        sink = AuthorizedSubprocess(
            authority_reader=AuthorityReader(),
            repository_root=Path.cwd(),
            runner=runner,
        )

        with self.assertRaises(ExecutionAuthorizationError):
            sink.run(None, invocation)

        self.assertEqual(calls, [])


    def test_unauthorized_patch_fails_before_audit_write_or_git(self) -> None:
        calls: list[tuple[object, ...]] = []

        def runner(*args: object, **kwargs: object) -> object:
            calls.append((args, kwargs))
            return object()

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = root / "isolated-workspace"
            workspace.mkdir()
            audit_path = workspace / "audit" / "candidate.diff"
            invocation = ExecutionInvocation(
                intent_ref="research_state/control_plane/p0r2/intents/patch.json",
                entry_id="callable:test:patch",
                effect=SideEffect.GIT_MUTATION,
                operation="PATCH_APPLY",
                argv=("git", "apply", str(audit_path)),
                cwd=str(workspace),
                runner=RunnerIdentity(
                    module="research_automation.control_plane.sink_guard",
                    callable_name="AuthorizedPatchApplier.apply",
                    source_ref="research_automation/control_plane/sink_guard.py",
                    source_sha256="a" * 64,
                ),
                resource_paths=(
                    str(workspace / "strategy" / "candidate.py"),
                    str(audit_path),
                ),
            )
            sink = AuthorizedPatchApplier(
                authority_reader=AuthorityReader(),
                repository_root=root,
                runner=runner,
            )

            with self.assertRaises(ExecutionAuthorizationError):
                sink.apply(
                    None,
                    invocation,
                    "--- a/strategy/candidate.py\n"
                    "+++ b/strategy/candidate.py\n"
                    "@@ -1 +1 @@\n"
                    "-before\n"
                    "+after\n",
                    audit_path=audit_path,
                )

            self.assertFalse(audit_path.exists())
            self.assertEqual(calls, [])

    def test_valid_intent_allows_runner_only_after_lease_validation(self) -> None:
        lease_tests = ExecutionLeaseBindingTests()
        calls: list[tuple[object, ...]] = []

        def runner(*args: object, **kwargs: object) -> object:
            calls.append((args, kwargs))
            return "completed"

        with lease_tests._live_lease(with_intent=True) as (
            root,
            _,
            _,
            lease,
            intent_info,
        ):
            self.assertIsInstance(intent_info, dict)
            invocation = ExecutionInvocation(
                intent_ref=str(intent_info["intent_ref"]),
                entry_id="callable:test:subprocess",
                effect=SideEffect.START_SUBPROCESS,
                operation="SUBPROCESS",
                argv=("python", "-V"),
                cwd=str(intent_info["resource_root"]),
                runner=RunnerIdentity(
                    module="research_automation.control_plane.sink_guard",
                    callable_name="AuthorizedSubprocess.run",
                    source_ref=str(intent_info["runner_source_ref"]),
                    source_sha256=str(intent_info["runner_source_sha256"]),
                ),
                resource_paths=(str(intent_info["resource_root"]),),
            )
            sink = AuthorizedSubprocess(
                authority_reader=AuthorityReader(),
                repository_root=root,
                runner=runner,
            )

            result = sink.run(lease, invocation)

        self.assertEqual(result, "completed")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0][0], ["python", "-V"])

    def test_intent_byte_tampering_is_rejected_before_runner(self) -> None:
        lease_tests = ExecutionLeaseBindingTests()
        calls: list[tuple[object, ...]] = []

        def runner(*args: object, **kwargs: object) -> object:
            calls.append((args, kwargs))
            return "must-not-run"

        with lease_tests._live_lease(with_intent=True) as (
            root,
            _,
            _,
            lease,
            intent_info,
        ):
            intent_path = root / str(intent_info["intent_ref"])
            payload = json.loads(intent_path.read_text(encoding="utf-8"))
            payload["argv"] = ["python", "-c", "forged"]
            payload["intent_payload_sha256"] = hashlib.sha256(
                canonical_json(
                    {
                        key: value
                        for key, value in payload.items()
                        if key != "intent_payload_sha256"
                    }
                ).encode("utf-8")
            ).hexdigest()
            intent_path.write_bytes(canonical_json(payload).encode("utf-8"))
            invocation = ExecutionInvocation(
                intent_ref=str(intent_info["intent_ref"]),
                entry_id="callable:test:subprocess",
                effect=SideEffect.START_SUBPROCESS,
                operation="SUBPROCESS",
                argv=("python", "-V"),
                cwd=str(intent_info["resource_root"]),
                runner=RunnerIdentity(
                    module="research_automation.control_plane.sink_guard",
                    callable_name="AuthorizedSubprocess.run",
                    source_ref=str(intent_info["runner_source_ref"]),
                    source_sha256=str(intent_info["runner_source_sha256"]),
                ),
                resource_paths=(str(intent_info["resource_root"]),),
            )
            sink = AuthorizedSubprocess(
                authority_reader=AuthorityReader(),
                repository_root=root,
                runner=runner,
            )

            with self.assertRaises(ExecutionAuthorizationError):
                sink.run(lease, invocation)

        self.assertEqual(calls, [])

    def test_resource_escape_is_rejected_before_runner(self) -> None:
        lease_tests = ExecutionLeaseBindingTests()
        calls: list[tuple[object, ...]] = []

        def runner(*args: object, **kwargs: object) -> object:
            calls.append((args, kwargs))
            return "must-not-run"

        with lease_tests._live_lease(with_intent=True) as (
            root,
            _,
            _,
            lease,
            intent_info,
        ):
            invocation = ExecutionInvocation(
                intent_ref=str(intent_info["intent_ref"]),
                entry_id="callable:test:subprocess",
                effect=SideEffect.START_SUBPROCESS,
                operation="SUBPROCESS",
                argv=("python", "-V"),
                cwd=str(intent_info["resource_root"]),
                runner=RunnerIdentity(
                    module="research_automation.control_plane.sink_guard",
                    callable_name="AuthorizedSubprocess.run",
                    source_ref=str(intent_info["runner_source_ref"]),
                    source_sha256=str(intent_info["runner_source_sha256"]),
                ),
                resource_paths=(str(root.parent),),
            )
            sink = AuthorizedSubprocess(
                authority_reader=AuthorityReader(),
                repository_root=root,
                runner=runner,
            )

            with self.assertRaises(ExecutionAuthorizationError):
                sink.run(lease, invocation)

        self.assertEqual(calls, [])

    def test_valid_patch_checks_then_applies_without_unsafe_path(self) -> None:
        lease_tests = ExecutionLeaseBindingTests()

        with lease_tests._live_lease(
            with_intent=True,
            intent_kind="patch",
        ) as (root, _, _, lease, intent_info):
            workspace = Path(str(intent_info["workspace"]))
            target_path = Path(str(intent_info["target_path"]))
            audit_path = Path(str(intent_info["audit_path"]))
            calls: list[tuple[list[str], dict[str, object]]] = []

            def runner(argv: list[str], **kwargs: object) -> object:
                calls.append((list(argv), dict(kwargs)))
                self.assertTrue(audit_path.is_file())
                if "--check" not in argv:
                    target_path.write_text("after\n", encoding="utf-8")
                return SimpleNamespace(returncode=0, stdout="", stderr="")

            invocation = ExecutionInvocation(
                intent_ref=str(intent_info["intent_ref"]),
                entry_id="callable:test:patch",
                effect=SideEffect.GIT_MUTATION,
                operation="PATCH_APPLY",
                argv=("git", "apply", str(audit_path)),
                cwd=str(workspace),
                runner=RunnerIdentity(
                    module="research_automation.control_plane.sink_guard",
                    callable_name="AuthorizedPatchApplier.apply",
                    source_ref=str(intent_info["runner_source_ref"]),
                    source_sha256=str(intent_info["runner_source_sha256"]),
                ),
                resource_paths=(str(target_path), str(audit_path)),
            )
            diff_text = (
                "--- a/strategy/candidate.py\n"
                "+++ b/strategy/candidate.py\n"
                "@@ -1 +1 @@\n"
                "-before\n"
                "+after\n"
            )
            sink = AuthorizedPatchApplier(
                authority_reader=AuthorityReader(),
                repository_root=root,
                runner=runner,
            )

            sink.apply(
                lease,
                invocation,
                diff_text,
                audit_path=audit_path,
            )

            self.assertEqual(target_path.read_text(encoding="utf-8"), "after\n")
            self.assertEqual(audit_path.read_text(encoding="utf-8"), diff_text)
            self.assertEqual(
                [call[0] for call in calls],
                [
                    ["git", "apply", "--check", str(audit_path)],
                    ["git", "apply", str(audit_path)],
                ],
            )
            self.assertTrue(all("--unsafe-path" not in argv for argv, _ in calls))
            self.assertTrue(all(kwargs["cwd"] == str(workspace) for _, kwargs in calls))

    def test_patch_check_failure_never_reaches_apply(self) -> None:
        lease_tests = ExecutionLeaseBindingTests()
        with lease_tests._live_lease(
            with_intent=True,
            intent_kind="patch",
        ) as (root, _, _, lease, intent_info):
            workspace = Path(str(intent_info["workspace"]))
            target_path = Path(str(intent_info["target_path"]))
            audit_path = Path(str(intent_info["audit_path"]))
            calls: list[list[str]] = []

            def runner(argv: list[str], **kwargs: object) -> object:
                calls.append(list(argv))
                return SimpleNamespace(returncode=1, stdout="rejected", stderr="bad patch")

            invocation = ExecutionInvocation(
                intent_ref=str(intent_info["intent_ref"]),
                entry_id="callable:test:patch",
                effect=SideEffect.GIT_MUTATION,
                operation="PATCH_APPLY",
                argv=("git", "apply", str(audit_path)),
                cwd=str(workspace),
                runner=RunnerIdentity(
                    module="research_automation.control_plane.sink_guard",
                    callable_name="AuthorizedPatchApplier.apply",
                    source_ref=str(intent_info["runner_source_ref"]),
                    source_sha256=str(intent_info["runner_source_sha256"]),
                ),
                resource_paths=(str(target_path), str(audit_path)),
            )
            sink = AuthorizedPatchApplier(
                authority_reader=AuthorityReader(),
                repository_root=root,
                runner=runner,
            )

            with self.assertRaises(ExecutionAuthorizationError):
                sink.apply(
                    lease,
                    invocation,
                    "--- a/strategy/candidate.py\n"
                    "+++ b/strategy/candidate.py\n"
                    "@@ -1 +1 @@\n"
                    "-before\n"
                    "+after\n",
                    audit_path=audit_path,
                )

            self.assertTrue(audit_path.is_file())
            self.assertEqual(calls, [["git", "apply", "--check", str(audit_path)]])
            self.assertEqual(target_path.read_text(encoding="utf-8"), "before\n")


if __name__ == "__main__":
    unittest.main()
