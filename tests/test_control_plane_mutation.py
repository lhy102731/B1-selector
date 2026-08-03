import sys
import hashlib
import shutil
from datetime import datetime, timezone
from dataclasses import replace
import json
from io import BytesIO
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import (
    Actor,
    Phase,
    SideEffect,
    canonical_json,
)


ROOT_SECRET = "mutation-test-authority-root-capability-0123456789abcdef"
_AUTHORITY_TEMPORARY = None
_AUTHORITY_PATH_PATCH = None
_AUTHORITY_GRANT = None
_TRANSACTION_SEQUENCE = 0


def setUpModule():
    global _AUTHORITY_TEMPORARY, _AUTHORITY_PATH_PATCH
    global _AUTHORITY_GRANT
    _AUTHORITY_TEMPORARY = tempfile.TemporaryDirectory()
    authority_root = Path(_AUTHORITY_TEMPORARY.name)
    _AUTHORITY_PATH_PATCH = patch.multiple(
        stores_module,
        _AUTHORITY_STORE_PATH=authority_root / "authority.sqlite3",
        _OPERATIONAL_STORE_PATH=authority_root / "operational.sqlite3",
    )
    _AUTHORITY_PATH_PATCH.start()
    stores_module._expected_schema_sha256.cache_clear()
    stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
    authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
    actor = Actor("mutation-test", "automation", "mutation-test-invocation")
    identity = stores_module.AuthorityIdentity("a" * 64, "b" * 64, "c" * 64)
    p0_envelope = authority._provision_authorization(
        phase=Phase.P0,
        attempt_id="p0-mutation-policy",
        actor=actor,
        identity=identity,
        expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
        allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
    )
    p0_grant = authority.claim_authorization(
        p0_envelope,
        expected_phase=Phase.P0,
        expected_attempt_id="p0-mutation-policy",
        actor=actor,
        identity=identity,
    )
    policy_ticket = authority._issue_task_ticket(
        p0_grant,
        {
            "task_id": "P0-MUTATION-POLICY-SEED",
            "objective": "Seed the reviewed policy for mutation tests.",
            "dependencies": [],
            "idempotency_key": "p0-mutation-policy-seed",
            "task_spec_ref": "research_state/control_plane/p0/mutation-policy.json",
            "task_spec_sha256": "1" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_state/control_plane/policies/"],
            "forbidden_files": ["data/"],
            "baseline_ref": "research_state/control_plane/p0/baseline.json",
            "baseline_sha256": "2" * 64,
            "input_evidence_refs": [],
        },
        allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
    )
    policy_sha256 = "3" * 64
    connection = stores_module.sqlite3.connect(stores_module._AUTHORITY_STORE_PATH)
    try:
        connection.execute(
            "INSERT INTO reviewed_entry_policies_v1 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                policy_sha256, "4" * 64, "5" * 64, "6" * 64,
                actor.actor_id, actor.actor_type, actor.invocation_id,
                policy_ticket.ticket_id, "P0", "p0-mutation-policy",
                identity.plan_hash, identity.scope_hash,
                identity.instruction_policy_hash,
                "2026-07-30T00:00:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO active_entry_policy_v1 VALUES (1, ?, ?)",
            (policy_sha256, "2026-07-30T00:00:00+00:00"),
        )
        connection.commit()
    finally:
        connection.close()
    envelope = authority._provision_authorization(
        phase=Phase.P4,
        attempt_id="p4-mutation-test",
        actor=actor,
        identity=identity,
        expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
        allowed_side_effects=(
            SideEffect.READ,
            SideEffect.WRITE_STAGING,
            SideEffect.GIT_MUTATION,
            SideEffect.START_SUBPROCESS,
        ),
    )
    _AUTHORITY_GRANT = authority.claim_authorization(
        envelope,
        expected_phase=Phase.P4,
        expected_attempt_id="p4-mutation-test",
        actor=actor,
        identity=identity,
    )


def tearDownModule():
    _AUTHORITY_PATH_PATCH.stop()
    stores_module._expected_schema_sha256.cache_clear()
    _AUTHORITY_TEMPORARY.cleanup()


class _ContainerHarness:
    def __init__(
        self, transaction, runtime, exit_code, stdout, hang, lease, policy,
        cleanup_returncode,
        git_stdout,
        git_hang,
        git_unavailable,
        git_callback,
    ):
        self._transaction = transaction
        self._runtime = runtime
        self._exit_code = exit_code
        self._stdout = stdout
        self._hang = hang
        self.cleanup_commands = []
        self.container_env = None
        self.lease = lease
        self.policy = policy
        self._cleanup_returncode = cleanup_returncode
        self._git_stdout = git_stdout
        self._git_hang = git_hang
        self._git_unavailable = git_unavailable
        self._git_callback = git_callback

    def apply(self, patch_bytes):
        subprocess_module = __import__("subprocess")
        real_run = subprocess_module.run
        real_popen = subprocess_module.Popen

        class FakePopen:
            def __new__(nested_class, command, **kwargs):
                if (
                    Path(command[0]).name.lower() == "git.exe"
                    and not self._git_stdout
                    and not self._git_hang
                    and not self._git_unavailable
                    and self._git_callback is None
                ):
                    return real_popen(command, **kwargs)
                return super().__new__(nested_class)

            def __init__(nested_self, command, **kwargs):
                if Path(command[0]).name.lower() == "git.exe":
                    if self._git_unavailable:
                        raise FileNotFoundError("git missing")
                    if self._git_callback is not None:
                        callback = self._git_callback
                        self._git_callback = None
                        callback()
                    nested_self.returncode = None if self._git_hang else 0
                    nested_self.stdout = BytesIO(self._git_stdout)
                    nested_self.stderr = BytesIO()
                    return
                self.container_env = dict(kwargs["env"])
                verb = command[1]
                if verb == "create":
                    nested_self.returncode = 0
                    nested_self.stdout = BytesIO()
                    cidfile = next(
                        value.split("=", 1)[1]
                        for value in command
                        if value.startswith("--cidfile=")
                    )
                    Path(cidfile).write_text("a" * 64, encoding="ascii")
                elif verb == "start":
                    nested_self.returncode = None if self._hang else self._exit_code
                    nested_self.stdout = BytesIO(self._stdout)
                else:
                    self.cleanup_commands.append(tuple(command[1:]))
                    nested_self.returncode = self._cleanup_returncode
                    nested_self.stdout = BytesIO()
                nested_self.stderr = BytesIO()

            def poll(nested_self):
                return nested_self.returncode

            def wait(nested_self, timeout=None):
                return nested_self.returncode

            def kill(nested_self):
                nested_self.returncode = -9

        def run(command, **kwargs):
            with patch("subprocess.Popen", real_popen):
                return real_run(command, **kwargs)

        with patch(
            "research_automation.control_plane.mutation.subprocess.run",
            run,
        ), patch(
            "research_automation.control_plane.mutation.subprocess.Popen",
            FakePopen,
        ):
            return self._transaction.apply(patch_bytes)


def _transaction(*, repository_root, allowed_files, **kwargs):
    global _TRANSACTION_SEQUENCE
    from research_automation.control_plane.mutation import MutationTransaction

    selected_tests = kwargs.pop(
        "selected_tests",
        (("python", "-c", "pass"),),
    )
    sandbox_exit_code = kwargs.pop("sandbox_exit_code", 0)
    sandbox_stdout = kwargs.pop("sandbox_stdout", b"")
    sandbox_hang = kwargs.pop("sandbox_hang", False)
    cleanup_returncode = kwargs.pop("cleanup_returncode", 0)
    git_stdout = kwargs.pop("git_stdout", b"")
    git_hang = kwargs.pop("git_hang", False)
    git_unavailable = kwargs.pop("git_unavailable", False)
    git_callback = kwargs.pop("git_callback", None)
    support_files = tuple(kwargs.get("support_files", ()))
    runtime_value = shutil.which("docker") or shutil.which("podman")
    git_value = shutil.which("git")
    if runtime_value is None or git_value is None:
        raise unittest.SkipTest("trusted runtime executable is unavailable")
    runtime = Path(runtime_value).resolve()
    git_runtime = Path(git_value).resolve()
    _TRANSACTION_SEQUENCE += 1
    policy = {
        "schema_version": "control_plane.mutation_sandbox_policy.v2",
        "policy_id": f"p4-mutation-test-sandbox-{_TRANSACTION_SEQUENCE}",
        "repository_root": Path(repository_root).resolve().as_posix(),
        "allowed_files": list(allowed_files),
        "support_files": list(support_files),
        "selected_tests": [list(command) for command in selected_tests],
        "container_runtime_path": runtime.as_posix(),
        "container_runtime_sha256": hashlib.sha256(runtime.read_bytes()).hexdigest(),
        "container_image": "python@sha256:" + "1" * 64,
        "daemon_uri": "npipe:////./pipe/dockerDesktopLinuxEngine",
        "git_runtime_path": git_runtime.as_posix(),
        "git_runtime_sha256": hashlib.sha256(git_runtime.read_bytes()).hexdigest(),
    }
    policy_bytes = canonical_json(policy).encode("utf-8")
    policy_sha256 = hashlib.sha256(policy_bytes).hexdigest()
    authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
    task_suffix = policy_sha256[:24]
    ticket = authority._issue_task_ticket(
        _AUTHORITY_GRANT,
        {
            "task_id": f"P4-MUTATION-{task_suffix}",
            "objective": "Exercise one Authority-bound mutation transaction.",
            "dependencies": [],
            "idempotency_key": f"p4-mutation-{task_suffix}",
            "task_spec_ref": f"research_state/control_plane/p4/task_specs/{task_suffix}.json",
            "task_spec_sha256": policy_sha256,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": ["mutation-sandbox-policy"],
            },
            "allowed_files": list(allowed_files),
            "forbidden_files": ["data/", "D:/KBase/"],
            "baseline_ref": "research_state/control_plane/p4/baseline.json",
            "baseline_sha256": "8" * 64,
            "input_evidence_refs": [
                {
                    "evidence_id": "mutation-sandbox-policy",
                    "evidence_ref": f"research_state/control_plane/p4/{task_suffix}.json",
                    "evidence_sha256": policy_sha256,
                    "status": "VERIFIED",
                }
            ],
        },
        allowed_side_effects=(
            SideEffect.READ,
            SideEffect.WRITE_STAGING,
            SideEffect.GIT_MUTATION,
            SideEffect.START_SUBPROCESS,
        ),
    )
    lease = authority._begin_task(ticket)
    transaction = MutationTransaction(
        repository_root=repository_root,
        allowed_files=allowed_files,
        selected_tests=selected_tests,
        authority_lease=lease,
        sandbox_policy_bytes=policy_bytes,
        **kwargs,
    )
    return _ContainerHarness(
        transaction,
        runtime,
        sandbox_exit_code,
        sandbox_stdout,
        sandbox_hang,
        lease,
        policy,
        cleanup_returncode,
        git_stdout,
        git_hang,
        git_unavailable,
        git_callback,
    )


class MutationTransactionVerticalSliceTests(unittest.TestCase):
    @staticmethod
    def _bounded_process_fixture(stdout: bytes):
        class FakeProcess:
            def __init__(self) -> None:
                self.returncode = 0
                self.stdout = BytesIO(stdout)
                self.stderr = BytesIO(b"stderr")

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                del timeout
                return self.returncode

            def kill(self):
                self.returncode = -9

        return FakeProcess()

    def test_bounded_process_closes_output_streams_after_success(self):
        from research_automation.control_plane.mutation import (
            _run_bounded_process,
        )

        process = self._bounded_process_fixture(b"stdout")
        with tempfile.TemporaryDirectory() as tmp, patch(
            "research_automation.control_plane.mutation.subprocess.Popen",
            return_value=process,
        ):
            _run_bounded_process(
                ("synthetic",),
                cwd=Path(tmp),
                env={},
                timeout_seconds=1,
                output_limit_bytes=1024,
            )

        self.assertTrue(process.stdout.closed)
        self.assertTrue(process.stderr.closed)

    def test_bounded_process_closes_output_streams_after_rejection(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            _run_bounded_process,
        )

        process = self._bounded_process_fixture(b"xx")
        with tempfile.TemporaryDirectory() as tmp, patch(
            "research_automation.control_plane.mutation.subprocess.Popen",
            return_value=process,
        ), self.assertRaisesRegex(MutationRejected, "output exceeded limit"):
            _run_bounded_process(
                ("synthetic",),
                cwd=Path(tmp),
                env={},
                timeout_seconds=1,
                output_limit_bytes=1,
            )

        self.assertTrue(process.stdout.closed)
        self.assertTrue(process.stderr.closed)

    def test_traversal_patch_is_rejected_without_touching_source_tree(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            outside = root.parent / "escape.py"
            patch = b"""diff --git a/../escape.py b/../escape.py
new file mode 100644
--- /dev/null
+++ b/../escape.py
@@ -0,0 +1 @@
+SECRET = True
"""
            transaction = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
            )
            with self.assertRaises(MutationRejected):
                transaction.apply(patch)
            self.assertEqual(source.read_text(encoding="utf-8"), "VALUE = 1\n")
            self.assertFalse(outside.exists())

    def test_valid_patch_applies_only_in_disposable_workspace(self):
        from research_automation.control_plane.mutation import MutationTransaction

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            patch = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            result = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
            ).apply(patch)
            self.assertEqual(result.changed_files, ("allowed.py",))
            self.assertEqual(result.files["allowed.py"], b"VALUE = 2\n")
            self.assertEqual(source.read_bytes(), b"VALUE = 1\n")

    def test_python_compile_failure_discards_workspace(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            patch = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = (
"""
            with self.assertRaises(MutationRejected):
                _transaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                ).apply(patch)
            self.assertEqual(source.read_bytes(), b"VALUE = 1\n")

    def test_patch_cannot_change_allowlisted_file_into_symlink(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            patch = b"""diff --git a/allowed.py b/allowed.py
deleted file mode 100644
new file mode 120000
index 76d6bb0..2ef267e
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+../escape.py
"""
            with self.assertRaisesRegex(
                MutationRejected,
                "unsafe patch object mode",
            ):
                _transaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                ).apply(patch)
            self.assertEqual(source.read_bytes(), b"VALUE = 1\n")

    def test_alternate_patch_header_cannot_escape_allowlist(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            outside = root.parent / "escape.py"
            patch = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/../escape.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            with self.assertRaisesRegex(MutationRejected, "patch path is unsafe"):
                _transaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                ).apply(patch)
            self.assertEqual(source.read_bytes(), b"VALUE = 1\n")
            self.assertFalse(outside.exists())

    def test_headerless_second_file_section_is_rejected(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
--- /dev/null
+++ b/conftest.py
@@ -0,0 +1 @@
+ESCAPED = True
"""
            with self.assertRaisesRegex(MutationRejected, "ambiguous patch header"):
                _transaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                ).apply(patch_bytes)
            self.assertEqual(source.read_bytes(), b"VALUE = 1\n")

    def test_windows_case_cannot_bypass_forbidden_path(self):
        from research_automation.control_plane.mutation import MutationRejected

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            target = root / "DATA" / "x.py"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"VALUE = 1\n")
            with self.assertRaisesRegex(MutationRejected, "intersects forbidden files"):
                _transaction(
                    repository_root=root,
                    allowed_files=("DATA/x.py",),
                )

    def test_windows_trailing_dot_alias_cannot_bypass_forbidden_path(self):
        from research_automation.control_plane.mutation import MutationRejected

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            target = root / "data" / "x.py"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"VALUE = 1\n")
            with self.assertRaisesRegex(MutationRejected, "intersects forbidden files"):
                _transaction(
                    repository_root=root,
                    allowed_files=("data./x.py",),
                )

    def test_oversized_patch_is_rejected_before_git(self):
        from research_automation.control_plane.mutation import MutationRejected

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            with self.assertRaisesRegex(MutationRejected, "patch exceeds size limit"):
                _transaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                ).apply(b"x" * (4 * 1024 * 1024 + 1))

    def test_source_tree_drift_rejects_validated_workspace_result(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            with self.assertRaisesRegex(
                MutationRejected,
                "source tree changed during mutation",
            ):
                _transaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                    git_callback=lambda: source.write_bytes(b"VALUE = 99\n"),
                ).apply(patch_bytes)

    def test_git_apply_timeout_is_translated_to_mutation_rejection(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            transaction = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
                git_hang=True,
            )
            with patch(
                "research_automation.control_plane.mutation.time.monotonic",
                side_effect=[0.0, 31.0] + [31.0] * 10,
            ), self.assertRaisesRegex(MutationRejected, "Git timed out"):
                transaction.apply(patch_bytes)

    def test_git_spawn_failure_is_translated_to_mutation_rejection(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            with self.assertRaisesRegex(MutationRejected, "Git unavailable"):
                _transaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                    git_unavailable=True,
                ).apply(patch_bytes)

    def test_git_output_is_bounded(self):
        from research_automation.control_plane.mutation import MutationRejected

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            transaction = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
                git_stdout=b"x" * (1024 * 1024 + 1),
            )
            with self.assertRaisesRegex(MutationRejected, "Git output exceeded limit"):
                transaction.apply(patch_bytes)

    def test_delete_patch_is_rejected_before_workspace_mutation(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            patches = {
                "extended-header": b"""diff --git a/allowed.py b/allowed.py
deleted file mode 100644
--- a/allowed.py
+++ /dev/null
@@ -1 +0,0 @@
-VALUE = 1
""",
                "dev-null-header": b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ /dev/null
@@ -1 +0,0 @@
-VALUE = 1
""",
            }
            for variant, patch_bytes in patches.items():
                with self.subTest(variant=variant), self.assertRaisesRegex(
                    MutationRejected,
                    "unsupported patch operation",
                ):
                    _transaction(
                        repository_root=root,
                        allowed_files=("allowed.py",),
                    ).apply(patch_bytes)
            self.assertEqual(source.read_bytes(), b"VALUE = 1\n")

    def test_git_binary_patch_is_rejected_before_workspace_mutation(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "blob.bin"
            source.write_bytes(b"old")
            patch_bytes = b"""diff --git a/blob.bin b/blob.bin
GIT binary patch
literal 3
KcmZQzWC8#H2mk;8
"""
            with self.assertRaisesRegex(
                MutationRejected,
                "unsupported patch operation",
            ):
                _transaction(
                    repository_root=root,
                    allowed_files=("blob.bin",),
                ).apply(patch_bytes)
            self.assertEqual(source.read_bytes(), b"old")

    def test_non_content_patch_operations_are_rejected(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        operations = {
            "rename": "rename from allowed.py\nrename to other.py",
            "copy": "copy from allowed.py\ncopy to other.py",
            "mode-only": "old mode 100644\nnew mode 100755",
        }
        for operation, metadata in operations.items():
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "project"
                root.mkdir()
                source = root / "allowed.py"
                source.write_bytes(b"VALUE = 1\n")
                (root / "other.py").write_bytes(b"OTHER = 1\n")
                patch_bytes = (
                    "diff --git a/allowed.py b/allowed.py\n"
                    f"{metadata}\n"
                ).encode("utf-8")
                with self.assertRaisesRegex(
                    MutationRejected,
                    "unsupported patch operation",
                ):
                    _transaction(
                        repository_root=root,
                        allowed_files=("allowed.py", "other.py"),
                    ).apply(patch_bytes)
                self.assertEqual(source.read_bytes(), b"VALUE = 1\n")

    def test_allowlisted_parent_symlink_is_rejected(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            real = root / "real"
            real.mkdir()
            source = real / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            alias = root / "alias"
            try:
                alias.symlink_to(real, target_is_directory=True)
            except OSError as error:
                self.skipTest(f"directory symlink unavailable: {error}")
            patch_bytes = b"""diff --git a/alias/allowed.py b/alias/allowed.py
--- a/alias/allowed.py
+++ b/alias/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            with self.assertRaisesRegex(
                MutationRejected,
                "allowlisted source is unsafe",
            ):
                _transaction(
                    repository_root=root,
                    allowed_files=("alias/allowed.py",),
                ).apply(patch_bytes)
            self.assertEqual(source.read_bytes(), b"VALUE = 1\n")

    def test_allowlisted_file_inside_nested_repository_is_rejected(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            nested = root / "vendor" / "sub"
            nested.mkdir(parents=True)
            (nested / ".git").mkdir()
            source = nested / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            patch_bytes = b"""diff --git a/vendor/sub/allowed.py b/vendor/sub/allowed.py
--- a/vendor/sub/allowed.py
+++ b/vendor/sub/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            with self.assertRaisesRegex(MutationRejected, "nested repository"):
                _transaction(
                    repository_root=root,
                    allowed_files=("vendor/sub/allowed.py",),
                ).apply(patch_bytes)
            self.assertEqual(source.read_bytes(), b"VALUE = 1\n")

    def test_mutation_result_has_closed_immutable_file_domain(self):
        from research_automation.control_plane.mutation import MutationTransaction

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            (root / "other.py").write_bytes(b"OTHER = 1\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            result = _transaction(
                repository_root=root,
                allowed_files=("allowed.py", "other.py"),
            ).apply(patch_bytes)
            expected = {"allowed.py"}
            self.assertEqual(set(result.changed_files), expected)
            self.assertEqual(set(result.files), expected)
            self.assertEqual(set(result.before_sha256), expected)
            self.assertEqual(set(result.after_sha256), expected)
            with self.assertRaises(TypeError):
                result.after_sha256["forged.py"] = "0" * 64

    def test_selected_test_failure_discards_workspace_result(self):
        from research_automation.control_plane.mutation import MutationRejected

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            with self.assertRaisesRegex(MutationRejected, "selected test failed"):
                _transaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                    selected_tests=(("python", "-c", "raise SystemExit(7)"),),
                    sandbox_exit_code=7,
                ).apply(patch_bytes)
            self.assertEqual(source.read_bytes(), b"VALUE = 1\n")

    def test_selected_test_output_limit_forces_container_cleanup(self):
        from research_automation.control_plane.mutation import MutationRejected

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            transaction = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
                sandbox_stdout=b"x" * (8 * 1024 * 1024 + 1),
            )
            with self.assertRaisesRegex(MutationRejected, "output exceeded limit"):
                transaction.apply(patch_bytes)
            cleanup_verbs = [
                command[0] for command in transaction.cleanup_commands
            ]
            self.assertGreaterEqual(len(cleanup_verbs), 4)
            self.assertEqual(cleanup_verbs[:4], ["rm", "container"] * 2)

    def test_selected_test_timeout_forces_container_cleanup(self):
        from research_automation.control_plane.mutation import MutationRejected

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            transaction = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
                sandbox_hang=True,
            )
            clock_calls = 0

            def fake_monotonic():
                nonlocal clock_calls
                clock_calls += 1
                return (
                    0.0
                    if clock_calls < 50
                    else 121.0 + (clock_calls - 50) * 0.2
                )

            with patch(
                "research_automation.control_plane.mutation.time.monotonic",
                side_effect=fake_monotonic,
            ), self.assertRaisesRegex(MutationRejected, "selected test timed out"):
                transaction.apply(patch_bytes)
            cleanup_verbs = [
                command[0] for command in transaction.cleanup_commands
            ]
            self.assertGreaterEqual(len(cleanup_verbs), 4)
            self.assertEqual(cleanup_verbs[:4], ["rm", "container"] * 2)

    def test_daemon_failure_cannot_masquerade_as_container_cleanup(self):
        from research_automation.control_plane.mutation import MutationRejected

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            transaction = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
                cleanup_returncode=125,
            )
            with self.assertRaisesRegex(MutationRejected, "IN_DOUBT"):
                transaction.apply(patch_bytes)

    def test_selected_test_uses_disposable_support_files_and_records_actor(self):
        from research_automation.control_plane.mutation import MutationTransaction

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            source = root / "allowed.py"
            source.write_bytes(b"VALUE = 1\n")
            (root / "fixture.txt").write_bytes(b"fixture\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            command = (
                "python",
                "-c",
                "from pathlib import Path; "
                "assert Path('allowed.py').read_text() == 'VALUE = 2\\n'; "
                "assert Path('fixture.txt').read_text() == 'fixture\\n'",
            )
            result = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
                support_files=("fixture.txt",),
                selected_tests=(command,),
            ).apply(patch_bytes)
            self.assertEqual(result.actor.actor_id, "mutation-test")
            receipt = result.selected_test_receipts[0]
            self.assertEqual(receipt.argv, command)
            self.assertEqual(receipt.returncode, 0)
            self.assertEqual(receipt.container_image, "python@sha256:" + "1" * 64)
            self.assertRegex(receipt.container_runtime_sha256, r"[0-9a-f]{64}\Z")
            self.assertEqual(source.read_bytes(), b"VALUE = 1\n")

    def test_selected_test_is_containerized_without_host_side_effect(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            untouched = root / "untouched.txt"
            untouched.write_bytes(b"original\n")
            patch_bytes = b"""diff --git a/allowed.py b/allowed.py
--- a/allowed.py
+++ b/allowed.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
"""
            command = (
                "python",
                "-c",
                "from pathlib import Path; "
                f"Path({str(untouched)!r}).write_text('tampered')",
            )
            transaction = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
                selected_tests=(command,),
            )
            result = transaction.apply(patch_bytes)
            controls = result.selected_test_receipts[0].sandbox_controls
            self.assertIn("--pull=never", controls)
            self.assertIn("--network=none", controls)
            self.assertIn("--read-only", controls)
            self.assertIn("workspace-mount=readonly", controls)
            self.assertEqual(
                transaction.container_env["DOCKER_HOST"],
                "npipe:////./pipe/dockerDesktopLinuxEngine",
            )
            self.assertNotIn("DOCKER_CONTEXT", transaction.container_env)
            self.assertNotIn("USERPROFILE", transaction.container_env)
            self.assertEqual(untouched.read_bytes(), b"original\n")

    def test_selected_test_command_cannot_reference_host_executable(self):
        from research_automation.control_plane.mutation import MutationRejected

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            with self.assertRaisesRegex(MutationRejected, "container command"):
                _transaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                    selected_tests=((sys.executable, "-c", "pass"),),
                )

    def test_caller_cannot_self_pin_unbound_container_runtime(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            runtime = root.parent / "docker.exe"
            shutil.copy2(Path(shutil.which("where")), runtime)
            bound = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
            )
            policy = dict(bound.policy)
            policy["container_runtime_path"] = str(runtime)
            policy["container_runtime_sha256"] = hashlib.sha256(
                runtime.read_bytes()
            ).hexdigest()
            unbound_policy = canonical_json(policy).encode("utf-8")
            with self.assertRaisesRegex(MutationRejected, "not Authority-bound"):
                MutationTransaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                    selected_tests=(("python", "-c", "pass"),),
                    authority_lease=bound.lease,
                    sandbox_policy_bytes=unbound_policy,
                )

    def test_non_p4_authority_lease_cannot_authorize_mutation(self):
        from research_automation.control_plane.mutation import (
            MutationRejected,
            MutationTransaction,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            (root / "allowed.py").write_bytes(b"VALUE = 1\n")
            bound = _transaction(
                repository_root=root,
                allowed_files=("allowed.py",),
            )
            binding = stores_module.AuthorityReader().execution_lease_binding(
                bound.lease
            )
            with patch.object(
                stores_module.AuthorityReader,
                "execution_lease_binding",
                return_value=replace(binding, phase=Phase.P3),
            ), self.assertRaisesRegex(MutationRejected, "requires P4 authority"):
                MutationTransaction(
                    repository_root=root,
                    allowed_files=("allowed.py",),
                    selected_tests=(("python", "-c", "pass"),),
                    authority_lease=bound.lease,
                    sandbox_policy_bytes=canonical_json(bound.policy).encode("utf-8"),
                )


if __name__ == "__main__":
    unittest.main()
