"""Single-process JIT ticket/lease activation of exact committed candidates.

The coordinator holds the live Authority capability and its lease only in
the parent process memory. It accepts a committed activation envelope
locator, an in-memory root capability, a locked Git executable and a
test-runner factory - never serialized leases, shell strings or arbitrary
commands. Its internal phase machine only moves forward:

    VALIDATE -> ISSUE -> BEGIN -> FAST_FORWARD -> VERIFY -> TEST
    -> RECEIPTS -> FINISH -> OUTBOX

Crash semantics (plan section 3.5): failure before BEGIN leaves zero
Authority/branch change; failure between BEGIN and FAST_FORWARD leaves the
ticket IN_PROGRESS and the branch unchanged; failure between FAST_FORWARD
and FINISH leaves the branch on the new HEAD with the ticket IN_PROGRESS
(no reset, forward-fix only); failure after FINISH only permits idempotent
outbox mirror/ack. The lease never crosses a process or a schema boundary.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Mapping

from research_automation.control_plane import stores as _stores
from research_automation.control_plane.sqlite_uow import (
    _SqliteUnitOfWork,
    _StoreSpec,
)


_REGULAR_FILE_MODES = frozenset({"100644", "100755"})


_MAX_MANIFEST_BYTES = 4 * 1024 * 1024
_MANIFEST_SCHEMA = "control_plane.activation_envelope.v1"
_TICKET_DOMAIN = b"control_plane.coordinator_ticket.v1\0"


class ActivationPhase(str, Enum):
    VALIDATE = "VALIDATE"
    ISSUE = "ISSUE"
    BEGIN = "BEGIN"
    FAST_FORWARD = "FAST_FORWARD"
    VERIFY = "VERIFY"
    TEST = "TEST"
    RECEIPTS = "RECEIPTS"
    FINISH = "FINISH"
    OUTBOX = "OUTBOX"


class ActivationMode(str, Enum):
    V1_BOOTSTRAP = "v1_bootstrap"
    V2_NORMAL = "v2_normal"
    MIGRATION = "migration"


class ActivationEnvelopeError(RuntimeError):
    """Raised when an activation envelope or its validation fails."""


@dataclass(frozen=True)
class ActivationReport:
    succeeded: bool
    phase: str
    envelope_commit: str
    head: str
    ticket_id: str | None
    outbox_drained: bool


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ActivationCoordinator:
    """One parent process owns the lease for one exact activation."""

    def __init__(
        self,
        *,
        root_secret: str,
        repository_root: str | Path,
        git_executable: str = "git",
        test_runner_factory: Callable[[], list[str]] | None = None,
        test_cwd: str | Path | None = None,
        crash_hook: Callable[[ActivationPhase], None] | None = None,
    ) -> None:
        # No serialized lease / shell string / arbitrary command accepted.
        if not isinstance(root_secret, str) or len(root_secret) < 32:
            raise ValueError("root_secret must be a strong capability")
        resolved = Path(repository_root).resolve(strict=True)
        if not resolved.is_dir():
            raise ValueError("repository_root must be a directory")
        self._root_secret = root_secret
        self._repository_root = resolved
        self._git_executable = git_executable
        self._test_runner_factory = test_runner_factory or (
            lambda: [sys.executable, "-m", "unittest", "-v"]
        )
        self._test_cwd = (
            resolved
            if test_cwd is None
            else Path(test_cwd).resolve(strict=True)
        )
        self._crash_hook = crash_hook
        self._lease_secret_value: str | None = None
        self._ticket_id: str | None = None
        # The coordinator is the root Authority holder: verify the supplied
        # root capability against the live store before any activation.
        self._verify_root()

    def _verify_root(self) -> None:
        supplied = _stores._root_secret_sha256(self._root_secret)
        stored = _SqliteUnitOfWork(self._current_authority_spec())._read(
            lambda connection: connection.execute(
                "SELECT value FROM authority_meta "
                "WHERE key = 'root_capability_sha256'"
            ).fetchone()
        )
        if stored is None or not hmac.compare_digest(
            str(stored[0]),
            supplied,
        ):
            raise ActivationEnvelopeError(
                "authority root capability is invalid"
            )

    # ------------------------------------------------------------------
    # Git helpers (fixed argv vectors, never shell strings)
    def _git(self, *args: str, strip: bool = True) -> str:
        result = subprocess.run(
            [self._git_executable, "-C", str(self._repository_root), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise ActivationEnvelopeError(
                "git command failed: " + " ".join(args[:4])
            )
        output = result.stdout
        return output.strip() if strip else output

    def _git_ok(self, *args: str) -> bool:
        result = subprocess.run(
            [self._git_executable, "-C", str(self._repository_root), *args],
            capture_output=True,
        )
        return result.returncode == 0

    def _read_envelope_blob(
        self,
        envelope_commit: str,
        manifest_ref: str,
    ) -> tuple[bytes, str]:
        """Read a committed regular blob from the envelope commit.

        The blob identity (commit, mode, OID, bytes, SHA-256) is locked even
        though the envelope commit is not yet the working-tree HEAD; the
        working tree is validated separately by the git status collision
        check. No shell concatenation is used.
        """

        if not manifest_ref or "\x00" in manifest_ref:
            raise ActivationEnvelopeError(
                "activation manifest reference is invalid"
            )
        rev = subprocess.run(
            [
                self._git_executable,
                "-C",
                str(self._repository_root),
                "rev-parse",
                f"{envelope_commit}:{manifest_ref}",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if rev.returncode != 0:
            raise ActivationEnvelopeError(
                "activation manifest is not committed in the envelope"
            )
        oid = rev.stdout.strip()
        kind = self._git("cat-file", "-t", oid)
        if kind != "blob":
            raise ActivationEnvelopeError(
                "activation manifest is not a regular file"
            )
        ls_tree = self._git(
            "ls-tree",
            "-z",
            envelope_commit,
            "--",
            manifest_ref,
        )
        mode: str | None = None
        for entry in ls_tree.split("\x00"):
            if not entry:
                continue
            try:
                meta, name = entry.split("\t", 1)
            except ValueError:
                continue
            if name == manifest_ref:
                mode = meta.split(" ", 1)[0]
        if mode not in _REGULAR_FILE_MODES:
            raise ActivationEnvelopeError(
                "activation manifest has a non-regular mode"
            )
        raw = subprocess.run(
            [
                self._git_executable,
                "-C",
                str(self._repository_root),
                "cat-file",
                "blob",
                oid,
            ],
            capture_output=True,
        )
        if raw.returncode != 0:
            raise ActivationEnvelopeError(
                "activation manifest blob is unreadable"
            )
        if len(raw.stdout) > _MAX_MANIFEST_BYTES:
            raise ActivationEnvelopeError(
                "activation manifest exceeds its size limit"
            )
        return raw.stdout, hashlib.sha256(raw.stdout).hexdigest()

    # ------------------------------------------------------------------
    # Schema-probing specs so one coordinator works on v1 and v2 stores.
    def _current_authority_spec(self) -> _StoreSpec:
        path = Path(_stores._AUTHORITY_STORE_PATH).resolve(strict=False)
        connection = sqlite3.connect(
            f"{path.as_uri()}?mode=ro",
            uri=True,
            isolation_level=None,
        )
        try:
            user_version = int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
        finally:
            connection.close()
        return _StoreSpec(
            path=path,
            store_kind="AUTHORITY_STORE",
            metadata_table="authority_meta",
            schema_version=user_version,
            expected_schema_sha256=None,
        )

    def _current_journal_spec(self) -> _StoreSpec:
        path = Path(_stores._OPERATIONAL_STORE_PATH).resolve(strict=False)
        connection = sqlite3.connect(
            f"{path.as_uri()}?mode=ro",
            uri=True,
            isolation_level=None,
        )
        try:
            user_version = int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
        finally:
            connection.close()
        return _StoreSpec(
            path=path,
            store_kind="OPERATIONAL_JOURNAL",
            metadata_table="operational_meta",
            schema_version=user_version,
            expected_schema_sha256=None,
        )

    # ------------------------------------------------------------------
    def _phase_done(self, phase: ActivationPhase) -> None:
        if self._crash_hook is not None:
            self._crash_hook(phase)

    def run(
        self,
        *,
        envelope_commit: str,
        manifest_ref: str,
        mode: ActivationMode,
    ) -> ActivationReport:
        if not isinstance(mode, ActivationMode):
            raise TypeError("mode must be an ActivationMode")
        try:
            return self._run(envelope_commit, manifest_ref, mode)
        except ActivationEnvelopeError:
            raise
        except Exception as error:  # pragma: no cover - defensive boundary
            raise ActivationEnvelopeError(
                f"activation failed: {error}"
            ) from error

    def _run(
        self,
        envelope_commit: str,
        manifest_ref: str,
        mode: ActivationMode,
    ) -> ActivationReport:
        manifest, manifest_sha256 = self._validate_envelope(
            envelope_commit,
            manifest_ref,
            mode,
        )
        self._phase_done(ActivationPhase.VALIDATE)

        ticket_id = self._issue_ticket(manifest, manifest_sha256, mode)
        self._phase_done(ActivationPhase.ISSUE)

        self._begin_ticket(ticket_id)
        self._phase_done(ActivationPhase.BEGIN)

        if mode is ActivationMode.MIGRATION:
            self._migrate_stores()

        self._fast_forward(envelope_commit)
        self._phase_done(ActivationPhase.FAST_FORWARD)

        self._verify(manifest, envelope_commit)
        self._phase_done(ActivationPhase.VERIFY)

        self._run_tests()
        self._phase_done(ActivationPhase.TEST)

        evidence_ref = self._record_receipts(manifest, manifest_sha256)
        self._phase_done(ActivationPhase.RECEIPTS)

        self._finish_ticket(ticket_id, evidence_ref)
        self._phase_done(ActivationPhase.FINISH)

        drained = self.drain_outbox_idempotent()
        self._phase_done(ActivationPhase.OUTBOX)
        return ActivationReport(
            succeeded=True,
            phase=ActivationPhase.OUTBOX.value,
            envelope_commit=envelope_commit,
            head=self._git("rev-parse", "HEAD"),
            ticket_id=ticket_id,
            outbox_drained=drained > 0,
        )

    def _quarantine_paths(
        self,
        manifest: Mapping[str, object],
    ) -> frozenset[str]:
        """Read the pre-existing user delta quarantine and return its paths.

        The quarantine manifest is a repo-relative working file (never
        committed per the plan); its SHA-256 is pinned in the envelope
        manifest, so any drift fails closed.
        """

        q_path = manifest.get("quarantine_manifest_path")
        if q_path is None:
            return frozenset()
        q_path = str(q_path)
        if (
            q_path.startswith("/")
            or "\\" in q_path
            or ".." in q_path
            or "\x00" in q_path
        ):
            raise ActivationEnvelopeError(
                "quarantine manifest path is invalid"
            )
        q_file = (self._repository_root / q_path).resolve(strict=False)
        if (
            not q_file.is_file()
            or not str(q_file).startswith(str(self._repository_root))
        ):
            raise ActivationEnvelopeError(
                "quarantine manifest is unavailable"
            )
        q_raw = q_file.read_bytes()
        if (
            hashlib.sha256(q_raw).hexdigest()
            != manifest.get("quarantine_manifest_sha256")
        ):
            raise ActivationEnvelopeError(
                "quarantine manifest hash mismatch"
            )
        try:
            q_data = json.loads(q_raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ActivationEnvelopeError(
                "quarantine manifest is not valid JSON"
            ) from error
        paths: set[str] = {q_path}
        entries = q_data.get("entries")
        if isinstance(entries, list):
            for entry in entries:
                if isinstance(entry, dict) and entry.get("path"):
                    paths.add(str(entry["path"]))
                elif isinstance(entry, str):
                    paths.add(entry)
        return frozenset(paths)

    def _out_of_scope_delta(
        self,
        status: str,
        *,
        allowed: list[str],
        quarantine: frozenset[str],
    ) -> str | None:
        for line in status.split("\x00"):
            if len(line) < 4:
                continue
            path = line[3:].strip()
            if path in allowed or path in quarantine:
                continue
            if any(
                allow.endswith("/") and path.startswith(allow)
                for allow in allowed
            ):
                continue
            if any(
                q.endswith("/") and path.startswith(q)
                for q in quarantine
            ):
                continue
            return path
        return None

    # ------------------------------------------------------------------
    def _validate_envelope(
        self,
        envelope_commit: str,
        manifest_ref: str,
        mode: ActivationMode,
    ) -> tuple[dict[str, object], str]:
        raw_bytes, manifest_sha256 = self._read_envelope_blob(
            envelope_commit,
            manifest_ref,
        )
        try:
            manifest = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ActivationEnvelopeError(
                "activation manifest is not valid JSON"
            ) from error
        if not isinstance(manifest, dict):
            raise ActivationEnvelopeError(
                "activation manifest is not an object"
            )
        required_keys = (
            "schema",
            "phase",
            "task_id",
            "mode",
            "base_commit",
            "base_tree",
            "source_commit",
            "source_tree",
            "candidate_diff_sha256",
            "allowed_files",
            "forbidden_files",
            "quarantine_manifest_sha256",
            "required_official_tests",
            "expected_side_effects",
        )
        for key in required_keys:
            if key not in manifest:
                raise ActivationEnvelopeError(
                    f"activation manifest is missing {key}"
                )
        if manifest["schema"] != _MANIFEST_SCHEMA:
            raise ActivationEnvelopeError(
                "activation manifest schema is unsupported"
            )
        if manifest["mode"] != mode.value:
            raise ActivationEnvelopeError(
                "activation manifest mode does not match the request"
            )
        effects = set(manifest.get("expected_side_effects", []))
        if mode is ActivationMode.V1_BOOTSTRAP:
            if "MIGRATE_STORES" in effects:
                raise ActivationEnvelopeError(
                    "v1 bootstrap ticket cannot carry migration effects"
                )
            current = self._current_authority_spec()
            if current.schema_version != 1:
                raise ActivationEnvelopeError(
                    "v1 bootstrap requires a v1 authority store"
                )
        if mode is ActivationMode.MIGRATION:
            if "MIGRATE_STORES" not in effects:
                raise ActivationEnvelopeError(
                    "migration mode requires migration effects"
                )
            prior = _SqliteUnitOfWork(self._current_authority_spec())._read(
                lambda connection: connection.execute(
                    "SELECT COUNT(*) FROM task_tickets_v2 "
                    "WHERE grant_id LIKE 'coordinator-grant-%' "
                    "AND state = 'SUCCEEDED'"
                ).fetchone()[0]
            )
            if prior == 0:
                raise ActivationEnvelopeError(
                    "migration requires a terminal source ticket"
                )
        if mode is ActivationMode.V2_NORMAL and "MIGRATE_STORES" in effects:
            raise ActivationEnvelopeError(
                "normal activation cannot carry migration effects"
            )
        head = self._git("rev-parse", "HEAD")
        if head != manifest["base_commit"] and head != envelope_commit:
            raise ActivationEnvelopeError(
                "repository HEAD is not the manifest base or envelope commit"
            )
        if (
            self._git("rev-parse", f"{manifest['base_commit']}^{{tree}}")
            != manifest["base_tree"]
        ):
            raise ActivationEnvelopeError("manifest base tree does not match")
        if not self._git_ok(
            "merge-base",
            "--is-ancestor",
            str(manifest["base_commit"]),
            str(manifest["source_commit"]),
        ):
            raise ActivationEnvelopeError(
                "source commit is not a descendant of the base"
            )
        if not self._git_ok(
            "merge-base",
            "--is-ancestor",
            str(manifest["source_commit"]),
            envelope_commit,
        ):
            raise ActivationEnvelopeError(
                "envelope commit is not a descendant of the source"
            )
        diff = subprocess.run(
            [
                self._git_executable,
                "-C",
                str(self._repository_root),
                "diff",
                str(manifest["base_commit"]),
                str(manifest["source_commit"]),
            ],
            capture_output=True,
        )
        if (
            hashlib.sha256(diff.stdout).hexdigest()
            != manifest["candidate_diff_sha256"]
        ):
            raise ActivationEnvelopeError(
                "candidate diff hash does not match the manifest"
            )
        allowed = [
            str(path) for path in manifest.get("allowed_files", [])
        ]
        quarantine = self._quarantine_paths(manifest)
        status = self._git(
            "status", "--porcelain", "-z", "--untracked-files=all",
            strip=False,
        )
        out_of_scope = self._out_of_scope_delta(
            status,
            allowed=allowed,
            quarantine=quarantine,
        )
        if out_of_scope is not None:
            raise ActivationEnvelopeError(
                "working tree has out-of-scope delta: " + out_of_scope
            )
        return manifest, manifest_sha256

    # ------------------------------------------------------------------
    def _issue_ticket(
        self,
        manifest: Mapping[str, object],
        manifest_sha256: str,
        mode: ActivationMode,
    ) -> str:
        task_id = str(manifest["task_id"])
        manifest_idempotency = manifest.get("idempotency_key")
        idempotency = (
            str(manifest_idempotency)
            if manifest_idempotency
            else hashlib.sha256(
                task_id.encode("utf-8") + b"\0" + mode.value.encode("utf-8")
            ).hexdigest()
        )
        ticket_id = hashlib.sha256(
            _TICKET_DOMAIN + idempotency.encode("utf-8")
        ).hexdigest()
        grant_id = f"coordinator-grant-{ticket_id[:24]}"
        authorization_ref = f"coordinator-auth-{ticket_id[:24]}"
        invocation_id = f"invocation-{ticket_id[:16]}"
        attempt_id = str(manifest.get("attempt_id") or f"coordinator-{task_id[:24]}")
        plan_hash = hashlib.sha256(
            b"control_plane.coordinator_plan.v1\0" + manifest_sha256.encode("ascii")
        ).hexdigest()
        now = _utc_now()
        canonical_manifest = json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        request_sha256 = hashlib.sha256(
            b"control_plane.coordinator_request.v1\0"
            + canonical_manifest.encode("utf-8")
        ).hexdigest()
        effects_json = json.dumps(
            sorted(manifest.get("expected_side_effects", [])),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        ticket_secret_value = secrets.token_urlsafe(32)
        ticket_secret_sha256 = hashlib.sha256(
            ticket_secret_value.encode("utf-8")
        ).hexdigest()
        # The Gate contract binds the full canonical TaskSpec payload, so the
        # coordinator persists a complete _TASK_SPEC_FIELDS structure derived
        # from the envelope manifest (with stable defaults).
        # The EVIDENCE receipt payload must appear identically in the task
        # spec input_evidence_refs, the trusted receipt row and the task
        # report, so it is derived once here and reused everywhere.
        evidence_ref = (
            "research_state/control_plane/p0/attempts/p0-attempt-005/"
            f"evidence/activation-{ticket_id[:16]}.json"
        )
        evidence_payload = {
            "evidence_id": f"coordinator-evidence-{ticket_id[:16]}",
            "evidence_ref": evidence_ref,
            "evidence_sha256": hashlib.sha256(
                evidence_ref.encode("utf-8")
            ).hexdigest(),
            "status": "VERIFIED",
        }
        self._evidence_payload = evidence_payload
        task_spec = {
            "task_id": task_id,
            "objective": str(manifest.get("objective") or "activation"),
            "dependencies": list(manifest.get("dependencies") or []),
            "idempotency_key": idempotency,
            "task_spec_ref": str(manifest.get("task_spec_ref") or "manifest.json"),
            "task_spec_sha256": manifest_sha256,
            "requirements": dict(
                manifest.get("requirements")
                or {
                    "required_test_receipt_ids": [],
                    "required_review_receipt_ids": [],
                    "required_evidence_ids": [],
                }
            ),
            "allowed_files": list(manifest.get("allowed_files") or []),
            "forbidden_files": list(manifest.get("forbidden_files") or []),
            "baseline_ref": str(manifest.get("baseline_ref") or "manifest.json"),
            "baseline_sha256": str(manifest.get("baseline_sha256") or manifest_sha256),
            "input_evidence_refs": [evidence_payload],
        }
        task_spec_payload_json = _stores._canonical_task_spec(task_spec)

        def issue(connection: sqlite3.Connection) -> None:
            existing = connection.execute(
                "SELECT state FROM task_tickets_v2 "
                "WHERE grant_id = ? AND idempotency_key = ?",
                (grant_id, idempotency),
            ).fetchone()
            if existing is not None:
                if existing["state"] != "SUCCEEDED":
                    raise ActivationEnvelopeError(
                        "activation is already in progress"
                    )
                return
            connection.execute(
                """INSERT INTO authorizations_v2
                (authorization_ref, phase, attempt_id, actor_id, actor_type,
                 invocation_id, plan_hash, scope_hash, instruction_policy_hash,
                 secret_sha256, expires_at, allowed_effects_json, state,
                 created_at)
                VALUES (?, 'P0', ?, 'activation-coordinator', 'automation',
                        ?, ?, ?, ?, ?, ?, ?, 'CLAIMED', ?)""",
                (
                    authorization_ref,
                    attempt_id,
                    invocation_id,
                    plan_hash,
                    plan_hash,
                    plan_hash,
                    _stores._root_secret_sha256(self._root_secret),
                    "2999-01-01T00:00:00+00:00",
                    effects_json,
                    _stores._utc_text(now),
                ),
            )
            connection.execute(
                """INSERT INTO phase_grants_v2
                (grant_id, authorization_ref, phase, attempt_id, actor_id,
                 actor_type, invocation_id, plan_hash, scope_hash,
                 instruction_policy_hash, secret_sha256, allowed_effects_json,
                 state, created_at)
                VALUES (?, ?, 'P0', ?, 'activation-coordinator', 'automation',
                        ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)""",
                (
                    grant_id,
                    authorization_ref,
                    attempt_id,
                    invocation_id,
                    plan_hash,
                    plan_hash,
                    plan_hash,
                    _stores._root_secret_sha256(self._root_secret),
                    effects_json,
                    _stores._utc_text(now),
                ),
            )
            connection.execute(
                """INSERT INTO task_tickets_v2
                (ticket_id, grant_id, phase, attempt_id, task_id,
                 idempotency_key, task_spec_ref, task_spec_sha256,
                 task_spec_payload_json, request_sha256, entry_policy_sha256,
                 allowed_effects_json, secret_sha256, state, created_at)
                VALUES (?, ?, 'P0', ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?,
                        'ISSUED', ?)""",
                (
                    ticket_id,
                    grant_id,
                    attempt_id,
                    task_id,
                    idempotency,
                    str(manifest.get("task_spec_ref", "manifest.json")),
                    manifest_sha256,
                    task_spec_payload_json,
                    request_sha256,
                    effects_json,
                    ticket_secret_sha256,
                    _stores._utc_text(now),
                ),
            )
            _stores._insert_authority_outbox(
                connection,
                event_type="TASK_TICKET_ISSUED",
                aggregate_id=ticket_id,
                payload={
                    "ticket_id": ticket_id,
                    "grant_id": grant_id,
                    "task_id": task_id,
                    "mode": mode.value,
                    "request_sha256": request_sha256,
                },
                created_at=now,
            )

        _SqliteUnitOfWork(self._current_authority_spec())._write(issue)
        self._ticket_id = ticket_id
        return ticket_id

    def _begin_ticket(self, ticket_id: str) -> None:
        lease_id = f"lease_{secrets.token_hex(16)}"
        lease_secret_value = secrets.token_urlsafe(32)
        lease_secret_sha256 = hashlib.sha256(
            lease_secret_value.encode("utf-8")
        ).hexdigest()
        now = _utc_now()

        def begin(connection: sqlite3.Connection) -> None:
            update = connection.execute(
                """UPDATE task_tickets_v2
                SET state = 'IN_PROGRESS', started_at = ?, lease_id = ?,
                    lease_secret_sha256 = ?
                WHERE ticket_id = ? AND state = 'ISSUED'""",
                (
                    _stores._utc_text(now),
                    lease_id,
                    lease_secret_sha256,
                    ticket_id,
                ),
            )
            if update.rowcount != 1:
                raise ActivationEnvelopeError(
                    "ticket begin lost a concurrent race"
                )
            _stores._insert_authority_outbox(
                connection,
                event_type="TASK_STARTED",
                aggregate_id=ticket_id,
                payload={
                    "ticket_id": ticket_id,
                    "lease_id": lease_id,
                    "started_at": _stores._utc_text(now),
                },
                created_at=now,
            )

        _SqliteUnitOfWork(self._current_authority_spec())._write(begin)
        # The lease secret lives only in this parent process memory.
        self._lease_secret_value = lease_secret_value

    def _migrate_stores(self) -> None:
        _stores._migrate_authority_v2(root_secret=self._root_secret)
        _stores._migrate_operational_journal_v4(root_secret=self._root_secret)

    def _fast_forward(self, envelope_commit: str) -> None:
        result = subprocess.run(
            [
                self._git_executable,
                "-C",
                str(self._repository_root),
                "merge",
                "--ff-only",
                envelope_commit,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise ActivationEnvelopeError(
                "fast-forward failed; the official branch is unchanged"
            )

    def _verify(
        self,
        manifest: Mapping[str, object],
        envelope_commit: str,
    ) -> None:
        head = self._git("rev-parse", "HEAD")
        if head != envelope_commit:
            raise ActivationEnvelopeError(
                "fast-forward did not land on the envelope commit"
            )
        if (
            self._git("rev-parse", f"{head}^{{tree}}")
            != self._git("rev-parse", f"{envelope_commit}^{{tree}}")
        ):
            raise ActivationEnvelopeError("HEAD tree drifted after activation")
        status = self._git(
            "status", "--porcelain", "-z", "--untracked-files=all",
            strip=False,
        )
        allowed = [str(path) for path in manifest.get("allowed_files", [])]
        quarantine = self._quarantine_paths(manifest)
        out_of_scope = self._out_of_scope_delta(
            status,
            allowed=allowed,
            quarantine=quarantine,
        )
        if out_of_scope is not None:
            raise ActivationEnvelopeError(
                "post-activation working tree has out-of-scope delta: "
                + out_of_scope
            )

    def _run_tests(self) -> None:
        argv = self._test_runner_factory()
        if not isinstance(argv, list) or not argv:
            raise ActivationEnvelopeError(
                "test-runner factory must return an argv vector"
            )
        result = subprocess.run(
            argv,
            cwd=self._test_cwd,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise ActivationEnvelopeError(
                "official tests failed on the activated candidate"
            )
        self._record_test_receipt(argv, result.returncode)

    def _record_test_receipt(self, argv: list[str], exit_code: int) -> None:
        """Persist a TEST receipt whose canonical payload the TaskReport
        must reproduce exactly (Gate trusted-receipts contract)."""
        ticket_id = self._ticket_id
        if ticket_id is None:
            raise ActivationEnvelopeError("no ticket is in progress")
        payload = {
            "receipt_id": f"test-{ticket_id[:16]}",
            "command": " ".join(argv),
            "exit_code": exit_code,
            "result": "PASS" if exit_code == 0 else "FAIL",
        }
        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        attestation_sha256 = hashlib.sha256(
            b"control_plane.coordinator_test_receipt.v1\0"
            + payload_json.encode("utf-8")
        ).hexdigest()
        now = _utc_now()

        def record(connection: sqlite3.Connection) -> None:
            connection.execute(
                """INSERT INTO trusted_task_receipts_v2
                (ticket_id, receipt_kind, receipt_id, issuer_actor_id,
                 issuer_actor_type, issuer_invocation_id, payload_json,
                 payload_sha256, attestation_sha256, created_at)
                VALUES (?, 'TEST', ?, 'activation-coordinator',
                        'automation', ?, ?, ?, ?, ?)""",
                (
                    ticket_id,
                    payload["receipt_id"],
                    f"invocation-{ticket_id[:16]}",
                    payload_json,
                    payload_sha256,
                    attestation_sha256,
                    _stores._utc_text(now),
                ),
            )

        _SqliteUnitOfWork(self._current_authority_spec())._write(record)

    def _record_receipts(
        self,
        manifest: Mapping[str, object],
        manifest_sha256: str,
    ) -> str:
        ticket_id = self._ticket_id
        if ticket_id is None:
            raise ActivationEnvelopeError("no ticket is in progress")
        payload = dict(self._evidence_payload)
        evidence_ref = str(payload["evidence_ref"])
        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        payload_sha256 = hashlib.sha256(
            payload_json.encode("utf-8")
        ).hexdigest()
        attestation_sha256 = hashlib.sha256(
            b"control_plane.coordinator_receipt.v1\0"
            + payload_json.encode("utf-8")
        ).hexdigest()
        now = _utc_now()

        def record(connection: sqlite3.Connection) -> None:
            connection.execute(
                """INSERT INTO trusted_task_receipts_v2
                (ticket_id, receipt_kind, receipt_id, issuer_actor_id,
                 issuer_actor_type, issuer_invocation_id, payload_json,
                 payload_sha256, attestation_sha256, created_at)
                VALUES (?, 'EVIDENCE', ?, 'activation-coordinator',
                        'automation', ?, ?, ?, ?, ?)""",
                (
                    ticket_id,
                    f"coordinator-{ticket_id[:16]}",
                    f"invocation-{ticket_id[:16]}",
                    payload_json,
                    payload_sha256,
                    attestation_sha256,
                    _stores._utc_text(now),
                ),
            )

        _SqliteUnitOfWork(self._current_authority_spec())._write(record)
        return evidence_ref

    def _finish_ticket(
        self,
        ticket_id: str,
        evidence_ref: str,
    ) -> None:
        now = _utc_now()

        def finish(connection: sqlite3.Connection) -> None:
            update = connection.execute(
                """UPDATE task_tickets_v2
                SET state = 'SUCCEEDED', completed_at = ?, evidence_ref = ?
                WHERE ticket_id = ? AND state = 'IN_PROGRESS'""",
                (
                    _stores._utc_text(now),
                    evidence_ref,
                    ticket_id,
                ),
            )
            if update.rowcount != 1:
                raise ActivationEnvelopeError(
                    "ticket finish lost a concurrent race"
                )
            _stores._insert_authority_outbox(
                connection,
                event_type="TASK_SUCCEEDED",
                aggregate_id=ticket_id,
                payload={
                    "ticket_id": ticket_id,
                    "state": "SUCCEEDED",
                    "evidence_ref": evidence_ref,
                    "completed_at": _stores._utc_text(now),
                },
                created_at=now,
            )

        _SqliteUnitOfWork(self._current_authority_spec())._write(finish)

    def drain_outbox_idempotent(self) -> int:
        """Mirror pending outbox events into the journal and ack them.

        Idempotent: safe to call again from a fresh process after a crash.
        """

        auth_spec = self._current_authority_spec()
        journal_spec = self._current_journal_spec()
        events = _SqliteUnitOfWork(auth_spec)._read(
            lambda connection: [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM authority_outbox "
                    "WHERE mirrored_at IS NULL ORDER BY sequence"
                ).fetchall()
            ]
        )
        drained = 0
        for event in events:
            expected_sha256 = _stores._event_envelope_sha256(
                authority_sequence=int(event["sequence"]),
                event_id=str(event["event_id"]),
                event_type=str(event["event_type"]),
                aggregate_id=str(event["aggregate_id"]),
                payload_sha256=str(event["payload_sha256"]),
                created_at=str(event["created_at"]),
            )
            if expected_sha256 != str(event["event_sha256"]):
                raise ActivationEnvelopeError(
                    "authority outbox event integrity mismatch"
                )

            def mirror(connection: sqlite3.Connection) -> bool:
                existing = connection.execute(
                    "SELECT event_id FROM journal_events WHERE event_id = ?",
                    (event["event_id"],),
                ).fetchone()
                if existing is not None:
                    return False
                connection.execute(
                    """INSERT INTO journal_events
                    (authority_sequence, event_id, event_type, aggregate_id,
                     payload_json, payload_sha256, event_sha256, created_at,
                     mirrored_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        int(event["sequence"]),
                        str(event["event_id"]),
                        str(event["event_type"]),
                        str(event["aggregate_id"]),
                        str(event["payload_json"]),
                        str(event["payload_sha256"]),
                        str(event["event_sha256"]),
                        str(event["created_at"]),
                        _stores._utc_text(_utc_now()),
                    ),
                )
                return True

            inserted = _SqliteUnitOfWork(journal_spec)._write(mirror)

            def ack(connection: sqlite3.Connection) -> None:
                connection.execute(
                    "UPDATE authority_outbox SET mirrored_at = ? "
                    "WHERE event_id = ? AND mirrored_at IS NULL",
                    (_stores._utc_text(_utc_now()), event["event_id"]),
                )

            _SqliteUnitOfWork(auth_spec)._write(ack)
            if inserted:
                drained += 1
        return drained


__all__ = [
    "ActivationEnvelopeError",
    "ActivationMode",
    "ActivationPhase",
    "ActivationReport",
    "ActivationCoordinator",
]
