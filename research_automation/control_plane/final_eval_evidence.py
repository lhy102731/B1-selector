"""Final-eval result evidence: content-addressed object + per-ticket fixed
claim + pre-staging verification (CR010-R02).

The orchestrator may only enter RESULT_STAGED after every invariant holds:

  - the result OBJECT is a content-addressed committed blob at
    ``<volume>/objects/<object_sha256>.json`` whose bytes hash matches;
  - the per-ticket fixed CLAIM is a committed blob at
    ``<volume>/claims/<ticket_id>.json`` whose bytes hash matches and whose
    document binds ticket_id, binding_id, object_ref and object_sha256;
  - object and claim live in the SAME evidence volume (objects/ and
    claims/ are siblings under one volume root);
  - the claim is the ticket's unique fixed claim (one path per ticket,
    create-only publication);
  - a dangling object ref, an orphan claim, a wrong content hash, a stale
    expected version or a claim that does not reference the object fails
    closed and NEVER enters RESULT_STAGED.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .contracts import canonical_json
from .git_evidence import GitBlobReader

FINAL_EVAL_RESULT_CLAIM_SCHEMA = "control_plane.final_eval_result_claim.v1"
_MAX_RESULT_EVIDENCE_BYTES = 4 * 1024 * 1024
_OBJECTS_DIR = "objects"
_CLAIMS_DIR = "claims"
_GIT_IDENTITY = ("Control Plane Final Eval", "control-plane-final-eval@example.invalid")


class FinalEvalEvidenceError(RuntimeError):
    """Base error for final-eval result evidence."""


@dataclass(frozen=True, slots=True)
class FinalEvalEvidenceRefs:
    """The four committed bindings the Authority CAS stages."""

    object_ref: str
    object_sha256: str
    claim_ref: str
    claim_sha256: str

    def to_payload(self) -> dict[str, object]:
        return {
            "object_ref": self.object_ref,
            "object_sha256": self.object_sha256,
            "claim_ref": self.claim_ref,
            "claim_sha256": self.claim_sha256,
        }


def _volume_root(ref: str, dir_name: str) -> str:
    """Return the volume root for a ref under <volume>/<dir_name>/<name>."""
    parts = ref.replace("\\", "/").split("/")
    try:
        index = parts.index(dir_name)
    except ValueError as error:
        raise FinalEvalEvidenceError(
            f"evidence ref is not under a {dir_name}/ namespace: {ref}"
        ) from error
    if index == 0 or index != len(parts) - 2:
        raise FinalEvalEvidenceError(
            f"evidence ref is malformed: {ref}"
        )
    return "/".join(parts[:index])


def verify_result_evidence(
    binding_id: str,
    *,
    ticket_id: str,
    object_ref: str,
    object_sha256: str,
    claim_ref: str,
    claim_sha256: str,
    repository_root: str | Path,
    expected_outcome: str | None = None,
) -> None:
    """Fail closed unless every pre-staging invariant holds (CR010-R02).

    ``expected_outcome`` (CR-010 F-01) is the outcome DERIVED from the real
    worker exit code by the orchestrator; the committed claim must agree
    with it.  A sink that publishes a different outcome (e.g. SUCCEEDED for
    a failed worker) can never stage the result.
    """
    if not binding_id or not isinstance(binding_id, str):
        raise FinalEvalEvidenceError("binding_id must be non-empty")
    if not ticket_id or not isinstance(ticket_id, str):
        raise FinalEvalEvidenceError("ticket_id must be non-empty")
    if not isinstance(object_sha256, str) or len(object_sha256) != 64:
        raise FinalEvalEvidenceError("object_sha256 must be a SHA-256 digest")
    if not isinstance(claim_sha256, str) or len(claim_sha256) != 64:
        raise FinalEvalEvidenceError("claim_sha256 must be a SHA-256 digest")

    # 1. object and claim live in the SAME evidence volume.
    object_volume = _volume_root(object_ref, _OBJECTS_DIR)
    claim_volume = _volume_root(claim_ref, _CLAIMS_DIR)
    if object_volume != claim_volume:
        raise FinalEvalEvidenceError(
            "result object and claim are not in the same evidence volume"
        )

    # 2. the object is content-addressed: <volume>/objects/<sha>.json
    object_name = object_ref.replace("\\", "/").split("/")[-1]
    if object_name != f"{object_sha256}.json":
        raise FinalEvalEvidenceError(
            "result object path is not content-addressed by its sha256"
        )

    # 3. the claim is the ticket's UNIQUE fixed claim path.
    claim_name = claim_ref.replace("\\", "/").split("/")[-1]
    if claim_name != f"{ticket_id}.json":
        raise FinalEvalEvidenceError(
            "result claim is not the ticket's unique fixed claim path"
        )

    reader = GitBlobReader(repository_root)

    # 4. the object must be a committed blob whose bytes hash matches.
    try:
        object_blob = reader.read(
            object_ref,
            max_bytes=_MAX_RESULT_EVIDENCE_BYTES,
            evidence_name="final-eval result object",
        )
    except Exception as error:
        raise FinalEvalEvidenceError(
            f"result object is not a committed blob: {object_ref}"
        ) from error
    if object_blob.sha256 != object_sha256:
        raise FinalEvalEvidenceError(
            "result object content hash does not match the declared sha256"
        )

    # 5. the claim must be a committed blob whose bytes hash matches.
    try:
        claim_blob = reader.read(
            claim_ref,
            max_bytes=_MAX_RESULT_EVIDENCE_BYTES,
            evidence_name="final-eval result claim",
        )
    except Exception as error:
        raise FinalEvalEvidenceError(
            f"result claim is not a committed blob: {claim_ref}"
        ) from error
    if claim_blob.sha256 != claim_sha256:
        raise FinalEvalEvidenceError(
            "result claim content hash does not match the declared sha256"
        )

    # 6. the claim document must bind ticket/binding/object (no orphan).
    try:
        document = json.loads(claim_blob.raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FinalEvalEvidenceError(
            "result claim is not valid JSON"
        ) from error
    if not isinstance(document, dict):
        raise FinalEvalEvidenceError("result claim must be a JSON object")
    if str(document.get("schema_version")) != FINAL_EVAL_RESULT_CLAIM_SCHEMA:
        raise FinalEvalEvidenceError("result claim schema is unsupported")
    if str(document.get("binding_id", "")) != binding_id:
        raise FinalEvalEvidenceError(
            "result claim does not bind the expected binding_id"
        )
    if str(document.get("ticket_id", "")) != ticket_id:
        raise FinalEvalEvidenceError(
            "result claim does not bind the expected ticket_id"
        )
    if str(document.get("object_ref", "")) != object_ref:
        raise FinalEvalEvidenceError(
            "result claim does not reference the staged object"
        )
    if str(document.get("object_sha256", "")) != object_sha256:
        raise FinalEvalEvidenceError(
            "result claim object hash does not match the staged object"
        )
    outcome = document.get("outcome")
    if outcome not in {"SUCCEEDED", "FAILED", "TIMEOUT", "CRASHED"}:
        raise FinalEvalEvidenceError("result claim outcome is not terminal")
    if expected_outcome is not None:
        if expected_outcome not in {"SUCCEEDED", "FAILED", "TIMEOUT", "CRASHED"}:
            raise FinalEvalEvidenceError(
                "expected_outcome must be a terminal outcome"
            )
        if outcome != expected_outcome:
            raise FinalEvalEvidenceError(
                "result claim outcome does not match the worker-derived "
                f"outcome (claim={outcome} expected={expected_outcome})"
            )

    # CR-010 B-07: the RESULT OBJECT document is parsed and STRICTLY bound
    # to the claim and the worker result -- path/hash checks alone are not
    # enough.  Schema, binding/ticket identity, exit code and outcome must
    # be byte-identical between object, claim and the derived worker
    # result, so an object can never express a different conclusion than
    # the claim or the Authority terminal.
    try:
        object_document = json.loads(object_blob.raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FinalEvalEvidenceError(
            "result object is not valid JSON"
        ) from error
    if not isinstance(object_document, dict):
        raise FinalEvalEvidenceError("result object must be a JSON object")
    if (
        str(object_document.get("schema_version"))
        != "control_plane.final_eval_worker_result.v1"
    ):
        raise FinalEvalEvidenceError("result object schema is unsupported")
    if str(object_document.get("binding_id", "")) != binding_id:
        raise FinalEvalEvidenceError(
            "result object does not bind the expected binding_id"
        )
    if str(object_document.get("ticket_id", "")) != ticket_id:
        raise FinalEvalEvidenceError(
            "result object does not bind the expected ticket_id"
        )
    object_exit_code = object_document.get("exit_code")
    if type(object_exit_code) is not int or not (
        0 <= object_exit_code <= 255
    ):
        raise FinalEvalEvidenceError(
            "result object exit code is invalid"
        )
    claim_exit_code = document.get("exit_code")
    # CR-010 C0 (Phase B): the CLAIM's exit code is validated with the
    # SAME exact-integer contract -- bool/float/negative/>255 can never
    # pass, even when the object side is well-formed.
    if type(claim_exit_code) is not int or not (
        0 <= claim_exit_code <= 255
    ):
        raise FinalEvalEvidenceError("result claim exit code is invalid")
    if claim_exit_code != object_exit_code:
        raise FinalEvalEvidenceError(
            "result object exit code differs from the claim"
        )
    object_outcome = str(object_document.get("outcome", ""))
    if object_outcome != str(outcome):
        raise FinalEvalEvidenceError(
            "result object outcome differs from the claim"
        )
    if object_outcome != str(expected_outcome or outcome):
        raise FinalEvalEvidenceError(
            "result object outcome differs from the worker-derived outcome"
        )


class FinalEvalResultPublisher:
    """Create-only publisher for one ticket's result object + fixed claim.

    Writes the content-addressed object and the per-ticket fixed claim with
    exclusive create (a second publish for the same ticket with identical
    bytes is idempotent; different bytes conflict), then commits both as
    add-only evidence so the pre-staging verification can dereference them
    as committed blobs.
    """

    def __init__(
        self,
        *,
        repository_root: str | Path,
        evidence_volume: str,
        git_identity: tuple[str, str] = _GIT_IDENTITY,
        crash_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._repository_root = Path(repository_root).resolve(strict=True)
        volume = str(evidence_volume).replace("\\", "/").strip("/")
        if (
            not volume
            or ".." in volume.split("/")
            or not volume.startswith("research_state/control_plane/")
        ):
            raise FinalEvalEvidenceError(
                "evidence volume must live under research_state/control_plane/"
            )
        self._volume = volume
        self._git_identity = git_identity
        self._crash_hook = crash_hook

    def _git(self, *args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(self._repository_root), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise FinalEvalEvidenceError(
                "git evidence command failed: " + " ".join(args[:6])
            )
        return result.stdout

    def _write_exclusive(self, rel_ref: str, raw: bytes) -> str:
        """CR-010 B-07: atomic create-only publication.

        The bytes are staged in a UNIQUE same-volume temp file, flushed +
        fsynced, then linked to the final path with ``os.link`` (atomic
        create-only).  A hard crash mid-write leaves only an orphan temp --
        never a partial file that could be mistaken for the final object;
        the final path appears only with fully-fsynced bytes.
        """
        path = self._repository_root / rel_ref
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(
            f".tmp-{path.name}-{os.getpid()}-{secrets.token_hex(8)}"
        )
        fd = os.open(
            temp_path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o644,
        )
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
        except Exception:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        try:
            os.link(temp_path, path)
        except FileExistsError:
            try:
                existing = path.read_bytes()
            except OSError as error:
                raise FinalEvalEvidenceError(
                    "create-only evidence is unreadable: " + rel_ref
                ) from error
            if existing != raw:
                raise FinalEvalEvidenceError(
                    "create-only evidence conflict: " + rel_ref
                )
            return "IDEMPOTENT_EXISTING"
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        # best-effort parent-directory durability barrier
        try:
            dir_fd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
        return "CREATED"

    def _commit_if_changed(self, refs: list[str]) -> None:
        self._git("add", "--", *refs)
        # commit only when the working tree actually differs from HEAD
        if self._git("status", "--porcelain", "--", *refs).strip():
            self._git(
                "-c", f"user.name={self._git_identity[0]}",
                "-c", f"user.email={self._git_identity[1]}",
                "commit", "--quiet", "-m",
                "audit: final-eval result object + fixed claim (add-only)",
            )

    def publish(
        self,
        binding_id: str,
        ticket_id: str,
        result_document: dict[str, object],
        *,
        outcome: str,
    ) -> FinalEvalEvidenceRefs:
        """Write + commit the object and the fixed claim, return the refs."""
        if outcome not in {"SUCCEEDED", "FAILED", "TIMEOUT", "CRASHED"}:
            raise FinalEvalEvidenceError("outcome must be terminal")
        object_raw = canonical_json(result_document).encode("utf-8")
        object_sha256 = hashlib.sha256(object_raw).hexdigest()
        object_ref = f"{self._volume}/{_OBJECTS_DIR}/{object_sha256}.json"
        claim_document = {
            "schema_version": FINAL_EVAL_RESULT_CLAIM_SCHEMA,
            "binding_id": binding_id,
            "ticket_id": ticket_id,
            "object_ref": object_ref,
            "object_sha256": object_sha256,
            "exit_code": result_document.get("exit_code"),
            "outcome": outcome,
            # NOTE: no wall-clock field on purpose -- the claim bytes must be
            # deterministic so a crash-replay republish is byte-identical
            # (create-only idempotency) instead of a conflict.
        }
        claim_raw = canonical_json(claim_document).encode("utf-8")
        claim_sha256 = hashlib.sha256(claim_raw).hexdigest()
        claim_ref = f"{self._volume}/{_CLAIMS_DIR}/{ticket_id}.json"

        self._write_exclusive(object_ref, object_raw)
        # CR-010 A3: the object publication finished -- a hard crash here
        # must be reproducible by a fresh process (the object exists but
        # the claim has not been published yet)
        self._maybe_crash("CRASH_AFTER.OBJECT_WRITTEN")
        self._write_exclusive(claim_ref, claim_raw)
        self._maybe_crash("CRASH_AFTER.CLAIM_WRITTEN")
        self._commit_if_changed([object_ref, claim_ref])
        return FinalEvalEvidenceRefs(
            object_ref=object_ref,
            object_sha256=object_sha256,
            claim_ref=claim_ref,
            claim_sha256=claim_sha256,
        )

    def _maybe_crash(self, boundary: str) -> None:
        if self._crash_hook is not None:
            self._crash_hook(boundary)


__all__ = [
    "FINAL_EVAL_RESULT_CLAIM_SCHEMA",
    "FinalEvalEvidenceError",
    "FinalEvalEvidenceRefs",
    "FinalEvalResultPublisher",
    "verify_result_evidence",
]
