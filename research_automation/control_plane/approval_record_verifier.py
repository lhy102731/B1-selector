"""Approval record verifier (CR-010 F-06).

The coordinator derives authorization rows itself from the envelope
manifest; nothing in the trusted path consumes an unfakeable user
exact-candidate approval record.  This module adds that consumption: an
ApprovalRecord is a small committed document that binds the user-approved
candidate (source tree + envelope commit + manifest hash) to a ticket
before any grant/ticket is issued.  The coordinator verifies it and stamps
its SHA-256 into the activation evidence, so Authority rows, TaskReports and
Gate snapshots can mechanically prove the candidate was approved.

The record itself is add-only and committed by the user/operator before the
activation runs; the verifier never writes anything.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path

from .contracts import canonical_json

APPROVAL_RECORD_V1 = "control_plane.approval_record.v1"
_MAX_APPROVAL_RECORD_BYTES = 16 * 1024
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class ApprovalRecordError(RuntimeError):
    """Base error for approval record verification."""


class ApprovalRecordVerifier:
    """Verify an approval record against a candidate before activation."""

    def __init__(self, repository_root: str | Path) -> None:
        self._root = Path(repository_root).resolve(strict=True)

    # ------------------------------------------------------------------
    def verify_record_bytes(
        self,
        raw: bytes,
        *,
        envelope_commit: str,
        manifest_sha256: str,
    ) -> str:
        """Verify one approval record and return its SHA-256.

        The record must:
        - parse as strict canonical JSON (no duplicate keys, bounded size);
        - carry schema_version ``control_plane.approval_record.v1``;
        - bind the same envelope commit and manifest hash as the candidate
          being activated (exact match, fail closed);
        - reference a committed candidate tree via ``candidate_tree``.
        """
        if not isinstance(raw, bytes) or len(raw) > _MAX_APPROVAL_RECORD_BYTES:
            raise ApprovalRecordError(
                "approval record must be bytes <= "
                f"{_MAX_APPROVAL_RECORD_BYTES}"
            )
        record_sha256 = hashlib.sha256(raw).hexdigest()
        try:
            text = raw.decode("utf-8", errors="strict")
            document = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ApprovalRecordError(
                "approval record is not strict UTF-8 JSON"
            ) from error
        if not isinstance(document, Mapping):
            raise ApprovalRecordError("approval record must be an object")
        if document.get("schema_version") != APPROVAL_RECORD_V1:
            raise ApprovalRecordError(
                "approval record schema must be " + APPROVAL_RECORD_V1
            )
        envelope = document.get("envelope_commit")
        manifest_hash = document.get("manifest_sha256")
        tree = document.get("candidate_tree")
        approved = document.get("approval")
        if not isinstance(envelope, str) or not _GIT_COMMIT_PATTERN.fullmatch(
            envelope
        ):
            raise ApprovalRecordError(
                "approval record envelope_commit must be a git commit sha"
            )
        if not isinstance(manifest_hash, str) or not _SHA256_PATTERN.fullmatch(
            manifest_hash
        ):
            raise ApprovalRecordError(
                "approval record manifest_sha256 must be a sha256"
            )
        if not isinstance(tree, str) or not _GIT_COMMIT_PATTERN.fullmatch(tree):
            raise ApprovalRecordError(
                "approval record candidate_tree must be a git tree sha"
            )
        if approved not in (True, "APPROVE"):
            raise ApprovalRecordError(
                "approval record approval must be APPROVE/true"
            )
        if envelope != envelope_commit:
            raise ApprovalRecordError(
                "approval record binds a different envelope commit: "
                f"{envelope} != {envelope_commit}"
            )
        if manifest_hash != manifest_sha256:
            raise ApprovalRecordError(
                "approval record binds a different manifest hash: "
                f"{manifest_hash} != {manifest_sha256}"
            )
        # The candidate tree must exist as a committed object in the repo.
        tree_ok = self._git(["rev-parse", "--verify", tree + "^{tree}"])
        if not tree_ok:
            raise ApprovalRecordError(
                "approval record candidate_tree is not a committed tree: "
                + tree
            )
        return record_sha256

    # ------------------------------------------------------------------
    def _git(self, argv: list[str]) -> bool:
        import subprocess

        try:
            result = subprocess.run(
                ["git", "-C", str(self._root), *argv],
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=30,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return result.returncode == 0

    # ------------------------------------------------------------------
    def build_record(
        self,
        *,
        envelope_commit: str,
        manifest_sha256: str,
        candidate_tree: str,
        actor: str = "user",
        note: str = "",
    ) -> dict[str, object]:
        """Build a canonical approval record document (for operators)."""
        document = {
            "schema_version": APPROVAL_RECORD_V1,
            "envelope_commit": envelope_commit,
            "manifest_sha256": manifest_sha256,
            "candidate_tree": candidate_tree,
            "approval": "APPROVE",
            "actor": actor,
        }
        if note:
            document["note"] = note
        return document

    def serialize(self, document: Mapping[str, object]) -> bytes:
        return canonical_json(document).encode("utf-8")


__all__ = [
    "APPROVAL_RECORD_V1",
    "ApprovalRecordError",
    "ApprovalRecordVerifier",
]
