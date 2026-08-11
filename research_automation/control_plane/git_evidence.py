"""Committed-Git-blob evidence reader (P0-CR-008 Slice A).

Reads canonical SHA-256 evidence from committed regular Git blobs only.
Fails closed on uncommitted, dirty, symlink, submodule, tree, case-alias,
or traversal references. No shell concatenation is used anywhere; the Git
executable is invoked as a fixed argv vector.
"""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path


class GitEvidenceError(RuntimeError):
    """Raised when evidence cannot be read from committed Git blobs."""


@dataclass(frozen=True, slots=True)
class CommittedGitBlob:
    """Canonical committed blob identity and raw bytes."""

    commit: str
    mode: str
    oid: str
    byte_count: int
    raw: bytes
    sha256: str


_REGULAR_FILE_MODES = frozenset({"100644", "100755"})


class GitBlobReader:
    """Read committed regular blobs from a locked Git executable."""

    __slots__ = ("_repository_root", "_git_executable")

    def __init__(
        self,
        repository_root: str | Path,
        *,
        git_executable: str = "git",
    ) -> None:
        try:
            root = Path(repository_root).resolve(strict=True)
        except OSError as error:
            raise GitEvidenceError(
                "git evidence repository root is unavailable"
            ) from error
        if not root.is_dir():
            raise GitEvidenceError("git evidence repository root is not a directory")
        self._repository_root = root
        self._git_executable = git_executable

    # ------------------------------------------------------------------
    def _git(self, *args: str, check: bool = True) -> str:
        argv = [self._git_executable, "-C", str(self._repository_root), *args]
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if check and result.returncode != 0:
            raise GitEvidenceError(
                "git evidence command failed: "
                + " ".join(argv[:6])
                + ("..." if len(argv) > 6 else "")
            )
        return result.stdout

    def _git_bytes(self, *args: str) -> bytes:
        argv = [self._git_executable, "-C", str(self._repository_root), *args]
        result = subprocess.run(argv, capture_output=True)
        if result.returncode != 0:
            raise GitEvidenceError(
                "git evidence command failed: "
                + " ".join(argv[:6])
                + ("..." if len(argv) > 6 else "")
            )
        return result.stdout

    def current_head(self) -> str:
        return self._git("rev-parse", "HEAD").strip()

    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_reference(reference: str) -> str:
        if not isinstance(reference, str) or not reference:
            raise GitEvidenceError("evidence reference is empty")
        if "\x00" in reference:
            raise GitEvidenceError("evidence reference contains NUL")
        if reference.startswith("/"):
            raise GitEvidenceError(
                "evidence reference must be repository-relative"
            )
        if "\\" in reference:
            raise GitEvidenceError(
                "evidence reference uses unsupported path separators"
            )
        parts = reference.split("/")
        if any(part in ("", ".", "..") for part in parts):
            raise GitEvidenceError(
                "evidence reference must not contain empty, dot or "
                "parent path segments"
            )
        return reference

    @staticmethod
    def _parse_mode_for(reference: str, ls_tree_output: str) -> str:
        # ls-tree -z emits "<mode> <type> <oid>\t<name>\0" per entry.
        for entry in ls_tree_output.split("\x00"):
            if not entry:
                continue
            try:
                meta, name = entry.split("\t", 1)
            except ValueError:
                continue
            if name == reference:
                return meta.split(" ", 1)[0]
        raise GitEvidenceError(f"evidence reference is not a committed blob: {reference}")

    # ------------------------------------------------------------------
    def read(
        self,
        reference: str,
        *,
        max_bytes: int,
        evidence_name: str,
        commit: str | None = None,
    ) -> CommittedGitBlob:
        ref = self._normalize_reference(reference)
        head = self.current_head()
        resolved_commit = (commit or head).strip()

        # The path must resolve in the committed tree.
        rev = subprocess.run(
            [
                self._git_executable,
                "-C",
                str(self._repository_root),
                "rev-parse",
                f"{resolved_commit}:{ref}",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if rev.returncode != 0:
            raise GitEvidenceError(
                f"{evidence_name} reference is not committed: {ref}"
            )
        oid = rev.stdout.strip()

        kind = self._git("cat-file", "-t", oid).strip()
        if kind != "blob":
            raise GitEvidenceError(
                f"{evidence_name} reference is not a regular file: {ref}"
            )

        ls_tree = self._git(
            "ls-tree", "-z", resolved_commit, "--", ref
        )
        mode = self._parse_mode_for(ref, ls_tree)
        if mode not in _REGULAR_FILE_MODES:
            raise GitEvidenceError(
                f"{evidence_name} reference has non-regular mode {mode}: {ref}"
            )

        # The path must be tracked and its working copy unmodified.
        tracked = subprocess.run(
            [
                self._git_executable,
                "-C",
                str(self._repository_root),
                "ls-files",
                "--error-unmatch",
                "--",
                ref,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if tracked.returncode != 0:
            raise GitEvidenceError(
                f"{evidence_name} reference is not tracked: {ref}"
            )
        status = subprocess.run(
            [
                self._git_executable,
                "-C",
                str(self._repository_root),
                "status",
                "--porcelain",
                "--",
                ref,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if status.stdout.strip():
            raise GitEvidenceError(
                f"{evidence_name} working copy is dirty: {ref}"
            )

        raw_bytes = self._git_bytes("cat-file", "blob", oid)
        if len(raw_bytes) > max_bytes:
            raise GitEvidenceError(
                f"{evidence_name} evidence exceeds its size limit"
            )
        return CommittedGitBlob(
            commit=resolved_commit,
            mode=mode,
            oid=oid,
            byte_count=len(raw_bytes),
            raw=raw_bytes,
            sha256=hashlib.sha256(raw_bytes).hexdigest(),
        )
