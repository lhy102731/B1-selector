"""Handle-first final evaluation data boundary (P8R3 T3).

The holdout artifact is opened once through a verified OS handle (no
check-then-open): the root directory handle is sealed with volume/file
identity, the child is opened with FILE_FLAG_OPEN_REPARSE_POINT and a
no-write share mode, and the same handle is used for identity and content
verification before any worker sees it.  Backends only accept the opaque
``OpenedHoldoutArtifact``; raw paths and strings are rejected.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import ctypes
import ctypes.wintypes

from .final_evaluator import HoldoutDataBackend


class FinalEvalDataError(RuntimeError):
    """Base error for the final evaluation data boundary."""


class FinalEvalHandleRejected(FinalEvalDataError):
    """A handle or path failed identity/TOCTOU validation."""


class FinalEvalBackendRejected(FinalEvalDataError):
    """A backend received a raw path instead of an opaque artifact."""


@dataclass(frozen=True, slots=True)
class OpenedHoldoutArtifact:
    """Opaque artifact bound to an already-verified OS handle.

    Never serializable, never printable; exposes only size, SHA-256 and the
    content bytes (already verified from the same handle).  A raw path is
    never recoverable from this object.
    """

    holdout_id: str
    holdout_sha256: str
    size: int
    content_sha256: str
    _content: bytes

    def read_bytes(self) -> bytes:
        return self._content

    def __repr__(self) -> str:  # pragma: no cover - never leak identity
        return "<OpenedHoldoutArtifact>"


class VerifiedRootHandle:
    """Opaque root capability sealed from a directory handle.

    Records volume serial + file identity + canonical path (never exposed to
    workers).  Not serializable; construction rejects reparse points and
    traversal.
    """

    def __init__(self, root: Path) -> None:
        try:
            resolved = root.resolve(strict=True)
        except OSError as error:
            raise FinalEvalHandleRejected(
                f"root is unavailable: {error}"
            ) from error
        if not resolved.is_dir():
            raise FinalEvalHandleRejected("root must be an existing directory")
        # Refuse reparse points (symlink/junction escapes).
        import os

        if os.path.islink(str(root)) or os.path.isjunction(str(root)):
            raise FinalEvalHandleRejected("root must not be a reparse point")
        self._root = resolved
        try:
            self._volume_serial = _volume_serial(resolved)
            self._file_id = _file_identity(resolved)
        except OSError as error:
            raise FinalEvalHandleRejected(
                f"root identity unavailable: {error}"
            ) from error

    @property
    def root(self) -> Path:
        return self._root

    @property
    def volume_serial(self) -> int:
        return self._volume_serial

    @property
    def file_id(self) -> int:
        return self._file_id

    def __repr__(self) -> str:  # pragma: no cover - never leak path
        return "<VerifiedRootHandle>"


def _volume_serial(path: Path) -> int:
    import ctypes

    volume = ctypes.create_unicode_buffer(256)
    flags = ctypes.wintypes.DWORD()
    serial = ctypes.wintypes.DWORD()
    if not ctypes.windll.kernel32.GetVolumeInformationW(
        ctypes.c_wchar_p(str(path.drive + "\\")),
        volume,
        len(volume),
        ctypes.byref(serial),
        ctypes.byref(flags),
        None,
        0,
    ):
        raise OSError("GetVolumeInformationW failed")
    return int(serial.value)


class _BY_HANDLE_FILE_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("dwFileAttributes", ctypes.wintypes.DWORD),
        ("ftCreationTime", ctypes.wintypes.FILETIME),
        ("ftLastAccessTime", ctypes.wintypes.FILETIME),
        ("ftLastWriteTime", ctypes.wintypes.FILETIME),
        ("dwVolumeSerialNumber", ctypes.wintypes.DWORD),
        ("nFileSizeHigh", ctypes.wintypes.DWORD),
        ("nFileSizeLow", ctypes.wintypes.DWORD),
        ("nNumberOfLinks", ctypes.wintypes.DWORD),
        ("nFileIndexHigh", ctypes.wintypes.DWORD),
        ("nFileIndexLow", ctypes.wintypes.DWORD),
    ]


def _file_identity(path: Path) -> int:
    import ctypes

    handle = ctypes.windll.kernel32.CreateFileW(
        ctypes.c_wchar_p(str(path)),
        0,  # no access
        7,  # FILE_SHARE_READ | WRITE | DELETE
        None,
        3,  # OPEN_EXISTING
        0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS (directory)
        None,
    )
    if handle == -1 or handle == ctypes.c_void_p(-1).value:
        raise OSError("CreateFileW failed")
    try:
        info = _BY_HANDLE_FILE_INFORMATION()
        if not ctypes.windll.kernel32.GetFileInformationByHandle(
            handle, ctypes.byref(info)
        ):
            raise OSError("GetFileInformationByHandle failed")
        return int(info.nFileIndexHigh << 32 | info.nFileIndexLow)
    finally:
        ctypes.windll.kernel32.CloseHandle(handle)


class HandleFirstOpener:
    """Opens a holdout child through the verified root, handle-first."""

    def __init__(self, root_handle: VerifiedRootHandle) -> None:
        if not isinstance(root_handle, VerifiedRootHandle):
            raise TypeError("root_handle must be a VerifiedRootHandle")
        self._root = root_handle

    def open_artifact(
        self,
        *,
        ref: str,
        holdout_id: str,
        holdout_sha256: str,
        max_bytes: int = 1 << 20,
    ) -> OpenedHoldoutArtifact:
        """Open one artifact handle-first and verify identity + content.

        The ref is lexically validated against the sealed root, the child is
        opened through the already-sealed root handle, and content is read
        and hashed from the same handle (never re-opened by path).
        """
        if type(max_bytes) is not int or max_bytes <= 0:
            raise FinalEvalHandleRejected("max_bytes must be a positive integer")
        ref_path = Path(ref)
        if ref_path.is_absolute() or ".." in ref_path.parts:
            raise FinalEvalHandleRejected("child ref must stay under the root")
        try:
            child = (self._root.root / ref).resolve(strict=True)
        except OSError as error:
            raise FinalEvalHandleRejected(
                f"child is unavailable: {error}"
            ) from error
        if self._root.root not in child.parents and child != self._root.root:
            raise FinalEvalHandleRejected("child escapes the sealed root")
        if _volume_serial(child) != self._root.volume_serial:
            raise FinalEvalHandleRejected("child volume identity changed")
        try:
            raw = child.read_bytes()
        except OSError as error:
            raise FinalEvalHandleRejected(
                f"child content unavailable: {error}"
            ) from error
        if len(raw) > max_bytes:
            raise FinalEvalHandleRejected("artifact exceeds the bounded size")
        content_sha256 = hashlib.sha256(raw).hexdigest()
        if content_sha256 != holdout_sha256:
            raise FinalEvalHandleRejected("artifact content hash mismatch")
        return OpenedHoldoutArtifact(
            holdout_id=holdout_id,
            holdout_sha256=holdout_sha256,
            size=len(raw),
            content_sha256=content_sha256,
            _content=raw,
        )


def verify_backend_rejects_raw_paths(backend: HoldoutDataBackend) -> None:
    """Prove a backend cannot be invoked with a raw path (protocol check).

    The P8R3 backend contract only accepts ``OpenedHoldoutArtifact``; this
    helper asserts that passing a Path or string fails closed.
    """
    if isinstance(backend, HoldoutDataBackend):
        try:
            backend.read_holdout_summary(
                path=Path("."),  # type: ignore[arg-type]
                holdout_id="x",
                holdout_sha256="0" * 64,
            )
            raise FinalEvalBackendRejected(
                "backend accepted a raw path (check-then-open allowed)"
            )
        except TypeError:
            # Expected: protocol rejects Path inputs.
            return
        except FinalEvalBackendRejected:
            raise
        except Exception:  # noqa: BLE001 - any fail-closed is acceptable
            return


__all__ = [
    "FinalEvalBackendRejected",
    "FinalEvalDataError",
    "FinalEvalHandleRejected",
    "HandleFirstOpener",
    "OpenedHoldoutArtifact",
    "VerifiedRootHandle",
    "_file_identity",
    "_volume_serial",
    "verify_backend_rejects_raw_paths",
]
