"""Cross-process serialization for BaoStock sessions.

BaoStock maintains process-global socket/session state and must not be used by
multiple repository processes at once.  The lock is non-blocking on purpose:
an accidental second caller fails visibly instead of corrupting or hanging the
active session.
"""

from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from typing import Callable, Iterator, ParamSpec, TypeVar


P = ParamSpec("P")
R = TypeVar("R")


class BaoStockConcurrencyError(RuntimeError):
    """Raised when another process already owns the BaoStock session."""


def _lock_path() -> Path:
    configured = os.environ.get("BAOSTOCK_LOCK_PATH", "").strip()
    return (
        Path(configured).expanduser().resolve()
        if configured
        else Path(tempfile.gettempdir()) / "a_share_quant_selector_baostock.lock"
    )


@contextmanager
def baostock_process_lock() -> Iterator[Path]:
    path = _lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    locked = False
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except OSError as exc:
            raise BaoStockConcurrencyError(
                f"BaoStock session already active; lock={path}"
            ) from exc

        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()).encode("ascii"))
        handle.flush()
        handle.seek(0)
        yield path
    finally:
        if locked:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def serialized_baostock(function: Callable[P, R]) -> Callable[P, R]:
    @wraps(function)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        with baostock_process_lock():
            return function(*args, **kwargs)

    return wrapped


__all__ = [
    "BaoStockConcurrencyError",
    "baostock_process_lock",
    "serialized_baostock",
]
