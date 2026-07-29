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

from utils.process_lock import ProcessConcurrencyError, process_lock


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
    try:
        with process_lock(_lock_path(), "BaoStock session") as path:
            yield path
    except ProcessConcurrencyError as error:
        raise BaoStockConcurrencyError(str(error)) from error


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
