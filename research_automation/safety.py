"""safety.py -- Research-Branch write guard (Rule #1).

The automation layer may ONLY write under a small allowlist of output roots.
It must NEVER write to the production memory (registry_*/snapshot_*/handoff_*/*_memory.yaml)
or anywhere else in the repo. There is no auto-merge / auto-promotion in this layer.
"""
from __future__ import annotations

from pathlib import Path

# Repo root = parent of research_automation/
_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONTROL_PLANE_ROOT = (_REPO_ROOT / "research_state" / "control_plane").resolve()

# Generated artifacts may only land here (relative to repo root).
SAFE_WRITE_ROOTS = (
    "research_automation/_output",
    "ag2_research/discussions",
    "research_state/control_plane",
)

# Files the automation layer must never touch (defense in depth).
FORBIDDEN_NAME_PREFIXES = ("registry_", "snapshot_", "handoff_")
FORBIDDEN_NAME_SUFFIXES = ("_memory.yaml",)


class UnsafeWriteError(RuntimeError):
    """Raised when the automation layer attempts to write outside the allowlist."""


def repo_root() -> Path:
    return _REPO_ROOT


def output_root() -> Path:
    """Single root for all generated artifacts."""
    return _REPO_ROOT / "research_automation" / "_output"


def is_safe_path(path: str | Path) -> bool:
    """True iff `path` resolves inside an allowed output root."""
    p = Path(path).resolve()
    for root in SAFE_WRITE_ROOTS:
        base = (_REPO_ROOT / root).resolve()
        try:
            p.relative_to(base)
            return True
        except ValueError:
            continue
    return False


def assert_safe_path(path: str | Path) -> Path:
    """Guard: allow writes ONLY inside the output roots.

    Anything inside SAFE_WRITE_ROOTS is fine (delta artifacts like registry_entry.yaml /
    snapshot_delta.yaml live there). Anything outside is rejected -- with a specific
    message when it looks like a production-memory file (registry_*/snapshot_*/handoff_*/_memory).
    """
    p = Path(path)
    resolved = p.resolve()
    try:
        resolved.relative_to(_CONTROL_PLANE_ROOT)
    except ValueError:
        in_control_plane = False
    else:
        in_control_plane = True
    if in_control_plane and (
        p.name.startswith(FORBIDDEN_NAME_PREFIXES)
        or p.name.endswith(FORBIDDEN_NAME_SUFFIXES)
    ):
        raise UnsafeWriteError(
            f"Refusing legacy-memory filename '{p.name}' inside the control-plane state root."
        )
    if is_safe_path(p):
        return p
    name = p.name
    if name.startswith(FORBIDDEN_NAME_PREFIXES) or name.endswith(FORBIDDEN_NAME_SUFFIXES):
        raise UnsafeWriteError(
            f"Refusing to write production-memory file '{name}' outside the output roots. "
            f"Automation may only write deltas under {SAFE_WRITE_ROOTS}."
        )
    raise UnsafeWriteError(
        f"Path '{p}' is outside the safe output roots {SAFE_WRITE_ROOTS}."
    )
