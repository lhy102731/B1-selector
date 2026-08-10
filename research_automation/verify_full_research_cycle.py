"""verify_full_research_cycle.py - legacy full-cycle harness quarantine boundary.

The legacy end-to-end wiring acceptance harness previously spawned
run_research_cycle.py and asserted real research side effects. The legacy
AutonomousRunnerV1 cycle is legacy_unaudited and no real research is
authorized, so this harness is quarantined as a NO-EFFECT migration boundary:

- importing this module performs no I/O and spawns no subprocess;
- main() takes a read-only production-boundary hash snapshot, prints the
  quarantine status, and returns fail-closed exit code 3;
- no run output directories are created and no research/backtest/campaign
  side effect is started.

The authorized replacement is the P6 control-plane Campaign adapter; this
boundary stays closed until a later control-plane slice explicitly
re-enables it.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

PROD_BOUNDARY = [
    _REPO_ROOT / "strategy" / "brick_chart_strategy.py",
    _REPO_ROOT / "backtest_brick_v2.py",
    _REPO_ROOT / "utils" / "technical.py",
    _REPO_ROOT / "config" / "strategy_params.yaml",
]

#: Fail-closed exit code. 3 matches run_research_cycle.py's legacy block.
QUARANTINE_EXIT_CODE = 3

QUARANTINE_MESSAGE = (
    "[verify_full_research_cycle] QUARANTINED: the legacy full-cycle harness "
    "is a no-effect migration boundary; run_research_cycle.py is "
    "legacy_unaudited and no real research/backtest/campaign is authorized."
)


def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else "MISSING"


def snapshot_production_boundary() -> dict[str, str]:
    """Return read-only hashes of the production boundary files."""
    return {str(p): sha256(p) for p in PROD_BOUNDARY}


def main() -> int:
    boundary = snapshot_production_boundary()
    print("=" * 64)
    print("Full Research Cycle Wiring acceptance test (legacy harness)")
    print("=" * 64)
    print(f"[boundary] read-only snapshot: {len(boundary)} files")
    for path, digest in boundary.items():
        print(f"[boundary] {Path(path).name}: {digest[:12]}")
    print()
    print(QUARANTINE_MESSAGE)
    print("=" * 64)
    print("RESULT: QUARANTINED (fail-closed, no subprocess, no runs, no side effects)")
    print("=" * 64)
    return QUARANTINE_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())
