"""C0 campaign executor child (CR-010 A4).

The complete 24-cycle offline campaign executes in its OWN parent-observed
OS child: this module is the fixed ``-m`` entry the controller spawns.  The
module body performs no network access and no sensitive import; the child
installs ``NetworkGuard`` and runs the deny probe FIRST (in main), then
lazy-imports the campaign machinery and drives the one campaign into its
root.  The single-line JSON stdout carries the campaign payload plus the
executor's own OS identity -- never a secret.
"""

from __future__ import annotations

import json
import os
import sys


def _main(argv: list[str]) -> int:
    if len(argv) != 6:
        sys.stderr.write("EXECUTOR_ARGC\n")
        return 1
    seed = int(argv[1])
    cycles = int(argv[2])
    root_path = argv[3]
    attempt_id = argv[4]
    fixture_ref = argv[5]
    from research_automation.control_plane.rollout_chaos_worker import (
        NetworkGuard,
        _process_started_at_ns,
    )

    NetworkGuard.install()
    NetworkGuard.deny_probe()
    from pathlib import Path

    from research_automation.control_plane import rollout_chaos

    main, root = rollout_chaos._run_main_campaign(
        seed,
        cycles,
        root_override=Path(root_path),
        attempt_id=attempt_id,
        fixture_ref=fixture_ref,
    )
    document = {
        "schema_version": "control_plane.c0_campaign_executor_result.v1",
        "main": main,
        "root": str(root),
        "executor_identity": {
            "pid": os.getpid(),
            "started_at_ns": _process_started_at_ns(),
        },
        "guard_attempts": NetworkGuard.attempts,
    }
    line = json.dumps(document, ensure_ascii=True, sort_keys=True)
    if "\n" in line:
        sys.stderr.write("EXECUTOR_NEWLINE\n")
        return 1
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv))
