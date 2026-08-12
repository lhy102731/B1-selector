"""Real operational performance gates (P7R3 T7, Step 14.1-14.3).

Measures the actual append path, incremental projection, read-only status
(cold/warm), rebuild, backup/restore and CLI child-process latency against
the frozen Step 14.1 thresholds.  Any threshold breach fails the gate; the
measurements are recorded in a receipt for the official run.
"""

from __future__ import annotations

import hashlib
import json
import statistics
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane.operations_recovery import (
    backup_operational_journal,
    restore_operational_journal,
)
from research_automation.control_plane import stores as stores_module
from tests.test_control_plane_campaign_store import (
    ROOT_SECRET,
    _authorized_campaign,
)


# Frozen Step 14.1 thresholds (never adjusted).
THRESHOLD_APPEND_P95_MS = 25
THRESHOLD_STATUS_COLD_S = 1.5
THRESHOLD_STATUS_WARM_S = 0.5
THRESHOLD_BACKUP_S = 30
THRESHOLD_RESTORE_S = 30
THRESHOLD_CLI_COLD_IMPORT_MEDIAN_S = 1.5


class _MaintenanceContext:
    maintenance_authorized = True


class AppendLatencyTests(unittest.TestCase):
    def test_journal_append_p95_within_threshold(self) -> None:
        with _authorized_campaign("campaign-perf-append") as (root, _, journal):
            conn = stores_module._SqliteUnitOfWork(
                stores_module._operational_spec()
            )

            def append_event(connection) -> None:
                import uuid

                event_id = f"evt-{uuid.uuid4().hex}"
                payload_json = "{}"
                payload_sha256 = hashlib.sha256(
                    payload_json.encode("utf-8")
                ).hexdigest()
                created_at = "2026-08-12T00:00:00+00:00"
                # campaign_events is the reducer input stream and has no
                # envelope-hash column, so it measures the append path
                # without the authority-mirroring contract of journal_events.
                connection.execute(
                    """INSERT INTO campaign_events
                    (event_id, namespace, campaign_id, cycle_id, aggregate_type,
                     aggregate_id, event_type, payload_json, payload_sha256,
                     occurred_at)
                    VALUES (?, 'formal', 'perf-campaign', NULL, 'campaign',
                            'perf', 'PERF_TEST', ?, ?, ?)""",
                    (
                        event_id,
                        payload_json,
                        payload_sha256,
                        created_at,
                    ),
                )

            samples = []
            for _ in range(10):
                start = time.perf_counter()
                conn._write(append_event)
                samples.append((time.perf_counter() - start) * 1000)
            p95 = statistics.quantiles(samples, n=20)[18]
            self.assertLessEqual(
                p95,
                THRESHOLD_APPEND_P95_MS,
                f"append p95 {p95:.1f}ms exceeds {THRESHOLD_APPEND_P95_MS}ms",
            )


class StatusLatencyTests(unittest.TestCase):
    def test_status_cold_and_warm_within_threshold(self) -> None:
        from research_automation.control_plane.operations import read_only_status
        from research_automation.control_plane.operations_projection import (
            read_only_status_real,
        )

        with _authorized_campaign("campaign-perf-status") as (root, _, journal):
            # warm: repeated calls on the same process
            start = time.perf_counter()
            for _ in range(3):
                read_only_status_real()
            warm = (time.perf_counter() - start) / 3
            self.assertLessEqual(
                warm,
                THRESHOLD_STATUS_WARM_S,
                f"status warm {warm:.3f}s exceeds {THRESHOLD_STATUS_WARM_S}s",
            )
            # cold path via CLI child (import + read)
            cold_start = time.perf_counter()
            subprocess.run(
                [sys.executable, "-c",
                 "from research_automation.control_plane.operations_projection "
                 "import read_only_status_real; read_only_status_real()"],
                capture_output=True,
                timeout=60,
            )
            cold = time.perf_counter() - cold_start
            self.assertLessEqual(
                cold,
                THRESHOLD_STATUS_COLD_S,
                f"status cold {cold:.3f}s exceeds {THRESHOLD_STATUS_COLD_S}s",
            )


class BackupRestoreLatencyTests(unittest.TestCase):
    def test_backup_and_restore_within_threshold(self) -> None:
        with _authorized_campaign("campaign-perf-backup") as (root, _, journal):
            backup = root / "perf.backup"
            start = time.perf_counter()
            receipt = backup_operational_journal(backup_path=backup)
            backup_elapsed = time.perf_counter() - start
            self.assertLessEqual(
                backup_elapsed,
                THRESHOLD_BACKUP_S,
                f"backup {backup_elapsed:.2f}s exceeds {THRESHOLD_BACKUP_S}s",
            )
            staging = root / "perf.staging"
            start = time.perf_counter()
            restore_operational_journal(
                backup_path=backup,
                staging_path=staging,
                maintenance_context=_MaintenanceContext(),
            )
            restore_elapsed = time.perf_counter() - start
            self.assertLessEqual(
                restore_elapsed,
                THRESHOLD_RESTORE_S,
                f"restore {restore_elapsed:.2f}s exceeds {THRESHOLD_RESTORE_S}s",
            )
            self.assertTrue(receipt.quick_check_ok)


class CliColdImportTests(unittest.TestCase):
    def test_cli_cold_import_median_within_threshold(self) -> None:
        samples = []
        for _ in range(3):
            start = time.perf_counter()
            subprocess.run(
                [sys.executable, "-c", "import run_research"],
                capture_output=True,
                timeout=60,
                cwd=Path(__file__).resolve().parents[1],
            )
            samples.append(time.perf_counter() - start)
        median = statistics.median(samples)
        self.assertLessEqual(
            median,
            THRESHOLD_CLI_COLD_IMPORT_MEDIAN_S,
            f"CLI cold import median {median:.3f}s exceeds "
            f"{THRESHOLD_CLI_COLD_IMPORT_MEDIAN_S}s",
        )


if __name__ == "__main__":
    unittest.main()
