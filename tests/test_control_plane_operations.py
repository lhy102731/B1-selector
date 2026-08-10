from __future__ import annotations

import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from research_automation.control_plane import operations


class JournalDurabilityContractTests(unittest.TestCase):
    """P7R2-T2: journal durability contract (WAL, busy timeout, one short
    single-writer transaction) on synthetic fixtures only."""

    def make_fixture(self) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "journal.sqlite3"
        connection = sqlite3.connect(path)
        try:
            connection.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        finally:
            connection.close()
        return path

    def test_journal_mode_is_wal_after_durable_open(self) -> None:
        path = self.make_fixture()
        with operations.with_durable_journal_transaction(path) as connection:
            journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            self.assertEqual("wal", journal_mode)
        snapshot = operations.journal_durability(path)
        self.assertEqual("wal", snapshot.journal_mode)

    def test_busy_timeout_is_set_explicitly(self) -> None:
        path = self.make_fixture()
        with operations.with_durable_journal_transaction(path, busy_timeout_ms=4_000) as connection:
            self.assertEqual(4_000, connection.execute("PRAGMA busy_timeout").fetchone()[0])
        snapshot = operations.journal_durability(path, busy_timeout_ms=4_000)
        self.assertEqual(4_000, snapshot.busy_timeout_ms)

    def test_write_is_one_short_immediate_transaction(self) -> None:
        path = self.make_fixture()
        started = time.monotonic()
        with operations.with_durable_journal_transaction(path) as connection:
            connection.execute("INSERT INTO events(value) VALUES ('one')")
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 5.0)
        connection = sqlite3.connect(path)
        try:
            count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(1, count)

    def test_concurrent_writer_waits_within_busy_timeout(self) -> None:
        path = self.make_fixture()
        with operations.with_durable_journal_transaction(path):
            pass
        blocker_started = threading.Event()

        def blocker() -> None:
            connection = sqlite3.connect(path, timeout=1.0, isolation_level=None)
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute("INSERT INTO events(value) VALUES ('blocker')")
                blocker_started.set()
                time.sleep(1.0)
                connection.commit()
            finally:
                connection.close()

        thread = threading.Thread(target=blocker)
        thread.start()
        try:
            self.assertTrue(blocker_started.wait(5))
            started = time.monotonic()
            with operations.with_durable_journal_transaction(path, busy_timeout_ms=2_000) as connection:
                connection.execute("INSERT INTO events(value) VALUES ('second')")
            elapsed = time.monotonic() - started
            self.assertGreaterEqual(elapsed, 0.5)
            self.assertLess(elapsed, 10.0)
        finally:
            thread.join(10)

    def test_rollback_on_error_leaves_no_partial_write(self) -> None:
        path = self.make_fixture()
        with self.assertRaises(RuntimeError):
            with operations.with_durable_journal_transaction(path) as connection:
                connection.execute("INSERT INTO events(value) VALUES ('rolled')")
                raise RuntimeError("boom")
        connection = sqlite3.connect(path)
        try:
            count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(0, count)

    def test_durability_snapshot_contains_contract_fields(self) -> None:
        path = self.make_fixture()
        with operations.with_durable_journal_transaction(path) as connection:
            connection.execute("INSERT INTO events(value) VALUES ('snapshot')")
        snapshot = operations.journal_durability(path)
        self.assertTrue(snapshot.path.endswith("journal.sqlite3"))
        self.assertEqual("wal", snapshot.journal_mode)
        self.assertIsInstance(snapshot.busy_timeout_ms, int)
        self.assertIsInstance(snapshot.synchronous, str)
        self.assertTrue(snapshot.single_writer)

    def test_durable_connection_rejects_protected_store_paths(self) -> None:
        for protected in ("research_state/control_plane/authority/authority.sqlite3", "research_state/control_plane/operational/operational.sqlite3"):
            with self.assertRaises(operations.ProtectedStoreError):
                with operations.with_durable_journal_transaction(Path(protected)):
                    pass


class ProjectionRecoveryContractTests(unittest.TestCase):
    """P7R2-T3: incremental last_sequence projection, offline full rebuild with
    integrity/progress/thresholds, and SQLite backup/restore on synthetic
    fixtures only."""

    def make_journal_fixture(self) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "journal.sqlite3"
        connection = sqlite3.connect(path)
        try:
            connection.execute(
                """
                CREATE TABLE events (
                    sequence INTEGER PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
        finally:
            connection.close()
        return path

    def seed_events(self, path: Path, count: int, *, start: int = 1) -> None:
        connection = sqlite3.connect(path)
        try:
            for index in range(start, start + count):
                connection.execute(
                    "INSERT INTO events(sequence, event_type, aggregate_id, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
                    (index, "TEST_EVENT", f"agg-{index}", "{}", "2026-08-10T00:00:00+00:00"),
                )
            connection.commit()
        finally:
            connection.close()

    def test_incremental_projection_processes_only_new_events(self) -> None:
        path = self.make_journal_fixture()
        self.seed_events(path, 5)
        first = operations.incremental_project(path)
        self.assertEqual(5, first.last_sequence)
        self.assertEqual(5, first.events_processed)
        self.assertTrue(first.integrity_ok)
        self.seed_events(path, 3, start=6)
        second = operations.incremental_project(path)
        self.assertEqual(8, second.last_sequence)
        self.assertEqual(3, second.events_processed)

    def test_incremental_projection_with_no_new_events(self) -> None:
        path = self.make_journal_fixture()
        self.seed_events(path, 4)
        first = operations.incremental_project(path)
        second = operations.incremental_project(path)
        self.assertEqual(4, second.last_sequence)
        self.assertEqual(0, second.events_processed)
        self.assertEqual(first.last_sequence, second.last_sequence)

    def test_offline_rebuild_scans_all_and_reports_progress(self) -> None:
        path = self.make_journal_fixture()
        self.seed_events(path, 12)
        steps: list[int] = []

        def progress(processed: int, total: int) -> None:
            steps.append(processed)

        result = operations.rebuild_projection(path, progress=progress)
        self.assertEqual(12, result.last_sequence)
        self.assertEqual(12, result.events_processed)
        self.assertTrue(result.integrity_ok)
        self.assertTrue(steps)
        self.assertEqual(12, steps[-1])

    def test_rebuild_threshold_aborts_without_touching_projection(self) -> None:
        path = self.make_journal_fixture()
        self.seed_events(path, 100)
        with self.assertRaises(operations.ProjectionThresholdError):
            operations.rebuild_projection(path, max_events=10)
        connection = sqlite3.connect(path)
        try:
            rows = connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='projections'").fetchall()
        finally:
            connection.close()
        self.assertEqual(0, len(rows))

    def test_integrity_gap_raises(self) -> None:
        path = self.make_journal_fixture()
        connection = sqlite3.connect(path)
        try:
            connection.execute(
                "INSERT INTO events(sequence, event_type, aggregate_id, payload_json, created_at) VALUES (1, 'E', 'a', '{}', 'now')"
            )
            connection.execute(
                "INSERT INTO events(sequence, event_type, aggregate_id, payload_json, created_at) VALUES (100, 'E', 'b', '{}', 'now')"
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(operations.ProjectionIntegrityError):
            operations.incremental_project(path)

    def test_backup_and_restore_preserve_rows(self) -> None:
        path = self.make_journal_fixture()
        self.seed_events(path, 7)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        backup = Path(directory.name) / "backup.sqlite3"
        restored = Path(directory.name) / "restored.sqlite3"
        operations.backup_journal(path, backup)
        connection = sqlite3.connect(path)
        try:
            connection.execute("DELETE FROM events")
            connection.commit()
        finally:
            connection.close()
        operations.restore_journal(backup, restored)
        connection = sqlite3.connect(restored)
        try:
            count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            max_sequence = connection.execute("SELECT MAX(sequence) FROM events").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(7, count)
        self.assertEqual(7, max_sequence)

    def test_backup_reports_progress(self) -> None:
        path = self.make_journal_fixture()
        self.seed_events(path, 5)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        backup = Path(directory.name) / "backup.sqlite3"
        steps: list[int] = []

        def progress(_status: int, remaining: int, total: int) -> None:
            steps.append(total - remaining)

        operations.backup_journal(path, backup, progress=progress)
        self.assertTrue(steps)
        self.assertEqual(steps[-1], steps[-1])  # last step reached final state

    def test_projection_and_backup_reject_protected_paths(self) -> None:
        for protected in ("research_state/control_plane/authority/authority.sqlite3", "research_state/control_plane/operational/operational.sqlite3"):
            with self.assertRaises(operations.ProtectedStoreError):
                operations.incremental_project(Path(protected))
            with self.assertRaises(operations.ProtectedStoreError):
                operations.backup_journal(Path(protected), Path("backup.sqlite3"))
            with self.assertRaises(operations.ProtectedStoreError):
                operations.restore_journal(Path("backup.sqlite3"), Path(protected))


class PerformanceGateTests(unittest.TestCase):
    """P7R2-T9: reproducible synthetic performance baselines and the
    steady-state overhead budget on synthetic fixtures only."""

    def test_environment_baseline_records_required_versions(self) -> None:
        baseline = operations.performance_environment_baseline()
        self.assertIn("machine", baseline)
        self.assertIn("disk", baseline)
        self.assertIn("python", baseline)
        self.assertIn("sqlite", baseline)
        self.assertIsInstance(baseline["machine"], str)
        self.assertIsInstance(baseline["disk"], str)
        self.assertIsInstance(baseline["python"], str)
        self.assertIsInstance(baseline["sqlite"], str)
        self.assertTrue(baseline["python"].startswith("3."))

    def test_100k_projection_measured_with_budget(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "journal.sqlite3"
        connection = sqlite3.connect(path)
        try:
            connection.execute(
                "CREATE TABLE events (sequence INTEGER PRIMARY KEY, event_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL)"
            )
            connection.executemany(
                "INSERT INTO events(sequence, event_type, aggregate_id, payload_json, created_at) VALUES (?, 'E', 'a', '{}', '2026-08-10T00:00:00+00:00')",
                [(index,) for index in range(1, 100_001)],
            )
            connection.commit()
        finally:
            connection.close()
        measurement = operations.measure_synthetic_performance(
            journal_path=path,
            event_count=100_000,
        )
        self.assertEqual(100_000, measurement["events_processed"])
        self.assertLessEqual(measurement["elapsed_seconds"], measurement["budget_seconds"])
        self.assertIn("elapsed_seconds", measurement)
        self.assertIn("events_per_second", measurement)
        self.assertIn("projection_last_sequence", measurement)
        self.assertEqual(100_000, measurement["projection_last_sequence"])

    def test_10k_packet_context_measured(self) -> None:
        measurement = operations.measure_packet_context_construction(
            packet_count=10_000,
            max_bytes_per_packet=128,
        )
        self.assertEqual(10_000, measurement["packet_count"])
        self.assertGreaterEqual(measurement["context_bytes"], 0)
        self.assertIn("elapsed_seconds", measurement)
        self.assertIn("budget_seconds", measurement)
        self.assertLessEqual(measurement["elapsed_seconds"], measurement["budget_seconds"])

    def test_overhead_budget_does_not_exceed_five_percent(self) -> None:
        report = operations.performance_overhead_report(
            cycle_wall_seconds=100.0,
            control_plane_wall_seconds=4.0,
        )
        self.assertEqual(4.0, report["overhead_percent"])
        self.assertTrue(report["within_budget"])
        self.assertLessEqual(report["overhead_percent"], 5.0)
        self.assertFalse(report["real_data"])

    def test_overhead_budget_fails_when_breached(self) -> None:
        report = operations.performance_overhead_report(
            cycle_wall_seconds=100.0,
            control_plane_wall_seconds=6.0,
        )
        self.assertFalse(report["within_budget"])
        self.assertGreater(report["overhead_percent"], 5.0)

    def test_performance_surfaces_reject_invalid_inputs(self) -> None:
        with self.assertRaises(ValueError):
            operations.performance_overhead_report(cycle_wall_seconds=0, control_plane_wall_seconds=0)
        with self.assertRaises(FileNotFoundError):
            operations.measure_synthetic_performance(
                journal_path=Path("missing.sqlite3"),
                event_count=100,
            )


if __name__ == "__main__":
    unittest.main()
