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


if __name__ == "__main__":
    unittest.main()
