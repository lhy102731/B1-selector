from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from utils.process_lock import ProcessConcurrencyError, process_lock


class ProcessLockTests(unittest.TestCase):
    def test_second_owner_fails_without_waiting(self):
        with TemporaryDirectory() as temp:
            lock_path = Path(temp) / "pipeline.lock"

            with process_lock(lock_path, "daily pipeline"):
                with self.assertRaisesRegex(
                    ProcessConcurrencyError,
                    "daily pipeline already active",
                ):
                    with process_lock(lock_path, "daily pipeline"):
                        self.fail("second owner must never enter the critical section")

            self.assertEqual(str(os.getpid()), lock_path.read_text(encoding="ascii"))


if __name__ == "__main__":
    unittest.main()
