from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from research_automation.control_plane.stores import (
    StoreConfigurationError,
    trusted_bootstrap,
)


class TrustedBootstrapTests(unittest.TestCase):
    def test_same_path_fails_before_a_store_is_created(self) -> None:
        with TemporaryDirectory() as tmp:
            shared_path = Path(tmp) / "control-plane.sqlite3"

            with self.assertRaisesRegex(
                StoreConfigurationError,
                "different SQLite files",
            ):
                trusted_bootstrap(
                    authority_path=shared_path,
                    operational_path=shared_path,
                )

            self.assertFalse(shared_path.exists())

    def test_bootstrap_provisions_two_physical_store_files(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority" / "authority.sqlite3"
            operational_path = root / "operational" / "operational.sqlite3"

            receipt = trusted_bootstrap(
                authority_path=authority_path,
                operational_path=operational_path,
            )

            self.assertEqual(receipt.authority_path, authority_path.resolve())
            self.assertEqual(receipt.operational_path, operational_path.resolve())
            self.assertTrue(authority_path.is_file())
            self.assertTrue(operational_path.is_file())
            self.assertFalse(os.path.samefile(authority_path, operational_path))
            self.assertGreater(authority_path.stat().st_size, 0)
            self.assertGreater(operational_path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
