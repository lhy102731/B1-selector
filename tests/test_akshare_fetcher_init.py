from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from utils.akshare_fetcher import AKShareFetcher


class AKShareFetcherInitTests(unittest.TestCase):
    def test_empty_data_directory_delegates_universe_discovery_to_thsdk_rebuild(self):
        with TemporaryDirectory() as temp:
            data_dir = Path(temp) / "data"
            fetcher = AKShareFetcher(data_dir)

            with patch("tools.rebuild_all_ths.rebuild", return_value=0) as rebuild:
                result = fetcher.init_full_data()

            self.assertEqual(0, result)
            rebuild.assert_called_once_with(
                data_dir,
                max_stocks=None,
                commit=True,
            )


if __name__ == "__main__":
    unittest.main()
