import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from utils.baostock_lock import BaoStockConcurrencyError, baostock_process_lock


class BaoStockLockTests(unittest.TestCase):
    def test_second_process_fails_instead_of_running_concurrently(self):
        holder_code = (
            "import sys\n"
            "from utils.baostock_lock import baostock_process_lock\n"
            "with baostock_process_lock():\n"
            "    print('LOCKED', flush=True)\n"
            "    sys.stdin.readline()\n"
        )
        challenger_code = (
            "from utils.baostock_lock import "
            "BaoStockConcurrencyError, baostock_process_lock\n"
            "try:\n"
            "    with baostock_process_lock():\n"
            "        raise SystemExit(3)\n"
            "except BaoStockConcurrencyError:\n"
            "    raise SystemExit(0)\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            env = os.environ.copy()
            env["BAOSTOCK_LOCK_PATH"] = str(Path(directory) / "bs.lock")
            holder = subprocess.Popen(
                [sys.executable, "-c", holder_code],
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                self.assertEqual(holder.stdout.readline().strip(), "LOCKED")
                challenger = subprocess.run(
                    [sys.executable, "-c", challenger_code],
                    cwd=Path(__file__).resolve().parents[1],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(challenger.returncode, 0, challenger.stderr)
            finally:
                if holder.stdin is not None:
                    holder.stdin.write("\n")
                    holder.stdin.flush()
                holder.wait(timeout=10)
                for stream in (holder.stdin, holder.stdout, holder.stderr):
                    if stream is not None:
                        stream.close()

    def test_second_session_fails_instead_of_running_concurrently(self):
        with tempfile.TemporaryDirectory() as directory:
            old = os.environ.get("BAOSTOCK_LOCK_PATH")
            os.environ["BAOSTOCK_LOCK_PATH"] = str(Path(directory) / "bs.lock")
            try:
                with baostock_process_lock():
                    with self.assertRaises(BaoStockConcurrencyError):
                        with baostock_process_lock():
                            self.fail("second BaoStock session must never start")
            finally:
                if old is None:
                    os.environ.pop("BAOSTOCK_LOCK_PATH", None)
                else:
                    os.environ["BAOSTOCK_LOCK_PATH"] = old

    def test_incremental_fetch_rejects_a_second_baostock_session(self):
        from utils.akshare_fetcher import AKShareFetcher

        fake_baostock = SimpleNamespace(
            login=lambda: SimpleNamespace(error_code="1", error_msg="not used"),
            logout=lambda: None,
        )
        with tempfile.TemporaryDirectory() as directory:
            old = os.environ.get("BAOSTOCK_LOCK_PATH")
            os.environ["BAOSTOCK_LOCK_PATH"] = str(Path(directory) / "bs.lock")
            fetcher = AKShareFetcher(directory)
            try:
                with (
                    patch.dict(sys.modules, {"baostock": fake_baostock}),
                    patch.object(fetcher, "_fetch_stock_update_eastmoney", return_value=None),
                    patch.object(fetcher, "_fetch_stock_update_tencent", return_value=None),
                    baostock_process_lock(),
                ):
                    with self.assertRaises(BaoStockConcurrencyError):
                        fetcher.fetch_stock_update("000001", days=1)
            finally:
                if old is None:
                    os.environ.pop("BAOSTOCK_LOCK_PATH", None)
                else:
                    os.environ["BAOSTOCK_LOCK_PATH"] = old


if __name__ == "__main__":
    unittest.main()
