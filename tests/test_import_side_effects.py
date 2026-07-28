from __future__ import annotations

import builtins
from contextlib import ExitStack, contextmanager, redirect_stdout
import importlib
import io
import os
import runpy
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _guarded_text_open(original):
    def guarded(file, mode="r", *args, **kwargs):
        if any(flag in str(mode) for flag in ("w", "a", "x", "+")):
            raise AssertionError(f"import attempted a file write: {file}")
        return original(file, mode, *args, **kwargs)

    return guarded


def _guarded_os_open(original):
    write_flags = (
        os.O_WRONLY
        | os.O_RDWR
        | os.O_CREAT
        | os.O_TRUNC
        | os.O_APPEND
    )

    def guarded(path, flags, *args, **kwargs):
        if flags & write_flags:
            raise AssertionError(f"import attempted an os.open write: {path}")
        return original(path, flags, *args, **kwargs)

    return guarded


@contextmanager
def reject_import_side_effects():
    def reject(*args, **kwargs):
        raise AssertionError("import attempted a filesystem/network/process effect")

    with ExitStack() as stack:
        stack.enter_context(
            patch("builtins.open", side_effect=_guarded_text_open(builtins.open))
        )
        stack.enter_context(
            patch("io.open", side_effect=_guarded_text_open(io.open))
        )
        stack.enter_context(
            patch("os.open", side_effect=_guarded_os_open(os.open))
        )
        for target in (
            "os.mkdir",
            "os.makedirs",
            "os.remove",
            "os.unlink",
            "os.rename",
            "os.replace",
            "os.rmdir",
            "os.utime",
            "socket.create_connection",
            "socket.socket.connect",
            "socket.socket.connect_ex",
            "socket.socket.bind",
            "subprocess.Popen",
        ):
            stack.enter_context(patch(target, side_effect=reject))
        stack.enter_context(patch.object(sys, "dont_write_bytecode", True))
        yield


class ImportSideEffectTests(unittest.TestCase):
    def test_control_plane_cold_import_is_quiet_and_under_five_seconds(self) -> None:
        samples: list[float] = []
        probe = (
            "import sys; "
            "import research_automation.control_plane; "
            "assert 'autogen' not in sys.modules; "
            "assert 'ag2_research.orchestrator' not in sys.modules"
        )
        for sample_index in range(3):
            started = time.perf_counter()
            completed = subprocess.run(
                [sys.executable, "-B", "-c", probe],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            elapsed = time.perf_counter() - started
            samples.append(elapsed)

            with self.subTest(sample=sample_index + 1, elapsed=elapsed):
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertEqual(completed.stdout, "")
                self.assertEqual(completed.stderr, "")
                self.assertLessEqual(elapsed, 5.0)

        self.assertEqual(len(samples), 3)

    def test_research_automation_public_exports_are_loaded_lazily(self) -> None:
        managed_prefixes = ("research_automation", "ag2_research")
        saved_modules = {
            name: module
            for name, module in tuple(sys.modules.items())
            if any(
                name == prefix or name.startswith(f"{prefix}.")
                for prefix in managed_prefixes
            )
        }
        for name in saved_modules:
            sys.modules.pop(name, None)

        try:
            with patch("dotenv.load_dotenv", return_value=False):
                package = importlib.import_module("research_automation")

                self.assertFalse(
                    "research_automation.autonomous_runner" in sys.modules,
                    "package import eagerly loaded autonomous_runner",
                )
                self.assertFalse(
                    "research_automation.experiment_runner" in sys.modules,
                    "package import eagerly loaded experiment_runner",
                )
                self.assertIn("TaskQueue", package.__all__)

                exported = package.TaskQueue

                self.assertEqual(exported.__name__, "TaskQueue")
                self.assertIn(
                    "research_automation.task_queue",
                    sys.modules,
                )
                self.assertFalse(
                    "research_automation.autonomous_runner" in sys.modules,
                    "lightweight export loaded autonomous_runner",
                )
                self.assertFalse(
                    "research_automation.experiment_runner" in sys.modules,
                    "lightweight export loaded experiment_runner",
                )

                for name in package.__all__:
                    with self.subTest(public_export=name):
                        self.assertIsNotNone(getattr(package, name))
        finally:
            for name in tuple(sys.modules):
                if any(
                    name == prefix or name.startswith(f"{prefix}.")
                    for prefix in managed_prefixes
                ):
                    sys.modules.pop(name, None)
            sys.modules.update(saved_modules)

    def test_build_daily_ret_cache_import_does_not_touch_data(self) -> None:
        script = PROJECT_ROOT / "build_daily_ret_cache.py"

        with reject_import_side_effects():
            with patch.object(Path, "exists", return_value=True):
                with patch(
                    "pandas.read_parquet",
                    side_effect=AssertionError("import attempted to read parquet data"),
                ):
                    namespace = runpy.run_path(
                        str(script),
                        run_name="import_probe_build_daily_ret_cache",
                    )

        self.assertTrue(callable(namespace.get("main")))

    def test_build_daily_ret_cache_main_preserves_explicit_cli_work(self) -> None:
        module = importlib.import_module("build_daily_ret_cache")
        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            cache_dir.mkdir()
            indicator_path = cache_dir / "000001.parquet"
            indicator_path.touch()
            output = Path(tmp) / "daily_ret.parquet"
            temporary = output.with_suffix(output.suffix + ".tmp")
            recent = module.pd.Timestamp.now().normalize()
            indicator_frame = module.pd.DataFrame(
                {
                    "date": [recent - module.pd.Timedelta(days=1), recent],
                    "close": [10.0, 10.5],
                }
            )
            with patch.object(module, "CACHE_DIR", cache_dir):
                with patch.object(module, "OUT", output):
                    with patch.object(
                        module.pd,
                        "read_parquet",
                        return_value=indicator_frame,
                    ) as read_parquet:
                        with patch(
                            "pandas.DataFrame.to_parquet",
                        ) as to_parquet:
                            with patch.object(module.os, "replace") as replace:
                                with redirect_stdout(io.StringIO()):
                                    status = module.main()

        self.assertEqual(status, 0)
        read_parquet.assert_called_once_with(
            indicator_path,
            columns=["date", "close"],
        )
        to_parquet.assert_called_once_with(temporary, index=False)
        replace.assert_called_once_with(temporary, output)

    def test_llm_connectivity_import_does_not_construct_research_config(self) -> None:
        script = PROJECT_ROOT / "tools" / "diagnostics" / "test_llm_connectivity.py"
        fake_ag2 = ModuleType("ag2_research")
        fake_autogen = ModuleType("autogen")

        def reject_config_construction() -> object:
            raise AssertionError(
                "import attempted to construct live LLM configuration"
            )

        fake_ag2.ResearchConfig = reject_config_construction  # type: ignore[attr-defined]
        with reject_import_side_effects():
            with patch.dict(
                sys.modules,
                {"ag2_research": fake_ag2, "autogen": fake_autogen},
            ):
                namespace = runpy.run_path(
                    str(script),
                    run_name="import_probe_test_llm_connectivity",
                )

        self.assertTrue(callable(namespace.get("main")))

    def test_llm_connectivity_main_preserves_explicit_config_construction(self) -> None:
        script = PROJECT_ROOT / "tools" / "diagnostics" / "test_llm_connectivity.py"
        namespace = runpy.run_path(
            str(script),
            run_name="cli_probe_test_llm_connectivity",
        )
        constructions: list[str] = []

        class FakeResearchConfig:
            def __init__(self) -> None:
                constructions.append("constructed")

            def list_profiles(self) -> list[str]:
                return []

        fake_ag2 = ModuleType("ag2_research")
        fake_ag2.ResearchConfig = FakeResearchConfig  # type: ignore[attr-defined]
        fake_autogen = ModuleType("autogen")
        with patch.dict(
            sys.modules,
            {"ag2_research": fake_ag2, "autogen": fake_autogen},
        ):
            with redirect_stdout(io.StringIO()):
                status = namespace["main"]()

        self.assertEqual(status, 0)
        self.assertEqual(constructions, ["constructed"])

    def test_fetch_active_cap_import_does_not_create_output_directories(self) -> None:
        script = PROJECT_ROOT / "tools" / "data" / "fetch_active_cap.py"

        with reject_import_side_effects():
            namespace = runpy.run_path(
                str(script),
                run_name="import_probe_fetch_active_cap",
            )

        self.assertTrue(callable(namespace.get("main")))

    def test_fetch_active_cap_main_preserves_missing_source_exit_status(self) -> None:
        module = importlib.import_module("tools.data.fetch_active_cap")
        with TemporaryDirectory() as tmp:
            missing_source = Path(tmp) / "missing.vdat"
            with patch.object(module, "VDAT_PATH", missing_source):
                with redirect_stdout(io.StringIO()):
                    status = module.main([])

        self.assertEqual(status, 1)


if __name__ == "__main__":
    unittest.main()
