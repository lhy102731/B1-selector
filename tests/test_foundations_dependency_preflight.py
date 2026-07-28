from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import unittest
from importlib import metadata
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from research_automation.foundations.dependency_preflight import (
    DependencyPreflightError,
    inspect_dependency_environment,
    require_dependency_environment,
)


class DependencyPreflightTests(unittest.TestCase):
    @staticmethod
    def _write_lock(root: Path, records: list[tuple[str, str]]) -> Path:
        lock_path = root / "control-plane.lock"
        lines: list[str] = []
        for index, (name, version) in enumerate(records, start=1):
            lines.append(f"{name}=={version} \\")
            lines.append(f"    --hash=sha256:{index:064x}")
        lock_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return lock_path

    def test_exact_hashed_lock_matches_installed_distributions(self) -> None:
        with TemporaryDirectory() as temporary:
            lock_path = Path(temporary) / "control-plane.lock"
            lock_bytes = (
                "example-package==1.2.3 \\\n"
                "    --hash=sha256:" + "a" * 64 + " \\\n"
                "    --hash=sha256:" + "b" * 64 + "\n"
                "second_pkg==2.0.0 \\\n"
                "    --hash=sha256:" + "c" * 64 + "\n"
            ).encode("utf-8")
            lock_path.write_bytes(lock_bytes)
            installed = {
                "example-package": "1.2.3",
                "second-pkg": "2.0.0",
            }

            with patch(
                "research_automation.foundations.dependency_preflight.metadata.version",
                side_effect=lambda name: installed[name],
            ):
                report = inspect_dependency_environment(lock_path)

            self.assertEqual(report.status, "PASS")
            self.assertEqual(report.issue_codes, ())
            self.assertEqual(
                [check.distribution for check in report.checked_distributions],
                ["example-package", "second-pkg"],
            )
            self.assertEqual(
                report.lock_sha256,
                hashlib.sha256(lock_bytes).hexdigest(),
            )

    def test_missing_and_mismatched_distributions_are_reported(self) -> None:
        with TemporaryDirectory() as temporary:
            lock_path = self._write_lock(
                Path(temporary),
                [("missing-package", "1.0"), ("wrong-package", "2.0")],
            )

            def installed_version(name: str) -> str:
                if name == "missing-package":
                    raise metadata.PackageNotFoundError(name)
                return "9.9"

            with patch(
                "research_automation.foundations.dependency_preflight.metadata.version",
                side_effect=installed_version,
            ):
                report = inspect_dependency_environment(lock_path)

            self.assertEqual(report.status, "FAIL")
            self.assertEqual(
                report.issue_codes,
                ("MISSING_DISTRIBUTION", "VERSION_MISMATCH"),
            )
            self.assertEqual(
                [check.status for check in report.checked_distributions],
                ["MISSING", "MISMATCH"],
            )

    def test_non_exact_or_unhashed_lock_content_fails_closed(self) -> None:
        invalid_documents = (
            "package==1.0\n",
            "package>=1.0 \\\n    --hash=sha256:" + "a" * 64 + "\n",
            "package @ https://example.invalid/package.whl\n",
            "-e ../package\n",
            "package[extra]==1.0 \\\n    --hash=sha256:" + "a" * 64 + "\n",
            "package==1.0/invalid \\\n    --hash=sha256:" + "a" * 64 + "\n",
            "package==1.0\n    --hash=sha256:" + "a" * 64 + "\n",
            (
                "some_package==1.0 \\\n    --hash=sha256:"
                + "a" * 64
                + "\nsome-package==1.0 \\\n    --hash=sha256:"
                + "b" * 64
                + "\n"
            ),
            "\ufeffpackage==1.0 \\\n    --hash=sha256:" + "a" * 64 + "\n",
        )
        for document in invalid_documents:
            with self.subTest(document=document), TemporaryDirectory() as temporary:
                lock_path = Path(temporary) / "control-plane.lock"
                lock_path.write_text(document, encoding="utf-8")

                report = inspect_dependency_environment(lock_path)

                self.assertEqual(report.status, "FAIL")
                self.assertEqual(report.issue_codes, ("LOCK_INVALID",))
                self.assertEqual(report.checked_distributions, ())

    def test_require_preflight_returns_pass_or_raises_with_the_failed_report(self) -> None:
        with TemporaryDirectory() as temporary:
            lock_path = self._write_lock(
                Path(temporary),
                [("example-package", "1.0")],
            )
            with patch(
                "research_automation.foundations.dependency_preflight.metadata.version",
                return_value="1.0",
            ):
                passed = require_dependency_environment(lock_path)
            self.assertEqual(passed.status, "PASS")

            with patch(
                "research_automation.foundations.dependency_preflight.metadata.version",
                return_value="2.0",
            ):
                with self.assertRaisesRegex(
                    DependencyPreflightError,
                    "DEPENDENCY_PREFLIGHT_FAILED",
                ) as captured:
                    require_dependency_environment(lock_path)

            self.assertEqual(captured.exception.report.status, "FAIL")
            self.assertEqual(
                captured.exception.report.issue_codes,
                ("VERSION_MISMATCH",),
            )

    def test_unsupported_runtime_is_reported_without_importing_locked_packages(self) -> None:
        with TemporaryDirectory() as temporary:
            lock_path = self._write_lock(
                Path(temporary),
                [("example-package", "1.0")],
            )
            with (
                patch(
                    "research_automation.foundations.dependency_preflight."
                    "platform.python_implementation",
                    return_value="PyPy",
                ),
                patch(
                    "research_automation.foundations.dependency_preflight.metadata.version",
                    return_value="1.0",
                ) as version_lookup,
            ):
                report = inspect_dependency_environment(lock_path)

            self.assertEqual(report.status, "FAIL")
            self.assertEqual(report.issue_codes, ("UNSUPPORTED_RUNTIME",))
            self.assertEqual(report.python_implementation, "PyPy")
            version_lookup.assert_called_once_with("example-package")

    def test_repository_lock_is_hashed_and_covers_the_direct_dependency_set(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        input_path = repository_root / "requirements" / "control-plane.in"
        lock_path = repository_root / "requirements" / "control-plane.lock"
        lock_text = lock_path.read_text(encoding="utf-8")
        pins = {
            re.sub(r"[-_.]+", "-", name).lower(): version
            for name, version in re.findall(
                r"(?m)^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s\\]+)\s*\\$",
                lock_text,
            )
        }

        self.assertEqual(
            {
                "ag2",
                "httpx",
                "jsonschema",
                "openai",
                "pydantic",
                "python-dotenv",
                "pyyaml",
            }
            - set(pins),
            set(),
        )
        self.assertIn("pip-tools==7.6.0", input_path.read_text(encoding="utf-8"))
        self.assertNotIn("--index-url", lock_text)
        self.assertNotIn("--trusted-host", lock_text)
        with patch(
            "research_automation.foundations.dependency_preflight.metadata.version",
            side_effect=lambda name: pins[name],
        ):
            report = inspect_dependency_environment(lock_path)

        self.assertEqual(report.status, "PASS")
        self.assertEqual(len(report.checked_distributions), len(pins))

    def test_preflight_imports_only_the_standard_library(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        script = (
            "import json,sys;"
            f"sys.path.insert(0,{str(repository_root)!r});"
            "import research_automation.foundations.dependency_preflight;"
            "forbidden=('autogen','dotenv','httpx','jsonschema','openai','pydantic','yaml');"
            "print(json.dumps(sorted(name for name in forbidden if name in sys.modules)))"
        )

        completed = subprocess.run(
            [sys.executable, "-I", "-c", script],
            cwd=repository_root,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stderr, "")
        self.assertEqual(json.loads(completed.stdout), [])


if __name__ == "__main__":
    unittest.main()
