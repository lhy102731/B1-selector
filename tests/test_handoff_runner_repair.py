from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import MagicMock, patch

import yaml

import research_automation.handoff_runner_repair as repair_module
from research_automation.handoff_runner_repair import repair_handoff_runner


def _mechanism_document(name: str = "novel_liquidity_conditioning") -> dict:
    return {
        "handoff_type": "kbase_discovery",
        "schema_version": 1,
        "created_at": "2026-07-24T00:00:00+00:00",
        "strategy_id": "brick",
        "topic": "repair mechanism runner",
        "status": "APPROVED",
        "result": {
            "transcript": [
                {"stage": "source_librarian", "output": {"brief_id": "brief-1"}},
                {"stage": "alpha_hunter", "output": {"mechanism": "liquidity"}},
                {
                    "stage": "factor_engineer",
                    "output": {
                        "research_mechanism": {
                            "name": name,
                            "mechanism": "condition signal-day features on liquidity",
                            "validation_plan": ["three fixed rolling folds"],
                        }
                    },
                },
            ]
        },
    }


def _complete_diff(allowed_files: list[str]) -> str:
    chunks = []
    for relative in allowed_files:
        chunks.append(
            f"diff --git a/{relative} b/{relative}\n"
            f"--- a/{relative}\n"
            f"+++ b/{relative}\n"
            "@@ -1 +1 @@\n"
            "-OLD_VALUE = 1\n"
            "+NEW_VALUE = 1\n"
        )
    return "".join(chunks)


class HandoffRunnerRepairTests(unittest.TestCase):
    def test_unauthorized_repair_fails_before_directory_or_model(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            output = root / "repair"
            handoff.write_text(
                yaml.safe_dump(_mechanism_document(), allow_unicode=True),
                encoding="utf-8",
            )

            with patch("research_automation.handoff_runner_repair.subprocess.run") as run:
                result = repair_handoff_runner(
                    handoff_path=handoff,
                    output_dir=output,
                )

            self.assertFalse(result.ok)
            self.assertEqual("unauthorized", result.status)
            self.assertFalse(output.exists())
            run.assert_not_called()

    def test_mechanism_only_handoff_builds_dedicated_dry_run_prompt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            output = root / "repair"
            handoff.write_text(
                yaml.safe_dump(_mechanism_document(), allow_unicode=True),
                encoding="utf-8",
            )

            result = repair_handoff_runner(
                handoff_path=handoff,
                output_dir=output,
                dry_run=True,
            )

            self.assertTrue(result.ok)
            self.assertEqual("dry_run", result.status)
            self.assertEqual(["novel_liquidity_conditioning"], result.factor_names)
            runner = next(path for path in result.allowed_files if path.startswith("research/"))
            focused_test = next(path for path in result.allowed_files if path.startswith("tests/"))
            prompt = Path(result.prompt_path).read_text(encoding="utf-8")
            self.assertIn(runner, prompt)
            self.assertIn(focused_test, prompt)
            archived = json.loads((output / "repair_result.json").read_text(encoding="utf-8"))
            self.assertEqual("dry_run", archived["status"])

    def test_generated_diff_must_include_runner_test_and_bridge(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            handoff.write_text(
                yaml.safe_dump(_mechanism_document(), allow_unicode=True),
                encoding="utf-8",
            )
            preview = repair_handoff_runner(
                handoff_path=handoff,
                output_dir=root / "preview",
                dry_run=True,
            )
            runner = next(path for path in preview.allowed_files if path.startswith("research/"))
            diff = (
                f"diff --git a/{runner} b/{runner}\n"
                "new file mode 100644\n"
                "--- /dev/null\n"
                f"+++ b/{runner}\n"
                "@@ -0,0 +1 @@\n"
                "+VALUE = 1\n"
                "diff --git a/research_automation/discovery_execution_bridge.py "
                "b/research_automation/discovery_execution_bridge.py\n"
                "--- a/research_automation/discovery_execution_bridge.py\n"
                "+++ b/research_automation/discovery_execution_bridge.py\n"
                "@@ -1 +1 @@\n"
                "-old\n"
                "+new\n"
            )
            model_result = CompletedProcess(
                args=["claude"], returncode=0, stdout=diff, stderr=""
            )

            with patch(
                "research_automation.handoff_runner_repair.subprocess.run",
                return_value=model_result,
            ):
                result = repair_handoff_runner(
                    handoff_path=handoff,
                    output_dir=root / "repair",
                    skip_code_review=True,
                )

            self.assertFalse(result.ok)
            self.assertEqual("diff_rejected", result.status)
            self.assertIn("required files missing", result.error)

    def test_unstructured_approve_word_does_not_pass_code_review(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            handoff.write_text(
                yaml.safe_dump(_mechanism_document(), allow_unicode=True),
                encoding="utf-8",
            )
            preview = repair_handoff_runner(
                handoff_path=handoff,
                output_dir=root / "preview",
                dry_run=True,
            )
            model_result = CompletedProcess(
                args=["claude"],
                returncode=0,
                stdout=_complete_diff(preview.allowed_files),
                stderr="",
            )
            reviewer = MagicMock()
            reviewer.generate_reply.return_value = (
                "The patch could be APPROVE-worthy after a structured review is supplied."
            )

            with (
                patch(
                    "research_automation.handoff_runner_repair.subprocess.run",
                    return_value=model_result,
                ),
                patch(
                    "ag2_research.agents.create_agents",
                    return_value={"code_reviewer": reviewer},
                ),
            ):
                result = repair_handoff_runner(
                    handoff_path=handoff,
                    output_dir=root / "repair",
                )

            self.assertFalse(result.ok)
            self.assertEqual("code_reviewer_rejected", result.status)

    def test_successful_repair_runs_every_changed_test_module(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            handoff.write_text(
                yaml.safe_dump(_mechanism_document(), allow_unicode=True),
                encoding="utf-8",
            )
            preview = repair_handoff_runner(
                handoff_path=handoff,
                output_dir=root / "preview",
                dry_run=True,
            )
            diff = _complete_diff(preview.allowed_files)
            commands: list[list[str]] = []

            def fake_run(command, **kwargs):
                commands.append([str(item) for item in command])
                if command[0] == "claude":
                    return CompletedProcess(command, 0, stdout=diff, stderr="")
                return CompletedProcess(command, 0, stdout="", stderr="")

            with patch(
                "research_automation.handoff_runner_repair.subprocess.run",
                side_effect=fake_run,
            ):
                result = repair_handoff_runner(
                    handoff_path=handoff,
                    output_dir=root / "repair",
                    skip_code_review=True,
                )

            focused_test = next(
                path for path in preview.allowed_files if path.startswith("tests/")
            )
            expected_module = focused_test.removesuffix(".py").replace("/", ".")
            self.assertTrue(result.ok)
            self.assertEqual("repaired", result.status)
            self.assertTrue(
                any(command[-2:] == ["unittest", expected_module] for command in commands),
                commands,
            )

    def test_failed_changed_test_reverses_exact_generated_diff(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "research").mkdir()
            (root / "research_automation").mkdir()
            (root / "tests").mkdir()
            (root / "tests" / "__init__.py").write_text("", encoding="utf-8")
            bridge = root / "research_automation" / "discovery_execution_bridge.py"
            bridge.write_text("OLD_VALUE = 1\n", encoding="utf-8")
            handoff = root / "handoff.yaml"
            handoff.write_text(
                yaml.safe_dump(_mechanism_document(), allow_unicode=True),
                encoding="utf-8",
            )
            real_run = subprocess.run
            real_run(["git", "init", "-q"], cwd=root, check=True)

            with patch.object(repair_module, "PROJECT_ROOT", root):
                preview = repair_handoff_runner(
                    handoff_path=handoff,
                    output_dir=root / "preview",
                    dry_run=True,
                )
                runner = next(
                    path for path in preview.allowed_files if path.startswith("research/")
                )
                focused_test = next(
                    path for path in preview.allowed_files if path.startswith("tests/")
                )
                diff = (
                    f"diff --git a/{runner} b/{runner}\n"
                    "new file mode 100644\n"
                    "--- /dev/null\n"
                    f"+++ b/{runner}\n"
                    "@@ -0,0 +1 @@\n"
                    "+VALUE = 1\n"
                    f"diff --git a/{focused_test} b/{focused_test}\n"
                    "new file mode 100644\n"
                    "--- /dev/null\n"
                    f"+++ b/{focused_test}\n"
                    "@@ -0,0 +1,5 @@\n"
                    "+import unittest\n"
                    "+class GeneratedTests(unittest.TestCase):\n"
                    "+    def test_gate(self):\n"
                    "+        self.fail('generated gate failure')\n"
                    "+\n"
                    "diff --git a/research_automation/discovery_execution_bridge.py "
                    "b/research_automation/discovery_execution_bridge.py\n"
                    "--- a/research_automation/discovery_execution_bridge.py\n"
                    "+++ b/research_automation/discovery_execution_bridge.py\n"
                    "@@ -1 +1 @@\n"
                    "-OLD_VALUE = 1\n"
                    "+NEW_VALUE = 1\n"
                )

                def model_then_real(command, **kwargs):
                    if command[0] == "claude":
                        return CompletedProcess(command, 0, stdout=diff, stderr="")
                    return real_run(command, **kwargs)

                with patch(
                    "research_automation.handoff_runner_repair.subprocess.run",
                    side_effect=model_then_real,
                ):
                    result = repair_handoff_runner(
                        handoff_path=handoff,
                        output_dir=root / "repair",
                        skip_code_review=True,
                    )

            self.assertFalse(result.ok)
            self.assertEqual("tests_failed_rolled_back", result.status)
            self.assertEqual("OLD_VALUE = 1\n", bridge.read_text(encoding="utf-8"))
            self.assertFalse((root / runner).exists())
            self.assertFalse((root / focused_test).exists())
            self.assertEqual(diff, (root / "repair" / "applied.diff").read_text(encoding="utf-8"))
            self.assertTrue((root / "repair" / "rollback.log").is_file())

    def test_changed_test_timeout_is_archived_and_rolled_back(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            handoff.write_text(
                yaml.safe_dump(_mechanism_document(), allow_unicode=True),
                encoding="utf-8",
            )
            preview = repair_handoff_runner(
                handoff_path=handoff,
                output_dir=root / "preview",
                dry_run=True,
            )
            diff = _complete_diff(preview.allowed_files)

            def fake_run(command, **kwargs):
                if command[0] == "claude":
                    return CompletedProcess(command, 0, stdout=diff, stderr="")
                if "unittest" in command:
                    raise subprocess.TimeoutExpired(command, kwargs.get("timeout", 0))
                return CompletedProcess(command, 0, stdout="", stderr="")

            with patch(
                "research_automation.handoff_runner_repair.subprocess.run",
                side_effect=fake_run,
            ):
                result = repair_handoff_runner(
                    handoff_path=handoff,
                    output_dir=root / "repair",
                    skip_code_review=True,
                )

            self.assertFalse(result.ok)
            self.assertEqual("tests_failed_rolled_back", result.status)
            self.assertIn("TimeoutExpired", result.error)
            self.assertTrue((root / "repair" / "rollback.log").is_file())

    def test_rollback_exception_is_fail_closed_and_archived(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = root / "handoff.yaml"
            handoff.write_text(
                yaml.safe_dump(_mechanism_document(), allow_unicode=True),
                encoding="utf-8",
            )
            preview = repair_handoff_runner(
                handoff_path=handoff,
                output_dir=root / "preview",
                dry_run=True,
            )
            diff = _complete_diff(preview.allowed_files)

            def fake_run(command, **kwargs):
                if command[0] == "claude":
                    return CompletedProcess(command, 0, stdout=diff, stderr="")
                if "unittest" in command:
                    return CompletedProcess(command, 1, stdout="", stderr="failed")
                if "--reverse" in command:
                    raise subprocess.TimeoutExpired(command, kwargs.get("timeout", 0))
                return CompletedProcess(command, 0, stdout="", stderr="")

            with patch(
                "research_automation.handoff_runner_repair.subprocess.run",
                side_effect=fake_run,
            ):
                result = repair_handoff_runner(
                    handoff_path=handoff,
                    output_dir=root / "repair",
                    skip_code_review=True,
                )

            self.assertFalse(result.ok)
            self.assertEqual("tests_failed_rollback_failed", result.status)
            rollback_log = (root / "repair" / "rollback.log").read_text(encoding="utf-8")
            self.assertIn("TimeoutExpired", rollback_log)
            self.assertTrue((root / "repair" / "repair_result.json").is_file())


if __name__ == "__main__":
    unittest.main()
