from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import textwrap
import traceback
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from research_automation.foundations.settings import (
    MissingInvocationSettingError,
    ProjectSettingsError,
    load_project_settings,
)


class ProjectSettingsTests(unittest.TestCase):
    @staticmethod
    def _write_minimal_config(root: Path) -> Path:
        path = root / "settings.yaml"
        path.write_text(
            """
llm:
  default: primary
  profiles:
    primary:
      api_key: "${TEST_API_KEY}"
      base_url: "${TEST_BASE_URL:-https://example.invalid/v1}"
      model: "${TEST_MODEL:-model-a}"
      temperature: 0.2
      timeout: 30
agents:
  worker:
    profile: primary
    name: Worker
    description: Test worker
workflows:
  simple:
    description: Simple workflow
    agents: [worker]
    coordinator: worker
roundtable:
  participants:
    - profile: primary
      label: Primary
  coordinator: primary
control_layer: {}
""".lstrip(),
            encoding="utf-8",
        )
        return path

    def test_explicit_environment_loads_without_mutating_process_environment(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            before = dict(os.environ)

            settings = load_project_settings(
                config_path,
                environ={"TEST_API_KEY": "DUMMY-KEY"},
            )

            self.assertEqual(settings.default_profile, "primary")
            self.assertEqual(
                settings.require_invocation_profile("primary")
                .api_key.get_secret_value(),
                "DUMMY-KEY",
            )
            self.assertEqual(dict(os.environ), before)

    def test_selected_sources_follow_the_documented_precedence(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = self._write_minimal_config(root)
            env_path = root / "selected.env"
            env_path.write_text(
                "TEST_API_KEY=FILE-KEY\nTEST_MODEL=file-model\n",
                encoding="utf-8",
            )

            settings = load_project_settings(
                config_path,
                env_file=env_path,
                environ={"TEST_API_KEY": "PROCESS-KEY"},
                overrides={"TEST_API_KEY": "OVERRIDE-KEY"},
            )

            profile = settings.require_invocation_profile("primary")
            self.assertEqual(profile.api_key.get_secret_value(), "OVERRIDE-KEY")
            self.assertEqual(profile.model, "file-model")

    def test_metadata_overrides_are_applied_and_unconsumed_overrides_are_rejected(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            config_path.write_text(
                config_path.read_text(encoding="utf-8").replace(
                    "      timeout: 30\n",
                    "      timeout: 30\n"
                    '      provider_id: "${TEST_PROVIDER:-provider-a}"\n',
                ),
                encoding="utf-8",
            )
            baseline = load_project_settings(config_path, environ={})
            overridden = load_project_settings(
                config_path,
                environ={},
                overrides={"TEST_PROVIDER": "provider-b"},
            )

            self.assertEqual(
                overridden.inspect()["profiles"]["primary"]["provider_id"],
                "provider-b",
            )
            self.assertNotEqual(
                baseline.public_identity_sha256,
                overridden.public_identity_sha256,
            )

        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            config_path.write_text(
                config_path.read_text(encoding="utf-8").replace(
                    "control_layer: {}",
                    'control_layer:\n  note: "${UNCONSUMED_VALUE}"',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ProjectSettingsError, "unknown override"):
                load_project_settings(
                    config_path,
                    environ={},
                    overrides={"UNCONSUMED_VALUE": "ignored"},
                )

    def test_explicit_sources_reject_non_string_entries(self) -> None:
        invalid_sources = (
            {"environ": {"UNUSED_VALUE": 1}},
            {"environ": {1: "value"}},
            {"overrides": {"TEST_API_KEY": 1}},
        )
        for source in invalid_sources:
            with self.subTest(source=source), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))

                with self.assertRaisesRegex(
                    ProjectSettingsError,
                    "environment source",
                ):
                    load_project_settings(config_path, **source)

    def test_inspection_succeeds_without_secret_or_ambient_dotenv_loading(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = self._write_minimal_config(root)
            (root / ".env").write_text(
                "TEST_API_KEY=AMBIENT-KEY\n",
                encoding="utf-8",
            )

            settings = load_project_settings(config_path, environ={})
            inspection = settings.inspect()

            self.assertEqual(
                inspection["profiles"]["primary"]["credential_status"],
                "MISSING",
            )
            self.assertNotIn("AMBIENT-KEY", json.dumps(inspection))
            with self.assertRaisesRegex(
                MissingInvocationSettingError,
                "MISSING_CREDENTIAL",
            ):
                settings.require_invocation_profile("primary")

    def test_blank_credentials_are_missing_at_the_invocation_seam(self) -> None:
        for credential in (" \t ", " PADDED-KEY "):
            with self.subTest(credential=credential), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                settings = load_project_settings(
                    config_path,
                    environ={"TEST_API_KEY": credential},
                )

                self.assertEqual(
                    settings.inspect()["profiles"]["primary"]["credential_status"],
                    "MISSING",
                )
                with self.assertRaisesRegex(
                    MissingInvocationSettingError,
                    "MISSING_CREDENTIAL",
                ):
                    settings.require_invocation_profile("primary")

    def test_duplicate_yaml_keys_fail_closed(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            text = config_path.read_text(encoding="utf-8").replace(
                "      timeout: 30\n",
                "      timeout: 30\n      timeout: 31\n",
            )
            config_path.write_text(text, encoding="utf-8")

            with self.assertRaisesRegex(
                ProjectSettingsError,
                "duplicate",
            ):
                load_project_settings(config_path, environ={})

    def test_credentials_require_an_exact_environment_reference(self) -> None:
        invalid_values = ("PLAINTEXT-KEY", "${TEST_API_KEY:-FALLBACK-KEY}")
        for invalid in invalid_values:
            with self.subTest(invalid=invalid), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                text = config_path.read_text(encoding="utf-8").replace(
                    "${TEST_API_KEY}",
                    invalid,
                )
                config_path.write_text(text, encoding="utf-8")

                with self.assertRaisesRegex(
                    ProjectSettingsError,
                    "credential reference",
                ):
                    load_project_settings(config_path, environ={})

    def test_credential_environment_names_cannot_enter_public_profile_fields(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            text = config_path.read_text(encoding="utf-8").replace(
                "      timeout: 30\n",
                "      timeout: 30\n"
                "      extra_params:\n"
                '        leaked: "${TEST_API_KEY}"\n',
            )
            config_path.write_text(text, encoding="utf-8")

            with self.assertRaisesRegex(
                ProjectSettingsError,
                "credential.*public profile",
            ):
                load_project_settings(
                    config_path,
                    environ={"TEST_API_KEY": "MUST-NOT-LEAK"},
                )

    def test_unknown_profile_fields_and_explicit_overrides_fail_closed(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            text = config_path.read_text(encoding="utf-8").replace(
                "      timeout: 30\n",
                "      timeout: 30\n      typo_timeout: 31\n",
            )
            config_path.write_text(text, encoding="utf-8")

            with self.assertRaisesRegex(ProjectSettingsError, "unknown profile"):
                load_project_settings(config_path, environ={})

        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            with self.assertRaisesRegex(ProjectSettingsError, "unknown override"):
                load_project_settings(
                    config_path,
                    environ={},
                    overrides={"UNREFERENCED_SETTING": "value"},
                )

    def test_invalid_profile_values_fail_closed_without_coercion(self) -> None:
        replacements = (
            ("timeout: 30", 'timeout: "30"'),
            ("temperature: 0.2", "temperature: .nan"),
            ('model: "${TEST_MODEL:-model-a}"', 'model: " "'),
            (
                'base_url: "${TEST_BASE_URL:-https://example.invalid/v1}"',
                "base_url: ftp://example.invalid/v1",
            ),
            (
                'base_url: "${TEST_BASE_URL:-https://example.invalid/v1}"',
                "base_url: https://example.invalid:99999/v1",
            ),
            (
                'base_url: "${TEST_BASE_URL:-https://example.invalid/v1}"',
                'base_url: "https://bad host.invalid/v1"',
            ),
            (
                'base_url: "${TEST_BASE_URL:-https://example.invalid/v1}"',
                "base_url: https://example.invalid/v1?token=public-leak",
            ),
            (
                'base_url: "${TEST_BASE_URL:-https://example.invalid/v1}"',
                "base_url: https://example.invalid/v1#fragment",
            ),
            ("timeout: 30", "timeout: -1"),
        )
        for original, replacement in replacements:
            with self.subTest(replacement=replacement), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                text = config_path.read_text(encoding="utf-8").replace(
                    original,
                    replacement,
                )
                config_path.write_text(text, encoding="utf-8")

                with self.assertRaisesRegex(
                    ProjectSettingsError,
                    "profile values",
                ):
                    load_project_settings(config_path, environ={})

    def test_non_finite_nested_public_values_fail_during_load(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            text = config_path.read_text(encoding="utf-8").replace(
                "      timeout: 30\n",
                "      timeout: 30\n"
                "      extra_params:\n"
                "        invalid: .nan\n",
            )
            config_path.write_text(text, encoding="utf-8")

            with self.assertRaisesRegex(
                ProjectSettingsError,
                "public configuration",
            ) as captured:
                load_project_settings(config_path, environ={})

            self.assertIsNone(captured.exception.__cause__)
            self.assertIsNone(captured.exception.__context__)

    def test_profile_validation_errors_are_safely_normalized(self) -> None:
        replacements = (
            (
                'base_url: "${TEST_BASE_URL:-https://example.invalid/v1}"',
                'base_url: "http://[::1/MUST-NOT-LEAK"',
            ),
            (
                "      timeout: 30\n",
                "      timeout: 30\n"
                "      extra_params:\n"
                "        1: MUST-NOT-LEAK\n",
            ),
        )
        for original, replacement in replacements:
            with self.subTest(replacement=replacement), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                config_path.write_text(
                    config_path.read_text(encoding="utf-8").replace(
                        original,
                        replacement,
                    ),
                    encoding="utf-8",
                )

                with self.assertRaises(ProjectSettingsError) as captured:
                    load_project_settings(config_path, environ={})

                rendered = "".join(traceback.format_exception(captured.exception))
                self.assertNotIn("MUST-NOT-LEAK", rendered)
                self.assertIsNone(captured.exception.__cause__)
                self.assertIsNone(captured.exception.__context__)

    def test_agent_workflow_and_roundtable_references_are_closed(self) -> None:
        replacements = (
            (
                "  worker:\n    profile: primary",
                "  worker:\n    profile: missing-profile",
            ),
            ("    agents: [worker]", "    agents: [missing-agent]"),
            ("    coordinator: worker", "    coordinator: missing-agent"),
            ("  coordinator: primary", "  coordinator: missing-profile"),
            (
                "    - profile: primary\n      label: Primary",
                "    - profile: primary\n      label: Primary\n"
                "    - profile: primary\n      label: Primary",
            ),
        )
        for original, replacement in replacements:
            with self.subTest(replacement=replacement), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                text = config_path.read_text(encoding="utf-8").replace(
                    original,
                    replacement,
                    1,
                )
                config_path.write_text(text, encoding="utf-8")

                with self.assertRaisesRegex(
                    ProjectSettingsError,
                    "reference integrity",
                ):
                    load_project_settings(config_path, environ={})

    def test_nested_and_malformed_references_fail_with_a_stable_error(self) -> None:
        replacements = (
            (
                "    profile: primary",
                "    profile: []",
            ),
            (
                "    agents: [worker]",
                "    agents: [[worker]]",
            ),
            (
                "    coordinator: worker",
                "    coordinator: worker\n"
                "    roundtable:\n"
                "      participants:\n"
                "        - profile: missing-profile\n"
                "          label: Missing",
            ),
        )
        for original, replacement in replacements:
            with self.subTest(replacement=replacement), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                config_path.write_text(
                    config_path.read_text(encoding="utf-8").replace(
                        original,
                        replacement,
                        1,
                    ),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(
                    ProjectSettingsError,
                    "reference integrity",
                ):
                    load_project_settings(config_path, environ={})

    def test_workflow_and_roundtable_control_roles_are_local_members(self) -> None:
        workflow_replacements = (
            ("    coordinator: worker", "    coordinator: outsider"),
            (
                "    agents: [worker]",
                "    agents: [worker]\n    pipeline_order: [outsider]",
            ),
        )
        for original, replacement in workflow_replacements:
            with self.subTest(replacement=replacement), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                text = config_path.read_text(encoding="utf-8").replace(
                    "workflows:\n",
                    "  outsider:\n"
                    "    profile: primary\n"
                    "    name: Outsider\n"
                    "    description: Outsider\n"
                    "workflows:\n",
                )
                config_path.write_text(
                    text.replace(original, replacement, 1),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(ProjectSettingsError, "reference integrity"):
                    load_project_settings(config_path, environ={})

        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            text = config_path.read_text(encoding="utf-8").replace(
                "agents:\n",
                "    secondary:\n"
                '      api_key: "${SECONDARY_API_KEY}"\n'
                "      model: model-b\n"
                "      base_url: https://secondary.invalid/v1\n"
                "      temperature: 0.2\n"
                "      timeout: 30\n"
                "agents:\n",
            )
            config_path.write_text(
                text.replace(
                    "  coordinator: primary",
                    "  coordinator: secondary",
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ProjectSettingsError, "reference integrity"):
                load_project_settings(config_path, environ={})

    def test_public_identity_is_deterministic_and_secret_value_independent(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            first = load_project_settings(
                config_path,
                environ={"TEST_API_KEY": "FIRST-SECRET"},
            )
            second = load_project_settings(
                config_path,
                environ={"TEST_API_KEY": "SECOND-SECRET"},
            )
            changed_model = load_project_settings(
                config_path,
                environ={
                    "TEST_API_KEY": "FIRST-SECRET",
                    "TEST_MODEL": "model-b",
                },
            )

            self.assertEqual(
                first.public_identity_sha256,
                second.public_identity_sha256,
            )
            self.assertNotEqual(
                first.public_identity_sha256,
                changed_model.public_identity_sha256,
            )
            self.assertNotIn(
                "FIRST-SECRET",
                json.dumps(first.public_manifest()),
            )
            self.assertNotIn(
                "FIRST-SECRET",
                repr(first)
                + repr(first.inspect())
                + repr(first.require_invocation_profile("primary")),
            )

    def test_returned_views_cannot_mutate_owned_settings(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            text = config_path.read_text(encoding="utf-8").replace(
                "      timeout: 30\n",
                "      timeout: 30\n"
                "      extra_params:\n"
                "        reasoning:\n"
                "          effort: high\n",
            )
            config_path.write_text(text, encoding="utf-8")
            settings = load_project_settings(
                config_path,
                environ={"TEST_API_KEY": "DUMMY-KEY"},
            )
            identity_before = settings.public_identity_sha256

            manifest = settings.public_manifest()
            inspection = settings.inspect()
            invocation = settings.require_invocation_profile("primary")
            unresolved = settings.unresolved_document()
            manifest["profiles"]["primary"]["extra_params"]["reasoning"][
                "effort"
            ] = "low"
            inspection["profiles"]["primary"]["extra_params"]["reasoning"][
                "effort"
            ] = "low"
            invocation.extra_params["reasoning"]["effort"] = "low"
            unresolved["agents"]["worker"]["description"] = "changed"

            self.assertEqual(
                settings.public_manifest()["profiles"]["primary"]["extra_params"]
                ["reasoning"]["effort"],
                "high",
            )
            self.assertEqual(settings.public_identity_sha256, identity_before)
            self.assertEqual(
                settings.unresolved_document()["agents"]["worker"]["description"],
                "Test worker",
            )

    def test_public_identity_ignores_mapping_insertion_order(self) -> None:
        with TemporaryDirectory() as first_temporary, TemporaryDirectory() as second_temporary:
            first_path = self._write_minimal_config(Path(first_temporary))
            second_path = self._write_minimal_config(Path(second_temporary))
            first_text = first_path.read_text(encoding="utf-8").replace(
                "      timeout: 30\n",
                "      timeout: 30\n"
                "      extra_params:\n"
                "        alpha: one\n"
                "        beta: two\n",
            )
            second_text = second_path.read_text(encoding="utf-8").replace(
                "      timeout: 30\n",
                "      timeout: 30\n"
                "      extra_params:\n"
                "        beta: two\n"
                "        alpha: one\n",
            )
            first_path.write_text(first_text, encoding="utf-8")
            second_path.write_text(second_text, encoding="utf-8")

            first = load_project_settings(first_path, environ={})
            second = load_project_settings(second_path, environ={})

            self.assertEqual(
                first.public_identity_sha256,
                second.public_identity_sha256,
            )

    def test_public_identity_binds_non_secret_project_configuration(self) -> None:
        replacements = (
            ("description: Test worker", "description: Changed worker"),
            ("description: Simple workflow", "description: Changed workflow"),
            ("control_layer: {}", "control_layer:\n  mode: advisory"),
        )
        for original, replacement in replacements:
            with self.subTest(replacement=replacement), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                baseline = load_project_settings(config_path, environ={})
                config_path.write_text(
                    config_path.read_text(encoding="utf-8").replace(
                        original,
                        replacement,
                    ),
                    encoding="utf-8",
                )
                changed = load_project_settings(config_path, environ={})

                self.assertNotEqual(
                    baseline.public_identity_sha256,
                    changed.public_identity_sha256,
                )

    def test_unknown_or_future_document_contracts_fail_closed(self) -> None:
        prefixes = (
            "typo_section: {}\n",
            "schema_version: control_plane.project_settings.v2\n",
        )
        for prefix in prefixes:
            with self.subTest(prefix=prefix), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                config_path.write_text(
                    prefix + config_path.read_text(encoding="utf-8"),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(
                    ProjectSettingsError,
                    "document contract|schema version",
                ):
                    load_project_settings(config_path, environ={})

    def test_loader_errors_do_not_retain_secret_input_or_parser_causes(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = Path(temporary) / "settings.yaml"
            config_path.write_text(
                "llm:\n  profiles:\n    primary:\n"
                "      api_key: [LEAKME\n",
                encoding="utf-8",
            )

            with self.assertRaises(ProjectSettingsError) as captured:
                load_project_settings(config_path, environ={})

            rendered = "".join(traceback.format_exception(captured.exception))
            self.assertNotIn("LEAKME", rendered)
            self.assertIsNone(captured.exception.__cause__)
            self.assertIsNone(captured.exception.__context__)

    def test_env_file_errors_do_not_retain_secret_bytes(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = self._write_minimal_config(root)
            env_path = root / "selected.env"
            env_path.write_bytes(b"TEST_API_KEY=MUST-NOT-LEAK\xff")

            with self.assertRaises(ProjectSettingsError) as captured:
                load_project_settings(
                    config_path,
                    env_file=env_path,
                    environ={},
                )

            rendered = "".join(traceback.format_exception(captured.exception))
            self.assertNotIn("MUST-NOT-LEAK", rendered)
            self.assertIsNone(captured.exception.__cause__)
            self.assertIsNone(captured.exception.__context__)

    def test_invocation_model_override_is_explicit_and_validated(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            config_path.write_text(
                config_path.read_text(encoding="utf-8").replace(
                    "      timeout: 30\n",
                    "      timeout: 30\n"
                    "      provider_id: provider-a\n"
                    "      transport: openai-compatible\n"
                    "      retry_policy_ref: retry.standard\n"
                    "      tokenizer_ref: tokenizer.model-a\n"
                    "      pricing_ref: pricing.unknown\n",
                ),
                encoding="utf-8",
            )
            settings = load_project_settings(
                config_path,
                environ={"TEST_API_KEY": "DUMMY-KEY"},
            )

            profile = settings.require_invocation_profile(
                "primary",
                model_override="model-override",
            )

            self.assertEqual(profile.model, "model-override")
            self.assertEqual(profile.provider_id, "provider-a")
            self.assertEqual(profile.transport, "openai-compatible")
            self.assertEqual(profile.retry_policy_ref, "retry.standard")
            self.assertEqual(profile.tokenizer_ref, "tokenizer.model-a")
            self.assertEqual(profile.pricing_ref, "pricing.unknown")
            with self.assertRaisesRegex(ProjectSettingsError, "model override"):
                settings.require_invocation_profile(
                    "primary",
                    model_override=" ",
                )

    def test_real_project_profiles_load_in_golden_order_without_side_effects(self) -> None:
        config_path = Path(__file__).resolve().parents[1] / "ag2_research" / "config.yaml"
        before_environment = dict(os.environ)
        logger = logging.getLogger("autogen.oai.client")
        before_logger_level = logger.level
        before_autogen_modules = {
            name for name in sys.modules if name == "autogen" or name.startswith("autogen.")
        }

        settings = load_project_settings(config_path, environ={})
        inspection = settings.inspect()

        self.assertEqual(
            list(inspection["profiles"]),
            [
                "gpt55",
                "grok43",
                "gemini35flash",
                "deepseekv4",
                "glm51",
                "doubao",
                "kimi_hs",
                "minimax_hs",
                "kimi",
                "mimo",
                "minimax",
            ],
        )
        unresolved = settings.unresolved_document()
        self.assertEqual(
            list(unresolved["agents"]),
            [
                "research_proposer",
                "data_validator",
                "experiment_executor",
                "risk_controller",
                "strategy_synthesizer",
                "pipeline_controller",
                "research_director",
                "system_orchestrator",
                "theory_builder",
                "falsification_officer",
                "constraint_geometry_auditor",
                "source_librarian",
                "alpha_hunter",
                "factor_engineer",
                "data_expansion_researcher",
                "regime_researcher",
                "parameter_researcher",
                "statistician",
                "research_historian",
                "code_reviewer",
            ],
        )
        self.assertEqual(
            list(unresolved["workflows"]),
            [
                "brainstorm",
                "review",
                "proposal_gate",
                "solo",
                "director_only",
                "kbase_discovery",
                "kbase_source_brief",
                "kbase_factor_handoff",
                "kbase_source_first_discovery",
                "kbase_roundtable_discovery",
            ],
        )
        self.assertTrue(
            all(
                profile["credential_status"] == "MISSING"
                for profile in inspection["profiles"].values()
            )
        )
        self.assertEqual(dict(os.environ), before_environment)
        self.assertEqual(logger.level, before_logger_level)
        self.assertEqual(
            {
                name
                for name in sys.modules
                if name == "autogen" or name.startswith("autogen.")
            },
            before_autogen_modules,
        )

    def test_foundations_settings_import_is_side_effect_free(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        script = textwrap.dedent(
            f"""
            import json
            import logging
            import os
            import sys

            before_environment = dict(os.environ)
            logger = logging.getLogger("autogen.oai.client")
            before_logger = (logger.level, len(logger.handlers), logger.propagate)
            sys.path.insert(0, {str(repository_root)!r})
            import research_automation.foundations.settings
            after_logger = (logger.level, len(logger.handlers), logger.propagate)
            loaded_forbidden = sorted(
                name for name in sys.modules
                if name == "autogen"
                or name.startswith("autogen.")
                or name == "ag2_research.orchestrator"
            )
            print(json.dumps({{
                "environment_unchanged": before_environment == dict(os.environ),
                "logger_unchanged": before_logger == after_logger,
                "loaded_forbidden": loaded_forbidden,
            }}))
            """
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
        self.assertEqual(
            json.loads(completed.stdout),
            {
                "environment_unchanged": True,
                "logger_unchanged": True,
                "loaded_forbidden": [],
            },
        )


if __name__ == "__main__":
    unittest.main()
