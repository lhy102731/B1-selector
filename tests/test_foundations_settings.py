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
from unittest.mock import patch

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

    def test_selected_env_file_must_exist_and_parse_without_silent_fallback(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = self._write_minimal_config(root)
            missing = root / "missing.env"
            with self.assertRaisesRegex(ProjectSettingsError, "env file"):
                load_project_settings(
                    config_path,
                    env_file=missing,
                    environ={"TEST_API_KEY": "AMBIENT-KEY"},
                )

            malformed = root / "malformed.env"
            malformed.write_text(
                "TEST_API_KEY=FILE-KEY\nnot a dotenv binding\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ProjectSettingsError, "env file"):
                load_project_settings(config_path, env_file=malformed, environ={})

            malformed.write_text(
                "\ufeffTEST_API_KEY=FILE-KEY\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ProjectSettingsError, "env file"):
                load_project_settings(config_path, env_file=malformed, environ={})

    def test_numeric_profile_references_follow_precedence(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = self._write_minimal_config(root)
            config_path.write_text(
                config_path.read_text(encoding="utf-8").replace(
                    "      temperature: 0.2\n      timeout: 30\n",
                    "      temperature: ${TEST_TEMPERATURE:-0.4}\n"
                    "      timeout: ${TEST_TIMEOUT:-30}\n"
                    "      max_retries: ${TEST_RETRIES:-2}\n"
                    "      max_tokens: ${TEST_MAX_TOKENS:-400}\n",
                ),
                encoding="utf-8",
            )

            settings = load_project_settings(
                config_path,
                environ={"TEST_API_KEY": "DUMMY-KEY"},
                overrides={
                    "TEST_TEMPERATURE": "0.6",
                    "TEST_TIMEOUT": "45",
                    "TEST_RETRIES": "3",
                    "TEST_MAX_TOKENS": "500",
                },
            )
            profile = settings.require_invocation_profile("primary")
            self.assertEqual(profile.temperature, 0.6)
            self.assertEqual(profile.timeout, 45)
            self.assertEqual(profile.max_retries, 3)
            self.assertEqual(profile.max_tokens, 500)

    def test_secret_like_nested_fields_fail_closed(self) -> None:
        variants = (
            (
                "      extra_params:\n"
                '        api_key: "${SECONDARY_KEY}"\n',
                "api_key",
            ),
            (
                "      extra_params:\n"
                '        clientSecret: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                "        headers:\n"
                '          Authorization: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        APISecretKey: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        secretValue: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        authHeader: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        bearer: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        authorizationHeader: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        X_APIKEY: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        privateKeyMaterial: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        credentialBlob: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        clientSecretValue: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        apiKeyMaterial: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                "      extra_params:\n"
                '        proxyAuthorizationHeader: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                'control_layer:\n  service_token: "PLAINTEXT-MUST-NOT-LEAK"\n',
                "secret-like",
            ),
            (
                'control_layer:\n  note: "${TEST_API_KEY}"\n',
                "credential environment",
            ),
            (
                'control_layer:\n  note: "${SECONDARY_SECRET}"\n',
                "secret environment",
            ),
            (
                'control_layer:\n  note: "${UNDECLARED_PUBLIC_VALUE}"\n',
                "public environment reference",
            ),
            (
                'control_layer:\n  note: "${UNDECLARED_PUBLIC_VALUE:-default}"\n',
                "public environment reference",
            ),
            (
                "      extra_params:\n"
                '        note: "${UNDECLARED_PUBLIC_VALUE:-default}"\n',
                "public environment reference",
            ),
        )
        for replacement, message in variants:
            with self.subTest(message=message), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                if replacement.startswith("      "):
                    text = config_path.read_text(encoding="utf-8").replace(
                        "      timeout: 30\n",
                        "      timeout: 30\n" + replacement,
                    )
                else:
                    text = config_path.read_text(encoding="utf-8").replace(
                        "control_layer: {}\n",
                        replacement,
                    )
                config_path.write_text(text, encoding="utf-8")
                with self.assertRaisesRegex(ProjectSettingsError, message) as captured:
                    load_project_settings(
                        config_path,
                        environ={
                            "TEST_API_KEY": "DUMMY-KEY",
                            "SECONDARY_KEY": "SECONDARY-MUST-NOT-LEAK",
                            "SECONDARY_SECRET": "SECRET-MUST-NOT-LEAK",
                            "UNDECLARED_PUBLIC_VALUE": "MUST-NOT-BE-PROJECTED",
                        },
                    )
                self.assertNotIn("SECONDARY-MUST-NOT-LEAK", str(captured.exception))
                self.assertNotIn("PLAINTEXT-MUST-NOT-LEAK", str(captured.exception))

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
                    'control_layer:\n  state_root: "${STATE_ROOT:-default-root}"',
                ),
                encoding="utf-8",
            )

            baseline = load_project_settings(config_path, environ={})
            overridden = load_project_settings(
                config_path,
                environ={},
                overrides={"STATE_ROOT": "override-root"},
            )

            self.assertEqual(
                baseline.public_manifest()["control_layer"]["state_root"],
                "default-root",
            )
            self.assertEqual(
                overridden.public_manifest()["control_layer"]["state_root"],
                "override-root",
            )

        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            with self.assertRaisesRegex(ProjectSettingsError, "unknown override"):
                load_project_settings(
                    config_path,
                    environ={},
                    overrides={"UNREFERENCED_SETTING": "value"},
                )

    def test_non_profile_sections_resolve_references_with_one_precedence_chain(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            text = config_path.read_text(encoding="utf-8")
            text = text.replace(
                "  default: primary\n",
                "  default: primary\n"
                "  usage_targets:\n"
                '    daily_budget: "${DAILY_BUDGET:-100}"\n',
            )
            text = text.replace(
                "    description: Test worker\n",
                '    description: "${AGENT_DESCRIPTION:-Test worker}"\n',
            )
            text = text.replace(
                "control_layer: {}\n",
                'control_layer:\n  state_root: "${STATE_ROOT:-default-root}"\n',
            )
            config_path.write_text(text, encoding="utf-8")

            settings = load_project_settings(
                config_path,
                env_file=None,
                environ={
                    "TEST_API_KEY": "DUMMY-KEY",
                    "DAILY_BUDGET": "file-budget",
                    "AGENT_DESCRIPTION": "environment-description",
                    "STATE_ROOT": "environment-root",
                },
                overrides={
                    "DAILY_BUDGET": "override-budget",
                    "STATE_ROOT": "override-root",
                },
            )
            manifest = settings.public_manifest()
            self.assertEqual(
                manifest["usage_targets"]["daily_budget"],
                "override-budget",
            )
            self.assertEqual(
                manifest["agents"]["worker"]["description"],
                "environment-description",
            )
            self.assertEqual(
                manifest["control_layer"]["state_root"],
                "override-root",
            )
            self.assertEqual(
                settings.unresolved_document()["control_layer"]["state_root"],
                "${STATE_ROOT:-default-root}",
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

    @unittest.skipUnless(os.name == "nt", "Windows environment names are case-insensitive")
    def test_windows_environment_names_remain_case_insensitive_after_copy(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = self._write_minimal_config(Path(temporary))
            config_path.write_text(
                config_path.read_text(encoding="utf-8").replace(
                    "${TEST_API_KEY}",
                    "${MixedCase_API_KEY}",
                ),
                encoding="utf-8",
            )

            settings = load_project_settings(
                config_path,
                environ={"MIXEDCASE_API_KEY": "DUMMY-KEY"},
            )

            self.assertEqual(
                settings.require_invocation_profile("primary")
                .api_key.get_secret_value(),
                "DUMMY-KEY",
            )

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

    def test_legacy_roster_required_display_fields_fail_during_load(self) -> None:
        removals = (
            "    name: Worker\n",
            "    description: Test worker\n",
            "    description: Simple workflow\n",
        )
        for removal in removals:
            with self.subTest(removal=removal), TemporaryDirectory() as temporary:
                config_path = self._write_minimal_config(Path(temporary))
                config_path.write_text(
                    config_path.read_text(encoding="utf-8").replace(removal, "", 1),
                    encoding="utf-8",
                )
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


class ResearchConfigAdapterTests(unittest.TestCase):
    @staticmethod
    def _import_adapter_class():
        logger = logging.getLogger("autogen.oai.client")
        original_level = logger.level
        try:
            with patch("dotenv.load_dotenv"):
                from ag2_research.config import ResearchConfig
        finally:
            logger.setLevel(original_level)
        return ResearchConfig

    def test_adapter_builds_the_legacy_llm_shape_at_invocation(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = ProjectSettingsTests._write_minimal_config(Path(temporary))
            research_config = self._import_adapter_class()(
                config_path,
                environ={"TEST_API_KEY": "DUMMY-KEY"},
            )

            self.assertEqual(
                research_config.get_llm_config(),
                {
                    "config_list": [
                        {
                            "model": "model-a",
                            "api_key": "DUMMY-KEY",
                            "base_url": "https://example.invalid/v1",
                            "max_retries": 6,
                        }
                    ],
                    "temperature": 0.2,
                    "timeout": 30,
                },
            )

    def test_adapter_inspection_works_without_a_credential(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = ProjectSettingsTests._write_minimal_config(Path(temporary))
            research_config = self._import_adapter_class()(
                config_path,
                environ={},
            )

            self.assertEqual(research_config.default_profile, "primary")
            self.assertEqual(research_config.list_profiles(), ["primary"])
            self.assertEqual(
                research_config._raw["llm"]["profiles"]["primary"]["api_key"],
                "${TEST_API_KEY}",
            )
            with self.assertRaisesRegex(
                MissingInvocationSettingError,
                "MISSING_CREDENTIAL",
            ):
                research_config.get_llm_config()

    def test_adapter_runtime_views_use_resolved_non_secret_references(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = ProjectSettingsTests._write_minimal_config(Path(temporary))
            text = config_path.read_text(encoding="utf-8")
            text = text.replace(
                "  default: primary\n",
                '  default: "${DEFAULT_PROFILE:-primary}"\n',
            )
            text = text.replace(
                "    name: Worker\n",
                '    name: "${AGENT_NAME:-Worker}"\n',
            )
            text = text.replace(
                "    description: Test worker\n",
                '    description: "${AGENT_DESCRIPTION:-Test worker}"\n',
            )
            text = text.replace(
                "control_layer: {}\n",
                'control_layer:\n  state_root: "${STATE_ROOT:-default-root}"\n',
            )
            config_path.write_text(text, encoding="utf-8")
            research_config = self._import_adapter_class()(
                config_path,
                environ={
                    "TEST_API_KEY": "DUMMY-KEY",
                    "AGENT_NAME": "Resolved worker",
                    "AGENT_DESCRIPTION": "Resolved description",
                    "STATE_ROOT": "resolved-root",
                },
            )

            self.assertEqual(research_config.default_profile, "primary")
            self.assertEqual(research_config._raw["agents"]["worker"]["name"], "Resolved worker")
            self.assertEqual(
                research_config.get_agent("worker")["description"],
                "Resolved description",
            )
            self.assertEqual(
                research_config._raw["control_layer"]["state_root"],
                "resolved-root",
            )
            self.assertEqual(
                research_config.unresolved_document()["control_layer"]["state_root"],
                "${STATE_ROOT:-default-root}",
            )

    def test_adapter_returns_detached_redacted_configuration_views(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = ProjectSettingsTests._write_minimal_config(Path(temporary))
            research_config = self._import_adapter_class()(
                config_path,
                environ={"TEST_API_KEY": "DUMMY-KEY"},
            )

            raw = research_config._raw
            profiles = research_config.profiles
            agents = research_config.agents
            workflows = research_config.workflows
            agent = research_config.get_agent("worker")
            workflow = research_config.get_workflow("simple")
            raw["agents"]["worker"]["description"] = "changed"
            profiles["primary"]["model"] = "changed"
            agents["worker"]["description"] = "changed"
            workflows["simple"]["description"] = "changed"
            agent["description"] = "changed"
            workflow["description"] = "changed"

            self.assertEqual(
                research_config.get_agent("worker")["description"],
                "Test worker",
            )
            self.assertEqual(
                research_config.get_workflow("simple")["description"],
                "Simple workflow",
            )
            self.assertEqual(research_config.profiles["primary"]["model"], "model-a")
            redacted = json.dumps(
                {
                    "raw": research_config._raw,
                    "profiles": research_config.profiles,
                    "agents": research_config.agents,
                    "workflows": research_config.workflows,
                }
            )
            self.assertNotIn("DUMMY-KEY", redacted)
            self.assertIn("${TEST_API_KEY}", redacted)

    def test_adapter_preserves_routing_aliases_and_optional_llm_fields(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = ProjectSettingsTests._write_minimal_config(Path(temporary))
            config_path.write_text(
                config_path.read_text(encoding="utf-8").replace(
                    "      timeout: 30\n",
                    "      timeout: 30\n"
                    "      max_retries: 4\n"
                    "      max_tokens: 123\n"
                    "      extra_params:\n"
                    "        reasoning:\n"
                    "          effort: high\n",
                ),
                encoding="utf-8",
            )
            research_config = self._import_adapter_class()(
                config_path,
                environ={"TEST_API_KEY": "DUMMY-KEY"},
            )

            overridden = research_config.get_llm_config(model="model-override")
            self.assertEqual(
                overridden,
                {
                    "config_list": [
                        {
                            "model": "model-override",
                            "api_key": "DUMMY-KEY",
                            "base_url": "https://example.invalid/v1",
                            "max_retries": 4,
                            "extra_body": {
                                "reasoning": {"effort": "high"},
                            },
                        }
                    ],
                    "temperature": 0.2,
                    "timeout": 30,
                    "max_tokens": 123,
                },
            )
            overridden["config_list"][0]["extra_body"]["reasoning"]["effort"] = "low"
            self.assertEqual(
                research_config.get_llm_config()["config_list"][0]["extra_body"]
                ["reasoning"]["effort"],
                "high",
            )
            self.assertEqual(
                research_config.llm_config["config_list"][0]["model"],
                "model-a",
            )
            self.assertEqual(
                research_config.list_agents(),
                [
                    {
                        "id": "worker",
                        "name": "Worker",
                        "description": "Test worker",
                    }
                ],
            )
            self.assertEqual(
                research_config.list_workflows(),
                [{"id": "simple", "description": "Simple workflow"}],
            )
            self.assertIsNone(research_config.get_agent("missing"))
            self.assertIsNone(research_config.get_workflow("missing"))
            with self.assertRaises(KeyError):
                research_config.get_llm_config("missing")
            self.assertEqual(
                research_config.get_agent_llm_config("missing")["config_list"][0]
                ["model"],
                "model-a",
            )

    def test_adapter_reload_replaces_state_only_after_complete_validation(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = ProjectSettingsTests._write_minimal_config(Path(temporary))
            research_config = self._import_adapter_class()(
                config_path,
                environ={"TEST_API_KEY": "DUMMY-KEY"},
            )
            config_path.write_text(
                config_path.read_text(encoding="utf-8").replace(
                    "      timeout: 30\n",
                    "      timeout: 30\n      typo_timeout: 31\n",
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ProjectSettingsError):
                research_config.load(
                    environ={"TEST_API_KEY": "REPLACEMENT-KEY"},
                )

            self.assertEqual(research_config.default_profile, "primary")
            self.assertEqual(
                research_config.get_llm_config()["config_list"][0]["api_key"],
                "DUMMY-KEY",
            )

    def test_real_project_adapter_preserves_catalog_order_without_credentials(self) -> None:
        config_path = Path(__file__).resolve().parents[1] / "ag2_research" / "config.yaml"
        research_config = self._import_adapter_class()(config_path, environ={})

        self.assertEqual(
            research_config.list_profiles(),
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
        self.assertEqual(len(research_config.list_agents()), 20)
        self.assertEqual(research_config.list_agents()[0]["id"], "research_proposer")
        self.assertEqual(research_config.list_agents()[-1]["id"], "code_reviewer")
        self.assertEqual(
            [item["id"] for item in research_config.list_workflows()],
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
        with self.assertRaisesRegex(
            MissingInvocationSettingError,
            "MISSING_CREDENTIAL",
        ):
            research_config.get_llm_config("gpt55")

    def test_config_adapter_module_itself_has_no_import_side_effects(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        config_module_path = repository_root / "ag2_research" / "config.py"
        script = textwrap.dedent(
            f"""
            import importlib.util
            import json
            import logging
            import os
            import sys

            before_environment = dict(os.environ)
            logger = logging.getLogger("autogen.oai.client")
            before_logger = (logger.level, len(logger.handlers), logger.propagate)
            sys.path.insert(0, {str(repository_root)!r})
            spec = importlib.util.spec_from_file_location(
                "ag2_config_adapter_purity",
                {str(config_module_path)!r},
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
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
