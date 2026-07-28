from __future__ import annotations

import unittest
from typing import Literal

from jsonschema import Draft202012Validator
from pydantic import ValidationError

from research_automation.foundations.contract_registry import (
    ContractValidationError,
    ContractRegistry,
    StrictContractModel,
    UnknownContractVersionError,
)


class ExampleContract(StrictContractModel):
    schema_version: Literal["research.example.v1"]
    name: str


class TupleContract(StrictContractModel):
    schema_version: Literal["research.tuple.v1"]
    items: tuple[str, ...]


class ExampleContractV2(StrictContractModel):
    schema_version: Literal["research.example.v2"]
    display_name: str


class ContractRegistryTests(unittest.TestCase):
    def test_registered_contract_parses_to_a_frozen_typed_model(self) -> None:
        registry = ContractRegistry(
            version="research.contract_registry.v2",
            contracts={"research.example.v1": ExampleContract},
        )

        parsed = registry.parse_json(
            "research.example.v1",
            b'{"schema_version":"research.example.v1","name":"alpha"}',
        )

        self.assertIsInstance(parsed, ExampleContract)
        self.assertEqual(parsed.name, "alpha")
        with self.assertRaises(ValidationError):
            parsed.name = "changed"

    def test_unknown_expected_version_fails_closed(self) -> None:
        registry = ContractRegistry(
            version="research.contract_registry.v2",
            contracts={"research.example.v1": ExampleContract},
        )

        with self.assertRaises(UnknownContractVersionError):
            registry.parse_json(
                "research.example.v2",
                b'{"schema_version":"research.example.v2","name":"alpha"}',
            )

    def test_payload_cannot_select_a_different_schema_version(self) -> None:
        registry = ContractRegistry(
            version="research.contract_registry.v2",
            contracts={"research.example.v1": ExampleContract},
        )

        with self.assertRaises(ContractValidationError):
            registry.parse_json(
                "research.example.v1",
                b'{"schema_version":"research.example.v2","name":"alpha"}',
            )

    def test_unknown_fields_and_duplicate_keys_fail_closed(self) -> None:
        registry = ContractRegistry(
            version="research.contract_registry.v2",
            contracts={"research.example.v1": ExampleContract},
        )
        invalid_payloads = (
            b'{"schema_version":"research.example.v1","name":"alpha","extra":1}',
            b'{"schema_version":"research.example.v1","name":"alpha","name":"beta"}',
            b'{"schema_version":"research.example.v1","name":1}',
            b'{"schema_version":"research.example.v1","name":NaN}',
        )

        for raw in invalid_payloads:
            with self.subTest(raw=raw):
                with self.assertRaises(ContractValidationError):
                    registry.parse_json("research.example.v1", raw)

    def test_contract_bytes_are_bounded_before_validation(self) -> None:
        registry = ContractRegistry(
            version="research.contract_registry.v2",
            contracts={"research.example.v1": ExampleContract},
            max_json_bytes=32,
        )

        with self.assertRaisesRegex(ContractValidationError, "byte limit"):
            registry.parse_json(
                "research.example.v1",
                b'{"schema_version":"research.example.v1","name":"alpha"}',
            )

    def test_generated_schema_is_deterministic_draft_2020_12(self) -> None:
        registry = ContractRegistry(
            version="research.contract_registry.v2",
            contracts={"research.example.v1": ExampleContract},
        )

        first = registry.json_schema_bytes("research.example.v1")
        second = registry.json_schema_bytes("research.example.v1")
        schema = registry.json_schema("research.example.v1")

        self.assertEqual(first, second)
        self.assertEqual(
            schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema",
        )
        Draft202012Validator.check_schema(schema)

    def test_mapping_input_uses_strict_json_semantics_and_deeply_frozen_sequences(self) -> None:
        registry = ContractRegistry(
            version="research.contract_registry.v2",
            contracts={"research.tuple.v1": TupleContract},
        )

        parsed = registry.parse_mapping(
            "research.tuple.v1",
            {"schema_version": "research.tuple.v1", "items": ["a", "b"]},
        )

        self.assertEqual(parsed.items, ("a", "b"))
        self.assertIsInstance(parsed.items, tuple)

    def test_registry_key_must_match_the_models_literal_schema_version(self) -> None:
        with self.assertRaisesRegex(ValueError, "schema_version"):
            ContractRegistry(
                version="research.contract_registry.v2",
                contracts={"research.example.v2": ExampleContract},
            )

    def test_unregistered_migration_fails_closed(self) -> None:
        registry = ContractRegistry(
            version="research.contract_registry.v2",
            contracts={
                "research.example.v1": ExampleContract,
                "research.example.v2": ExampleContractV2,
            },
        )

        with self.assertRaisesRegex(ValueError, "migration"):
            registry.migrate_exact(
                "research.example.v1",
                "research.example.v2",
                {"schema_version": "research.example.v1", "name": "alpha"},
            )

    def test_registered_migration_is_exact_and_revalidated(self) -> None:
        registry = ContractRegistry(
            version="research.contract_registry.v2",
            contracts={
                "research.example.v1": ExampleContract,
                "research.example.v2": ExampleContractV2,
            },
            migrations={
                ("research.example.v1", "research.example.v2"): lambda source: {
                    "schema_version": "research.example.v2",
                    "display_name": source["name"],
                }
            },
        )

        migrated = registry.migrate_exact(
            "research.example.v1",
            "research.example.v2",
            {"schema_version": "research.example.v1", "name": "alpha"},
        )

        self.assertIsInstance(migrated, ExampleContractV2)
        self.assertEqual(migrated.display_name, "alpha")
        with self.assertRaisesRegex(ValueError, "migration"):
            registry.migrate_exact(
                "research.example.v2",
                "research.example.v1",
                {"schema_version": "research.example.v2", "display_name": "alpha"},
            )


if __name__ == "__main__":
    unittest.main()
