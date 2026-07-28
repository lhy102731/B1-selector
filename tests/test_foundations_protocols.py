from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import unittest
import warnings
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from research_automation.control_plane.contracts import Actor, SideEffect, canonical_json
from research_automation.foundations.artifact_identity import (
    ArtifactIdentity,
    DirectoryManifestEntry,
    artifact_identity_from_bytes,
    build_directory_manifest,
    directory_identity_from_manifest,
)
from research_automation.foundations.protocols import (
    APPROVED_AMENDMENT,
    IDENTICAL,
    IMMATERIAL_ALLOWLISTED,
    MATERIAL_UNAPPROVED,
    DatasetBinding,
    FeatureBoundary,
    FeatureField,
    FoldSelection,
    FoldSpec,
    LabelDefinition,
    ModelThresholdSpec,
    OutputContract,
    MaterialProtocolChangeError,
    ProtocolAmendment,
    ProtocolAmendmentStatement,
    ProtocolApprovalStatement,
    ProtocolApproval,
    ProtocolApprovalError,
    ProtocolDefinition,
    ProtocolMetadata,
    RosterMember,
    RunnerSpec,
    compile_execution_spec,
    diff_protocols,
    protocol_registry,
    protocol_sha256,
    require_protocol_conformant,
)
from research_automation.foundations.contract_registry import (
    ContractValidationError,
    UnknownContractVersionError,
)


def _artifact(
    content: bytes,
    *,
    content_schema: str,
    generation: str,
    kind: str,
    logical_role: str,
    producer: str = "unit-test",
) -> ArtifactIdentity:
    return artifact_identity_from_bytes(
        content,
        content_schema=content_schema,
        producer=producer,
        generation=generation,
        kind=kind,
        logical_role=logical_role,
    )


def _protocol() -> ProtocolDefinition:
    generation_manifest = _artifact(
        b"generation-1-manifest",
        content_schema="research.generation_manifest.v1",
        generation="generation-1",
        kind="manifest",
        logical_role="generation-manifest",
    )
    market_data = _artifact(
        b"market-data-generation-1",
        content_schema="research.market_data.v1",
        generation="generation-1",
        kind="dataset",
        logical_role="dataset-bars",
    )
    hyperparameters = _artifact(
        b"hyperparameters-v1",
        content_schema="research.hyperparameters.v1",
        generation="generation-1",
        kind="model-config",
        logical_role="model-hyperparameters",
    )
    code = _artifact(
        b"runner-source-v1",
        content_schema="research.source_snapshot.v1",
        generation="git-abc123",
        kind="source",
        logical_role="runner-source",
    )
    inputs = tuple(
        sorted(
            (generation_manifest, market_data, hyperparameters),
            key=lambda item: item.artifact_id,
        )
    )
    code_artifacts = (code,)
    datasets = tuple(
        sorted(
            (
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="test-1",
                    role="FOLD_TEST",
                    artifact_id=market_data.artifact_id,
                    window_start="2024-01-01",
                    window_end="2024-12-31",
                ),
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="test-2",
                    role="FOLD_TEST",
                    artifact_id=market_data.artifact_id,
                    window_start="2025-01-01",
                    window_end="2025-12-31",
                ),
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="train-1",
                    role="TRAIN",
                    artifact_id=market_data.artifact_id,
                    window_start="2020-01-01",
                    window_end="2022-12-31",
                ),
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="train-2",
                    role="TRAIN",
                    artifact_id=market_data.artifact_id,
                    window_start="2021-01-01",
                    window_end="2023-12-31",
                ),
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="validation-1",
                    role="VALIDATION",
                    artifact_id=market_data.artifact_id,
                    window_start="2023-01-01",
                    window_end="2023-12-31",
                ),
                DatasetBinding(
                    schema_version="research.dataset_binding.v1",
                    dataset_id="validation-2",
                    role="VALIDATION",
                    artifact_id=market_data.artifact_id,
                    window_start="2024-01-01",
                    window_end="2024-12-31",
                ),
            ),
            key=lambda item: item.dataset_id,
        )
    )
    return ProtocolDefinition(
        schema_version="research.protocol_definition.v1",
        protocol_id="brick-forward-v1",
        metadata=ProtocolMetadata(
            schema_version="research.protocol_metadata.v1",
            display_name="Brick forward validation",
            notes="fixture",
        ),
        generation_id="generation-1",
        generation_manifest_artifact_id=generation_manifest.artifact_id,
        universe_id="a-share-point-in-time-v1",
        calendar_id="sse-szse-trading-v1",
        adjustment_scheme_id="hfq-v1",
        validation_design="ROLLING_FORWARD",
        fold_window_policy_id="train3y-validate1y-test1y-v1",
        label=LabelDefinition(
            schema_version="research.label_definition.v1",
            label_id="return-after-entry-v1",
            entry_rule_id="signal-close-next-open",
            exit_rule_id="fixed-horizon-or-stop",
            horizon_trading_days=5,
        ),
        datasets=datasets,
        folds=(
            FoldSpec(
                schema_version="research.fold_spec.v1",
                fold_id="fold-1",
                train_dataset_id="train-1",
                validation_dataset_id="validation-1",
                test_dataset_id="test-1",
                purge_trading_days=5,
                embargo_trading_days=2,
            ),
            FoldSpec(
                schema_version="research.fold_spec.v1",
                fold_id="fold-2",
                train_dataset_id="train-2",
                validation_dataset_id="validation-2",
                test_dataset_id="test-2",
                purge_trading_days=5,
                embargo_trading_days=2,
            ),
        ),
        feature_boundary=FeatureBoundary(
            schema_version="research.feature_boundary.v1",
            boundary_id="brick-v2-0925-v1",
            feature_fields=(
                FeatureField(
                    schema_version="research.feature_field.v1",
                    name="entry_open_to_ma5_pct",
                    availability="ENTRY_DATE_OPEN",
                    reference_fields=("signal_day_ma5",),
                ),
                FeatureField(
                    schema_version="research.feature_field.v1",
                    name="entry_open_to_yellow_pct",
                    availability="ENTRY_DATE_OPEN",
                    reference_fields=("signal_day_yellow",),
                ),
                FeatureField(
                    schema_version="research.feature_field.v1",
                    name="overnight_gap_pct",
                    availability="ENTRY_DATE_OPEN",
                    reference_fields=("signal_day_close",),
                ),
                FeatureField(
                    schema_version="research.feature_field.v1",
                    name="white_line",
                    availability="SIGNAL_DAY_CLOSE",
                    reference_fields=("signal_day_close",),
                ),
            ),
            forbidden_feature_names=(
                "entry_date_close",
                "entry_date_high",
                "entry_date_low",
                "exit_date",
                "exit_price",
                "hold_days",
                "return_pct",
                "t1_close",
            ),
        ),
        code_artifacts=code_artifacts,
        input_artifacts=inputs,
        runner=RunnerSpec(
            schema_version="research.runner_spec.v1",
            runner_id="brick-v2-research",
            entrypoint="research.brick.runner:main",
            code_artifact_ids=(code.artifact_id,),
            argument_schema_sha256="a" * 64,
            compute_backend="CPU",
            backend_version="python-3.13",
        ),
        model=ModelThresholdSpec(
            schema_version="research.model_threshold_spec.v1",
            model_mode="TRAIN_NEW",
            model_family="ranker",
            model_artifact_id=None,
            hyperparameter_artifact_id=hyperparameters.artifact_id,
            selection_by_fold=(
                FoldSelection(
                    schema_version="research.fold_selection.v1",
                    fold_id="fold-1",
                    training_dataset_id="train-1",
                    threshold_source="VALIDATION_SELECTED",
                    threshold_dataset_ids=("validation-1",),
                    threshold_value=0.5,
                ),
                FoldSelection(
                    schema_version="research.fold_selection.v1",
                    fold_id="fold-2",
                    training_dataset_id="train-2",
                    threshold_source="VALIDATION_SELECTED",
                    threshold_dataset_ids=("validation-2",),
                    threshold_value=0.5,
                ),
            ),
            promotion_gate_id="strict-forward-v1",
        ),
        roster=(
            RosterMember(
                schema_version="research.roster_member.v1",
                role="factor_engineer",
                provider_profile_id="offline-local",
                model_id="deterministic-reviewer",
                public_identity_sha256="b" * 64,
                redacted=True,
            ),
        ),
        output_contracts=(
            OutputContract(
                schema_version="research.output_contract.v1",
                logical_role="fold-report",
                output_schema_id="research.fold_report.v1",
                destination_class="STAGING_ONLY",
            ),
        ),
        allowed_side_effects=(
            SideEffect.READ,
            SideEffect.RUN_RESEARCH,
            SideEffect.START_SUBPROCESS,
            SideEffect.WRITE_STAGING,
        ),
    )


def _approval(protocol: ProtocolDefinition) -> ProtocolApproval:
    approver = Actor(
        actor_id="independent-reviewer",
        actor_type="human",
        invocation_id="review-1",
    )
    statement = ProtocolApprovalStatement(
        schema_version="research.protocol_approval_statement.v1",
        approved_protocol_sha256=protocol_sha256(protocol),
        decision="APPROVED",
        approver=approver,
    )
    evidence = _artifact(
        canonical_json(statement.model_dump(mode="json")).encode("utf-8"),
        content_schema="research.protocol_approval_statement.v1",
        generation="approval-1",
        kind="review",
        logical_role="protocol-approval",
        producer=approver.actor_id,
    )
    return ProtocolApproval(
        schema_version="research.protocol_approval.v1",
        statement=statement,
        approval_evidence=evidence,
        evidence_trust="UNVERIFIED_EXTERNAL_STATEMENT",
    )


class ProtocolCompilerTests(unittest.TestCase):
    def test_valid_protocol_compiles_identically_with_a_stable_id(self) -> None:
        protocol = _protocol()
        first = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        second = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )

        self.assertEqual(first.conformance, IDENTICAL)
        self.assertEqual(first, second)
        self.assertEqual(first.execution_spec_id, second.execution_spec_id)
        self.assertRegex(first.execution_spec_id, r"^[0-9a-f]{64}$")
        self.assertNotIn("independent-protocol-approval", first.model_dump_json())

    def test_registry_is_strict_and_emits_a_stable_draft_schema(self) -> None:
        protocol = _protocol()
        registry = protocol_registry()
        first_schema = registry.json_schema_bytes("research.protocol_definition.v1")
        second_schema = registry.json_schema_bytes("research.protocol_definition.v1")
        self.assertEqual(first_schema, second_schema)
        parsed = registry.parse_mapping(
            "research.protocol_definition.v1",
            protocol.model_dump(mode="json"),
        )
        self.assertEqual(parsed, protocol)
        spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        parsed_spec = registry.parse_mapping(
            "research.execution_spec.v1",
            spec.model_dump(mode="json"),
        )
        self.assertEqual(parsed_spec, spec)
        self.assertEqual(parsed_spec.execution_spec_id, spec.execution_spec_id)

        extra = protocol.model_dump(mode="json")
        extra["unexpected"] = True
        with self.assertRaises(ContractValidationError):
            registry.parse_mapping("research.protocol_definition.v1", extra)
        with self.assertRaises(UnknownContractVersionError):
            registry.parse_mapping("research.protocol_definition.v2", extra)

        duplicate = json.dumps(
            protocol.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
        ).replace(
            '"protocol_id":"brick-forward-v1"',
            '"protocol_id":"brick-forward-v1","protocol_id":"forged"',
            1,
        ).encode("utf-8")
        with self.assertRaises(ContractValidationError):
            registry.parse_json("research.protocol_definition.v1", duplicate)

    def test_protocol_is_deeply_frozen_and_hash_ignores_mapping_order(self) -> None:
        protocol = _protocol()
        with self.assertRaises(ValidationError):
            protocol.protocol_id = "changed"  # type: ignore[misc]
        detached = protocol.model_dump(mode="python")
        detached["metadata"]["display_name"] = "detached"
        self.assertEqual(protocol.metadata.display_name, "Brick forward validation")

        payload = protocol.model_dump(mode="python")
        reordered = {key: payload[key] for key in reversed(tuple(payload))}
        self.assertEqual(protocol_sha256(protocol), protocol_sha256(protocol))
        self.assertEqual(
            protocol_sha256(
                ProtocolDefinition.model_validate(reordered, strict=True)
            ),
            protocol_sha256(protocol),
        )

    def test_nonfinite_threshold_and_secret_metadata_fail_closed(self) -> None:
        protocol = _protocol()
        for nonfinite in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(nonfinite=nonfinite):
                payload = protocol.model_dump(mode="json")
                payload["model"]["selection_by_fold"][0]["threshold_value"] = nonfinite
                with self.assertRaises(ContractValidationError):
                    protocol_registry().parse_mapping(
                        "research.protocol_definition.v1",
                        payload,
                    )
        with self.assertRaises(ValidationError):
            ProtocolMetadata(
                schema_version="research.protocol_metadata.v1",
                display_name="bad",
                notes="api_key=do-not-store",
            )
        for secret_note in (
            "api_key: SECRET",
            "token=SECRET",
            "private_key = SECRET",
            "password : SECRET",
            "secret : SECRET",
        ):
            with self.subTest(secret_note=secret_note):
                with self.assertRaises(ValidationError):
                    ProtocolMetadata(
                        schema_version="research.protocol_metadata.v1",
                        display_name="bad",
                        notes=secret_note,
                    )
        with self.assertRaises(ValidationError):
            ProtocolMetadata.model_validate(
                {
                    "schema_version": "research.protocol_metadata.v1",
                    "display_name": "bad",
                    "notes": "safe",
                    "api_key": "secret",
                },
                strict=True,
            )

        secret_roster = protocol.model_copy(
            update={
                "roster": (
                    protocol.roster[0].model_copy(
                        update={"provider_profile_id": "${API_KEY}"}
                    ),
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "credential material"):
            compile_execution_spec(
                secret_roster,
                approved_protocol=protocol,
                approval=_approval(protocol),
                amendment=None,
            )

    def test_each_fold_freezes_its_own_threshold_selection(self) -> None:
        protocol = _protocol()
        selections = (
            FoldSelection(
                schema_version="research.fold_selection.v1",
                fold_id="fold-1",
                training_dataset_id="train-1",
                threshold_source="VALIDATION_SELECTED",
                threshold_dataset_ids=("validation-1",),
                threshold_value=0.41,
            ),
            FoldSelection(
                schema_version="research.fold_selection.v1",
                fold_id="fold-2",
                training_dataset_id="train-2",
                threshold_source="VALIDATION_SELECTED",
                threshold_dataset_ids=("validation-2",),
                threshold_value=0.57,
            ),
        )
        variant = protocol.model_copy(
            update={
                "model": protocol.model.model_copy(
                    update={"selection_by_fold": selections}
                )
            }
        )

        parsed = protocol_registry().parse_mapping(
            "research.protocol_definition.v1",
            variant.model_dump(mode="json"),
        )

        self.assertEqual(
            tuple(item.threshold_value for item in parsed.model.selection_by_fold),
            (0.41, 0.57),
        )
        self.assertNotEqual(protocol_sha256(parsed), protocol_sha256(protocol))
        spec = compile_execution_spec(
            parsed,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        self.assertEqual(spec.conformance, MATERIAL_UNAPPROVED)
        self.assertNotIn("threshold_value", parsed.model.model_dump(exclude={"selection_by_fold"}))

        with self.assertRaisesRegex(ValidationError, "must not use selection datasets"):
            FoldSelection(
                schema_version="research.fold_selection.v1",
                fold_id="fold-1",
                training_dataset_id="train-1",
                threshold_source="PREREGISTERED",
                threshold_dataset_ids=("validation-1",),
                threshold_value=0.5,
            )
        with self.assertRaisesRegex(ValidationError, "requires a validation dataset"):
            FoldSelection(
                schema_version="research.fold_selection.v1",
                fold_id="fold-1",
                training_dataset_id="train-1",
                threshold_source="VALIDATION_SELECTED",
                threshold_dataset_ids=(),
                threshold_value=0.5,
            )

        preregistered = protocol.model_copy(
            update={
                "model": protocol.model.model_copy(
                    update={
                        "selection_by_fold": tuple(
                            selection.model_copy(
                                update={
                                    "threshold_source": "PREREGISTERED",
                                    "threshold_dataset_ids": (),
                                }
                            )
                            for selection in protocol.model.selection_by_fold
                        )
                    }
                )
            }
        )
        preregistered_spec = compile_execution_spec(
            preregistered,
            approved_protocol=preregistered,
            approval=_approval(preregistered),
            amendment=None,
        )
        self.assertEqual(preregistered_spec.conformance, IDENTICAL)

    def test_revalidation_errors_do_not_disclose_untrusted_values(self) -> None:
        protocol = _protocol()
        sentinel = "SENSITIVE-SENTINEL-DO-NOT-LOG"
        forged_metadata = ProtocolMetadata.model_construct(
            schema_version="research.protocol_metadata.v1",
            display_name="bad",
            notes=f"api_key={sentinel}",
        )
        forged_protocol = protocol.model_copy(update={"metadata": forged_metadata})

        with self.assertRaises(ValueError) as captured:
            protocol_sha256(forged_protocol)

        self.assertNotIn(sentinel, str(captured.exception))
        self.assertIsNone(captured.exception.__cause__)

        valid_approval = _approval(protocol)
        forged_statement = ProtocolApprovalStatement.model_construct(
            schema_version="research.protocol_approval_statement.v1",
            approved_protocol_sha256=protocol_sha256(protocol),
            decision="APPROVED",
            approver="APPROVAL-SENTINEL",
        )
        forged_approval = valid_approval.model_copy(
            update={"statement": forged_statement}
        )
        with warnings.catch_warnings(record=True) as observed:
            warnings.simplefilter("always")
            with self.assertRaises(ProtocolApprovalError) as approval_error:
                compile_execution_spec(
                    protocol,
                    approved_protocol=protocol,
                    approval=forged_approval,
                    amendment=None,
                )
        self.assertEqual(observed, [])
        self.assertNotIn("APPROVAL-SENTINEL", str(approval_error.exception))

    def test_conformance_requires_external_approval_for_each_material_change(self) -> None:
        approved = _protocol()
        approval = _approval(approved)
        metadata_variant = approved.model_copy(
            update={
                "metadata": approved.metadata.model_copy(
                    update={"notes": "metadata-only amendment"}
                )
            }
        )
        metadata_spec = compile_execution_spec(
            metadata_variant,
            approved_protocol=approved,
            approval=approval,
            amendment=None,
        )
        self.assertEqual(metadata_spec.conformance, IMMATERIAL_ALLOWLISTED)
        require_protocol_conformant(metadata_spec)

        material_variant = approved.model_copy(
            update={
                "label": approved.label.model_copy(
                    update={"horizon_trading_days": 6}
                )
            }
        )
        material_spec = compile_execution_spec(
            material_variant,
            approved_protocol=approved,
            approval=approval,
            amendment=None,
        )
        self.assertEqual(material_spec.conformance, MATERIAL_UNAPPROVED)
        with self.assertRaises(MaterialProtocolChangeError):
            require_protocol_conformant(material_spec)
        bypassed_guard = material_spec.model_copy(update={"conformance": IDENTICAL})
        with self.assertRaises(ValueError):
            require_protocol_conformant(bypassed_guard)

        differences = diff_protocols(approved, material_variant)
        material_paths = tuple(
            item.path for item in differences if item.classification == "MATERIAL"
        )
        amendment_approver = Actor(
            actor_id="amendment-reviewer",
            actor_type="human",
            invocation_id="review-2",
        )
        amendment_statement = ProtocolAmendmentStatement(
            schema_version="research.protocol_amendment_statement.v1",
            base_protocol_sha256=protocol_sha256(approved),
            executed_protocol_sha256=protocol_sha256(material_variant),
            changed_paths=material_paths,
            decision="APPROVED",
            approver=amendment_approver,
        )
        amendment_evidence = _artifact(
            canonical_json(amendment_statement.model_dump(mode="json")).encode("utf-8"),
            content_schema="research.protocol_amendment_statement.v1",
            generation="approval-2",
            kind="review",
            logical_role="protocol-amendment",
            producer=amendment_approver.actor_id,
        )
        amendment = ProtocolAmendment(
            schema_version="research.protocol_amendment.v1",
            statement=amendment_statement,
            amendment_evidence=amendment_evidence,
            evidence_trust="UNVERIFIED_EXTERNAL_STATEMENT",
        )
        amended_spec = compile_execution_spec(
            material_variant,
            approved_protocol=approved,
            approval=approval,
            amendment=amendment,
        )
        self.assertEqual(amended_spec.conformance, APPROVED_AMENDMENT)
        require_protocol_conformant(amended_spec)
        self.assertTrue(amended_spec.protocol_conformant)

        tampered_statement = amendment_statement.model_copy(
            update={"executed_protocol_sha256": "f" * 64}
        )
        with self.assertRaisesRegex(ValidationError, "canonical statement bytes"):
            ProtocolAmendment(
                schema_version="research.protocol_amendment.v1",
                statement=tampered_statement,
                amendment_evidence=amendment_evidence,
                evidence_trust="UNVERIFIED_EXTERNAL_STATEMENT",
            )

        import research_automation.foundations.protocols as protocols_module

        self.assertNotIn("execution_eligible", protocols_module.__all__)
        self.assertNotIn("require_execution_eligible", protocols_module.__all__)

    def test_compiler_cannot_self_approve_or_forge_approval_binding(self) -> None:
        protocol = _protocol()
        with self.assertRaises(ProtocolApprovalError):
            compile_execution_spec(
                protocol,
                approved_protocol=protocol,
                approval=None,
                amendment=None,
            )
        with self.assertRaises(ValidationError):
            ProtocolApprovalStatement(
                schema_version="research.protocol_approval_statement.v1",
                approved_protocol_sha256=protocol_sha256(protocol),
                decision="APPROVED",
                approver=Actor(
                    actor_id="research.protocol_compiler.v2",
                    actor_type="automation",
                    invocation_id="review-compiler",
                ),
            )

        approval = _approval(protocol)
        forged = approval.model_copy(
            update={
                "statement": approval.statement.model_copy(
                    update={"approved_protocol_sha256": "c" * 64}
                )
            }
        )
        with self.assertRaises(ProtocolApprovalError):
            compile_execution_spec(
                protocol,
                approved_protocol=protocol,
                approval=forged,
                amendment=None,
            )

        compiler_statement = ProtocolApprovalStatement.model_construct(
            schema_version="research.protocol_approval_statement.v1",
            approved_protocol_sha256=protocol_sha256(protocol),
            decision="APPROVED",
            approver=Actor(
                actor_id="research.protocol_compiler.v2",
                actor_type="automation",
                invocation_id="review-bypassed",
            ),
        )
        compiler_evidence = artifact_identity_from_bytes(
            canonical_json(compiler_statement.model_dump(mode="json")).encode("utf-8"),
            content_schema="research.protocol_approval_statement.v1",
            producer="research.protocol_compiler.v2",
            generation="approval-compiler",
            kind="review",
            logical_role="protocol-approval",
        )
        bypassed = ProtocolApproval.model_construct(
            schema_version="research.protocol_approval.v1",
            statement=compiler_statement,
            approval_evidence=compiler_evidence,
            evidence_trust="UNVERIFIED_EXTERNAL_STATEMENT",
        )
        with self.assertRaisesRegex(ProtocolApprovalError, "revalidation"):
            compile_execution_spec(
                protocol,
                approved_protocol=protocol,
                approval=bypassed,
                amendment=None,
            )

        wrong_kind = _artifact(
            canonical_json(approval.statement.model_dump(mode="json")).encode("utf-8"),
            content_schema="research.unrelated.v1",
            generation="approval-wrong",
            kind="dataset",
            logical_role="protocol-approval",
            producer=approval.approver.actor_id,
        )
        with self.assertRaises(ValidationError):
            ProtocolApproval(
                schema_version="research.protocol_approval.v1",
                statement=approval.statement,
                approval_evidence=wrong_kind,
                evidence_trust="UNVERIFIED_EXTERNAL_STATEMENT",
            )

    def test_approval_binds_the_canonical_statement_bytes(self) -> None:
        protocol = _protocol()
        approver = Actor(
            actor_id="independent-reviewer",
            actor_type="human",
            invocation_id="review-statement-1",
        )
        statement = ProtocolApprovalStatement(
            schema_version="research.protocol_approval_statement.v1",
            approved_protocol_sha256=protocol_sha256(protocol),
            decision="APPROVED",
            approver=approver,
        )
        statement_bytes = canonical_json(statement.model_dump(mode="json")).encode(
            "utf-8"
        )
        evidence = artifact_identity_from_bytes(
            statement_bytes,
            content_schema="research.protocol_approval_statement.v1",
            producer=approver.actor_id,
            generation="approval-1",
            kind="review",
            logical_role="protocol-approval",
        )

        approval = ProtocolApproval(
            schema_version="research.protocol_approval.v1",
            statement=statement,
            approval_evidence=evidence,
            evidence_trust="UNVERIFIED_EXTERNAL_STATEMENT",
        )
        spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=approval,
            amendment=None,
        )
        self.assertEqual(spec.conformance, IDENTICAL)

        arbitrary_evidence = artifact_identity_from_bytes(
            b"not-the-canonical-statement",
            content_schema="research.protocol_approval_statement.v1",
            producer=approver.actor_id,
            generation="approval-1",
            kind="review",
            logical_role="protocol-approval",
        )
        with self.assertRaisesRegex(ValidationError, "canonical statement bytes"):
            ProtocolApproval(
                schema_version="research.protocol_approval.v1",
                statement=statement,
                approval_evidence=arbitrary_evidence,
                evidence_trust="UNVERIFIED_EXTERNAL_STATEMENT",
            )

    def test_fold_roles_windows_and_threshold_selection_fail_closed(self) -> None:
        approved = _protocol()
        approval = _approval(approved)
        bad_role = approved.model_copy(
            update={
                "folds": (
                    approved.folds[0].model_copy(
                        update={"validation_dataset_id": "test-1"}
                    ),
                    approved.folds[1],
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "TRAIN, VALIDATION"):
            compile_execution_spec(
                bad_role,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

        overlapping = approved.model_copy(
            update={
                "datasets": tuple(
                    item.model_copy(
                        update={
                            "window_start": "2024-06-01",
                            "window_end": "2025-12-31",
                        }
                    )
                    if item.dataset_id == "test-2"
                    else item
                    for item in approved.datasets
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "chronological|overlap"):
            compile_execution_spec(
                overlapping,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

        test_selected = approved.model_copy(
            update={
                "model": approved.model.model_copy(
                    update={
                        "selection_by_fold": (
                            approved.model.selection_by_fold[0].model_copy(
                                update={"threshold_dataset_ids": ("test-1",)}
                            ),
                            approved.model.selection_by_fold[1],
                        )
                    }
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "fold-test"):
            compile_execution_spec(
                test_selected,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

        cross_fold_selected = approved.model_copy(
            update={
                "model": approved.model.model_copy(
                    update={
                        "selection_by_fold": (
                            approved.model.selection_by_fold[0].model_copy(
                                update={"threshold_dataset_ids": ("validation-2",)}
                            ),
                            approved.model.selection_by_fold[1],
                        )
                    }
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "another fold"):
            compile_execution_spec(
                cross_fold_selected,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

        stalled = approved.model_copy(
            update={
                "folds": (
                    approved.folds[0],
                    approved.folds[1].model_copy(
                        update={"train_dataset_id": "train-1"}
                    ),
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "move forward"):
            compile_execution_spec(
                stalled,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

    def test_feature_boundary_generation_and_side_effects_fail_closed(self) -> None:
        approved = _protocol()
        approval = _approval(approved)
        forbidden_field = FeatureField(
            schema_version="research.feature_field.v1",
            name="return_pct",
            availability="SIGNAL_DAY_CLOSE",
            reference_fields=("signal_day_close",),
        )
        bad_boundary = approved.feature_boundary.model_copy(
            update={
                "feature_fields": tuple(
                    sorted(
                        (*approved.feature_boundary.feature_fields, forbidden_field),
                        key=lambda item: item.name,
                    )
                )
            }
        )
        bad_feature_protocol = approved.model_copy(
            update={"feature_boundary": bad_boundary}
        )
        with self.assertRaisesRegex(ValueError, "future-data boundary"):
            compile_execution_spec(
                bad_feature_protocol,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

        bad_reference_boundary = approved.feature_boundary.model_copy(
            update={
                "feature_fields": tuple(
                    item.model_copy(update={"reference_fields": ("entry_date_close",)})
                    if item.name == "white_line"
                    else item
                    for item in approved.feature_boundary.feature_fields
                )
            }
        )
        bad_reference_protocol = approved.model_copy(
            update={"feature_boundary": bad_reference_boundary}
        )
        with self.assertRaisesRegex(ValueError, "future-data reference"):
            compile_execution_spec(
                bad_reference_protocol,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

        for forbidden_reference in (
            "signal_day_exit_price",
            "signal_day_return_pct",
            "signal_day_hold_days",
            "signal_day_t1_close",
            "signal_day_entry_date_close",
        ):
            with self.subTest(forbidden_reference=forbidden_reference):
                reference_boundary = approved.feature_boundary.model_copy(
                    update={
                        "feature_fields": tuple(
                            item.model_copy(
                                update={"reference_fields": (forbidden_reference,)}
                            )
                            if item.name == "white_line"
                            else item
                            for item in approved.feature_boundary.feature_fields
                        )
                    }
                )
                with self.assertRaisesRegex(ValueError, "future-data reference"):
                    compile_execution_spec(
                        approved.model_copy(
                            update={"feature_boundary": reference_boundary}
                        ),
                        approved_protocol=approved,
                        approval=approval,
                        amendment=None,
                    )

        bad_generation = approved.model_copy(update={"generation_id": "generation-2"})
        with self.assertRaisesRegex(ValueError, "generation"):
            compile_execution_spec(
                bad_generation,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

        bad_effects = approved.model_copy(
            update={"allowed_side_effects": (SideEffect.READ, SideEffect.WRITE_KBASE)}
        )
        with self.assertRaisesRegex(ValueError, "unsafe side effect"):
            compile_execution_spec(
                bad_effects,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

        bad_entrypoint = approved.model_copy(
            update={
                "runner": approved.runner.model_copy(
                    update={"entrypoint": "../../runner.py"}
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "module:function"):
            compile_execution_spec(
                bad_entrypoint,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

        with self.assertRaises(ValidationError):
            FeatureField(
                schema_version="research.feature_field.v1",
                name="Return-Pct",
                availability="SIGNAL_DAY_CLOSE",
                reference_fields=("signal_day_close",),
            )

        for forbidden_alias in (
            "entry_date_open",
            "entry_open",
            "entry_date_open_price",
            "entry_date_close_adj",
            "entry_date_high_pct",
            "entry_date_low_ratio",
            "t1_close_adjusted",
        ):
            with self.subTest(forbidden_alias=forbidden_alias):
                alias_field = FeatureField(
                    schema_version="research.feature_field.v1",
                    name=forbidden_alias,
                    availability="SIGNAL_DAY_CLOSE",
                    reference_fields=("signal_day_close",),
                )
                alias_boundary = approved.feature_boundary.model_copy(
                    update={
                        "feature_fields": tuple(
                            sorted(
                                tuple(
                                    item
                                    for item in approved.feature_boundary.feature_fields
                                    if item.name != "white_line"
                                )
                                + (alias_field,),
                                key=lambda item: item.name,
                            )
                        )
                    }
                )
                with self.assertRaisesRegex(ValueError, "future-data|raw entry-open"):
                    compile_execution_spec(
                        approved.model_copy(update={"feature_boundary": alias_boundary}),
                        approved_protocol=approved,
                        approval=approval,
                        amendment=None,
                    )

    def test_code_and_dataset_artifact_semantics_cannot_be_swapped(self) -> None:
        approved = _protocol()
        approval = _approval(approved)
        wrong_code = _artifact(
            b"dataset-disguised-as-code",
            content_schema="research.market_data.v1",
            generation="generation-1",
            kind="dataset",
            logical_role="dataset-bars",
        )
        bad_code = approved.model_copy(
            update={
                "code_artifacts": (wrong_code,),
                "runner": approved.runner.model_copy(
                    update={"code_artifact_ids": (wrong_code.artifact_id,)}
                ),
            }
        )
        with self.assertRaisesRegex(ValueError, "code artifact"):
            compile_execution_spec(
                bad_code,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

        market_data = next(
            item for item in approved.input_artifacts if item.logical_role == "dataset-bars"
        )
        wrong_dataset = _artifact(
            b"source-disguised-as-data",
            content_schema="research.source_snapshot.v1",
            generation="generation-1",
            kind="source",
            logical_role="runner-source",
        )
        bad_dataset = approved.model_copy(
            update={
                "input_artifacts": tuple(
                    sorted(
                        (
                            wrong_dataset
                            if item.artifact_id == market_data.artifact_id
                            else item
                            for item in approved.input_artifacts
                        ),
                        key=lambda item: item.artifact_id,
                    )
                ),
                "datasets": tuple(
                    item.model_copy(update={"artifact_id": wrong_dataset.artifact_id})
                    if item.artifact_id == market_data.artifact_id
                    else item
                    for item in approved.datasets
                ),
            }
        )
        with self.assertRaisesRegex(ValueError, "dataset artifact"):
            compile_execution_spec(
                bad_dataset,
                approved_protocol=approved,
                approval=approval,
                amendment=None,
            )

    def test_runner_code_accepts_an_ordered_directory_manifest_identity(self) -> None:
        protocol = _protocol()
        directory_manifest = build_directory_manifest(
            (
                DirectoryManifestEntry(
                    path="research/runner.py",
                    content_sha256="1" * 64,
                    byte_length=17,
                ),
                DirectoryManifestEntry(
                    path="research/support.py",
                    content_sha256="2" * 64,
                    byte_length=23,
                ),
            )
        )
        code_directory = directory_identity_from_manifest(
            directory_manifest,
            producer="unit-test",
            generation="git-abc123",
            logical_role="runner-source",
        )
        variant = protocol.model_copy(
            update={
                "code_artifacts": (code_directory,),
                "runner": protocol.runner.model_copy(
                    update={"code_artifact_ids": (code_directory.artifact_id,)}
                ),
            }
        )

        compiled = compile_execution_spec(
            variant,
            approved_protocol=variant,
            approval=_approval(variant),
            amendment=None,
        )

        self.assertEqual(compiled.conformance, IDENTICAL)

    def test_missing_semantic_fields_and_frozen_model_provenance_are_rejected(self) -> None:
        protocol = _protocol()
        payload = protocol.model_dump(mode="json")
        del payload["label"]["exit_rule_id"]
        with self.assertRaises(ContractValidationError):
            protocol_registry().parse_mapping("research.protocol_definition.v1", payload)

        forged = ProtocolDefinition.model_construct(
            **{
                **protocol.model_dump(mode="python"),
                "generation_id": "generation-forged",
            }
        )
        with self.assertRaisesRegex(ValueError, "revalidation"):
            compile_execution_spec(
                forged,
                approved_protocol=protocol,
                approval=_approval(protocol),
                amendment=None,
            )

        valid_spec = compile_execution_spec(
            protocol,
            approved_protocol=protocol,
            approval=_approval(protocol),
            amendment=None,
        )
        forged_spec = valid_spec.model_copy(
            update={"approved_protocol_sha256": "d" * 64}
        )
        with self.assertRaises(ContractValidationError):
            protocol_registry().parse_mapping(
                "research.execution_spec.v1",
                forged_spec.model_dump(mode="json"),
            )
        forged_self_approval = valid_spec.model_dump(mode="json")
        forged_self_approval.update(
            {
                "approved_protocol": None,
                "approved_protocol_sha256": valid_spec.executed_protocol_sha256,
                "approval": None,
                "amendment": None,
                "approval_evidence_artifact_id": "e" * 64,
                "conformance": IDENTICAL,
            }
        )
        with self.assertRaises(ContractValidationError):
            protocol_registry().parse_mapping(
                "research.execution_spec.v1",
                forged_self_approval,
            )

    def test_non_metadata_dimensions_are_material_by_default(self) -> None:
        approved = _protocol()
        approval = _approval(approved)
        variants = (
            approved.model_copy(update={"protocol_id": "different-id"}),
            approved.model_copy(update={"universe_id": "different-universe"}),
            approved.model_copy(
                update={
                    "folds": (
                        approved.folds[0].model_copy(
                            update={"purge_trading_days": 6}
                        ),
                        approved.folds[1],
                    )
                }
            ),
            approved.model_copy(
                update={
                    "runner": approved.runner.model_copy(
                        update={"compute_backend": "CUDA"}
                    )
                }
            ),
            approved.model_copy(
                update={
                    "feature_boundary": approved.feature_boundary.model_copy(
                        update={"boundary_id": "different-boundary"}
                    )
                }
            ),
            approved.model_copy(
                update={
                    "model": approved.model.model_copy(
                        update={"promotion_gate_id": "different-gate"}
                    )
                }
            ),
            approved.model_copy(
                update={
                    "model": approved.model.model_copy(
                        update={
                            "selection_by_fold": (
                                approved.model.selection_by_fold[0].model_copy(
                                    update={"threshold_value": 0.6}
                                ),
                                approved.model.selection_by_fold[1],
                            )
                        }
                    )
                }
            ),
            approved.model_copy(
                update={
                    "roster": (
                        approved.roster[0].model_copy(
                            update={"model_id": "different-public-model"}
                        ),
                    )
                }
            ),
            approved.model_copy(
                update={
                    "output_contracts": (
                        approved.output_contracts[0].model_copy(
                            update={"output_schema_id": "research.other_report.v1"}
                        ),
                    )
                }
            ),
        )
        for variant in variants:
            with self.subTest(variant=variant.protocol_id):
                spec = compile_execution_spec(
                    variant,
                    approved_protocol=approved,
                    approval=approval,
                    amendment=None,
                )
                self.assertEqual(spec.conformance, MATERIAL_UNAPPROVED)
                self.assertTrue(
                    any(item.classification == "MATERIAL" for item in spec.differences)
                )

    def test_compile_is_dry_and_never_reads_files_or_imports_legacy_runtime(self) -> None:
        protocol = _protocol()
        before = set(sys.modules)
        with patch.object(
            Path,
            "read_bytes",
            side_effect=AssertionError("protocol compilation must not read files"),
        ):
            spec = compile_execution_spec(
                protocol,
                approved_protocol=protocol,
                approval=_approval(protocol),
                amendment=None,
            )
        self.assertEqual(spec.conformance, IDENTICAL)
        self.assertNotIn("ag2_research.orchestrator", set(sys.modules) - before)

    def test_holdout_and_unsafe_side_effect_contracts_are_not_admitted_in_p1(self) -> None:
        with self.assertRaises(ValidationError):
            DatasetBinding(
                schema_version="research.dataset_binding.v1",
                dataset_id="holdout",
                role="FINAL_HOLDOUT",  # type: ignore[arg-type]
                artifact_id="a" * 64,
                window_start="2025-01-01",
                window_end="2025-12-31",
            )


class ProtocolImportPurityTests(unittest.TestCase):
    def test_protocol_module_import_is_side_effect_free(self) -> None:
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
            import research_automation.foundations.protocols
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
