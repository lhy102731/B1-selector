from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane import access as access_module
from research_automation.control_plane.access import _canonical_event
from research_automation.control_plane.access import (
    AccessConflictError,
    AccessEvent,
    AccessJournal,
    AccessOperation,
    DatasetRole,
    FinalHoldoutUnavailable,
    FoldTestAlreadyConsumed,
    FoldTestBroker,
    issue_fold_test_capability,
    issue_root_capability,
    InvalidTaintError,
    LineageError,
    Taint,
    TaintGraph,
)
from research_automation.control_plane.contracts import Actor, Phase, SideEffect
from research_automation.control_plane.sqlite_uow import SqliteSchemaError, SqliteUnitOfWorkError
from research_automation.control_plane.sqlite_uow import _SqliteUnitOfWork


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"
NOW = datetime(2026, 7, 30, 1, 2, 3, tzinfo=timezone.utc)


class OperationalFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        root = Path(self.temporary.name)
        self.authority_path = root / "authority.sqlite3"
        self.operational_path = root / "operational.sqlite3"
        self.paths = patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=self.authority_path,
            _OPERATIONAL_STORE_PATH=self.operational_path,
        )
        self.paths.start()
        stores_module._expected_schema_sha256.cache_clear()
        stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
        self.actor = Actor("reviewer", "human", "invocation-p3-tests")
        authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET, clock=lambda: NOW)
        identity = stores_module.AuthorityIdentity("a" * 64, "b" * 64, "c" * 64)
        envelope = authority._provision_authorization(
            phase=Phase.P3, attempt_id="p3-test-attempt", actor=self.actor,
            identity=identity, expires_at=NOW.replace(year=2027),
            allowed_side_effects=(SideEffect.READ, SideEffect.WRITE_CONTROL_PLANE),
        )
        self.grant = authority.claim_authorization(
            envelope, expected_phase=Phase.P3, expected_attempt_id="p3-test-attempt",
            actor=self.actor, identity=identity,
        )
        self.journal = AccessJournal(root_secret=ROOT_SECRET, grant=self.grant, clock=lambda: NOW)
        self.capability = None

    def tearDown(self) -> None:
        self.paths.stop()
        stores_module._expected_schema_sha256.cache_clear()
        self.temporary.cleanup()

    def event(self, event_id: str, **changes: object) -> AccessEvent:
        values: dict[str, object] = {
            "event_id": event_id,
            "operation": AccessOperation.READ,
            "actor_id": self.actor.actor_id,
            "actor_type": self.actor.actor_type,
            "invocation_id": self.actor.invocation_id,
            "run_id": "run-p3-001",
            "dataset_role": DatasetRole.TRAIN,
            "output_artifact_refs": (f"artifact:{event_id}",),
            "taint_out": (Taint.CLEAN,),
        }
        values.update(changes)
        return AccessEvent(**values)

    def seed_fold_source(self, event_id: str, ref: str, taint: Taint) -> None:
        event = self.event(
            event_id, operation=AccessOperation.READ, dataset_role=DatasetRole.FOLD_TEST,
            output_artifact_refs=(ref,), taint_out=(taint,),
        )
        payload_sha = _canonical_event(event)[1]
        _SqliteUnitOfWork(stores_module._operational_spec())._write(
            lambda connection: self.journal._insert_event(connection, event, payload_sha, NOW)
        )

    def append_event(self, event: AccessEvent) -> AccessEvent:
        if event.operation in (AccessOperation.READ, AccessOperation.MATERIALIZE) and not event.input_artifact_refs:
            capability = issue_root_capability(
                grant=self.grant,
                registration=self.registration(
                    event.output_artifact_refs[0], event.dataset_role, event.taint_out[0]
                ),
                actor=self.actor,
            )
            return self.journal.append_root(event, capability)
        return self.journal.append(event)

    def registration(
        self, artifact_ref: str, role: DatasetRole, taint: Taint,
        attempts: tuple[tuple[str, str, str, str], ...] = (),
    ) -> access_module.FrozenAccessRegistration:
        return access_module.FrozenAccessRegistration(
            "research_state/control_plane/p3/access-registry.json",
            "d" * 64,
            artifact_ref,
            role,
            taint,
            attempts,
            self.grant.grant_id,
            self.actor,
            access_module._REGISTRY_SEAL,
        )

    def fold_broker(self, *, candidate_id: str, protocol_id: str, fold_id: str,
                    artifact_ref: str, run_id: str = "run-p3-001") -> FoldTestBroker:
        capability = issue_fold_test_capability(
            grant=self.grant, candidate_id=candidate_id, protocol_id=protocol_id,
            fold_id=fold_id, artifact_ref=artifact_ref, run_id=run_id, actor=self.actor,
            registration=self.registration(
                artifact_ref, DatasetRole.FOLD_TEST, Taint.TEST_LABEL,
                ((candidate_id, protocol_id, fold_id, run_id),),
            ),
        )
        return FoldTestBroker(self.journal, capability, self.grant)


class AccessJournalTests(OperationalFixture):
    def test_required_actor_run_operation_and_role_fail_closed(self) -> None:
        for field in ("actor_id", "actor_type", "invocation_id", "run_id"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.event("evt-required", **{field: ""})
        with self.assertRaises(TypeError):
            self.event("evt-operation", operation="READ")
        with self.assertRaises(TypeError):
            self.event("evt-role", dataset_role="TRAIN")

    def test_append_is_durable_monotonic_and_replays_exactly(self) -> None:
        first = self.append_event(self.event("evt-001"))
        second = self.append_event(self.event("evt-002"))
        reopened = AccessJournal(root_secret=ROOT_SECRET, grant=self.grant, clock=lambda: NOW)

        self.assertEqual((first.sequence, second.sequence), (1, 2))
        self.assertEqual(reopened.list_for_run("run-p3-001"), (first, second))
        self.assertEqual(reopened.replay(), (first, second))
        self.assertEqual(reopened.append_root(
            self.event("evt-001"),
            issue_root_capability(
                grant=self.grant,
                registration=self.registration(
                    "artifact:evt-001", DatasetRole.TRAIN, Taint.CLEAN
                ),
                actor=self.actor,
            ),
        ), first)

    def test_conflicting_event_id_replay_is_rejected(self) -> None:
        self.append_event(self.event("evt-conflict"))
        with self.assertRaises(AccessConflictError):
            self.append_event(
                self.event("evt-conflict", dataset_role=DatasetRole.VALIDATION)
            )

    def test_concurrent_append_allocates_one_sequence_per_event(self) -> None:
        events = tuple(self.event(f"evt-concurrent-{index}") for index in range(16))
        with ThreadPoolExecutor(max_workers=8) as executor:
            stored = tuple(executor.map(self.append_event, events))
        self.assertEqual(
            sorted(event.sequence for event in stored),
            list(range(1, 17)),
        )

    def test_raw_or_unbounded_metadata_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.event("evt-raw", metadata=(("raw_dataframe", "payload"),))
        with self.assertRaises(ValueError):
            self.event("evt-big", metadata=(("status", "x" * 4097),))

    def test_final_holdout_is_unavailable_in_p3(self) -> None:
        with self.assertRaises(FinalHoldoutUnavailable):
            self.journal.append(
                self.event("evt-final", dataset_role=DatasetRole.FINAL_HOLDOUT)
            )
        with self.assertRaises(FinalHoldoutUnavailable):
            self.event("evt-final-taint", taint_out=(Taint.FINAL_HOLDOUT,))

    def test_direct_fold_test_read_and_materialize_are_rejected(self) -> None:
        for operation in (AccessOperation.READ, AccessOperation.MATERIALIZE):
            with self.subTest(operation=operation), self.assertRaises(FinalHoldoutUnavailable):
                self.journal.append(
                    self.event(
                        f"evt-direct-{operation.value}", operation=operation,
                        dataset_role=DatasetRole.FOLD_TEST,
                    )
                )

    def test_root_capability_requires_the_active_authority_actor(self) -> None:
        mismatched = Actor(
            self.actor.actor_id,
            "automation",
            self.actor.invocation_id,
        )
        with self.assertRaises(PermissionError):
            issue_root_capability(
                grant=self.grant,
                registration=self.registration(
                    "artifact:mismatched-root", DatasetRole.TRAIN, Taint.CLEAN
                ),
                actor=mismatched,
            )

        connection = sqlite3.connect(self.authority_path)
        try:
            connection.execute(
                "UPDATE phase_grants_v2 SET state = 'CLOSED' WHERE grant_id = ?",
                (self.grant.grant_id,),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(PermissionError):
            issue_root_capability(
                grant=self.grant,
                registration=self.registration(
                    "artifact:closed-grant", DatasetRole.TRAIN, Taint.CLEAN
                ),
                actor=self.actor,
            )

    def test_forged_grant_and_self_asserted_event_actor_are_rejected(self) -> None:
        forged = replace(
            self.grant,
            _bearer_secret=stores_module._BearerSecret("attacker-controlled-secret"),
        )
        with self.assertRaises(PermissionError):
            issue_root_capability(
                grant=forged,
                registration=self.registration(
                    "artifact:forged", DatasetRole.TRAIN, Taint.CLEAN
                ),
                actor=self.actor,
            )
        other = Actor("other-actor", "human", "other-invocation")
        with self.assertRaises(PermissionError):
            self.journal.append_root(
                self.event(
                    "evt-impersonated",
                    actor_id=other.actor_id,
                    actor_type=other.actor_type,
                    invocation_id=other.invocation_id,
                ),
                issue_root_capability(
                    grant=self.grant,
                    registration=self.registration(
                        "artifact:evt-impersonated", DatasetRole.TRAIN, Taint.CLEAN
                    ),
                    actor=self.actor,
                ),
            )

    def test_artifact_references_require_a_registered_namespace(self) -> None:
        with self.assertRaises(ValueError):
            self.event("evt-unregistered-ref", output_artifact_refs=("free form secret",))


class LineageAndTaintTests(OperationalFixture):
    def setUp(self) -> None:
        super().setUp()
        self.graph = TaintGraph(self.journal)

    def seed(self, ref: str, taint: Taint) -> None:
        self.append_event(
            self.event(
                f"seed-{ref}",
                output_artifact_refs=(ref,),
                taint_out=(taint,),
            )
        )

    def test_test_label_derivation_becomes_test_derived(self) -> None:
        self.seed("artifact:test-labels", Taint.TEST_LABEL)
        derived = self.graph.derive(
            inputs=("artifact:test-labels",),
            output="metric:rankic",
            event_id="derive-rankic",
            actor=self.actor,
            run_id="run-p3-001",
            dataset_role=DatasetRole.VALIDATION,
        )
        self.assertEqual(derived.taint_out, (Taint.TEST_LABEL, Taint.TEST_DERIVED))
        self.assertEqual(
            self.graph.taint_for("metric:rankic"),
            (Taint.TEST_LABEL, Taint.TEST_DERIVED),
        )

    def test_display_and_consume_propagate_taint(self) -> None:
        self.seed("artifact:test", Taint.TEST_LABEL)
        displayed = self.graph.expose(
            operation=AccessOperation.DISPLAY,
            inputs=("artifact:test",),
            outputs=("prompt:review",),
            event_id="display-test",
            actor=self.actor,
            run_id="run-p3-001",
            dataset_role=DatasetRole.VALIDATION,
        )
        consumed = self.graph.expose(
            operation=AccessOperation.CONSUME,
            inputs=("prompt:review",),
            outputs=("claim:review",),
            event_id="consume-test",
            actor=self.actor,
            run_id="run-p3-001",
            dataset_role=DatasetRole.VALIDATION,
        )
        self.assertEqual(displayed.taint_out, (Taint.TEST_LABEL,))
        self.assertEqual(consumed.taint_out, (Taint.TEST_LABEL,))

    def test_unknown_roots_and_cycles_are_rejected(self) -> None:
        with self.assertRaises(LineageError):
            self.graph.derive(
                inputs=("artifact:missing",), output="artifact:child", event_id="missing-root",
                actor=self.actor, run_id="run-p3-001",
                dataset_role=DatasetRole.TRAIN,
            )
        self.seed("artifact:root", Taint.CLEAN)
        self.graph.derive(
            inputs=("artifact:root",), output="artifact:child", event_id="derive-root-child",
            actor=self.actor, run_id="run-p3-001",
            dataset_role=DatasetRole.TRAIN,
        )
        with self.assertRaises(LineageError):
            self.graph.derive(
                inputs=("artifact:child",), output="artifact:root", event_id="cycle:root-child",
                actor=self.actor, run_id="run-p3-001",
                dataset_role=DatasetRole.TRAIN,
            )

    def test_invalid_taint_blocks_consumption(self) -> None:
        self.seed("artifact:invalid", Taint.INVALID)
        with self.assertRaises(InvalidTaintError):
            self.graph.expose(
                operation=AccessOperation.CONSUME,
                inputs=("artifact:invalid",), outputs=("claim:invalid",),
                event_id="consume-invalid", actor=self.actor,
                run_id="run-p3-001", dataset_role=DatasetRole.FOLD_TEST,
            )

    def test_export_propagates_taint_and_rejects_invalid(self) -> None:
        self.seed("artifact:test", Taint.TEST_LABEL)
        exported = self.graph.expose(
            operation=AccessOperation.EXPORT, inputs=("artifact:test",),
            outputs=("export:test",), event_id="export-test", actor=self.actor,
            run_id="run-p3-001", dataset_role=DatasetRole.VALIDATION,
        )
        self.assertEqual(exported.taint_out, (Taint.TEST_LABEL,))
        self.seed("artifact:invalid-export", Taint.INVALID)
        with self.assertRaises(InvalidTaintError):
            self.graph.expose(
                operation=AccessOperation.EXPORT,
                inputs=("artifact:invalid-export",), outputs=("export:invalid",),
                event_id="export-invalid", actor=self.actor,
                run_id="run-p3-001", dataset_role=DatasetRole.VALIDATION,
            )

    def test_projection_rebuild_restores_identical_taint(self) -> None:
        self.seed("artifact:root", Taint.TEST_LABEL)
        self.graph.derive(
            inputs=("artifact:root",), output="artifact:child", event_id="derive-child",
            actor=self.actor, run_id="run-p3-001",
            dataset_role=DatasetRole.FOLD_TEST,
        )
        connection = sqlite3.connect(self.operational_path)
        try:
            connection.execute("DELETE FROM taint_projection")
            connection.commit()
        finally:
            connection.close()
        self.journal.rebuild_taint_projection()
        self.assertEqual(
            self.graph.taint_for("artifact:child"),
            (Taint.TEST_LABEL, Taint.TEST_DERIVED),
        )


class FoldTestBrokerTests(OperationalFixture):
    def test_fold_test_is_consumed_before_handle_and_never_reopens(self) -> None:
        self.seed_fold_source("seed-fold", "dataset:fold-2025", Taint.TEST_LABEL)
        broker = self.fold_broker(
            candidate_id="candidate-frozen-a", protocol_id="protocol-frozen-a",
            fold_id="fold-2025", artifact_ref="dataset:fold-2025",
        )
        handle = broker.consume_once(
            candidate_id="candidate-frozen-a",
            protocol_id="protocol-frozen-a",
            fold_id="fold-2025",
            artifact_ref="dataset:fold-2025",
            event_id="consume-fold-2025",
            actor=self.actor,
            run_id="run-p3-001",
        )
        self.assertEqual(handle.artifact_ref, "dataset:fold-2025")
        with self.assertRaises(FoldTestAlreadyConsumed):
            self.fold_broker(
                candidate_id="candidate-frozen-a", protocol_id="protocol-frozen-a",
                fold_id="fold-2025", artifact_ref="dataset:fold-2025",
            ).consume_once(
                candidate_id="candidate-frozen-a",
                protocol_id="protocol-frozen-a",
                fold_id="fold-2025",
                artifact_ref="dataset:fold-2025",
                event_id="consume-fold-2025-retry",
                actor=self.actor,
                run_id="run-p3-001",
            )

    def test_concurrent_fold_test_has_exactly_one_winner(self) -> None:
        self.seed_fold_source("seed-concurrent-fold", "dataset:fold-concurrent", Taint.TEST_LABEL)

        def attempt(index: int) -> str:
            try:
                self.fold_broker(
                    candidate_id="candidate", protocol_id="protocol", fold_id="fold",
                    artifact_ref="dataset:fold-concurrent",
                ).consume_once(
                    candidate_id="candidate", protocol_id="protocol", fold_id="fold",
                    artifact_ref="dataset:fold-concurrent", event_id=f"consume-{index}",
                    actor=self.actor, run_id="run-p3-001",
                )
                return "CONSUMED"
            except FoldTestAlreadyConsumed:
                return "REJECTED"

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = tuple(executor.map(attempt, range(8)))
        self.assertEqual(results.count("CONSUMED"), 1)
        self.assertEqual(results.count("REJECTED"), 7)

    def test_same_fold_artifact_cannot_be_reopened_under_an_alias_candidate(self) -> None:
        self.seed_fold_source("seed-alias-fold", "dataset:fold-alias", Taint.TEST_LABEL)
        self.fold_broker(
            candidate_id="candidate-original", protocol_id="protocol-original",
            fold_id="fold-original", artifact_ref="dataset:fold-alias",
        ).consume_once(
            candidate_id="candidate-original", protocol_id="protocol-original",
            fold_id="fold-original", artifact_ref="dataset:fold-alias",
            event_id="consume-alias-original", actor=self.actor, run_id="run-p3-001",
        )
        with self.assertRaises(FoldTestAlreadyConsumed):
            self.fold_broker(
                candidate_id="candidate-alias", protocol_id="protocol-alias",
                fold_id="fold-alias", artifact_ref="dataset:fold-alias",
            ).consume_once(
                candidate_id="candidate-alias", protocol_id="protocol-alias",
                fold_id="fold-alias", artifact_ref="dataset:fold-alias",
                event_id="consume-alias-retry", actor=self.actor, run_id="run-p3-001",
            )

    def test_non_fold_test_source_cannot_be_consumed(self) -> None:
        self.append_event(
            self.event(
                "train-source", dataset_role=DatasetRole.TRAIN,
                output_artifact_refs=("artifact:train-source",),
                taint_out=(Taint.TEST_LABEL,),
            )
        )
        capability = issue_fold_test_capability(
            grant=self.grant, candidate_id="candidate", protocol_id="protocol",
            fold_id="fold", artifact_ref="artifact:train-source", run_id="run-p3-001",
            actor=self.actor,
            registration=self.registration(
                "artifact:train-source", DatasetRole.FOLD_TEST, Taint.TEST_LABEL,
                (("candidate", "protocol", "fold", "run-p3-001"),),
            ),
        )
        with self.assertRaises(PermissionError):
            FoldTestBroker(self.journal, capability, self.grant).consume_once(
                candidate_id="candidate", protocol_id="protocol", fold_id="fold",
                artifact_ref="artifact:train-source", event_id="consume-train",
                actor=self.actor, run_id="run-p3-001",
            )

    def test_fold_test_provenance_cannot_be_derived_before_consumption(self) -> None:
        self.seed_fold_source("protected-source", "dataset:protected", Taint.TEST_LABEL)
        event = self.event(
            "derive-before-consume", operation=AccessOperation.DERIVE,
            dataset_role=DatasetRole.FOLD_TEST,
            input_artifact_refs=("dataset:protected",), output_artifact_refs=("metric:protected",),
            taint_in=(Taint.TEST_LABEL,), taint_out=(Taint.TEST_LABEL, Taint.TEST_DERIVED),
        )
        with self.assertRaises(PermissionError):
            self.journal.append(event)

    def test_transitive_fold_test_descendant_rejects_another_actor(self) -> None:
        self.seed_fold_source("seed-transitive", "dataset:transitive", Taint.TEST_LABEL)
        self.fold_broker(
            candidate_id="candidate-transitive",
            protocol_id="protocol-transitive",
            fold_id="fold-transitive",
            artifact_ref="dataset:transitive",
        ).consume_once(
            candidate_id="candidate-transitive",
            protocol_id="protocol-transitive",
            fold_id="fold-transitive",
            artifact_ref="dataset:transitive",
            event_id="consume-transitive",
            actor=self.actor,
            run_id="run-p3-001",
        )
        child = self.event(
            "derive-transitive-child",
            operation=AccessOperation.DERIVE,
            dataset_role=DatasetRole.FOLD_TEST,
            input_artifact_refs=("dataset:transitive",),
            output_artifact_refs=("metric:transitive-child",),
            taint_in=(Taint.TEST_LABEL,),
            taint_out=(Taint.TEST_LABEL, Taint.TEST_DERIVED),
        )
        self.journal.append(child)
        other = Actor("other-reviewer", "human", "other-invocation")
        descendant = self.event(
            "derive-transitive-descendant",
            operation=AccessOperation.DERIVE,
            actor_id=other.actor_id,
            actor_type=other.actor_type,
            invocation_id=other.invocation_id,
            dataset_role=DatasetRole.FOLD_TEST,
            input_artifact_refs=("metric:transitive-child",),
            output_artifact_refs=("metric:transitive-descendant",),
            taint_in=(Taint.TEST_LABEL, Taint.TEST_DERIVED),
            taint_out=(Taint.TEST_LABEL, Taint.TEST_DERIVED),
        )
        with self.assertRaises(PermissionError):
            self.journal.append(descendant)

    def test_nonderive_output_retains_fold_provenance(self) -> None:
        self.seed_fold_source("seed-display-fold", "dataset:display-fold", Taint.TEST_LABEL)
        self.fold_broker(
            candidate_id="candidate-display",
            protocol_id="protocol-display",
            fold_id="fold-display",
            artifact_ref="dataset:display-fold",
        ).consume_once(
            candidate_id="candidate-display",
            protocol_id="protocol-display",
            fold_id="fold-display",
            artifact_ref="dataset:display-fold",
            event_id="consume-display-fold",
            actor=self.actor,
            run_id="run-p3-001",
        )
        self.journal.append(self.event(
            "display-fold-output",
            operation=AccessOperation.DISPLAY,
            dataset_role=DatasetRole.TRAIN,
            input_artifact_refs=("dataset:display-fold",),
            output_artifact_refs=("prompt:display-fold",),
            taint_in=(Taint.TEST_LABEL,),
            taint_out=(Taint.TEST_LABEL,),
        ))
        connection = sqlite3.connect(self.operational_path)
        try:
            connection.execute("DELETE FROM fold_test_attempts")
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(PermissionError):
            self.journal.append(self.event(
                "export-laundered-fold",
                operation=AccessOperation.EXPORT,
                dataset_role=DatasetRole.TRAIN,
                input_artifact_refs=("prompt:display-fold",),
                output_artifact_refs=("export:display-fold",),
                taint_in=(Taint.TEST_LABEL,),
                taint_out=(Taint.TEST_LABEL,),
            ))

    def test_every_fold_root_in_a_multi_input_event_must_be_consumed(self) -> None:
        self.seed_fold_source("seed-fold-a", "dataset:fold-a", Taint.TEST_LABEL)
        self.seed_fold_source("seed-fold-b", "dataset:fold-b", Taint.TEST_LABEL)
        self.fold_broker(
            candidate_id="candidate-a", protocol_id="protocol-a",
            fold_id="fold-a", artifact_ref="dataset:fold-a",
        ).consume_once(
            candidate_id="candidate-a", protocol_id="protocol-a", fold_id="fold-a",
            artifact_ref="dataset:fold-a", event_id="consume-fold-a",
            actor=self.actor, run_id="run-p3-001",
        )
        with self.assertRaises(PermissionError):
            self.journal.append(self.event(
                "derive-partially-covered-folds",
                operation=AccessOperation.DERIVE,
                dataset_role=DatasetRole.FOLD_TEST,
                input_artifact_refs=("dataset:fold-a", "dataset:fold-b"),
                output_artifact_refs=("metric:partial-folds",),
                taint_in=(Taint.TEST_LABEL,),
                taint_out=(Taint.TEST_LABEL, Taint.TEST_DERIVED),
            ))


class ProjectionIntegrityTests(OperationalFixture):
    def test_projection_rebuild_rejects_tampered_event_and_rolls_back(self) -> None:
        self.append_event(self.event("seed-integrity"))
        connection = sqlite3.connect(self.operational_path)
        try:
            connection.execute(
                "UPDATE access_events SET payload_sha256 = ? WHERE event_id = ?",
                ("0" * 64, "seed-integrity"),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(AccessConflictError):
            self.journal.rebuild_taint_projection()
        connection = sqlite3.connect(self.operational_path)
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM taint_projection WHERE artifact_ref = ?",
                ("artifact:seed-integrity",),
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(count, 1)

    def test_projection_rebuild_enforces_fold_consumption_provenance(self) -> None:
        self.seed_fold_source("seed-rebuild-fold", "dataset:rebuild-fold", Taint.TEST_LABEL)
        self.fold_broker(
            candidate_id="candidate-rebuild",
            protocol_id="protocol-rebuild",
            fold_id="fold-rebuild",
            artifact_ref="dataset:rebuild-fold",
        ).consume_once(
            candidate_id="candidate-rebuild",
            protocol_id="protocol-rebuild",
            fold_id="fold-rebuild",
            artifact_ref="dataset:rebuild-fold",
            event_id="consume-rebuild-fold",
            actor=self.actor,
            run_id="run-p3-001",
        )
        connection = sqlite3.connect(self.operational_path)
        try:
            connection.execute("DELETE FROM fold_test_attempts")
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(PermissionError):
            self.journal.rebuild_taint_projection()
        self.assertEqual(
            self.journal.taint_for("dataset:rebuild-fold"),
            (Taint.TEST_LABEL,),
        )


class OperationalMigrationTests(unittest.TestCase):
    def _provision_v1_pair(self, authority_path: Path, operational_path: Path) -> None:
        stores_module._provision_store(
            authority_path,
            store_kind="AUTHORITY_STORE",
            metadata_table="authority_meta",
            installation_id="a" * 64,
            root_capability_sha256=stores_module._root_secret_sha256(ROOT_SECRET),
        )
        original_schema = stores_module._OPERATIONAL_SCHEMA
        original_version = stores_module._OPERATIONAL_SCHEMA_VERSION
        try:
            stores_module._OPERATIONAL_SCHEMA = stores_module._OPERATIONAL_SCHEMA_V1
            stores_module._OPERATIONAL_SCHEMA_VERSION = 1
            stores_module._expected_schema_sha256.cache_clear()
            stores_module._provision_store(
                operational_path,
                store_kind="OPERATIONAL_JOURNAL",
                metadata_table="operational_meta",
                installation_id="a" * 64,
                root_capability_sha256=stores_module._root_secret_sha256(ROOT_SECRET),
            )
        finally:
            stores_module._OPERATIONAL_SCHEMA = original_schema
            stores_module._OPERATIONAL_SCHEMA_VERSION = original_version
            stores_module._expected_schema_sha256.cache_clear()

    def test_v1_migration_preserves_journal_and_authority_bytes(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._expected_schema_sha256.cache_clear()
                self._provision_v1_pair(authority_path, operational_path)
                connection = sqlite3.connect(operational_path)
                try:
                    connection.execute(
                        """INSERT INTO journal_events
                        (authority_sequence, event_id, event_type, aggregate_id,
                         payload_json, payload_sha256, event_sha256, created_at, mirrored_at)
                        VALUES (1, 'legacy-event', 'LEGACY', 'legacy', '{}', ?, ?, ?, ?)""",
                        ("b" * 64, "c" * 64, NOW.isoformat(), NOW.isoformat()),
                    )
                    connection.commit()
                finally:
                    connection.close()
                before = hashlib.sha256(authority_path.read_bytes()).hexdigest()
                self.assertTrue(
                    stores_module._migrate_operational_journal_v2(
                        root_secret=ROOT_SECRET
                    )
                )
                self.assertFalse(
                    stores_module._migrate_operational_journal_v2(
                        root_secret=ROOT_SECRET
                    )
                )
                self.assertEqual(
                    before, hashlib.sha256(authority_path.read_bytes()).hexdigest()
                )
                self.assertEqual(
                    stores_module.OperationalReader().read_identity().schema_version,
                    3,
                )
                self.assertEqual(stores_module.OperationalReader().event_count(), 1)

    def test_migration_rejects_schema_drift(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._expected_schema_sha256.cache_clear()
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                connection = sqlite3.connect(operational_path)
                try:
                    connection.execute("CREATE TABLE drift(value TEXT)")
                    connection.commit()
                finally:
                    connection.close()
                with self.assertRaises(SqliteSchemaError):
                    stores_module._migrate_operational_journal_v2(
                        root_secret=ROOT_SECRET
                    )

    def test_migration_rejects_hard_linked_authority_and_operational_paths(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._expected_schema_sha256.cache_clear()
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                original = hashlib.sha256(authority_path.read_bytes()).hexdigest()
                operational_path.unlink()
                os.link(authority_path, operational_path)
                with self.assertRaises(stores_module.StoreConfigurationError):
                    stores_module._migrate_operational_journal_v2(root_secret=ROOT_SECRET)
                self.assertEqual(original, hashlib.sha256(authority_path.read_bytes()).hexdigest())

    def test_mid_ddl_failure_rolls_back_the_entire_migration(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._expected_schema_sha256.cache_clear()
                self._provision_v1_pair(authority_path, operational_path)
                original_schema = stores_module._OPERATIONAL_SCHEMA
                try:
                    stores_module._OPERATIONAL_SCHEMA = (
                        stores_module._OPERATIONAL_SCHEMA_V1
                        + (original_schema[len(stores_module._OPERATIONAL_SCHEMA_V1)],)
                        + ("CREATE TABL invalid_migration(value TEXT)",)
                    )
                    with self.assertRaises(SqliteUnitOfWorkError):
                        stores_module._migrate_operational_journal_v2(
                            root_secret=ROOT_SECRET
                        )
                finally:
                    stores_module._OPERATIONAL_SCHEMA = original_schema
                    stores_module._expected_schema_sha256.cache_clear()
                connection = sqlite3.connect(operational_path)
                try:
                    version = connection.execute("PRAGMA user_version").fetchone()[0]
                    metadata_version = connection.execute(
                        "SELECT value FROM operational_meta WHERE key = 'schema_version'"
                    ).fetchone()[0]
                    access_table = connection.execute(
                        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'access_events'"
                    ).fetchone()
                finally:
                    connection.close()
                self.assertEqual(version, 1)
                self.assertEqual(metadata_version, "1")
                self.assertIsNone(access_table)

    def test_migration_rejects_future_schema_wrong_root_and_missing_store(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._expected_schema_sha256.cache_clear()
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                with self.assertRaises(stores_module.AuthorityRootError):
                    stores_module._migrate_operational_journal_v2(
                        root_secret="wrong-test-root-capability-0123456789abcdef"
                    )
                connection = sqlite3.connect(operational_path)
                try:
                    connection.execute("PRAGMA user_version = 99")
                    connection.commit()
                finally:
                    connection.close()
                with self.assertRaises(stores_module.SqliteFutureSchemaError):
                    stores_module._migrate_operational_journal_v2(
                        root_secret=ROOT_SECRET
                    )
                operational_path.unlink()
                with self.assertRaises(stores_module.StoreBootstrapIncompleteError):
                    stores_module._migrate_operational_journal_v2(
                        root_secret=ROOT_SECRET
                    )


if __name__ == "__main__":
    unittest.main()
