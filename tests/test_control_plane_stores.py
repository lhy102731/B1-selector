from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import sqlite3
import subprocess
from tempfile import TemporaryDirectory
from threading import Barrier, BrokenBarrierError, Event
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import Actor, Phase, SideEffect
from research_automation.control_plane.stores import (
    AuthorityReader,
    AuthorityIdentity,
    AuthorizationReplayError,
    StoreAlreadyBootstrappedError,
    StorePairDescriptor,
    StoreConfigurationError,
    read_store_pair_descriptor,
)
from research_automation.control_plane.sqlite_uow import (
    SqliteSchemaError,
    SqliteUnitOfWorkError,
)
from research_automation.control_plane.task_reports import build_task_report_v2


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"


class TrustedBootstrapTests(unittest.TestCase):
    def test_bootstrap_is_not_a_worker_visible_module_api(self) -> None:
        self.assertNotIn("trusted_bootstrap", stores_module.__all__)
        self.assertFalse(hasattr(stores_module, "trusted_bootstrap"))

    def test_callers_cannot_select_store_paths(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            attacker_authority = root / "attacker-authority.sqlite3"
            attacker_operational = root / "attacker-operational.sqlite3"

            with self.assertRaises(TypeError):
                stores_module._trusted_bootstrap(
                    authority_path=attacker_authority,
                    operational_path=attacker_operational,
                )

            self.assertFalse(attacker_authority.exists())
            self.assertFalse(attacker_operational.exists())

    def test_public_identity_hashes_cannot_open_the_authority_writer(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                self.assertNotIn(ROOT_SECRET.encode(), authority_path.read_bytes())
                self.assertNotIn(ROOT_SECRET.encode(), operational_path.read_bytes())

                with self.assertRaises(stores_module.AuthorityRootError):
                    stores_module._AuthorityStore(root_secret="a" * 64)

                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET
                )
                self.assertIsInstance(authority, stores_module._AuthorityStore)

    def test_same_path_fails_before_a_store_is_created(self) -> None:
        with TemporaryDirectory() as tmp:
            shared_path = Path(tmp) / "control-plane.sqlite3"

            with self.assertRaisesRegex(
                StoreConfigurationError,
                "different SQLite files",
            ):
                with patch.multiple(
                    stores_module,
                    _AUTHORITY_STORE_PATH=shared_path,
                    _OPERATIONAL_STORE_PATH=shared_path,
                ):
                    stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)

            self.assertFalse(shared_path.exists())

    def test_bootstrap_provisions_two_physical_store_files(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority" / "authority.sqlite3"
            operational_path = root / "operational" / "operational.sqlite3"

            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                receipt = stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)

            self.assertEqual(receipt.authority_path, authority_path.resolve())
            self.assertEqual(receipt.operational_path, operational_path.resolve())
            self.assertTrue(authority_path.is_file())
            self.assertTrue(operational_path.is_file())
            self.assertFalse(os.path.samefile(authority_path, operational_path))
            self.assertGreater(authority_path.stat().st_size, 0)
            self.assertGreater(operational_path.stat().st_size, 0)

    def test_bootstrapped_stores_share_one_random_pair_identity(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"

            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                receipt = stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                descriptor = read_store_pair_descriptor()

            self.assertIsInstance(descriptor, StorePairDescriptor)
            self.assertEqual(descriptor.installation_id, receipt.installation_id)
            self.assertEqual(descriptor.authority_kind, "AUTHORITY_STORE")
            self.assertEqual(
                descriptor.operational_kind,
                "OPERATIONAL_JOURNAL",
            )
            self.assertEqual(len(descriptor.installation_id), 64)

    def test_second_store_failure_publishes_no_partial_pair(self) -> None:
        class FailDuringSchemaConnection:
            def __init__(self, connection: sqlite3.Connection) -> None:
                self._connection = connection

            def execute(self, statement: str, *args: object):
                if statement.lstrip().startswith("CREATE TABLE"):
                    self._connection.execute(
                        "CREATE TABLE partial_bootstrap(value TEXT)"
                    )
                    raise sqlite3.OperationalError("simulated storage failure")
                return self._connection.execute(statement, *args)

            def executemany(self, *args: object):
                return self._connection.executemany(*args)

            def commit(self) -> None:
                self._connection.commit()

            def close(self) -> None:
                self._connection.close()

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            real_connect = sqlite3.connect
            connection_count = 0

            def flaky_connect(*args: object, **kwargs: object):
                nonlocal connection_count
                connection_count += 1
                connection = real_connect(*args, **kwargs)
                if connection_count == 2:
                    return FailDuringSchemaConnection(connection)
                return connection

            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ), patch.object(
                stores_module.sqlite3,
                "connect",
                side_effect=flaky_connect,
            ):
                with self.assertRaisesRegex(
                    stores_module.StoreBootstrapError,
                    "bootstrap failed",
                ):
                    stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)

            self.assertFalse(authority_path.exists())
            self.assertFalse(operational_path.exists())
            self.assertEqual(tuple(root.iterdir()), ())

    def test_successful_bootstrap_cannot_modify_stores_on_replay(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"

            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                before = {
                    path: (
                        hashlib.sha256(path.read_bytes()).hexdigest(),
                        path.stat().st_mtime_ns,
                    )
                    for path in (authority_path, operational_path)
                }

                with self.assertRaises(StoreAlreadyBootstrappedError):
                    stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)

                after = {
                    path: (
                        hashlib.sha256(path.read_bytes()).hexdigest(),
                        path.stat().st_mtime_ns,
                    )
                    for path in (authority_path, operational_path)
                }

            self.assertEqual(after, before)

    def test_ordinary_authority_reader_has_no_generic_sql_capability(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                receipt = stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                reader = AuthorityReader()
                identity = reader.read_identity()

            self.assertEqual(identity.installation_id, receipt.installation_id)
            self.assertEqual(identity.store_kind, "AUTHORITY_STORE")
            self.assertFalse(hasattr(reader, "__dict__"))
            for generic_api in (
                "connection",
                "execute",
                "read",
                "transaction",
                "unit_of_work",
                "write",
            ):
                with self.subTest(generic_api=generic_api):
                    self.assertFalse(hasattr(reader, generic_api))

    def test_authorization_envelope_can_be_claimed_exactly_once(self) -> None:
        now = datetime(2026, 7, 26, 1, 30, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-p0r2")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                recovered_envelope = (
                    authority._recover_pending_authorization_for_binding(
                        phase=Phase.P0,
                        attempt_id="p0r2-attempt-001",
                        actor=actor,
                        identity=identity,
                    )
                )
                self.assertEqual(
                    recovered_envelope.to_public_dict(),
                    envelope.to_public_dict(),
                )

                grant = authority.claim_authorization(
                    recovered_envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                with self.assertRaises(AuthorizationReplayError):
                    authority.claim_authorization(
                        envelope,
                        expected_phase=Phase.P0,
                        expected_attempt_id="p0r2-attempt-001",
                        actor=actor,
                        identity=identity,
                    )

            self.assertEqual(grant.phase, Phase.P0)
            self.assertEqual(grant.attempt_id, "p0r2-attempt-001")
            self.assertEqual(grant.actor, actor)
            self.assertEqual(grant.identity, identity)

    def test_authority_capability_secrets_never_enter_rendered_outputs(self) -> None:
        now = datetime(2026, 7, 26, 2, 0, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-secret-test")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )
        authority_secret = "AUTHORIZATION_SECRET_SENTINEL"
        grant_secret = "GRANT_SECRET_SENTINEL"

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ), patch.object(
                stores_module.secrets,
                "token_urlsafe",
                side_effect=(
                    authority_secret,
                    grant_secret,
                    "REPLAY_ATTEMPT_UNUSED_SECRET",
                ),
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                with self.assertRaises(AuthorizationReplayError) as caught:
                    authority.claim_authorization(
                        envelope,
                        expected_phase=Phase.P0,
                        expected_attempt_id="p0r2-attempt-001",
                        actor=actor,
                        identity=identity,
                    )

            logger = logging.getLogger("control-plane-secret-test")
            with self.assertLogs(logger, level="INFO") as captured:
                logger.info("envelope=%r grant=%r", envelope, grant)
            rendered = (
                repr(envelope),
                repr(grant),
                json.dumps(envelope.to_public_dict(), sort_keys=True),
                json.dumps(asdict(envelope), default=str, sort_keys=True),
                json.dumps(asdict(grant), default=str, sort_keys=True),
                str(caught.exception),
                "\n".join(captured.output),
                authority_path.read_bytes().decode("latin-1"),
            )
            for secret in (authority_secret, grant_secret):
                for output in rendered:
                    with self.subTest(secret=secret, output=output[:40]):
                        self.assertNotIn(secret, output)

    def test_concurrent_bootstrap_has_exactly_one_winner(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            start = Barrier(2)
            publish_race = Barrier(2)
            real_replace = os.replace

            def racing_replace(source, destination) -> None:
                if Path(destination) == authority_path:
                    try:
                        publish_race.wait(timeout=0.2)
                    except BrokenBarrierError:
                        pass
                real_replace(source, destination)

            def attempt_bootstrap():
                start.wait(timeout=2)
                try:
                    return (
                        "SUCCESS",
                        stores_module._trusted_bootstrap(root_secret=ROOT_SECRET),
                    )
                except stores_module.StoreError as error:
                    return ("REJECTED", error)

            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ), patch.object(
                stores_module.os,
                "replace",
                side_effect=racing_replace,
            ):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    results = tuple(executor.map(lambda _index: attempt_bootstrap(), range(2)))
                descriptor = read_store_pair_descriptor()

            successes = [value for status, value in results if status == "SUCCESS"]
            rejections = [
                value for status, value in results if status == "REJECTED"
            ]
            self.assertEqual(len(successes), 1, results)
            self.assertEqual(len(rejections), 1, results)
            self.assertNotIsInstance(
                rejections[0],
                stores_module.StoreBootstrapIncompleteError,
            )
            self.assertEqual(
                descriptor.installation_id,
                successes[0].installation_id,
            )
            self.assertEqual(
                tuple(root.glob(".*.bootstrap")),
                (),
            )

    def test_authority_reader_rejects_any_schema_drift(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                drift = sqlite3.connect(authority_path)
                try:
                    drift.execute("CREATE TABLE unreviewed_table(value TEXT)")
                    drift.commit()
                finally:
                    drift.close()

                with self.assertRaises(SqliteSchemaError):
                    AuthorityReader().read_identity()

    def test_outbox_replay_after_mirror_before_ack_is_idempotent(self) -> None:
        now = datetime(2026, 7, 26, 2, 30, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-outbox-replay")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                journal = stores_module._OperationalJournal(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                pending = authority._read_pending_outbox(limit=1)

                # Simulated crash boundary: journal commit succeeds, authority ack does not run.
                inserted = journal._mirror_event(pending[0])
                self.assertTrue(inserted)
                self.assertEqual(AuthorityReader().pending_outbox_count(), 1)
                self.assertEqual(stores_module.OperationalReader().event_count(), 1)

                replay = stores_module._mirror_authority_outbox(
                    authority,
                    journal,
                    limit=10,
                )

                self.assertEqual(replay.inserted_events, 0)
                self.assertEqual(replay.acknowledged_events, 1)
                self.assertEqual(AuthorityReader().pending_outbox_count(), 0)
                self.assertEqual(stores_module.OperationalReader().event_count(), 1)

    def test_outbox_mirror_rejects_a_journal_from_another_installation(self) -> None:
        now = datetime(2026, 7, 26, 2, 45, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-cross-pair-mirror")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_a = root / "a" / "authority.sqlite3"
            operational_a = root / "a" / "operational.sqlite3"
            authority_b = root / "b" / "authority.sqlite3"
            operational_b = root / "b" / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_a,
                _OPERATIONAL_STORE_PATH=operational_a,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_b,
                _OPERATIONAL_STORE_PATH=operational_b,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)

            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_a,
                _OPERATIONAL_STORE_PATH=operational_b,
            ):
                journal = stores_module._OperationalJournal(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                with self.assertRaisesRegex(
                    stores_module.StoreBootstrapError,
                    "pair identity mismatch",
                ):
                    stores_module._mirror_authority_outbox(
                        authority,
                        journal,
                        limit=10,
                    )
                self.assertEqual(AuthorityReader().pending_outbox_count(), 1)
                self.assertEqual(stores_module.OperationalReader().event_count(), 0)

    def test_authority_mutation_rolls_back_when_outbox_insert_fails(self) -> None:
        now = datetime(2026, 7, 26, 3, 0, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-outbox-atomicity")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                existing_event = authority._read_pending_outbox(limit=1)[0]
                colliding_suffix = existing_event.event_id.removeprefix("evt_")

                with patch.object(
                    stores_module.secrets,
                    "token_hex",
                    side_effect=("atomicrollback", colliding_suffix),
                ):
                    with self.assertRaises(SqliteUnitOfWorkError):
                        authority._provision_authorization(
                            phase=Phase.P0,
                            attempt_id="p0r2-attempt-002",
                            actor=actor,
                            identity=identity,
                            expires_at=now + timedelta(hours=1),
                            allowed_side_effects=(
                                SideEffect.WRITE_CONTROL_PLANE,
                            ),
                        )

                reader = AuthorityReader()
                self.assertIsNone(
                    reader.authorization_state("auth_atomicrollback")
                )
                self.assertEqual(reader.pending_outbox_count(), 1)

    def test_tampered_outbox_content_is_not_mirrored(self) -> None:
        now = datetime(2026, 7, 26, 3, 30, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-outbox-integrity")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                journal = stores_module._OperationalJournal(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                tamper = sqlite3.connect(authority_path)
                try:
                    original_payload, original_event_type = tamper.execute(
                        "SELECT payload_json, event_type FROM authority_outbox"
                    ).fetchone()
                    tamper.execute(
                        """
                        UPDATE authority_outbox
                        SET payload_json = payload_json || 'tampered'
                        """
                    )
                    tamper.commit()
                finally:
                    tamper.close()

                with self.assertRaises(stores_module.OutboxConflictError):
                    stores_module._mirror_authority_outbox(
                        authority,
                        journal,
                        limit=10,
                    )

                self.assertEqual(AuthorityReader().pending_outbox_count(), 1)
                self.assertEqual(stores_module.OperationalReader().event_count(), 0)

                tamper = sqlite3.connect(authority_path)
                try:
                    tamper.execute(
                        """
                        UPDATE authority_outbox
                        SET payload_json = ?, event_type = 'FORGED_PHASE_CLOSED'
                        """,
                        (original_payload,),
                    )
                    tamper.commit()
                finally:
                    tamper.close()

                with self.assertRaises(stores_module.OutboxConflictError):
                    stores_module._mirror_authority_outbox(
                        authority,
                        journal,
                        limit=10,
                    )

                self.assertEqual(AuthorityReader().pending_outbox_count(), 1)
                self.assertEqual(stores_module.OperationalReader().event_count(), 0)

                tamper = sqlite3.connect(authority_path)
                try:
                    tamper.execute(
                        """
                        UPDATE authority_outbox
                        SET event_type = ?, sequence = sequence + 10
                        """,
                        (original_event_type,),
                    )
                    tamper.commit()
                finally:
                    tamper.close()

                with self.assertRaises(stores_module.OutboxConflictError):
                    stores_module._mirror_authority_outbox(
                        authority,
                        journal,
                        limit=10,
                    )

                self.assertEqual(AuthorityReader().pending_outbox_count(), 1)
                self.assertEqual(stores_module.OperationalReader().event_count(), 0)

    def test_pending_outbox_blocks_phase_closure(self) -> None:
        now = datetime(2026, 7, 26, 4, 0, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-pending-outbox")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                journal = stores_module._OperationalJournal(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )

                with self.assertRaises(stores_module.PendingOutboxError):
                    authority._assert_outbox_drained_for_phase_close()

                stores_module._mirror_authority_outbox(
                    authority,
                    journal,
                    limit=10,
                )
                authority._assert_outbox_drained_for_phase_close()

    def test_expired_authorization_is_closed_and_audited_atomically(self) -> None:
        clock = [datetime(2026, 7, 26, 4, 30, tzinfo=timezone.utc)]
        actor = Actor("operator", "human", "invocation-expired-envelope")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: clock[0],
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=clock[0] + timedelta(minutes=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                clock[0] += timedelta(minutes=2)

                with self.assertRaises(stores_module.AuthorizationExpiredError):
                    authority.claim_authorization(
                        envelope,
                        expected_phase=Phase.P0,
                        expected_attempt_id="p0r2-attempt-001",
                        actor=actor,
                        identity=identity,
                    )

                reader = AuthorityReader()
                self.assertEqual(
                    reader.authorization_state(envelope.authorization_ref),
                    "EXPIRED",
                )
                self.assertEqual(reader.pending_outbox_count(), 2)

    def test_claim_rechecks_expiry_after_waiting_for_the_write_lock(self) -> None:
        clock = [datetime(2026, 7, 26, 4, 45, tzinfo=timezone.utc)]
        clock_reads = [0]
        claim_clock_sampled = Event()
        actor = Actor("operator", "human", "invocation-expiry-after-lock")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        def read_clock() -> datetime:
            clock_reads[0] += 1
            if clock_reads[0] >= 2:
                claim_clock_sampled.set()
            return clock[0]

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=read_clock,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=clock[0] + timedelta(minutes=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                blocker = sqlite3.connect(authority_path, isolation_level=None)
                blocker.execute("BEGIN IMMEDIATE")
                try:
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        future = pool.submit(
                            authority.claim_authorization,
                            envelope,
                            expected_phase=Phase.P0,
                            expected_attempt_id="p0r2-attempt-001",
                            actor=actor,
                            identity=identity,
                        )
                        claim_clock_sampled.wait(0.2)
                        clock[0] += timedelta(minutes=2)
                        blocker.rollback()
                        with self.assertRaises(
                            stores_module.AuthorizationExpiredError
                        ):
                            future.result(timeout=3)
                finally:
                    blocker.close()

                self.assertEqual(
                    AuthorityReader().authorization_state(
                        envelope.authorization_ref
                    ),
                    "EXPIRED",
                )

    def test_provision_rechecks_expiry_after_waiting_for_the_write_lock(self) -> None:
        clock = [datetime(2026, 7, 26, 4, 50, tzinfo=timezone.utc)]
        clock_sampled = Event()
        actor = Actor("operator", "human", "invocation-provision-after-lock")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        def read_clock() -> datetime:
            clock_sampled.set()
            return clock[0]

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=read_clock,
                )
                blocker = sqlite3.connect(authority_path, isolation_level=None)
                blocker.execute("BEGIN IMMEDIATE")
                try:
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        future = pool.submit(
                            authority._provision_authorization,
                            phase=Phase.P0,
                            attempt_id="p0r2-attempt-002",
                            actor=actor,
                            identity=identity,
                            expires_at=clock[0] + timedelta(minutes=1),
                            allowed_side_effects=(
                                SideEffect.WRITE_CONTROL_PLANE,
                            ),
                        )
                        clock_sampled.wait(0.2)
                        clock[0] += timedelta(minutes=2)
                        blocker.rollback()
                        with self.assertRaises(
                            stores_module.AuthorizationExpiredError
                        ):
                            future.result(timeout=3)
                finally:
                    blocker.close()

                self.assertEqual(AuthorityReader().pending_outbox_count(), 0)

    def test_wrong_authorization_bindings_do_not_consume_the_envelope(self) -> None:
        now = datetime(2026, 7, 26, 5, 0, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-binding-test")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                wrong_bindings = (
                    {
                        "expected_phase": Phase.P1,
                        "expected_attempt_id": "p0r2-attempt-001",
                        "actor": actor,
                        "identity": identity,
                    },
                    {
                        "expected_phase": Phase.P0,
                        "expected_attempt_id": "p0r2-attempt-002",
                        "actor": actor,
                        "identity": identity,
                    },
                    {
                        "expected_phase": Phase.P0,
                        "expected_attempt_id": "p0r2-attempt-001",
                        "actor": Actor(
                            "other-operator",
                            "human",
                            actor.invocation_id,
                        ),
                        "identity": identity,
                    },
                    {
                        "expected_phase": Phase.P0,
                        "expected_attempt_id": "p0r2-attempt-001",
                        "actor": Actor(
                            actor.actor_id,
                            "human",
                            "other-invocation",
                        ),
                        "identity": identity,
                    },
                    {
                        "expected_phase": Phase.P0,
                        "expected_attempt_id": "p0r2-attempt-001",
                        "actor": actor,
                        "identity": AuthorityIdentity(
                            plan_hash=identity.plan_hash,
                            scope_hash=identity.scope_hash,
                            instruction_policy_hash="d" * 64,
                        ),
                    },
                )

                for wrong in wrong_bindings:
                    with self.subTest(wrong=wrong):
                        with self.assertRaises(
                            stores_module.AuthorizationRejectedError
                        ):
                            authority.claim_authorization(envelope, **wrong)
                        self.assertEqual(
                            AuthorityReader().authorization_state(
                                envelope.authorization_ref
                            ),
                            "PENDING",
                        )

                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                self.assertEqual(grant.attempt_id, "p0r2-attempt-001")

    def test_concurrent_authorization_claim_has_one_winner(self) -> None:
        now = datetime(2026, 7, 26, 5, 30, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-claim-race")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                start = Barrier(2)

                def claim_once():
                    start.wait(timeout=2)
                    try:
                        return (
                            "SUCCESS",
                            authority.claim_authorization(
                                envelope,
                                expected_phase=Phase.P0,
                                expected_attempt_id="p0r2-attempt-001",
                                actor=actor,
                                identity=identity,
                            ),
                        )
                    except stores_module.AuthorizationReplayError as error:
                        return ("REPLAY", error)

                with ThreadPoolExecutor(max_workers=2) as executor:
                    results = tuple(executor.map(lambda _index: claim_once(), range(2)))

                self.assertEqual(
                    sum(status == "SUCCESS" for status, _value in results),
                    1,
                    results,
                )
                self.assertEqual(
                    sum(status == "REPLAY" for status, _value in results),
                    1,
                    results,
                )
                self.assertEqual(
                    AuthorityReader().authorization_state(
                        envelope.authorization_ref
                    ),
                    "CLAIMED",
                )
                self.assertEqual(AuthorityReader().pending_outbox_count(), 2)

    def test_task_ticket_idempotency_requires_identical_task_spec(self) -> None:
        now = datetime(2026, 7, 26, 6, 0, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-task-ticket")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )
        task_spec = {
            "task_id": "P0R2-T1-STORE-TRACER",
            "objective": "Prove task ticket idempotency.",
            "dependencies": [],
            "idempotency_key": "p0r2-store-tracer-001",
            "task_spec_ref": "research_state/control_plane/p0r2/task_specs/store.json",
            "task_spec_sha256": "d" * 64,
            "requirements": {
                "required_test_receipt_ids": ["store-tests"],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_automation/control_plane/stores.py"],
            "forbidden_files": ["data/"],
            "baseline_ref": "research_state/control_plane/p0r2/baseline.json",
            "baseline_sha256": "e" * 64,
            "input_evidence_refs": [],
        }

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(
                        SideEffect.READ,
                        SideEffect.WRITE_CONTROL_PLANE,
                    ),
                )
                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                recovered_grant = authority._recover_claimed_grant(
                    envelope.authorization_ref
                )
                self.assertEqual(recovered_grant, grant)
                grant = recovered_grant

                p1_envelope = authority._provision_authorization(
                    phase=Phase.P1,
                    attempt_id="p1-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                p1_grant = authority.claim_authorization(
                    p1_envelope,
                    expected_phase=Phase.P1,
                    expected_attempt_id="p1-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                with self.assertRaisesRegex(
                    stores_module.TaskTicketError,
                    "requires an active entry policy",
                ):
                    authority._issue_task_ticket(
                        p1_grant,
                        task_spec,
                        allowed_side_effects=(
                            SideEffect.WRITE_CONTROL_PLANE,
                        ),
                    )

                with self.assertRaises(stores_module.TaskTicketError):
                    authority._issue_task_ticket(
                        grant,
                        task_spec,
                        allowed_side_effects=(SideEffect.RUN_RESEARCH,),
                    )

                first = authority._issue_task_ticket(
                    grant,
                    task_spec,
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                replay = authority._issue_task_ticket(
                    grant,
                    task_spec,
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                with self.assertRaises(
                    stores_module.TaskTicketIdempotencyError
                ):
                    authority._issue_task_ticket(
                        grant,
                        task_spec,
                        allowed_side_effects=(SideEffect.READ,),
                    )
                changed_spec = dict(task_spec)
                changed_spec["objective"] = "Different task semantics."
                with self.assertRaises(
                    stores_module.TaskTicketIdempotencyError
                ):
                    authority._issue_task_ticket(
                        grant,
                        changed_spec,
                        allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                    )

                self.assertEqual(replay.ticket_id, first.ticket_id)
                self.assertEqual(replay.task_id, first.task_id)
                self.assertEqual(
                    replay.allowed_side_effects,
                    (SideEffect.WRITE_CONTROL_PLANE,),
                )
                self.assertEqual(
                    AuthorityReader().task_ticket_state(first.ticket_id),
                    "ISSUED",
                )

    def test_task_ticket_finishes_exactly_once_through_a_lease(self) -> None:
        now = [datetime(2026, 7, 26, 6, 30, tzinfo=timezone.utc)]
        actor = Actor("operator", "human", "invocation-task-finish")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )
        task_spec = {
            "task_id": "P0R2-T1-TASK-FINISH",
            "objective": "Prove one terminal task transition.",
            "dependencies": [],
            "idempotency_key": "p0r2-task-finish-001",
            "task_spec_ref": "research_state/control_plane/p0r2/task_specs/finish.json",
            "task_spec_sha256": "d" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_automation/control_plane/stores.py"],
            "forbidden_files": ["data/"],
            "baseline_ref": "research_state/control_plane/p0r2/baseline.json",
            "baseline_sha256": "e" * 64,
            "input_evidence_refs": [],
        }

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now[0],
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now[0] + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                ticket = authority._issue_task_ticket(
                    grant,
                    task_spec,
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                lease = authority._begin_task(ticket)
                self.assertEqual(
                    lease.allowed_side_effects,
                    (SideEffect.WRITE_CONTROL_PLANE,),
                )
                now[0] -= timedelta(minutes=1)
                with self.assertRaises(stores_module.TaskTicketStateError):
                    authority._finish_task(
                        lease,
                        outcome="SUCCEEDED",
                        evidence_ref="evidence/task-finish.json",
                    )
                self.assertEqual(
                    AuthorityReader().task_ticket_state(ticket.ticket_id),
                    "IN_PROGRESS",
                )

                now[0] += timedelta(minutes=2)
                snapshot = authority._finish_task(
                    lease,
                    outcome="SUCCEEDED",
                    evidence_ref="evidence/task-finish.json",
                )

                with self.assertRaises(stores_module.TaskTicketStateError):
                    authority._finish_task(
                        lease,
                        outcome="SUCCEEDED",
                        evidence_ref="evidence/task-finish.json",
                    )

                self.assertEqual(snapshot.state, "SUCCEEDED")
                self.assertEqual(snapshot.ticket_id, ticket.ticket_id)
                self.assertEqual(
                    AuthorityReader().task_ticket_state(ticket.ticket_id),
                    "SUCCEEDED",
                )

    def test_trusted_task_receipt_is_idempotent_by_content(self) -> None:
        now = datetime(2026, 7, 26, 7, 0, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-receipt")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )
        task_spec = {
            "task_id": "P0R2-T1-RECEIPT",
            "objective": "Record one trusted receipt.",
            "dependencies": [],
            "idempotency_key": "p0r2-receipt-001",
            "task_spec_ref": "research_state/control_plane/p0r2/task_specs/receipt.json",
            "task_spec_sha256": "d" * 64,
            "requirements": {
                "required_test_receipt_ids": ["store-tests"],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_automation/control_plane/stores.py"],
            "forbidden_files": ["data/"],
            "baseline_ref": "research_state/control_plane/p0r2/baseline.json",
            "baseline_sha256": "e" * 64,
            "input_evidence_refs": [],
        }
        receipt = {
            "receipt_id": "store-tests",
            "command": "python -m unittest tests.test_control_plane_stores -v",
            "exit_code": 0,
            "result": "PASS",
        }
        issuer = Actor(
            "trusted-test-runner",
            "automation",
            "invocation-store-tests",
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                ticket = authority._issue_task_ticket(
                    grant,
                    task_spec,
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                lease = authority._begin_task(ticket)

                with self.assertRaises(stores_module.TaskTicketError):
                    authority._record_task_receipt(
                        lease,
                        attestation=object(),
                    )
                with self.assertRaises(stores_module.TaskTicketError):
                    authority._attest_task_receipt(
                        lease,
                        receipt_kind="TEST",
                        issuer=actor,
                        payload=receipt,
                    )

                attestation = authority._attest_task_receipt(
                    lease,
                    receipt_kind="TEST",
                    issuer=issuer,
                    payload=receipt,
                )
                forged_attestation = stores_module.TrustedReceiptAttestation(
                    ticket_id=attestation.ticket_id,
                    receipt_kind=attestation.receipt_kind,
                    receipt_id=attestation.receipt_id,
                    issuer=attestation.issuer,
                    payload_json=attestation.payload_json,
                    payload_sha256=attestation.payload_sha256,
                    attestation_sha256="0" * 64,
                )
                with self.assertRaises(stores_module.TaskTicketError):
                    authority._record_task_receipt(
                        lease,
                        attestation=forged_attestation,
                    )
                authority._record_task_receipt(lease, attestation=attestation)
                authority._record_task_receipt(lease, attestation=attestation)
                changed = dict(receipt)
                changed["result"] = "FAIL"
                changed_attestation = authority._attest_task_receipt(
                    lease,
                    receipt_kind="TEST",
                    issuer=issuer,
                    payload=changed,
                )
                with self.assertRaises(stores_module.TrustedReceiptConflictError):
                    authority._record_task_receipt(
                        lease,
                        attestation=changed_attestation,
                    )

                self.assertEqual(
                    AuthorityReader().trusted_receipt_count(ticket.ticket_id),
                    1,
                )
                in_doubt = authority._mark_task_in_doubt(
                    ticket.ticket_id,
                    evidence_ref="evidence/task-crash-reconciliation.json",
                )
                self.assertEqual(in_doubt.state, "IN_DOUBT")
                self.assertEqual(
                    AuthorityReader().task_ticket_state(ticket.ticket_id),
                    "IN_DOUBT",
                )

    def test_task_report_binding_is_cross_checked_against_authority(self) -> None:
        now = [datetime(2026, 7, 26, 7, 30, tzinfo=timezone.utc)]
        actor = Actor("operator", "human", "invocation-report-binding")
        receipt_issuer = Actor(
            "trusted-evidence-runner",
            "automation",
            "invocation-report-evidence",
        )
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )
        test_receipt = {
            "receipt_id": "store-tests",
            "command": "python -m unittest tests.test_control_plane_stores -v",
            "exit_code": 0,
            "result": "PASS",
        }
        input_evidence = {
            "evidence_id": "p0r2-baseline",
            "evidence_ref": "research_state/control_plane/p0r2/baseline.json",
            "evidence_sha256": "f" * 64,
            "status": "VERIFIED",
        }
        task_spec = {
            "task_id": "P0R2-T1-REPORT-BINDING",
            "objective": "Cross-check TaskReport against trusted authority.",
            "dependencies": [],
            "idempotency_key": "p0r2-report-binding-001",
            "task_spec_ref": "research_state/control_plane/p0r2/task_specs/report.json",
            "task_spec_sha256": "d" * 64,
            "requirements": {
                "required_test_receipt_ids": ["store-tests"],
                "required_review_receipt_ids": [],
                "required_evidence_ids": ["p0r2-baseline"],
            },
            "allowed_files": ["research_automation/control_plane/stores.py"],
            "forbidden_files": ["data/"],
            "baseline_ref": "research_state/control_plane/p0r2/baseline.json",
            "baseline_sha256": "e" * 64,
            "input_evidence_refs": [input_evidence],
        }

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority_path = root / "authority.sqlite3"
            operational_path = root / "operational.sqlite3"
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=authority_path,
                _OPERATIONAL_STORE_PATH=operational_path,
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now[0],
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now[0] + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                ticket = authority._issue_task_ticket(
                    grant,
                    task_spec,
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                lease = authority._begin_task(ticket)
                test_attestation = authority._attest_task_receipt(
                    lease,
                    receipt_kind="TEST",
                    issuer=receipt_issuer,
                    payload=test_receipt,
                )
                evidence_attestation = authority._attest_task_receipt(
                    lease,
                    receipt_kind="EVIDENCE",
                    issuer=receipt_issuer,
                    payload=input_evidence,
                )
                authority._record_task_receipt(
                    lease,
                    attestation=test_attestation,
                )
                authority._record_task_receipt(
                    lease,
                    attestation=evidence_attestation,
                )
                started_at = now[0]
                now[0] += timedelta(minutes=1)
                authority._finish_task(
                    lease,
                    outcome="SUCCEEDED",
                    evidence_ref="evidence/report-binding.json",
                )

                report = build_task_report_v2(
                    {
                        "plan_version": "V3.4.2-P0R2",
                        "phase": "P0",
                        "task_id": task_spec["task_id"],
                        "attempt_id": "p0r2-attempt-001",
                        "authorization_ref": envelope.authorization_ref,
                        "ticket_id": ticket.ticket_id,
                        "identity_binding": {
                            "plan_hash": identity.plan_hash,
                            "scope_hash": identity.scope_hash,
                            "instruction_policy_hash": (
                                identity.instruction_policy_hash
                            ),
                        },
                        "objective": task_spec["objective"],
                        "dependencies": task_spec["dependencies"],
                        "idempotency_key": task_spec["idempotency_key"],
                        "task_spec_ref": task_spec["task_spec_ref"],
                        "task_spec_sha256": task_spec["task_spec_sha256"],
                        "requirements": task_spec["requirements"],
                        "allowed_files": task_spec["allowed_files"],
                        "forbidden_files": task_spec["forbidden_files"],
                        "baseline_ref": task_spec["baseline_ref"],
                        "baseline_sha256": task_spec["baseline_sha256"],
                        "input_evidence_refs": task_spec["input_evidence_refs"],
                        "test_receipts": [test_receipt],
                        "review_receipts": [],
                        "review_findings": [],
                        "changed_files": [],
                        "external_invocations": [],
                        "side_effect_summary": {
                            "observed": [],
                            "unauthorized": [],
                        },
                        "ticket_state": "SUCCEEDED",
                        "started_at": started_at.isoformat(),
                        "completed_at": now[0].isoformat(),
                    }
                )
                binding = AuthorityReader().verify_task_report_binding(report)

                self.assertEqual(binding.ticket_id, ticket.ticket_id)
                self.assertEqual(binding.actor_id, actor.actor_id)
                self.assertEqual(binding.invocation_id, actor.invocation_id)
                self.assertEqual(
                    binding.allowed_side_effects,
                    (SideEffect.WRITE_CONTROL_PLANE,),
                )
                self.assertEqual(
                    binding.report_payload_sha256,
                    report["report_payload_sha256"],
                )
                self.assertEqual(
                    binding.terminal_evidence_ref,
                    "evidence/report-binding.json",
                )
                forged_draft = dict(report)
                for computed_field in (
                    "schema_version",
                    "unexpected_changes",
                    "outcome",
                    "reason_codes",
                    "report_payload_sha256",
                ):
                    forged_draft.pop(computed_field)
                forged_draft["objective"] = "Untrusted changed objective."
                forged_report = build_task_report_v2(forged_draft)
                with self.assertRaises(stores_module.TaskReportAuthorityError):
                    AuthorityReader().verify_task_report_binding(forged_report)

    def test_reviewed_entry_policy_activation_is_cas_bound_and_outboxed(self) -> None:
        now = datetime(2026, 7, 28, 8, 30, tzinfo=timezone.utc)
        actor = Actor("operator", "human", "invocation-policy-publish")
        reviewer = Actor("independent-reviewer", "llm", "review-policy-001")
        identity = AuthorityIdentity(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            instruction_policy_hash="c" * 64,
        )
        task_spec = {
            "task_id": "P0R2-T8-POLICY-ACTIVATE",
            "objective": "Activate one independently reviewed entry policy.",
            "dependencies": [],
            "idempotency_key": "p0r2-policy-activate-001",
            "task_spec_ref": "research_state/control_plane/p0r2/task_specs/policy.json",
            "task_spec_sha256": "d" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_state/control_plane/policies/"],
            "forbidden_files": ["data/"],
            "baseline_ref": "research_state/control_plane/p0r2/baseline.json",
            "baseline_sha256": "e" * 64,
            "input_evidence_refs": [],
        }

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.multiple(
                stores_module,
                _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
                _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
            ):
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET,
                    clock=lambda: now,
                )
                envelope = authority._provision_authorization(
                    phase=Phase.P0,
                    attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                    expires_at=now + timedelta(hours=1),
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                grant = authority.claim_authorization(
                    envelope,
                    expected_phase=Phase.P0,
                    expected_attempt_id="p0r2-attempt-001",
                    actor=actor,
                    identity=identity,
                )
                ticket = authority._issue_task_ticket(
                    grant,
                    task_spec,
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                lease = authority._begin_task(ticket)
                self.assertIsNone(ticket.entry_policy_sha256)
                self.assertIsNone(lease.entry_policy_sha256)
                before_outbox = AuthorityReader().pending_outbox_count()

                activated = authority._activate_reviewed_entry_policy(
                    lease,
                    reviewer=reviewer,
                    policy_sha256="1" * 64,
                    policy_payload_sha256="2" * 64,
                    inventory_payload_sha256="3" * 64,
                    review_receipt_sha256="4" * 64,
                    expected_active_sha256=None,
                )
                active = AuthorityReader().active_entry_policy()
                gate_snapshot = AuthorityReader().phase_gate_snapshot(
                    Phase.P0,
                    "p0r2-attempt-001",
                )

                self.assertEqual(active, activated)
                self.assertEqual(
                    gate_snapshot.active_entry_policy_sha256,
                    activated.policy_sha256,
                )
                self.assertEqual(active.policy_sha256, "1" * 64)
                self.assertEqual(active.reviewer, reviewer)
                self.assertEqual(
                    AuthorityReader().pending_outbox_count(),
                    before_outbox + 1,
                )
                next_task_spec = dict(task_spec)
                next_task_spec["task_id"] = "P0R2-T8-POST-POLICY"
                next_task_spec["idempotency_key"] = (
                    "p0r2-post-policy-ticket-001"
                )
                next_task_spec["task_spec_ref"] = (
                    "research_state/control_plane/p0r2/task_specs/"
                    "post-policy.json"
                )
                next_ticket = authority._issue_task_ticket(
                    grant,
                    next_task_spec,
                    allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
                )
                next_lease = authority._begin_task(next_ticket)
                lease_binding = AuthorityReader().execution_lease_binding(
                    next_lease
                )
                self.assertEqual(
                    next_ticket.entry_policy_sha256,
                    activated.policy_sha256,
                )
                self.assertEqual(
                    next_lease.entry_policy_sha256,
                    activated.policy_sha256,
                )
                self.assertEqual(
                    lease_binding.entry_policy_sha256,
                    activated.policy_sha256,
                )
                with self.assertRaises(
                    stores_module.EntryPolicyActivationConflictError
                ):
                    authority._activate_reviewed_entry_policy(
                        lease,
                        reviewer=reviewer,
                        policy_sha256="5" * 64,
                        policy_payload_sha256="6" * 64,
                        inventory_payload_sha256="7" * 64,
                        review_receipt_sha256="8" * 64,
                        expected_active_sha256=None,
                    )


class OperationalProvisioningTests(unittest.TestCase):
    """P0-CR-008 slice B/C: Operational journal is provisioned in WAL mode."""

    def test_operational_store_is_provisioned_in_wal_mode(self) -> None:
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
                operational_connection = sqlite3.connect(operational_path)
                authority_connection = sqlite3.connect(authority_path)
                try:
                    operational_mode = operational_connection.execute(
                        "PRAGMA journal_mode"
                    ).fetchone()[0]
                    authority_mode = authority_connection.execute(
                        "PRAGMA journal_mode"
                    ).fetchone()[0]
                finally:
                    operational_connection.close()
                    authority_connection.close()
            self.assertEqual(operational_mode, "wal")
            self.assertEqual(authority_mode, "delete")


class FinalEvalAuthorityMigrationTests(unittest.TestCase):
    """P0-CR-008 slice B: Authority v1 -> v2 migration contracts."""

    def _provision_v1_authority_pair(
        self,
        authority_path: Path,
        operational_path: Path,
        *,
        installation_id: str = "a" * 64,
    ) -> None:
        original_schema = stores_module._AUTHORITY_SCHEMA
        original_version = stores_module._AUTHORITY_SCHEMA_VERSION
        try:
            stores_module._AUTHORITY_SCHEMA = stores_module._AUTHORITY_SCHEMA_V1
            stores_module._AUTHORITY_SCHEMA_VERSION = 1
            stores_module._expected_schema_sha256.cache_clear()
            stores_module._provision_store(
                authority_path,
                store_kind="AUTHORITY_STORE",
                metadata_table="authority_meta",
                installation_id=installation_id,
                root_capability_sha256=stores_module._root_secret_sha256(
                    ROOT_SECRET
                ),
            )
        finally:
            stores_module._AUTHORITY_SCHEMA = original_schema
            stores_module._AUTHORITY_SCHEMA_VERSION = original_version
            stores_module._expected_schema_sha256.cache_clear()
        stores_module._provision_store(
            operational_path,
            store_kind="OPERATIONAL_JOURNAL",
            metadata_table="operational_meta",
            installation_id=installation_id,
            root_capability_sha256=stores_module._root_secret_sha256(ROOT_SECRET),
        )

    def test_authority_v1_to_v2_preserves_all_rows_and_hashes(self) -> None:
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
                self._provision_v1_authority_pair(
                    authority_path, operational_path
                )
                created_at = datetime(
                    2026, 8, 11, 1, 0, tzinfo=timezone.utc
                ).isoformat()
                connection = sqlite3.connect(authority_path)
                try:
                    connection.execute(
                        """INSERT INTO authority_outbox
                        (event_id, event_type, aggregate_id, payload_json,
                         payload_sha256, event_sha256, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (
                            "legacy-outbox-event",
                            "LEGACY",
                            "legacy",
                            "{}",
                            "b" * 64,
                            "c" * 64,
                            created_at,
                        ),
                    )
                    connection.commit()
                    before = connection.execute(
                        "SELECT * FROM authority_outbox WHERE event_id = ?",
                        ("legacy-outbox-event",),
                    ).fetchone()
                finally:
                    connection.close()
                self.assertTrue(
                    stores_module._migrate_authority_v2(
                        root_secret=ROOT_SECRET
                    )
                )
                self.assertFalse(
                    stores_module._migrate_authority_v2(
                        root_secret=ROOT_SECRET
                    )
                )
                connection = sqlite3.connect(authority_path)
                try:
                    version = connection.execute(
                        "PRAGMA user_version"
                    ).fetchone()[0]
                    metadata_version = connection.execute(
                        "SELECT value FROM authority_meta WHERE key = 'schema_version'"
                    ).fetchone()[0]
                    table = connection.execute(
                        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                        "AND name = 'final_eval_authorizations_v1'"
                    ).fetchone()
                    after = connection.execute(
                        "SELECT * FROM authority_outbox WHERE event_id = ?",
                        ("legacy-outbox-event",),
                    ).fetchone()
                finally:
                    connection.close()
                self.assertEqual(version, 2)
                self.assertEqual(metadata_version, "2")
                self.assertIsNotNone(table)
                self.assertEqual(tuple(after), tuple(before))

    def test_authority_migration_is_atomic_on_failure(self) -> None:
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
                self._provision_v1_authority_pair(
                    authority_path, operational_path
                )
                original_schema = stores_module._AUTHORITY_SCHEMA
                try:
                    stores_module._AUTHORITY_SCHEMA = (
                        stores_module._AUTHORITY_SCHEMA_V1
                        + ("CREATE TABL invalid_final_eval(value TEXT)",)
                    )
                    with self.assertRaises(SqliteUnitOfWorkError):
                        stores_module._migrate_authority_v2(
                            root_secret=ROOT_SECRET
                        )
                finally:
                    stores_module._AUTHORITY_SCHEMA = original_schema
                    stores_module._expected_schema_sha256.cache_clear()
                connection = sqlite3.connect(authority_path)
                try:
                    version = connection.execute(
                        "PRAGMA user_version"
                    ).fetchone()[0]
                    metadata_version = connection.execute(
                        "SELECT value FROM authority_meta WHERE key = 'schema_version'"
                    ).fetchone()[0]
                    table = connection.execute(
                        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                        "AND name = 'final_eval_authorizations_v1'"
                    ).fetchone()
                finally:
                    connection.close()
                self.assertEqual(version, 1)
                self.assertEqual(metadata_version, "1")
                self.assertIsNone(table)

    def test_future_or_drifted_authority_schema_fails_closed(self) -> None:
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
                self._provision_v1_authority_pair(
                    authority_path, operational_path
                )
                connection = sqlite3.connect(authority_path)
                try:
                    connection.execute("PRAGMA user_version = 99")
                    connection.commit()
                finally:
                    connection.close()
                with self.assertRaises(stores_module.SqliteFutureSchemaError):
                    stores_module._migrate_authority_v2(
                        root_secret=ROOT_SECRET
                    )
                connection = sqlite3.connect(authority_path)
                try:
                    connection.execute("PRAGMA user_version = 1")
                    connection.execute(
                        "CREATE TABLE drift(value TEXT)"
                    )
                    connection.commit()
                finally:
                    connection.close()
                with self.assertRaises(SqliteSchemaError):
                    stores_module._migrate_authority_v2(
                        root_secret=ROOT_SECRET
                    )

    def test_wrong_root_or_installation_identity_rejects_migration(self) -> None:
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
                self._provision_v1_authority_pair(
                    authority_path, operational_path
                )
                with self.assertRaises(stores_module.AuthorityRootError):
                    stores_module._migrate_authority_v2(
                        root_secret="wrong-test-root-capability-0123456789abcdef"
                    )
                connection = sqlite3.connect(authority_path)
                try:
                    connection.execute(
                        "UPDATE authority_meta SET value = ? WHERE key = 'installation_id'",
                        ("f" * 64,),
                    )
                    connection.commit()
                finally:
                    connection.close()
                with self.assertRaisesRegex(
                    StoreConfigurationError,
                    "installation identities differ",
                ):
                    stores_module._migrate_authority_v2(
                        root_secret=ROOT_SECRET
                    )


class FinalEvalBindingContractTests(unittest.TestCase):
    """P0-CR-008 slice B: FinalEval Authority v2 binding uniqueness and CAS."""

    _SAFE_SNAPSHOT_FIELDS = frozenset(
        {
            "ticket_id",
            "request_sha256",
            "authority_plan_hash",
            "research_plan_sha256",
            "campaign_id",
            "campaign_sha256",
            "holdout_id",
            "holdout_sha256",
            "nonce_fingerprint",
            "saga_state",
            "saga_version",
            "result_object_ref",
            "result_object_sha256",
            "result_claim_ref",
            "result_claim_sha256",
            "terminal_binding",
            "created_at",
            "updated_at",
        }
    )

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
        self.now = datetime(2026, 8, 11, 1, 0, tzinfo=timezone.utc)
        self.authority = stores_module._AuthorityStore(
            root_secret=ROOT_SECRET, clock=lambda: self.now
        )
        self.actor = Actor(
            "final-eval-runner", "automation", "invocation-fe-tests"
        )
        self.identity = stores_module.AuthorityIdentity(
            "a" * 64, "b" * 64, "c" * 64
        )
        self.grant = self._grant("fe-attempt-001")

    def _grant(self, attempt_id: str) -> stores_module.AuthorityGrant:
        envelope = self.authority._provision_authorization(
            phase=Phase.P0,
            attempt_id=attempt_id,
            actor=self.actor,
            identity=self.identity,
            expires_at=self.now.replace(year=2027),
            allowed_side_effects=(
                SideEffect.READ,
                SideEffect.WRITE_CONTROL_PLANE,
            ),
        )
        return self.authority.claim_authorization(
            envelope,
            expected_phase=Phase.P0,
            expected_attempt_id=attempt_id,
            actor=self.actor,
            identity=self.identity,
        )

    def _begin(
        self,
        grant: stores_module.AuthorityGrant,
        *,
        nonce: str = "nonce-001",
        plan: str = "1" * 64,
        holdout_id: str = "holdout-a",
        holdout_sha: str = "2" * 64,
        campaign_id: str = "campaign-a",
        campaign_sha: str = "3" * 64,
        idempotency_key: str = "fe-bind-001",
        task_spec_ref: str = (
            "research_state/control_plane/p8/task_specs/final-eval.json"
        ),
        task_spec_sha: str = "4" * 64,
    ):
        return self.authority._begin_final_eval_binding(
            grant,
            research_plan_sha256=plan,
            campaign_id=campaign_id,
            campaign_sha256=campaign_sha,
            holdout_id=holdout_id,
            holdout_sha256=holdout_sha,
            nonce=nonce,
            idempotency_key=idempotency_key,
            task_spec_ref=task_spec_ref,
            task_spec_sha256=task_spec_sha,
        )

    def _stage_verified(self, binding_id, *, expected_version=3):
        """CR010-R02: stage with REAL committed object + fixed claim.

        Writes + commits the content-addressed object and the per-ticket
        fixed claim in the fixture root's git repo, then stages through the
        Authority CAS with the verified refs.
        """
        from research_automation.control_plane.final_eval_evidence import (
            FinalEvalResultPublisher,
        )

        root = Path(self.temporary.name)
        if not (root / ".git").exists():
            subprocess.run(
                ["git", "init", "--quiet"], cwd=root, check=True,
                capture_output=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Control Plane Tests"],
                cwd=root, check=True, capture_output=True,
            )
            subprocess.run(
                ["git", "config", "user.email",
                 "control-plane@example.invalid"],
                cwd=root, check=True, capture_output=True,
            )
        publisher = FinalEvalResultPublisher(
            repository_root=root,
            evidence_volume=(
                "research_state/control_plane/p8/evidence/"
                "final_eval_cr010_test"
            ),
        )
        refs = publisher.publish(
            binding_id,
            binding_id,
            {"binding_id": binding_id, "outcome": "SUCCEEDED"},
            outcome="SUCCEEDED",
        )
        return self.authority._stage_final_eval_result(
            binding_id,
            expected_version=expected_version,
            result_object_ref=refs.object_ref,
            result_object_sha256=refs.object_sha256,
            result_claim_ref=refs.claim_ref,
            result_claim_sha256=refs.claim_sha256,
            repository_root=root,
        )

    def tearDown(self) -> None:
        self.paths.stop()
        stores_module._expected_schema_sha256.cache_clear()
        self.temporary.cleanup()

    def test_final_eval_nonce_fingerprint_is_globally_unique(self) -> None:
        self._begin(self.grant, nonce="nonce-x", plan="1" * 64)
        with self.assertRaisesRegex(
            stores_module.FinalEvalBindingConflictError,
            "nonce",
        ):
            self._begin(
                self.grant,
                nonce="nonce-x",
                plan="9" * 64,
                holdout_id="holdout-other",
                holdout_sha="8" * 64,
                idempotency_key="fe-bind-002",
            )
        self._begin(
            self.grant,
            nonce="nonce-y",
            plan="9" * 64,
            holdout_id="holdout-other",
            holdout_sha="8" * 64,
            idempotency_key="fe-bind-003",
        )

    def test_same_plan_holdout_id_is_rejected_across_grants(self) -> None:
        self._begin(self.grant, plan="1" * 64, holdout_id="holdout-a")
        other_grant = self._grant("fe-attempt-002")
        with self.assertRaisesRegex(
            stores_module.FinalEvalBindingConflictError,
            "holdout",
        ):
            self._begin(
                other_grant,
                nonce="nonce-other",
                plan="1" * 64,
                holdout_id="holdout-a",
                idempotency_key="fe-bind-004",
            )
        self._begin(
            other_grant,
            nonce="nonce-other",
            plan="1" * 64,
            holdout_id="holdout-b",
            holdout_sha="5" * 64,
            idempotency_key="fe-bind-005",
        )

    def test_same_plan_holdout_hash_is_rejected_with_new_nonce_actor_or_invocation(
        self,
    ) -> None:
        self._begin(
            self.grant,
            plan="1" * 64,
            holdout_id="holdout-a",
            holdout_sha="2" * 64,
        )
        other_grant = self._grant("fe-attempt-003")
        with self.assertRaisesRegex(
            stores_module.FinalEvalBindingConflictError,
            "holdout",
        ):
            self._begin(
                other_grant,
                nonce="nonce-new-invocation",
                plan="1" * 64,
                holdout_id="holdout-renamed",
                holdout_sha="2" * 64,
                idempotency_key="fe-bind-006",
            )

    def test_plaintext_nonce_never_appears_in_db_outbox_log_or_evidence(
        self,
    ) -> None:
        self._begin(self.grant, nonce="secret-nonce-7f3a")
        connection = sqlite3.connect(self.authority_path)
        try:
            outbox_payloads = "\n".join(
                str(row[0])
                for row in connection.execute(
                    "SELECT payload_json FROM authority_outbox"
                ).fetchall()
            )
        finally:
            connection.close()
        self.assertNotIn("secret-nonce-7f3a", outbox_payloads)
        self.assertNotIn(b"secret-nonce-7f3a", self.authority_path.read_bytes())
        self.assertNotIn(
            b"secret-nonce-7f3a", self.operational_path.read_bytes()
        )

    def test_final_eval_binding_state_machine_rejects_skip_and_backward_cas(
        self,
    ) -> None:
        receipt = self._begin(self.grant)
        binding = receipt.binding
        self.assertEqual(binding.saga_state, "CONSUMED")
        self.assertEqual(binding.saga_version, 2)
        with self.assertRaises(stores_module.FinalEvalBindingStateError):
            self.authority._advance_final_eval_binding(
                binding.ticket_id,
                expected_state="AUTHORIZED",
                next_state="EVALUATING",
                expected_version=1,
            )
        with self.assertRaises(stores_module.FinalEvalBindingStateError):
            self.authority._advance_final_eval_binding(
                binding.ticket_id,
                expected_state="CONSUMED",
                next_state="AUTHORIZED",
                expected_version=2,
            )
        with self.assertRaises(stores_module.FinalEvalBindingStateError):
            self.authority._advance_final_eval_binding(
                binding.ticket_id,
                expected_state="CONSUMED",
                next_state="RESULT_STAGED",
                expected_version=2,
            )
        with self.assertRaises(stores_module.FinalEvalBindingStateError):
            self.authority._advance_final_eval_binding(
                binding.ticket_id,
                expected_state="CONSUMED",
                next_state="EVALUATING",
                expected_version=999,
            )
        advanced = self.authority._advance_final_eval_binding(
            binding.ticket_id,
            expected_state="CONSUMED",
            next_state="EVALUATING",
            expected_version=2,
        )
        self.assertEqual(advanced.saga_state, "EVALUATING")
        self.assertEqual(advanced.saga_version, 3)

    def test_result_claim_is_create_once_and_bound_to_one_ticket(self) -> None:
        receipt = self._begin(self.grant)
        binding_id = receipt.binding.ticket_id
        self.authority._advance_final_eval_binding(
            binding_id,
            expected_state="CONSUMED",
            next_state="EVALUATING",
            expected_version=2,
        )
        staged = self._stage_verified(binding_id)
        self.assertEqual(staged.saga_state, "RESULT_STAGED")
        self.assertEqual(staged.saga_version, 4)
        with self.assertRaises(stores_module.FinalEvalBindingStateError):
            self.authority._stage_final_eval_result(
                binding_id,
                expected_version=4,
                result_object_ref=(
                    "research_state/control_plane/p8/evidence/other-object.json"
                ),
                result_object_sha256="c" * 64,
                result_claim_ref=(
                    "research_state/control_plane/p8/evidence/result-claim.json"
                ),
                result_claim_sha256="b" * 64,
                repository_root=Path(self.temporary.name),
            )
        second = self._begin(
            self.grant,
            nonce="nonce-second",
            plan="7" * 64,
            holdout_id="holdout-c",
            holdout_sha="6" * 64,
            idempotency_key="fe-bind-007",
        )
        second_id = second.binding.ticket_id
        self.authority._advance_final_eval_binding(
            second_id,
            expected_state="CONSUMED",
            next_state="EVALUATING",
            expected_version=2,
        )
        # CR010-R02: the claim is the ticket's UNIQUE fixed claim path; a
        # second ticket cannot reuse a foreign claim (verification fails
        # closed before any CAS write).
        from research_automation.control_plane.final_eval_evidence import (
            FinalEvalEvidenceError,
        )

        with self.assertRaises(FinalEvalEvidenceError):
            self.authority._stage_final_eval_result(
                second_id,
                expected_version=3,
                result_object_ref=(
                    "research_state/control_plane/p8/evidence/second-object.json"
                ),
                result_object_sha256="d" * 64,
                result_claim_ref=(
                    "research_state/control_plane/p8/evidence/result-claim.json"
                ),
                result_claim_sha256="b" * 64,
                repository_root=Path(self.temporary.name),
            )

    def test_staging_without_repository_root_fails_closed(self) -> None:
        """CR010-R02: the Authority CAS refuses to stage a result when it
        cannot verify the committed object/claim (no repository root)."""
        receipt = self._begin(self.grant)
        binding_id = receipt.binding.ticket_id
        self.authority._advance_final_eval_binding(
            binding_id,
            expected_state="CONSUMED",
            next_state="EVALUATING",
            expected_version=2,
        )
        with self.assertRaises(stores_module.FinalEvalBindingError):
            self.authority._stage_final_eval_result(
                binding_id,
                expected_version=3,
                result_object_ref=(
                    "research_state/control_plane/p8/evidence/result-object.json"
                ),
                result_object_sha256="a" * 64,
                result_claim_ref=(
                    "research_state/control_plane/p8/evidence/result-claim.json"
                ),
                result_claim_sha256="b" * 64,
            )
        observed = self.authority.final_eval_binding_snapshot(binding_id)
        self.assertEqual(observed.saga_state, "EVALUATING")

    def test_recovery_scan_returns_safe_bindings_without_secret_or_holdout_path(
        self,
    ) -> None:
        self._begin(self.grant, nonce="nonce-scan-1")
        self._begin(
            self.grant,
            nonce="nonce-scan-2",
            plan="5" * 64,
            holdout_id="holdout-d",
            holdout_sha="4" * 64,
            idempotency_key="fe-bind-008",
        )
        snapshots = self.authority._scan_final_eval_bindings(
            states=("CONSUMED",)
        )
        self.assertEqual(len(snapshots), 2)
        for snapshot in snapshots:
            fields = asdict(snapshot)
            self.assertEqual(
                set(fields), FinalEvalBindingContractTests._SAFE_SNAPSHOT_FIELDS
            )
            for value in fields.values():
                if isinstance(value, str):
                    self.assertNotIn("nonce-scan", value)
                    self.assertNotIn("\\", value)
                    self.assertNotIn(":", value)
            self.assertNotIn("nonce", fields)
            self.assertNotIn("secret", fields)
        unrestricted = self.authority._scan_final_eval_bindings()
        self.assertEqual(len(unrestricted), 2)

    def test_recovery_lease_can_close_but_cannot_open_or_reissue_holdout(
        self,
    ) -> None:
        receipt = self._begin(self.grant)
        binding_id = receipt.binding.ticket_id
        self.authority._advance_final_eval_binding(
            binding_id,
            expected_state="CONSUMED",
            next_state="EVALUATING",
            expected_version=2,
        )
        self._stage_verified(binding_id)
        maintenance_grant = self._grant("fe-maintenance-attempt")
        task_spec = {
            "task_id": "P8-MAINTENANCE-RECOVERY",
            "objective": "recover a staged final-eval binding",
            "dependencies": [],
            "idempotency_key": "fe-recovery-ticket-001",
            "task_spec_ref": (
                "research_state/control_plane/p8/task_specs/recovery.json"
            ),
            "task_spec_sha256": "d" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_state/control_plane/p8/"],
            "forbidden_files": ["data/"],
            "baseline_ref": (
                "research_state/control_plane/p8/baselines/recovery.json"
            ),
            "baseline_sha256": "c" * 64,
            "input_evidence_refs": [],
        }
        maintenance_ticket = self.authority._issue_task_ticket(
            maintenance_grant,
            task_spec,
            allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
        )
        maintenance_lease = self.authority._begin_task(maintenance_ticket)
        recovery = self.authority._issue_final_eval_recovery_lease(
            maintenance_lease,
            binding_id=binding_id,
            evidence_ref=(
                "research_state/control_plane/p8/evidence/recovery.json"
            ),
        )
        terminal = self.authority._recover_final_eval_binding(
            recovery,
            terminal_state="SUCCEEDED",
            evidence_ref=(
                "research_state/control_plane/p8/evidence/closure.json"
            ),
        )
        self.assertEqual(terminal.saga_state, "AUTHORITY_TERMINAL")
        self.assertEqual(terminal.terminal_binding, "SUCCEEDED")
        self.assertEqual(
            AuthorityReader()
            .execution_lease_binding(maintenance_lease)
            .lease_id,
            maintenance_lease.lease_id,
        )
        connection = sqlite3.connect(self.authority_path)
        try:
            maintenance_state = connection.execute(
                "SELECT state FROM task_tickets_v2 WHERE ticket_id = ?",
                (maintenance_lease.ticket_id,),
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(maintenance_state, "IN_PROGRESS")
        with self.assertRaises(stores_module.FinalEvalRecoveryError):
            self.authority._recover_final_eval_binding(
                recovery,
                terminal_state="SUCCEEDED",
                evidence_ref=(
                    "research_state/control_plane/p8/evidence/closure.json"
                ),
            )
        with self.assertRaisesRegex(
            stores_module.FinalEvalBindingConflictError,
            "holdout",
        ):
            self._begin(
                self.grant,
                nonce="nonce-reissue-attempt",
                plan="1" * 64,
                holdout_id="holdout-a",
                idempotency_key="fe-bind-009",
            )

    def test_original_task_lease_secret_is_not_required_after_crash(self) -> None:
        receipt = self._begin(self.grant)
        binding_id = receipt.binding.ticket_id
        self.authority._advance_final_eval_binding(
            binding_id,
            expected_state="CONSUMED",
            next_state="EVALUATING",
            expected_version=2,
        )
        self._stage_verified(binding_id)
        fresh_authority = stores_module._AuthorityStore(
            root_secret=ROOT_SECRET, clock=lambda: self.now
        )
        maintenance_grant = self._grant("fe-maintenance-attempt-2")
        task_spec = {
            "task_id": "P8-MAINTENANCE-RECOVERY-2",
            "objective": "recover without the original lease secret",
            "dependencies": [],
            "idempotency_key": "fe-recovery-ticket-002",
            "task_spec_ref": (
                "research_state/control_plane/p8/task_specs/recovery.json"
            ),
            "task_spec_sha256": "e" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_state/control_plane/p8/"],
            "forbidden_files": ["data/"],
            "baseline_ref": (
                "research_state/control_plane/p8/baselines/recovery.json"
            ),
            "baseline_sha256": "b" * 64,
            "input_evidence_refs": [],
        }
        maintenance_ticket = fresh_authority._issue_task_ticket(
            maintenance_grant,
            task_spec,
            allowed_side_effects=(SideEffect.WRITE_CONTROL_PLANE,),
        )
        maintenance_lease = fresh_authority._begin_task(maintenance_ticket)
        recovery = fresh_authority._issue_final_eval_recovery_lease(
            maintenance_lease,
            binding_id=binding_id,
            evidence_ref=(
                "research_state/control_plane/p8/evidence/recovery.json"
            ),
        )
        terminal = fresh_authority._recover_final_eval_binding(
            recovery,
            terminal_state="IN_DOUBT",
            evidence_ref=(
                "research_state/control_plane/p8/evidence/closure.json"
            ),
        )
        self.assertEqual(terminal.saga_state, "AUTHORITY_TERMINAL")
        self.assertEqual(terminal.terminal_binding, "IN_DOUBT")


if __name__ == "__main__":
    unittest.main()
