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


if __name__ == "__main__":
    unittest.main()
