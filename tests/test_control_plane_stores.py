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
from threading import Barrier, BrokenBarrierError
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
                    stores_module._trusted_bootstrap()

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
                receipt = stores_module._trusted_bootstrap()

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
                receipt = stores_module._trusted_bootstrap()
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
                    stores_module._trusted_bootstrap()

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
                stores_module._trusted_bootstrap()
                before = {
                    path: (
                        hashlib.sha256(path.read_bytes()).hexdigest(),
                        path.stat().st_mtime_ns,
                    )
                    for path in (authority_path, operational_path)
                }

                with self.assertRaises(StoreAlreadyBootstrappedError):
                    stores_module._trusted_bootstrap()

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
                receipt = stores_module._trusted_bootstrap()
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
                stores_module._trusted_bootstrap()
                authority = stores_module._AuthorityStore(clock=lambda: now)
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
                stores_module._trusted_bootstrap()
                authority = stores_module._AuthorityStore(clock=lambda: now)
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
                    return ("SUCCESS", stores_module._trusted_bootstrap())
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
                stores_module._trusted_bootstrap()
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
                stores_module._trusted_bootstrap()
                authority = stores_module._AuthorityStore(clock=lambda: now)
                journal = stores_module._OperationalJournal(clock=lambda: now)
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
                stores_module._trusted_bootstrap()
                authority = stores_module._AuthorityStore(clock=lambda: now)
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

    def test_tampered_outbox_payload_is_not_mirrored(self) -> None:
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
                stores_module._trusted_bootstrap()
                authority = stores_module._AuthorityStore(clock=lambda: now)
                journal = stores_module._OperationalJournal(clock=lambda: now)
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
                stores_module._trusted_bootstrap()
                authority = stores_module._AuthorityStore(clock=lambda: now)
                journal = stores_module._OperationalJournal(clock=lambda: now)
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
                stores_module._trusted_bootstrap()
                authority = stores_module._AuthorityStore(clock=lambda: clock[0])
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
                stores_module._trusted_bootstrap()
                authority = stores_module._AuthorityStore(clock=lambda: now)
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


if __name__ == "__main__":
    unittest.main()
