from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
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


if __name__ == "__main__":
    unittest.main()
