from __future__ import annotations

import math
import unittest

from research_automation.control_plane import contracts as control_contracts
from research_automation.control_plane.contracts import (
    Actor,
    Phase,
    PolicyBinding,
    PolicyMismatchError,
    SideEffect,
    canonical_plan_scope_sha256,
    canonical_sha256,
    resolve_policy_source,
)


class CanonicalContractTests(unittest.TestCase):
    def test_canonical_hash_ignores_mapping_order(self) -> None:
        left = {"strategy": "brick", "scope": {"end": "2026-07-08", "start": "2020-01-01"}}
        right = {"scope": {"start": "2020-01-01", "end": "2026-07-08"}, "strategy": "brick"}

        self.assertEqual(canonical_sha256(left), canonical_sha256(right))

    def test_canonical_hash_normalizes_windows_path(self) -> None:
        windows = {"artifact_path": r"D:\research\run\result.json"}
        portable = {"artifact_path": "D:/research/run/result.json"}

        self.assertEqual(canonical_sha256(windows), canonical_sha256(portable))

    def test_canonical_hash_rejects_non_finite_float(self) -> None:
        for value in (math.nan, math.inf, -math.inf):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    canonical_sha256({"metric": value})

    def test_canonical_hash_excludes_only_declared_dynamic_fields(self) -> None:
        before = {"plan": "P0", "created_at": "2026-07-25T00:00:00Z", "token_count": 10}
        after = {"plan": "P0", "created_at": "2026-07-25T01:00:00Z", "token_count": 20}

        self.assertNotEqual(canonical_sha256(before), canonical_sha256(after))
        self.assertEqual(
            canonical_plan_scope_sha256(before),
            canonical_plan_scope_sha256(after),
        )

    def test_canonical_hash_rejects_non_string_mapping_keys(self) -> None:
        with self.assertRaises(TypeError):
            canonical_sha256({1: "numeric", "1": "text"})

    def test_actor_requires_complete_audit_identity(self) -> None:
        with self.assertRaisesRegex(ValueError, "actor_id"):
            Actor(actor_id="", actor_type="human", invocation_id="inv-1")
        with self.assertRaisesRegex(ValueError, "actor_type"):
            Actor(actor_id="actor-1", actor_type="unknown", invocation_id="inv-1")

    def test_actor_normalizes_identity_whitespace(self) -> None:
        actor = Actor(
            actor_id="  actor-1  ",
            actor_type="human",
            invocation_id="  invocation-1  ",
        )
        self.assertEqual(actor.actor_id, "actor-1")
        self.assertEqual(actor.invocation_id, "invocation-1")

    def test_identity_binding_requires_exact_sha256_values(self) -> None:
        binding = control_contracts.IdentityBinding(
            plan_hash="A" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )

        self.assertEqual(binding.plan_hash, "a" * 64)
        with self.assertRaises(ValueError):
            control_contracts.IdentityBinding(
                plan_hash="public-name-not-a-digest",
                scope_hash="b" * 64,
                policy_hash="c" * 64,
            )

    def test_capability_repr_does_not_expose_bearer_secrets(self) -> None:
        actor = Actor("tester", "human", "inv-redacted-repr")
        binding = control_contracts.IdentityBinding(
            plan_hash="a" * 64,
            scope_hash="b" * 64,
            policy_hash="c" * 64,
        )
        grant_secret = "grant-secret-must-not-leak"
        ticket_secret = "ticket-secret-must-not-leak"
        lease_secret = "lease-secret-must-not-leak"
        grant = control_contracts.PhaseGrant(
            grant_id="grant-1",
            bearer_secret=grant_secret,
            authorization_ref="authorization-1",
            phase=Phase.P0,
            actor=actor,
            identity_binding=binding,
            allowed_side_effects=(SideEffect.READ,),
        )
        ticket = control_contracts.TaskTicket(
            ticket_id="ticket-1",
            bearer_secret=ticket_secret,
            grant_id=grant.grant_id,
            authorization_ref=grant.authorization_ref,
            entry_id="callable:p0:read",
            effect=SideEffect.READ,
            resource_scope="D:/control/read.json",
            idempotency_key="read-1",
            actor=actor,
            identity_binding=binding,
        )
        lease = control_contracts.SideEffectLease(
            lease_id="lease-1",
            bearer_secret=lease_secret,
            ticket_id=ticket.ticket_id,
            grant_id=grant.grant_id,
            authorization_ref=grant.authorization_ref,
            entry_id=ticket.entry_id,
            effect=ticket.effect,
            resource_scope=ticket.resource_scope,
            actor=actor,
            identity_binding=binding,
        )

        for capability, secret in (
            (grant, grant_secret),
            (ticket, ticket_secret),
            (lease, lease_secret),
        ):
            with self.subTest(capability=type(capability).__name__):
                self.assertNotIn(secret, repr(capability))

    def test_phase_and_side_effect_values_are_closed(self) -> None:
        self.assertEqual(Phase.P0.value, "P0")
        self.assertEqual(Phase.P8.value, "P8")
        self.assertEqual(
            [effect.value for effect in SideEffect],
            [
                "READ",
                "WRITE_STAGING",
                "WRITE_CONTROL_PLANE",
                "RUN_RESEARCH",
                "WRITE_KBASE",
                "OPEN_HOLDOUT",
                "GIT_MUTATION",
                "WRITE_PRODUCTION_DATA",
                "WRITE_PRODUCTION_CONFIG",
                "DELETE_PATH",
                "NETWORK_EGRESS",
                "START_SUBPROCESS",
                "START_BACKGROUND_WORK",
                "SEND_NOTIFICATION",
                "EXPOSE_SERVICE",
            ],
        )
        with self.assertRaises(ValueError):
            Phase("P9")

    def test_policy_mismatch_requires_explicit_canonical_source(self) -> None:
        with self.assertRaises(PolicyMismatchError):
            resolve_policy_source(invocation_sha256=None, workspace_sha256="disk-hash")

        with self.assertRaises(PolicyMismatchError):
            resolve_policy_source(
                invocation_sha256="a" * 64,
                workspace_sha256=None,
                canonical_source="invocation",
            )

        with self.assertRaises(PolicyMismatchError):
            resolve_policy_source(
                invocation_sha256="invocation-hash",
                workspace_sha256="disk-hash",
                canonical_source="invocation",
            )

        with self.assertRaisesRegex(PolicyMismatchError, "differ"):
            resolve_policy_source(
                invocation_sha256="a" * 64,
                workspace_sha256="b" * 64,
                canonical_source="invocation",
            )

        with self.assertRaises(PolicyMismatchError):
            resolve_policy_source(
                invocation_sha256="same-hash",
                workspace_sha256="same-hash",
                canonical_source="invocation",
            )

        binding = resolve_policy_source(
            invocation_sha256="A" * 64,
            workspace_sha256="a" * 64,
            canonical_source="invocation",
        )
        self.assertEqual(binding.source, "invocation")
        self.assertEqual(binding.sha256, "a" * 64)
        self.assertFalse(binding.workspace_mismatch)

    def test_policy_binding_rejects_invalid_direct_construction(self) -> None:
        with self.assertRaises(PolicyMismatchError):
            PolicyBinding(source="caller_choice", sha256="not-a-hash", workspace_mismatch=False)
        with self.assertRaises(PolicyMismatchError):
            PolicyBinding(source="invocation", sha256="a" * 64, workspace_mismatch=True)


if __name__ == "__main__":
    unittest.main()
