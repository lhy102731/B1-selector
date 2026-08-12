"""Tests for the final evaluation terminal closure (P8R3 T5)."""

from __future__ import annotations

import unittest

from research_automation.control_plane.final_eval_closure import (
    FinalEvalClosureConflict,
    FinalEvalClosureRejected,
    close_completed_campaign,
)
from research_automation.control_plane import stores as stores_module
from tests.test_control_plane_campaign_store import (
    ROOT_SECRET,
    _authorized_campaign,
)

NOW = __import__("datetime").datetime(2026, 8, 12, 12, 0, tzinfo=__import__("datetime").timezone.utc)


def _claim(**overrides) -> dict:
    values = {
        "campaign_id": "campaign-final-1",
        "request_sha256": "a" * 64,
        "result_object_sha256": "b" * 64,
        "result_claim_sha256": "c" * 64,
        "verdict": "PASS",
        "evidence_ref": "research_state/control_plane/p8/attempts/p8-attempt-002/evidence/result.json",
    }
    values.update(overrides)
    return values


class FinalEvalClosureTests(unittest.TestCase):
    def test_close_appends_terminal_audit_and_closed_event(self) -> None:
        campaign_id = "campaign-closure-1"
        with _authorized_campaign(campaign_id) as (root, grant, journal):
            lease = self._lease(root, grant)
            payload = close_completed_campaign(
                lease=lease,
                campaign_id=campaign_id,
                fixed_claim=_claim(campaign_id=campaign_id),
                clock=lambda: NOW,
            )
            self.assertEqual(payload["promotion"], "MANUAL_ONLY")
            self.assertEqual(payload["campaign_id"], campaign_id)
            conn = __import__("sqlite3").connect(str(root / "operational.sqlite3"))
            row = conn.execute(
                "SELECT event_type FROM campaign_events WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
            conn.close()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "CAMPAIGN_CLOSED")

    def test_second_terminal_event_conflicts(self) -> None:
        campaign_id = "campaign-closure-2"
        with _authorized_campaign(campaign_id) as (root, grant, journal):
            lease = self._lease(root, grant)
            close_completed_campaign(
                lease=lease,
                campaign_id=campaign_id,
                fixed_claim=_claim(campaign_id=campaign_id),
                clock=lambda: NOW,
            )
            with self.assertRaises(FinalEvalClosureConflict):
                close_completed_campaign(
                    lease=lease,
                    campaign_id=campaign_id,
                    fixed_claim=_claim(campaign_id=campaign_id),
                    clock=lambda: NOW,
                )

    def test_rejects_claim_campaign_mismatch(self) -> None:
        campaign_id = "campaign-closure-3"
        with _authorized_campaign(campaign_id) as (root, grant, journal):
            lease = self._lease(root, grant)
            with self.assertRaises(FinalEvalClosureRejected):
                close_completed_campaign(
                    lease=lease,
                    campaign_id=campaign_id,
                    fixed_claim=_claim(campaign_id="different-campaign"),
                    clock=lambda: NOW,
                )

    def test_rejects_invalid_fixed_claim_fields(self) -> None:
        campaign_id = "campaign-closure-4"
        with _authorized_campaign(campaign_id) as (root, grant, journal):
            lease = self._lease(root, grant)
            claim = _claim(campaign_id=campaign_id)
            claim["extra"] = "unexpected"
            with self.assertRaises(FinalEvalClosureRejected):
                close_completed_campaign(
                    lease=lease,
                    campaign_id=campaign_id,
                    fixed_claim=claim,
                    clock=lambda: NOW,
                )

    def test_rejects_non_lease(self) -> None:
        with _authorized_campaign("campaign-closure-5") as (root, grant, journal):
            with self.assertRaises(FinalEvalClosureRejected):
                close_completed_campaign(
                    lease=object(),  # type: ignore[arg-type]
                    campaign_id="campaign-closure-5",
                    fixed_claim=_claim(campaign_id="campaign-closure-5"),
                    clock=lambda: NOW,
                )

    def _lease(self, root, grant):
        # A real TaskExecutionLease from the shared live authority would
        # require an active entry policy; the closure writer only checks
        # isinstance + allowed side effects, so construct a minimal real
        # lease-like object through the store's own issuance on the fixture
        # journal grant (policy-independent path is P0-only, so we use the
        # actual TaskExecutionLease dataclass directly).
        from research_automation.control_plane.stores import (
            TaskExecutionLease,
            _BearerSecret,
        )

        class _FakeLease(TaskExecutionLease):
            pass

        # TaskExecutionLease is a frozen dataclass with many fields; instead
        # of fabricating one, return a minimal object exposing the checked
        # attributes (allowed_side_effects) and tag it as a real lease via
        # a dedicated subclass marker is not needed: the writer checks
        # isinstance(TaskExecutionLease), so we build a real instance.
        import dataclasses

        fields = [f.name for f in dataclasses.fields(TaskExecutionLease)]
        values = {
            "ticket_id": "ticket-closure",
            "grant_id": grant.grant_id,
            "authorization_ref": "auth-closure",
            "attempt_id": "p8-attempt-002",
            "phase": stores_module.Phase.P8,
            "task_id": "P8R3-CLOSURE-TEST",
            "idempotency_key": f"closure-test-{grant.grant_id}",
            "task_spec_ref": "manifest.json",
            "task_spec_sha256": "1" * 64,
            "started_at": NOW,
            "lease_id": "lease-closure",
            "allowed_side_effects": (
                stores_module.SideEffect.WRITE_CONTROL_PLANE,
            ),
            "code_sha256": "3" * 64,
            "identity": stores_module.AuthorityIdentity(
                plan_hash="0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
                scope_hash="8c6b4a7547275728c7beef294cd8e5d56fdddf5da82509e09e88162e8c6243be",
                instruction_policy_hash="0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
            ),
        }
        for name in fields:
            if name not in values:
                values[name] = None
        lease = TaskExecutionLease(
            **{name: values[name] for name in fields}
        )
        return lease


if __name__ == "__main__":
    unittest.main()
