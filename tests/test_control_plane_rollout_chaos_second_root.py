"""CR-010 F-08 / B-04: second-root fresh-process replay tests.

The C0 campaign must be able to prove root-INDEPENDENT semantic replay:
a fresh process executing the same deterministic campaign against a
DIFFERENT fixture root must produce the same final state digest, semantic
state signature and scenario-log digest -- and the official payload can
never be pass=true while the replay is unproven (NOT_WIRED was a
fail-open hole).  Every comparison is between two INDEPENDENTLY collected
value sets (CR-010 B-04).
"""

from __future__ import annotations

import json
import unittest

from research_automation.control_plane import rollout_chaos


class SecondRootReplayTests(unittest.TestCase):
    def setUp(self) -> None:
        # CR-010 F-05: the tests remove their OWN disposable fixture
        # roots so every run starts genuinely fresh (the production code
        # never deletes an existing deterministic root automatically).
        import shutil as _shutil

        root = rollout_chaos._deterministic_root(20260811, 24)
        for target in (
            root,
            root.parent / (root.name + "-replay-2"),
        ):
            _shutil.rmtree(target, ignore_errors=True)

    def test_double_root_digests_and_signatures_match(self) -> None:
        """CR-010 B-04: two campaign runs against DIFFERENT roots (same
        seed/cycles) must produce byte-equal final state digests, semantic
        signatures and scenario-log digests -- the second root's values are
        collected by the second process itself."""
        from research_automation.control_plane.rollout_chaos_worker import (
            NetworkGuard,
        )

        NetworkGuard.install()
        NetworkGuard.deny_probe()
        try:
            main_a, root_a = rollout_chaos._run_main_campaign(
                20260811,
                24,
                root_override=rollout_chaos._deterministic_root(20260811, 24),
            )
        finally:
            NetworkGuard.uninstall()
        # fresh process + different root, returns its OWN observations
        second = rollout_chaos._fresh_process_replay_signature(20260811, 24)
        first_signature = rollout_chaos._semantic_state_signature(
            main_a, root_a
        )
        first_digest = str(main_a["final_state_digest"])
        first_log_digest = rollout_chaos._scenario_log_digest(
            list(main_a["scenario_log"])
        )
        self.assertEqual(first_signature, second["semantic_signature"])
        self.assertEqual(first_digest, second["final_state_digest"])
        self.assertEqual(first_log_digest, second["scenario_log_digest"])
        self.assertEqual(
            int(main_a["cycles_completed"]), int(second["cycles_completed"])
        )
        self.assertEqual(
            str(main_a["campaign_status"]), str(second["campaign_status"])
        )
        # the second root is a DIFFERENT root and a DIFFERENT OS process
        self.assertNotEqual(str(root_a), str(second["root_identity"]))
        self.assertGreater(int(second["pid"]), 0)

    def test_unknown_root_field_stays_significant(self) -> None:
        """CR-010 F-05: the semantic-signature normalization replaces the
        fixture root ONLY inside the KNOWN root-identity payload fields --
        a root string in an unknown payload field stays significant, so a
        hidden root-bearing payload drift changes the signature."""
        import json as _json

        from research_automation.control_plane.rollout_chaos import (
            _normalize_root_identity_fields,
        )

        root_text = "C:/fixture/root"
        known = _json.dumps({"repository_root": root_text, "x": 1})
        unknown = _json.dumps({"secret_path": root_text, "x": 1})
        normalized = _normalize_root_identity_fields(known, root_text)
        self.assertIn("<ROOT>", normalized)
        self.assertNotIn(root_text, normalized)
        # an unknown field keeps the root text byte-for-byte
        self.assertEqual(
            _normalize_root_identity_fields(unknown, root_text),
            unknown,
        )

    def test_payload_pass_requires_matched_second_root_replay(self) -> None:
        """CR-010 F-08: ChaosOutcome.to_payload() fails closed -- pass is
        true ONLY when the second-root fresh-process replay MATCHED; a
        recorded NOT_WIRED gap (or a mismatch) can never be pass=true."""
        outcome = rollout_chaos.ChaosOutcome(
            seed=20260811,
            cycles_requested=24,
            cycles_completed=24,
            scenario_log=("cycle 1: ok",),
            invariants=({"name": "campaign_completed", "passed": True},),
            negative_scenarios=(
                {"category": "lease_fencing_fail_closed", "passed": True},
            ),
            final_state_digest="1" * 64,
            campaign_status="COMPLETED",
            attempt_id="c0-attempt-003",
            worker_verify={
                "pid": 1234,
                "state_digest": "2" * 64,
                "outcome": "SUCCEEDED",
                "network_attempts": 1,
                "root_identity": "C:/fixture-root",
                "second_root_replay": "NOT_WIRED_ROOT_PATH_LEAK",
            },
        )
        payload = outcome.to_payload()
        self.assertFalse(payload["pass"])
        self.assertEqual(
            payload["worker_verify"]["second_root_replay"],
            "NOT_WIRED_ROOT_PATH_LEAK",
        )

    def test_payload_pass_requires_matched_not_mismatch(self) -> None:
        outcome = rollout_chaos.ChaosOutcome(
            seed=20260811,
            cycles_requested=24,
            cycles_completed=24,
            scenario_log=("cycle 1: ok",),
            invariants=({"name": "campaign_completed", "passed": True},),
            negative_scenarios=(
                {"category": "lease_fencing_fail_closed", "passed": True},
            ),
            final_state_digest="1" * 64,
            campaign_status="COMPLETED",
            attempt_id="c0-attempt-003",
            worker_verify={
                "pid": 1234,
                "state_digest": "2" * 64,
                "outcome": "SUCCEEDED",
                "network_attempts": 1,
                "root_identity": "C:/fixture-root",
                "second_root_replay": "MISMATCH",
            },
        )
        self.assertFalse(outcome.to_payload()["pass"])


if __name__ == "__main__":
    unittest.main()
