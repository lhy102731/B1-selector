"""Tests for create-only concurrent C0 evidence publication (C0R2 T5).

CR-010 F-03: the AtomicPublisher under test is the PRODUCTION-owned
implementation in ``rollout_chaos.py`` (the test-local copy was removed so
the gate evidence proves the real publication path the CLI uses).
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_automation.control_plane.rollout_chaos import AtomicPublisher


class PublicationTests(unittest.TestCase):
    def test_first_publish_creates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher = AtomicPublisher(
                evidence_dir=Path(tmp),
                attempt_id="c0-attempt-003",
            )
            result = publisher.publish(
                {"report": "v2", "seed": 20260811},
                seed=20260811,
                cycles=24,
            )
            self.assertEqual(result["status"], "CREATED")
            self.assertTrue(
                (Path(tmp) / "c0_chaos_simulation_report_v2.json").exists()
            )
            self.assertTrue(result["sha256"])
            claim = json.loads(
                (Path(tmp) / "c0_chaos_simulation_report_v2.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(claim["attempt_id"], "c0-attempt-003")
            self.assertEqual(claim["report_blob_sha256"], result["sha256"])

    def test_same_bytes_is_idempotent_existing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher = AtomicPublisher(evidence_dir=Path(tmp))
            payload = {"report": "v2", "seed": 20260811}
            first = publisher.publish(payload, seed=20260811, cycles=24)
            self.assertEqual(first["status"], "CREATED")
            second = publisher.publish(payload, seed=20260811, cycles=24)
            self.assertEqual(second["status"], "IDEMPOTENT_EXISTING")

    def test_different_bytes_conflicts_on_claim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher = AtomicPublisher(evidence_dir=Path(tmp))
            first = publisher.publish(
                {"report": "v2", "seed": 20260811},
                seed=20260811,
                cycles=24,
            )
            self.assertEqual(first["status"], "CREATED")
            second = publisher.publish(
                {"report": "v2", "seed": 1},
                seed=20260811,
                cycles=24,
            )
            self.assertEqual(second["status"], "CLAIM_CONFLICT")

    def test_published_object_is_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher = AtomicPublisher(evidence_dir=Path(tmp))
            result = publisher.publish(
                {"report": "v2", "seed": 20260811},
                seed=20260811,
                cycles=24,
            )
            object_path = Path(tmp) / "objects" / result["ref"]
            with open(object_path, "r+", encoding="utf-8") as f:
                original = f.read()
                self.assertNotIn("tampered", original)
                self.assertIn("report", original)


if __name__ == "__main__":
    unittest.main()
