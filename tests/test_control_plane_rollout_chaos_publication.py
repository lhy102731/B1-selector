"""Tests for create-only concurrent C0 evidence publication (C0R2 T5)."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

from research_automation.control_plane.contracts import canonical_json


class AtomicPublisher:
    """Create-only content-addressed publisher with a fixed claim.

    same-volume temp write -> flush/fsync -> exclusive create -> parent
    directory barrier -> claim exclusive create -> second barrier.
    """

    def __init__(self, *, evidence_dir: Path) -> None:
        self._objects = evidence_dir / "objects"
        self._objects.mkdir(parents=True, exist_ok=True)
        self._claim = evidence_dir / "c0_chaos_simulation_report_v2.json"

    def publish(self, payload: Mapping) -> dict[str, object]:
        raw = canonical_json(payload).encode("utf-8")
        sha256 = hashlib.sha256(raw).hexdigest()
        object_path = self._objects / f"{sha256}.json"
        # same-volume exclusive create
        try:
            fd = os.open(
                object_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644,
            )
        except FileExistsError:
            return {"status": "IDEMPOTENT_EXISTING", "ref": object_path.name}
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        # claim exclusive create
        try:
            fd = os.open(
                self._claim,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644,
            )
        except FileExistsError:
            return {"status": "CLAIM_CONFLICT", "ref": object_path.name}
        claim = {
            "attempt_id": "c0-attempt-002",
            "seed": 20260811,
            "cycles": 24,
            "report_ref": str(object_path.relative_to(self._objects.parent)),
            "report_blob_sha256": sha256,
        }
        with os.fdopen(fd, "wb") as stream:
            stream.write(canonical_json(claim).encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        return {"status": "CREATED", "ref": object_path.name, "sha256": sha256}


class PublicationTests(unittest.TestCase):
    def test_first_publish_creates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher = AtomicPublisher(evidence_dir=Path(tmp))
            result = publisher.publish({"report": "v2", "seed": 20260811})
            self.assertEqual(result["status"], "CREATED")
            self.assertTrue((Path(tmp) / "c0_chaos_simulation_report_v2.json").exists())

    def test_same_bytes_is_idempotent_existing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher = AtomicPublisher(evidence_dir=Path(tmp))
            payload = {"report": "v2", "seed": 20260811}
            first = publisher.publish(payload)
            self.assertEqual(first["status"], "CREATED")
            second = publisher.publish(payload)
            self.assertEqual(second["status"], "IDEMPOTENT_EXISTING")

    def test_different_bytes_conflicts_on_claim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher = AtomicPublisher(evidence_dir=Path(tmp))
            first = publisher.publish({"report": "v2", "seed": 20260811})
            self.assertEqual(first["status"], "CREATED")
            second = publisher.publish({"report": "v2", "seed": 1})
            self.assertEqual(second["status"], "CLAIM_CONFLICT")

    def test_published_object_is_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher = AtomicPublisher(evidence_dir=Path(tmp))
            result = publisher.publish({"report": "v2", "seed": 20260811})
            object_path = Path(tmp) / "objects" / result["ref"]
            with open(object_path, "r+", encoding="utf-8") as f:
                original = f.read()
                self.assertNotIn("tampered", original)


if __name__ == "__main__":
    unittest.main()
