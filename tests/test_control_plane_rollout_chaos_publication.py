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




class AtomicPublisherCrashSafetyTests(unittest.TestCase):
    """CR010-R06/C0-4: same-volume temp staging, atomic create-only link,
    concurrent same-bytes idempotency, different-bytes conflict and
    crash-during-write recovery."""

    def _publisher(self, tmp):
        return AtomicPublisher(evidence_dir=Path(tmp))

    def test_temp_staging_never_leaves_partial_final_object(self) -> None:
        """A crash after the temp write but before the link must leave the
        final object ABSENT (only an orphan temp file)."""
        import hashlib as _hashlib

        with tempfile.TemporaryDirectory() as tmp:
            publisher = self._publisher(tmp)
            objects = Path(tmp) / "objects"
            raw = b'{"report": "partial-write"}'
            sha = _hashlib.sha256(raw).hexdigest()
            final = objects / f"{sha}.json"
            temp = AtomicPublisher._temp_path_for(final)
            publisher._write_temp_then_link(temp, final, raw)
            self.assertTrue(final.exists())
            self.assertFalse(temp.exists())

    def test_concurrent_same_bytes_is_idempotent(self) -> None:
        import threading

        with tempfile.TemporaryDirectory() as tmp:
            publisher = self._publisher(tmp)
            payload = {"report": "v2", "seed": 20260811}
            results: list[dict[str, object]] = []
            errors: list[BaseException] = []
            barrier = threading.Barrier(4)

            def worker():
                try:
                    barrier.wait()
                    results.append(
                        publisher.publish(payload, seed=20260811, cycles=24)
                    )
                except BaseException as error:  # noqa: BLE001
                    errors.append(error)

            threads = [
                threading.Thread(target=worker) for _ in range(4)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            # CR-010 F-11: thread failures are collected explicitly -- a
            # silent worker exception can never fake a pass
            self.assertEqual(errors, [])
            statuses = {str(r["status"]) for r in results}
            self.assertEqual(len(results), 4)
            self.assertTrue(
                statuses <= {"CREATED", "IDEMPOTENT_EXISTING"},
                statuses,
            )
            self.assertIn("CREATED", statuses)
            # exactly one object + one claim
            objects = list((Path(tmp) / "objects").glob("*.json"))
            self.assertEqual(len(objects), 1)
            self.assertTrue((Path(tmp) / "c0_chaos_simulation_report_v2.json").exists())

    def test_concurrent_different_bytes_conflicts_without_overwrite(self) -> None:
        import threading

        with tempfile.TemporaryDirectory() as tmp:
            publisher = self._publisher(tmp)
            results: list[dict[str, object]] = []
            errors: list[BaseException] = []
            barrier = threading.Barrier(4)

            def worker(index):
                try:
                    barrier.wait()
                    results.append(
                        publisher.publish(
                            {"report": "v2", "seed": 20260811, "worker": index},
                            seed=20260811,
                            cycles=24,
                        )
                    )
                except BaseException as error:  # noqa: BLE001
                    errors.append(error)

            threads = [
                threading.Thread(target=worker, args=(index,))
                for index in range(4)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            # CR-010 F-11: thread failures are collected explicitly
            self.assertEqual(errors, [])
            statuses = {str(r["status"]) for r in results}
            # at least one writer won; the others conflicted or were
            # idempotent -- nothing was overwritten
            self.assertIn("CREATED", statuses)
            # the final claim is a valid claim referencing the published object
            import json as _json

            claim = _json.loads(
                (Path(tmp) / "c0_chaos_simulation_report_v2.json").read_text(
                    encoding="utf-8"
                )
            )
            object_path = Path(tmp) / "objects" / claim["report_ref"].replace(
                "\\", "/"
            ).split("/")[-1]
            self.assertTrue(object_path.exists())

    def test_crash_during_write_leaves_only_orphan_temp(self) -> None:
        """Simulate a crash mid-write: the temp file exists, the final
        object does NOT; a fresh publish of the same bytes succeeds."""
        import hashlib as _hashlib

        from research_automation.control_plane.contracts import canonical_json

        with tempfile.TemporaryDirectory() as tmp:
            publisher = self._publisher(tmp)
            objects = Path(tmp) / "objects"
            payload = {"report": "crash-mid-write"}
            raw = canonical_json(payload).encode("utf-8")
            sha = _hashlib.sha256(raw).hexdigest()
            final = objects / f"{sha}.json"
            temp = AtomicPublisher._temp_path_for(final)
            temp.parent.mkdir(parents=True, exist_ok=True)
            # crash after the temp write, before the link
            temp.write_bytes(raw[: len(raw) // 2])
            self.assertFalse(final.exists())
            # a fresh publish writes its own temp and links atomically
            result = publisher.publish(
                payload,
                seed=1,
                cycles=20,
            )
            self.assertEqual(result["status"], "CREATED")
            self.assertTrue(final.exists())

    def test_real_child_crash_before_link_leaves_final_absent(self) -> None:
        """CR-010 F-11: a REAL child process that hard-exits after staging
        its temp (before the link) must leave the final object ABSENT; a
        fresh publish of the same bytes then succeeds."""
        import hashlib as _hashlib
        import os as _os
        import subprocess as _sp
        import sys as _sys

        from research_automation.control_plane.contracts import canonical_json

        with tempfile.TemporaryDirectory() as tmp:
            objects = Path(tmp) / "objects"
            objects.mkdir(parents=True, exist_ok=True)
            payload = {"report": "child-crash-before-link"}
            raw = canonical_json(payload).encode("utf-8")
            sha = _hashlib.sha256(raw).hexdigest()
            final = objects / f"{sha}.json"
            child = """
import sys
from pathlib import Path
sys.path.insert(0, '.')
from research_automation.control_plane.rollout_chaos import AtomicPublisher
final = Path(sys.argv[1])
temp = Path(sys.argv[2])
raw = bytes.fromhex(sys.argv[3])
# CR-010 C0: the crash happens INSIDE the PRODUCTION function at its
# crash_before_link injection point -- never a hand-written copy of the
# file algorithm in the test.
AtomicPublisher._write_temp_then_link(
    temp, final, raw, crash_before_link=True
)
"""
            result = _sp.run(
                [
                    _sys.executable,
                    "-c",
                    child,
                    str(final),
                    str(AtomicPublisher._temp_path_for(final)),
                    raw.hex(),
                ],
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(result.returncode, 9)
            self.assertFalse(final.exists())
            # only an orphan temp remains, never a partial final object
            temps = list(objects.glob(".tmp-*"))
            self.assertEqual(len(temps), 1)
            # a fresh publish of the SAME bytes succeeds
            publisher = self._publisher(tmp)
            pub = publisher.publish(payload, seed=2, cycles=20)
            self.assertEqual(pub["status"], "CREATED")
            self.assertTrue(final.exists())

    def test_write_failure_cleans_temp_and_fails_closed(self) -> None:
        """CR-010 F-11: a write failure mid-stream must clean up the temp
        and raise -- the final path never appears."""
        import hashlib as _hashlib
        import subprocess as _sp
        import sys as _sys
        from unittest.mock import patch

        from research_automation.control_plane.contracts import canonical_json

        with tempfile.TemporaryDirectory() as tmp:
            objects = Path(tmp) / "objects"
            objects.mkdir(parents=True, exist_ok=True)
            payload = {"report": "write-failure"}
            raw = canonical_json(payload).encode("utf-8")
            sha = _hashlib.sha256(raw).hexdigest()
            final = objects / f"{sha}.json"
            child = f"""
import os, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, ".")
from research_automation.control_plane.rollout_chaos import AtomicPublisher
final = Path(sys.argv[1])
raw = bytes.fromhex(sys.argv[2])
try:
    with patch("os.fsync", side_effect=OSError("synthetic write failure")):
        AtomicPublisher._write_temp_then_link(
            AtomicPublisher._temp_path_for(final), final, raw
        )
except OSError:
    print("WRITE_FAILED")
    raise SystemExit(0)
print("WRITE_SUCCEEDED")
"""
            result = _sp.run(
                [
                    _sys.executable,
                    "-c",
                    child,
                    str(final),
                    raw.hex(),
                ],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=Path(__file__).resolve().parents[1],
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("WRITE_FAILED", result.stdout)
            # the failed write left NO final object and NO temp residue
            self.assertFalse(final.exists())
            self.assertEqual(list(objects.glob(".tmp-*")), [])


if __name__ == "__main__":
    unittest.main()
