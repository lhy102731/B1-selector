from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.foundations.immutable_release import ImmutableReleaseStore


class _ManifestAdapter:
    def validate(self, release: Path) -> str:
        document = json.loads((release / "manifest.json").read_text(encoding="utf-8"))
        return str(document["release_id"])


def _write_release(path: Path, release_id: str) -> None:
    path.mkdir(parents=True)
    (path / "manifest.json").write_text(
        json.dumps({"release_id": release_id}),
        encoding="utf-8",
    )


class ImmutableReleaseStoreTests(unittest.TestCase):
    def test_promote_failure_restores_current_previous_and_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            candidate = store.stage("v2")
            _write_release(candidate, "v2")
            real_replace = os.replace

            def fail_candidate_promotion(source: object, target: object) -> None:
                if Path(source) == candidate and Path(target) == root / "current":
                    raise PermissionError("injected candidate promotion failure")
                real_replace(source, target)

            with patch(
                "research_automation.foundations.immutable_release.os.replace",
                side_effect=fail_candidate_promotion,
            ):
                with self.assertRaisesRegex(
                    PermissionError,
                    "injected candidate promotion failure",
                ):
                    store.promote(candidate, expected_current_id="v1")

            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertEqual(
                ["candidate", "current", "previous"],
                sorted(path.name for path in root.iterdir()),
            )


if __name__ == "__main__":
    unittest.main()
