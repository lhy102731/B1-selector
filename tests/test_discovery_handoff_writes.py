from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from ag2_research.discovery_handoff import save_discovery_handoff
from research_automation.control_plane.contracts import SideEffect
from research_automation.control_plane.sink_guard import ExecutionAuthorizationError


class DiscoveryHandoffWriteTests(unittest.TestCase):
    def test_handoff_write_requires_authority_before_directory_creation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "handoffs"
            with self.assertRaises(ExecutionAuthorizationError):
                save_discovery_handoff(
                    {"status": "APPROVED", "result": {}},
                    topic="bounded test",
                    strategy_id="brick",
                    output_dir=output_dir,
                )

            self.assertFalse(output_dir.exists())

    def test_handoff_write_binds_directory_target_and_temp_file(self) -> None:
        created_at = datetime(2026, 7, 26, 12, 34, 56, 123456, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "handoffs"
            sink = MagicMock()
            sink.authorize.return_value = object()
            with patch(
                "ag2_research.discovery_handoff.AuthorizedPathMutation",
                return_value=sink,
            ):
                target = save_discovery_handoff(
                    {"status": "APPROVED", "result": {}},
                    topic="bounded test",
                    strategy_id="brick",
                    output_dir=output_dir,
                    created_at=created_at,
                    lease=object(),
                    invocation=object(),
                    authority_reader=MagicMock(),
                    repository_root=Path(tmp),
                )

            self.assertTrue(target.is_file())
            self.assertEqual(
                target,
                output_dir / "discovery_brick_APPROVED_20260726T123456123456Z.yaml",
            )
            authorization = sink.authorize.call_args.kwargs
            self.assertEqual("KBASE_WRITE", authorization["operation"])
            self.assertIs(SideEffect.WRITE_KBASE, authorization["effect"])
            self.assertEqual(
                authorization["paths"],
                (
                    output_dir,
                    target,
                    output_dir / f".{target.name}.tmp",
                ),
            )
            self.assertFalse((output_dir / f".{target.name}.tmp").exists())


if __name__ == "__main__":
    unittest.main()
