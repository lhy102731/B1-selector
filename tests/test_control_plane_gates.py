from __future__ import annotations

import unittest

from research_automation.control_plane.gates import (
    GateBuildError,
    PhaseGateBuilder,
)


class PhaseGateBuilderTests(unittest.TestCase):
    def test_caller_cannot_supply_computed_gate_fields(self) -> None:
        builder = PhaseGateBuilder()

        for field_name, value in (
            ("verdict", "PASS"),
            ("reason_codes", []),
            ("auto_advance", True),
            ("created_at", "2026-07-26T08:00:00Z"),
            ("gate_report_sha256", "a" * 64),
        ):
            with self.subTest(field_name=field_name):
                with self.assertRaisesRegex(
                    GateBuildError,
                    "computed fields",
                ):
                    builder.build({field_name: value})


if __name__ == "__main__":
    unittest.main()
