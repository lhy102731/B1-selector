"""P6R2 T6: campaign_metering deterministic resource observation tests."""

from __future__ import annotations

import json
import unittest

from research_automation.control_plane.campaign_metering import (
    ResourceObservation,
    ResourceObservationLimitError,
    resource_observation_sha256,
    validate_resource_observation,
)


class ResourceObservationTests(unittest.TestCase):
    def test_zero_observation(self) -> None:
        observation = ResourceObservation.zero()
        self.assertEqual(
            observation,
            ResourceObservation(0, 0, 0),
        )
        self.assertEqual(observation.tool_attempts, 0)
        self.assertEqual(observation.data_exposures, 0)
        self.assertEqual(observation.disk_growth_bytes, 0)

    def test_bounded_non_negative_integers(self) -> None:
        ResourceObservation(1, 2, 3)
        for kwargs in (
            {"tool_attempts": -1, "data_exposures": 0, "disk_growth_bytes": 0},
            {"tool_attempts": 0, "data_exposures": -1, "disk_growth_bytes": 0},
            {"tool_attempts": 0, "data_exposures": 0, "disk_growth_bytes": -1},
            {"tool_attempts": True, "data_exposures": 0, "disk_growth_bytes": 0},
            {"tool_attempts": 0, "data_exposures": 1.5, "disk_growth_bytes": 0},
            {"tool_attempts": 0, "data_exposures": 0, "disk_growth_bytes": 2**63},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    ResourceObservation(**kwargs)

    def test_payload_round_trip_is_canonical(self) -> None:
        observation = ResourceObservation(3, 4, 5)
        payload = observation.to_payload()
        self.assertEqual(payload["schema_version"], "control_plane.resource_observation.v1")
        self.assertEqual(
            ResourceObservation.from_payload(payload),
            observation,
        )
        self.assertEqual(
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            observation.to_canonical_json(),
        )

    def test_from_payload_rejects_malformed(self) -> None:
        observation = ResourceObservation(1, 2, 3)
        for payload in (
            None,
            [],
            {},
            {"schema_version": "control_plane.resource_observation.v1", "tool_attempts": 1},
            {**observation.to_payload(), "extra": True},
            {**observation.to_payload(), "schema_version": "other"},
            {**observation.to_payload(), "tool_attempts": -1},
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    ResourceObservation.from_payload(payload)

    def test_sha256_is_deterministic_and_distinct(self) -> None:
        a = ResourceObservation(1, 2, 3)
        b = ResourceObservation(1, 2, 3)
        c = ResourceObservation(1, 2, 4)
        self.assertEqual(resource_observation_sha256(a), resource_observation_sha256(b))
        self.assertNotEqual(resource_observation_sha256(a), resource_observation_sha256(c))
        self.assertEqual(len(resource_observation_sha256(a)), 64)
        with self.assertRaises(TypeError):
            resource_observation_sha256({"tool_attempts": 1})  # type: ignore[arg-type]

    def test_validate_passes_within_limits(self) -> None:
        observation = ResourceObservation(2, 3, 4)
        validate_resource_observation(
            observation,
            max_tool_attempts=2,
            max_data_exposures=3,
            max_disk_growth_bytes=4,
        )

    def test_validate_fails_closed_over_limit(self) -> None:
        cases = (
            (ResourceObservation(3, 0, 0), 2, 0, 0, "tool_attempts"),
            (ResourceObservation(0, 4, 0), 0, 3, 0, "data_exposures"),
            (ResourceObservation(0, 0, 5), 0, 0, 4, "disk_growth_bytes"),
        )
        for observation, mt, md, mg, field in cases:
            with self.subTest(field=field):
                with self.assertRaises(ResourceObservationLimitError):
                    validate_resource_observation(
                        observation,
                        max_tool_attempts=mt,
                        max_data_exposures=md,
                        max_disk_growth_bytes=mg,
                    )

    def test_validate_rejects_invalid_limits_and_types(self) -> None:
        observation = ResourceObservation(0, 0, 0)
        with self.assertRaises(TypeError):
            validate_resource_observation(  # type: ignore[arg-type]
                {"tool_attempts": 0},
                max_tool_attempts=1,
                max_data_exposures=1,
                max_disk_growth_bytes=1,
            )
        with self.assertRaises(ValueError):
            validate_resource_observation(
                observation,
                max_tool_attempts=-1,
                max_data_exposures=1,
                max_disk_growth_bytes=1,
            )


if __name__ == "__main__":
    unittest.main()
