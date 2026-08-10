from __future__ import annotations

import unittest
from types import SimpleNamespace

from research_automation.control_plane.campaign_invocation_binding import (
    InvocationBindingError,
    TrustedInvocationBinding,
    build_invocation_binding,
    construct_provider,
    invocation_binding_sha256,
    require_invocation_binding,
    require_provider_binding,
    verify_spawn_identity,
)
from research_automation.control_plane.campaign_lease import ProcessIdentity


class _FakeIdentityProvider:
    def __init__(self, current: ProcessIdentity) -> None:
        self._current = current

    def current(self) -> ProcessIdentity:
        return self._current

    def probe(self, host_id: str, pid: int) -> int | None:
        if (host_id, pid) == (self._current.host_id, self._current.pid):
            return self._current.process_started_at_ns
        return None


class _FakeProvider:
    provider_name = "fake-provider"
    profile = "offline-local"
    model = "deterministic-reviewer"
    config_sha256 = "2" * 64
    capability_sha256 = "3" * 64

    def invoke(self, request: object) -> object:
        return request


def _member(**overrides: object) -> SimpleNamespace:
    values = {
        "member_id": "member-001",
        "provider": "fake-provider",
        "profile": "offline-local",
        "model": "deterministic-reviewer",
        "role": "reviewer",
        "config_sha256": "2" * 64,
        "capability_sha256": "3" * 64,
        "prompt_sha256": "4" * 64,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _limits(**overrides: object) -> SimpleNamespace:
    values = {
        "currency": "USD",
        "max_input_tokens": 20,
        "max_output_tokens": 10,
        "max_cost": "0.1",
        "max_wall_time_ms": 1_000,
        "max_attempts": 2,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _identity_provider(
    current: ProcessIdentity | None = None,
) -> _FakeIdentityProvider:
    return _FakeIdentityProvider(
        current or ProcessIdentity("host-controller", 144, 44_000)
    )


class TrustedInvocationBindingTests(unittest.TestCase):
    def test_build_binding_binds_profile_pricing_retry_and_spawn_identity(
        self,
    ) -> None:
        binding = build_invocation_binding(
            member=_member(),
            limits=_limits(),
            identity_provider=_identity_provider(),
        )
        self.assertIsInstance(binding, TrustedInvocationBinding)
        self.assertEqual(binding.provider_profile_id, "offline-local")
        self.assertEqual(binding.provider, "fake-provider")
        self.assertEqual(binding.model, "deterministic-reviewer")
        self.assertEqual(binding.currency, "USD")
        self.assertEqual(binding.max_cost, "0.1")
        self.assertEqual(binding.max_attempts, 2)
        self.assertEqual(binding.max_wall_time_ms, 1_000)
        self.assertEqual(binding.host_id, "host-controller")
        self.assertEqual(binding.pid, 144)
        self.assertEqual(binding.process_started_at_ns, 44_000)

    def test_binding_payload_is_canonical_and_sha_is_stable(self) -> None:
        binding = build_invocation_binding(
            member=_member(),
            limits=_limits(),
            identity_provider=_identity_provider(),
        )
        payload = binding.to_payload()
        self.assertEqual(
            payload["schema_version"],
            "control_plane.trusted_invocation_binding.v1",
        )
        self.assertEqual(payload["pricing"]["currency"], "USD")
        self.assertEqual(payload["retry"]["max_attempts"], 2)
        self.assertEqual(payload["spawn_identity"]["host_id"], "host-controller")
        first = invocation_binding_sha256(binding)
        second = invocation_binding_sha256(
            build_invocation_binding(
                member=_member(),
                limits=_limits(),
                identity_provider=_identity_provider(),
            )
        )
        self.assertEqual(first, second)
        self.assertEqual(len(first), 64)

    def test_binding_sha_changes_when_spawn_identity_changes(self) -> None:
        member = _member()
        limits = _limits()
        first = invocation_binding_sha256(
            build_invocation_binding(
                member=member,
                limits=limits,
                identity_provider=_identity_provider(
                    ProcessIdentity("host-controller", 144, 44_000)
                ),
            )
        )
        second = invocation_binding_sha256(
            build_invocation_binding(
                member=member,
                limits=limits,
                identity_provider=_identity_provider(
                    ProcessIdentity("host-controller", 145, 44_000)
                ),
            )
        )
        self.assertNotEqual(first, second)

    def test_require_provider_binding_accepts_matching_provider(self) -> None:
        binding = build_invocation_binding(
            member=_member(),
            limits=_limits(),
            identity_provider=_identity_provider(),
        )
        require_provider_binding(_FakeProvider(), binding)

    def test_require_provider_binding_rejects_drifted_profile(self) -> None:
        binding = build_invocation_binding(
            member=_member(),
            limits=_limits(),
            identity_provider=_identity_provider(),
        )
        provider = _FakeProvider()
        provider.profile = "drifted-profile"
        with self.assertRaisesRegex(
            InvocationBindingError,
            "provider binding conflicts",
        ):
            require_provider_binding(provider, binding)

    def test_require_provider_binding_rejects_missing_identity_fields(self) -> None:
        binding = build_invocation_binding(
            member=_member(),
            limits=_limits(),
            identity_provider=_identity_provider(),
        )

        class _BareProvider:
            def invoke(self, request: object) -> object:
                return request

        with self.assertRaisesRegex(
            InvocationBindingError,
            "provider binding identity is invalid",
        ):
            require_provider_binding(_BareProvider(), binding)

    def test_require_provider_binding_rejects_non_invokable_provider(self) -> None:
        binding = build_invocation_binding(
            member=_member(),
            limits=_limits(),
            identity_provider=_identity_provider(),
        )
        with self.assertRaisesRegex(TypeError, "callable invoke"):
            require_provider_binding(object(), binding)

    def test_build_binding_rejects_invalid_profile_id(self) -> None:
        with self.assertRaisesRegex(
            InvocationBindingError,
            "provider_profile_id",
        ):
            build_invocation_binding(
                member=_member(profile="bad profile!"),
                limits=_limits(),
                identity_provider=_identity_provider(),
            )

    def test_build_binding_rejects_invalid_pricing(self) -> None:
        with self.assertRaisesRegex(InvocationBindingError, "currency"):
            build_invocation_binding(
                member=_member(),
                limits=_limits(currency="not a currency"),
                identity_provider=_identity_provider(),
            )
        with self.assertRaisesRegex(InvocationBindingError, "max_cost"):
            build_invocation_binding(
                member=_member(),
                limits=_limits(max_cost="-0.1"),
                identity_provider=_identity_provider(),
            )
        with self.assertRaisesRegex(InvocationBindingError, "max_input_tokens"):
            build_invocation_binding(
                member=_member(),
                limits=_limits(max_input_tokens=-1),
                identity_provider=_identity_provider(),
            )

    def test_build_binding_rejects_invalid_retry_bounds(self) -> None:
        with self.assertRaisesRegex(InvocationBindingError, "max_attempts"):
            build_invocation_binding(
                member=_member(),
                limits=_limits(max_attempts=0),
                identity_provider=_identity_provider(),
            )
        with self.assertRaisesRegex(InvocationBindingError, "max_attempts"):
            build_invocation_binding(
                member=_member(),
                limits=_limits(max_attempts=101),
                identity_provider=_identity_provider(),
            )
        with self.assertRaisesRegex(InvocationBindingError, "max_wall_time_ms"):
            build_invocation_binding(
                member=_member(),
                limits=_limits(max_wall_time_ms=0),
                identity_provider=_identity_provider(),
            )

    def test_build_binding_rejects_missing_limits_fields(self) -> None:
        limits = _limits()
        del limits.max_attempts
        with self.assertRaisesRegex(InvocationBindingError, "max_attempts"):
            build_invocation_binding(
                member=_member(),
                limits=limits,
                identity_provider=_identity_provider(),
            )

    def test_build_binding_rejects_non_identity_provider(self) -> None:
        with self.assertRaisesRegex(TypeError, "identity_provider"):
            build_invocation_binding(
                member=_member(),
                limits=_limits(),
                identity_provider=object(),
            )

    def test_require_invocation_binding_composes_both_gates(self) -> None:
        binding = require_invocation_binding(
            provider=_FakeProvider(),
            member=_member(),
            limits=_limits(),
            identity_provider=_identity_provider(),
        )
        self.assertIsInstance(binding, TrustedInvocationBinding)
        provider = _FakeProvider()
        provider.profile = "drifted-profile"
        with self.assertRaisesRegex(
            InvocationBindingError,
            "provider binding conflicts",
        ):
            require_invocation_binding(
                provider=provider,
                member=_member(),
                limits=_limits(),
                identity_provider=_identity_provider(),
            )

    def test_construct_provider_requires_binding_and_factory(self) -> None:
        binding = build_invocation_binding(
            member=_member(),
            limits=_limits(),
            identity_provider=_identity_provider(),
        )
        with self.assertRaisesRegex(
            TypeError,
            "TrustedInvocationBinding",
        ):
            construct_provider(object(), lambda payload: _FakeProvider())
        with self.assertRaisesRegex(TypeError, "factory"):
            construct_provider(binding, None)
        provider = construct_provider(
            binding,
            lambda payload: _FakeProvider(),
        )
        self.assertEqual(provider.profile, binding.profile)

    def test_construct_provider_rejects_mismatched_factory_output(self) -> None:
        binding = build_invocation_binding(
            member=_member(),
            limits=_limits(),
            identity_provider=_identity_provider(),
        )
        with self.assertRaisesRegex(
            InvocationBindingError,
            "provider binding conflicts",
        ):
            construct_provider(
                binding,
                lambda payload: _MisboundProvider(),
            )


    def test_verify_spawn_identity_accepts_matching_current_process(self) -> None:
        identity_provider = _identity_provider()
        binding = build_invocation_binding(
            member=_member(),
            limits=_limits(),
            identity_provider=identity_provider,
        )
        verify_spawn_identity(binding, identity_provider)

    def test_verify_spawn_identity_fails_closed_on_process_drift(self) -> None:
        identity_provider = _identity_provider()
        binding = build_invocation_binding(
            member=_member(),
            limits=_limits(),
            identity_provider=identity_provider,
        )
        identity_provider._current = ProcessIdentity(
            "host-other",
            999,
            999_999,
        )
        with self.assertRaisesRegex(
            InvocationBindingError,
            "spawn identity conflicts",
        ):
            verify_spawn_identity(binding, identity_provider)


class _MisboundProvider:
    provider_name = "fake-provider"
    profile = "drifted-profile"
    model = "deterministic-reviewer"
    config_sha256 = "2" * 64
    capability_sha256 = "3" * 64

    def invoke(self, request: object) -> object:
        return request


if __name__ == "__main__":
    unittest.main()
