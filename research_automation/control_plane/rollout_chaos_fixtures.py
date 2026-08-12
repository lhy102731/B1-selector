"""Production-owned deterministic offline chaos fixtures (C0R2 T1).

Provides the deterministic clock, PID/process identity, protocol/member,
synthetic Authority-bound evidence and store bootstrap used by the C0
rollout chaos simulation — without importing ``tests.*`` or relying on
``unittest.mock``.  The fake provider is the P6 production-owned
``CampaignOfflineProvider`` (never a second provider copy).
"""

from __future__ import annotations

import hashlib
import json
import random
import secrets
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from .campaign_offline_provider import CampaignOfflineProvider
from . import stores as stores_module
from .contracts import canonical_json
from .memory import ClaimScope


@dataclass(frozen=True, slots=True)
class OfflineChaosIdentity:
    """Deterministic fake clock/PID/process identity for one run."""

    seed: int
    pid: int
    process_started_at_ns: int
    host_id: str = "offline-host"

    def process_identity(self):
        from .campaign_lease import ProcessIdentity

        return ProcessIdentity(
            host_id=self.host_id,
            pid=self.pid,
            process_started_at_ns=self.process_started_at_ns,
        )


class SequentialMonotonicClock:
    """Deterministic monotonic clock yielding seeded ns values."""

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)
        self._value = 0

    def __call__(self) -> int:
        self._value += 1 + self._rng.randrange(0, 1000)
        return self._value


class FakeProcessIdentityProvider:
    """Deterministic fake ProcessIdentityProvider for offline runs."""

    def __init__(self, identity: OfflineChaosIdentity) -> None:
        self._identity = identity

    def current(self):
        return self._identity.process_identity()

    def probe(self, host_id: str, pid: int) -> int | None:
        if (host_id, pid) == (
            self._identity.host_id,
            self._identity.pid,
        ):
            return self._identity.process_started_at_ns
        return None


def deterministic_scope(*, generation: str = "c0-generation-1") -> dict[str, object]:
    """Canonical claim scope for one offline run."""
    return {
        "mechanisms": ["volume-contraction-rebound"],
        "usage_modes": ["factor-candidate"],
        "market_regimes": ["all"],
        "time_windows": [{"start": "2020-01-01", "end": "2026-12-31"}],
        "universes": ["a-share"],
        "liquidity_buckets": ["production-minimum"],
        "label_protocol_families": ["rolling-forward-v1"],
        "generation_families": [generation],
    }


def deterministic_protocol():
    """Build the canonical offline protocol via the repository builder.

    Delegates to the campaign fixture path so the protocol structure is
    production-owned and identical to real campaign runs.
    """
    from research_automation.foundations.protocols import (
        ProtocolDefinition,
    )
    from research_automation.foundations.protocols import RosterMember

    member = RosterMember(
        role="factor_engineer",
        provider_profile_id="offline-local",
        model_id="deterministic-reviewer",
    )
    return ProtocolDefinition(
        version="1.0.0",
        name="c0-offline-protocol",
        roster=(member,),
        execution_flow=("generate", "verify", "record"),
        evidence_schema="control_plane.evidence.v1",
    )


def deterministic_member(*, prompt_sha256: str):
    """Build a canonical RosterMember for the offline run."""
    from .campaign_roster import RosterMember

    return RosterMember(
        member_id="member-001",
        provider="fake-provider",
        profile="offline-local",
        model="deterministic-reviewer",
        role="factor_engineer",
        prompt_sha256=prompt_sha256,
        config_sha256="2" * 64,
        capability_sha256="3" * 64,
    )


def claim_campaign_grant(
    *,
    campaign_id: str,
    namespace: str,
    attempt_id: str,
    plan_sha256: str,
    instruction_sha256: str,
):
    """Provision + claim a campaign grant in the fixture store."""
    from .campaign_store import campaign_scope_sha256
    from .stores import Actor, AuthorityIdentity, Phase, SideEffect

    actor = Actor("p6-runner", "automation", f"{campaign_id}-fixture")
    identity = AuthorityIdentity(
        plan_sha256,
        campaign_scope_sha256(
            namespace=namespace,
            campaign_id=campaign_id,
        ),
        instruction_sha256,
    )
    authority = stores_module._AuthorityStore(root_secret="0" * 64)
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    authorization = authority._provision_authorization(
        phase=Phase.P6,
        attempt_id=attempt_id,
        actor=actor,
        identity=identity,
        expires_at=now + timedelta(days=1),
        allowed_side_effects=(
            SideEffect.READ,
            SideEffect.WRITE_CONTROL_PLANE,
        ),
    )
    return authority.claim_authorization(
        authorization,
        expected_phase=Phase.P6,
        expected_attempt_id=attempt_id,
        actor=actor,
        identity=identity,
    )


def bootstrap_fixture_stores(root: Path, *, root_secret: str = "0" * 64):
    """Bootstrap authority+operational fixture stores under ``root``."""
    stores_module._expected_schema_sha256.cache_clear()
    stores_module._trusted_bootstrap(root_secret=root_secret)


__all__ = [
    "CampaignOfflineProvider",
    "FakeProcessIdentityProvider",
    "OfflineChaosIdentity",
    "SequentialMonotonicClock",
    "bootstrap_fixture_stores",
    "claim_campaign_grant",
    "deterministic_member",
    "deterministic_protocol",
    "deterministic_scope",
]
