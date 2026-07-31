"""Scoped Learning decisions and bounded context projection for P5.

This module is read-only with respect to Learning Commit state.  It consumes
structured, already committed facts and never confers authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from collections.abc import Mapping, Sequence


_SCOPE_FIELDS = frozenset(
    {
        "mechanisms",
        "usage_modes",
        "market_regimes",
        "time_windows",
        "universes",
        "liquidity_buckets",
        "label_protocol_families",
        "generation_families",
    }
)


class ScopeMatch(str, Enum):
    EXACT = "EXACT"
    SUBSET = "SUBSET"
    OVERLAP = "OVERLAP"
    DISJOINT = "DISJOINT"


@dataclass(frozen=True, order=True)
class TimeWindow:
    start: date
    end: date

    @classmethod
    def from_mapping(cls, value: object) -> "TimeWindow":
        if not isinstance(value, Mapping) or set(value) != {"start", "end"}:
            raise ValueError("time window must contain only start and end")
        try:
            start = date.fromisoformat(str(value["start"]))
            end = date.fromisoformat(str(value["end"]))
        except ValueError as error:
            raise ValueError("time window dates must be canonical ISO dates") from error
        if start.isoformat() != value["start"] or end.isoformat() != value["end"]:
            raise ValueError("time window dates must be canonical ISO dates")
        if start > end:
            raise ValueError("time window start must not follow end")
        return cls(start=start, end=end)

    def overlaps(self, other: "TimeWindow") -> bool:
        return self.start <= other.end and other.start <= self.end

    def is_within(self, other: "TimeWindow") -> bool:
        return other.start <= self.start and self.end <= other.end

    def to_mapping(self) -> dict[str, str]:
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}


def _canonical_values(value: object, field_name: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or any(
            not isinstance(item, str)
            or not item
            or item != item.strip()
            or len(item) > 256
            for item in value
        )
        or value != sorted(set(value))
    ):
        raise ValueError(f"scope.{field_name} must be a sorted unique string array")
    return tuple(value)


@dataclass(frozen=True)
class ClaimScope:
    mechanisms: tuple[str, ...]
    usage_modes: tuple[str, ...]
    market_regimes: tuple[str, ...]
    time_windows: tuple[TimeWindow, ...]
    universes: tuple[str, ...]
    liquidity_buckets: tuple[str, ...]
    label_protocol_families: tuple[str, ...]
    generation_families: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: object) -> "ClaimScope":
        if not isinstance(value, Mapping) or set(value) != _SCOPE_FIELDS:
            raise ValueError("claim scope has an invalid field contract")
        raw_windows = value["time_windows"]
        if not isinstance(raw_windows, list) or not raw_windows:
            raise ValueError("scope.time_windows must be a non-empty array")
        windows = tuple(TimeWindow.from_mapping(item) for item in raw_windows)
        if windows != tuple(sorted(set(windows))):
            raise ValueError("scope.time_windows must be sorted and unique")
        return cls(
            mechanisms=_canonical_values(value["mechanisms"], "mechanisms"),
            usage_modes=_canonical_values(value["usage_modes"], "usage_modes"),
            market_regimes=_canonical_values(value["market_regimes"], "market_regimes"),
            time_windows=windows,
            universes=_canonical_values(value["universes"], "universes"),
            liquidity_buckets=_canonical_values(value["liquidity_buckets"], "liquidity_buckets"),
            label_protocol_families=_canonical_values(
                value["label_protocol_families"], "label_protocol_families"
            ),
            generation_families=_canonical_values(
                value["generation_families"], "generation_families"
            ),
        )

    def classify_proposal(self, learned: "ClaimScope") -> ScopeMatch:
        categorical_pairs = (
            (self.mechanisms, learned.mechanisms),
            (self.usage_modes, learned.usage_modes),
            (self.market_regimes, learned.market_regimes),
            (self.universes, learned.universes),
            (self.liquidity_buckets, learned.liquidity_buckets),
            (self.label_protocol_families, learned.label_protocol_families),
            (self.generation_families, learned.generation_families),
        )
        if any(set(proposal).isdisjoint(prior) for proposal, prior in categorical_pairs):
            return ScopeMatch.DISJOINT
        if not any(
            proposal.overlaps(prior)
            for proposal in self.time_windows
            for prior in learned.time_windows
        ):
            return ScopeMatch.DISJOINT
        if self == learned:
            return ScopeMatch.EXACT
        if all(set(proposal).issubset(prior) for proposal, prior in categorical_pairs) and all(
            any(window.is_within(prior) for prior in learned.time_windows)
            for window in self.time_windows
        ):
            return ScopeMatch.SUBSET
        return ScopeMatch.OVERLAP

    def intersection(self, other: "ClaimScope") -> dict[str, object] | None:
        relation = self.classify_proposal(other)
        if relation is ScopeMatch.DISJOINT:
            return None
        windows = sorted(
            {
                TimeWindow(max(left.start, right.start), min(left.end, right.end))
                for left in self.time_windows
                for right in other.time_windows
                if left.overlaps(right)
            }
        )
        return {
            "mechanisms": sorted(set(self.mechanisms) & set(other.mechanisms)),
            "usage_modes": sorted(set(self.usage_modes) & set(other.usage_modes)),
            "market_regimes": sorted(
                set(self.market_regimes) & set(other.market_regimes)
            ),
            "time_windows": [window.to_mapping() for window in windows],
            "universes": sorted(set(self.universes) & set(other.universes)),
            "liquidity_buckets": sorted(
                set(self.liquidity_buckets) & set(other.liquidity_buckets)
            ),
            "label_protocol_families": sorted(
                set(self.label_protocol_families)
                & set(other.label_protocol_families)
            ),
            "generation_families": sorted(
                set(self.generation_families) & set(other.generation_families)
            ),
        }


def _nonempty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a canonical non-empty string")
    return value


class LearningGate:
    """Mechanically classify proposal scope against safe Learning claims."""

    def classify(
        self,
        proposal: Mapping[str, object],
        claims: Sequence[Mapping[str, object]],
    ) -> dict[str, object]:
        if not isinstance(proposal, Mapping):
            raise ValueError("proposal must be a mapping")
        proposal_execution = _nonempty_string(
            proposal.get("execution_identity"), "proposal.execution_identity"
        )
        proposal_semantic = _nonempty_string(
            proposal.get("semantic_identity"), "proposal.semantic_identity"
        )
        proposal_scope = ClaimScope.from_mapping(proposal.get("scope"))
        if not isinstance(claims, Sequence) or isinstance(claims, (str, bytes)):
            raise ValueError("claims must be a sequence")
        matches: list[dict[str, object]] = []
        hard_blocks: list[str] = []
        warning_codes: set[str] = set()
        for raw_claim in claims:
            if not isinstance(raw_claim, Mapping):
                raise ValueError("claim must be a mapping")
            universal_rejection = raw_claim.get("universal_factor_rejection")
            if type(universal_rejection) is not bool:
                raise ValueError(
                    "claim.universal_factor_rejection must be an exact boolean"
                )
            if universal_rejection:
                raise ValueError(
                    "universal_factor_rejection is derived, not manually supplied"
                )
            claim_id = _nonempty_string(raw_claim.get("claim_id"), "claim.claim_id")
            learned_scope = ClaimScope.from_mapping(raw_claim.get("scope"))
            relation = proposal_scope.classify_proposal(learned_scope)
            exact_execution = raw_claim.get("execution_identity") == proposal_execution
            semantic_match = raw_claim.get("semantic_identity") == proposal_semantic
            if exact_execution:
                hard_blocks.append(claim_id)
            if semantic_match and not exact_execution:
                warning_codes.add("SEMANTIC_SIMILARITY_ONLY")
            matches.append(
                {
                    "claim_id": claim_id,
                    "scope_match": relation.value,
                    "applicable_scope": proposal_scope.intersection(learned_scope),
                    "exact_execution_identity": exact_execution,
                    "semantic_similarity": semantic_match,
                }
            )
        return {
            "schema_version": "control_plane.learning_gate_decision.v1",
            "enforcement": "HARD_BLOCK" if hard_blocks else "ALLOW",
            "hard_block_claim_ids": sorted(hard_blocks),
            "warning_codes": sorted(warning_codes),
            "matches": sorted(matches, key=lambda item: str(item["claim_id"])),
        }


__all__ = ["ClaimScope", "LearningGate", "ScopeMatch", "TimeWindow"]
